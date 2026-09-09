#include "network.hpp"
#include <algorithm>
#include <asm-generic/socket.h>
#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <deque>
#include <fcntl.h>
#include <iostream>
#include <memory>
#include <netdb.h>
#include <netinet/in.h>
#include <optional>
#include <ostream>
#include <ranges>
#include <string>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/timerfd.h>
#include <sys/types.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>

#define CLIENT_PORT "3490"
#define BACKEND_PORT "3491"

using namespace std;
bool init_server_addresses() {
  for (auto &s : server_list) {
    struct addrinfo hints{}, *res = nullptr;
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    int err = getaddrinfo(s.server_ip.c_str(), "3491", &hints, &res);
    if (err != 0 || !res) {
      std::cerr << "Критическая ошибка резолва " << s.server_ip << ": "
                << gai_strerror(err) << std::endl;
      return false;
    }

    // Копируем бинарный адрес прямо в структуру сервера
    std::memcpy(&s.addr, res->ai_addr, sizeof(sockaddr_in));
    freeaddrinfo(res);
  }
  return true;
}
namespace {
vector<int> erase_list;
std::unordered_map<int, std::unique_ptr<Session>> sessions;
} // namespace

class Timer {
private:
  Socket tfd;

public:
  Timer(int sec_start, int sec_interval) {
    int fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    this->tfd = Socket(fd);

    struct itimerspec ts{};
    ts.it_value.tv_sec = sec_start;
    ts.it_interval.tv_sec = sec_interval;
    timerfd_settime(tfd.get(), 0, &ts, nullptr);
  }
  Socket get_socket() { return move(tfd); }
};

optional<Socket> create_tcp_upstream() {
  optional<TCPserver> upstream;
  while (true) {
    auto active_server =
        server_list | std::views::filter(&Server::active_server);
    auto it = ranges::min_element(active_server, {}, &Server::active_connect);
    if (it == ranges::end(active_server)) {
      cout << "Доступных серверов нет" << endl;
      return nullopt;
    }
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (fd < 0) {
      return nullopt;
    }
    Socket upstream_sock(fd);
    // it->addr уже лежит в памяти, никакой задержки:
    int res = connect(fd, reinterpret_cast<struct sockaddr *>(&it->addr),
                      sizeof(it->addr));

    if (res < 0 && errno != EINPROGRESS) {
      cerr << "Не удалось создать tcp upstream";
      it->error_count++;
      if (it->error_count >= 2) {
        cout << "Сервер " + it->server_ip
             << " перестал выходить на связь. Отключаю" << endl;
        it->active_server = false;
      }
      continue;
    }
    return upstream_sock;
    it->active_connect++;
    break;
  }
}

void handle_read_event(Connection *conn, EpollManage &epoll) {
  auto [request, status] = recvall(conn->socket.get());
  if (!request.empty() && conn->peer) {
    conn->peer->out_buffer += request;
    cout << "Записано " << conn->peer->out_buffer << endl;
    epoll.epoll_enable_write(conn->peer);
  }
  if (status == Status::Disconect || status == Status::Error) {
    cout << "Отключаю" << conn->socket.get() << endl;
    auto client_fd = conn->socket.get();
    if (conn->peer) {
      conn->peer->close_on_empty = true;
      conn->peer->peer = nullptr;
    }
    epoll.epoll_remove(conn);
    conn->socket = Socket(-1);
    if (sessions.contains(client_fd))
      erase_list.push_back(client_fd);
  }
}

void handle_write_event(Connection *conn, EpollManage &epoll) {
  auto status = sendall(conn->socket.get(), conn->out_buffer);
  if (status == Status::Error) {
    cerr << "sendall error: " << strerror(errno) << endl;
  }
  if (conn->out_buffer.empty()) {
    cout << "Отправил" << endl;
    epoll.epoll_disable_write(conn);

    if (conn->close_on_empty) {
      auto fd = conn->socket.get();
      epoll.epoll_remove(conn);
      conn->socket = Socket(-1);
      if (sessions.contains(fd))
        erase_list.push_back(fd);
    }
  }
}

void handle_accept_socket(Connection *conn, EpollManage &epoll) {
  while (true) {
    auto [client, status] = accept_fd(conn->socket.get(), true);
    if (status != Status::Ok) {
      if (status == Status::Eagain) {
        break;
      } else {
        cerr << "accept_fd error: " << strerror(errno) << endl;
      }
      break;
    }
    auto client_fd = client.get();

    auto upstream = create_tcp_upstream();
    if (!upstream) {
      break;
    }
    cout << "upstream " << upstream->get() << endl;
    auto session = std::make_unique<Session>();
    session->client.socket = std::move(client);
    session->upstream.socket = std::move(*upstream);

    epoll.epoll_add_read(&session->client);
    epoll.epoll_add_read(&session->upstream);
    cout << "client_fd " << client_fd << endl;
    sessions[client_fd] = move(session);
    cout << client_fd << endl;
  }
}

void handle_timer_alarm(Connection *conn) {
  uint64_t expirations = 0;
  read(conn->socket.get(), &expirations, sizeof(expirations));
  auto inactive_server = server_list | std::views::filter([](const auto &s) {
                           return !s.active_server;
                         });
  for (auto &s : inactive_server) {
    if (auto upstream = TCPserver::create_tcp(s.server_ip.c_str(), BACKEND_PORT,
                                              SocketMode::Connector, true)) {
      s.active_server = true;
    }
  }
}

void clear_dead_socket() {

  for (int key : erase_list) {
    sessions.erase(key);
  }
  erase_list.clear();
}

void event_loop(TCPserver &server, EpollManage &epoll) {
  int ep_fd = epoll.epoll_create();
  auto ep_ev = epoll.epoll_ev;

  Connection lis_conn{.socket = server.get_socket(),
                      .peer = nullptr}; // Структура для прнимающего сервера
  epoll.epoll_add_read(&lis_conn);

  auto tfd = Timer(2, 2);
  Connection timer_conn{.socket = tfd.get_socket(), .peer = nullptr};
  epoll.epoll_add_read(&timer_conn);

  while (true) {
    int nfds = epoll_wait(ep_fd, ep_ev, 65, -1);
    if (nfds == -1) {
      if (errno == EINTR)
        continue;
      cerr << "epoll_wait error: " << strerror(errno) << endl;
      break;
    }

    for (int i = 0; i < nfds; i++) {
      auto *conn = static_cast<Connection *>(ep_ev[i].data.ptr);
      uint32_t revents = ep_ev[i].events;
      if (conn->socket.get() == -1) {
        continue;
      }

      if (conn == &timer_conn) {
        uint64_t expirations = 0;
        read(conn->socket.get(), &expirations, sizeof(expirations));
        auto inactive_server =
            server_list |
            std::views::filter([](const auto &s) { return !s.active_server; });
        for (auto &s : inactive_server) {
          if (auto upstream = TCPserver::create_tcp(
                  s.server_ip.c_str(), "3491", SocketMode::Connector, true)) {
            s.active_server = true;
            s.error_count = 0;
          }
        }

        continue;
      }

      if (conn == &listener_conn) {
        while (true) {
          auto [client, status] = accept_fd(conn->socket.get(), true);
          if (status != Status::Ok) {
            if (status == Status::Eagain) {
              break;
            } else {
              cerr << "accept_fd error: " << strerror(errno) << endl;
            }
            break;
          }
          auto client_fd = client.get();

          auto upstream = create_tcp_upstream();
          if (!upstream) {
            std::string err_resp =
                "HTTP/1.1 503 Service Unavailable\r\nContent-Length: "
                "0\r\nConnection: close\r\n\r\n";
            send(client.get(), err_resp.data(), err_resp.size(), 0);
            break;
          }
          cout << "upstream " << upstream->get() << endl;
          auto session = std::make_unique<Session>();
          session->client.socket = std::move(client);
          session->upstream.socket = std::move(*upstream);

          epoll.epoll_add_read(&session->client);
          epoll.epoll_add_read(&session->upstream);
          cout << "client_fd " << client_fd << endl;
          sessions[client_fd] = move(session);
          cout << client_fd << endl;
        }
        handle_timer_alarm(conn);
        continue;
      }
      if (conn == &lis_conn) {
        handle_accept_socket(conn, epoll);
        continue;
      }
      if (revents & EPOLLIN) {
        handle_read_event(conn, epoll);
      }
      if (revents & EPOLLOUT) {
        handle_write_event(conn, epoll);
      }
    }
    clear_dead_socket();
  }
}

int main() {
  if (auto server = TCPserver::create_tcp(NULL, CLIENT_PORT,
                                          SocketMode::Listener, true)) {

    EpollManage epoll;
    cout << "Начинаю цикл" << endl;
    event_loop(*server, epoll);
  } else {
    return 1;
  }
}
