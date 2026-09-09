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
#include <fcntl.h>
#include <iostream>
#include <netdb.h>
#include <netinet/in.h>
#include <optional>
#include <ostream>
#include <ranges>
#include <string>
#include <sys/epoll.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/timerfd.h>
#include <sys/types.h>
#include <system_error>
#include <unistd.h>
#include <vector>
#define BACKEND_PORT "3491"
#define CLIENT_PORT "3490"

namespace {
std::vector<Server> inactive_server;
} // namespace

bool init_server_addresses() {
  for (auto &s : server_list) {
    struct addrinfo hints{}, *res = nullptr;
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    int err = getaddrinfo(s.server_ip.c_str(), "3491", &hints, &res);
    if (err != 0 || !res) {
      std::cerr << "Critical resolve error " << s.server_ip << ": "
                << gai_strerror(err) << std::endl;
      return false;
    }

    std::memcpy(&s.addr, res->ai_addr, sizeof(sockaddr_in));
    freeaddrinfo(res);
  }
  return true;
}
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
  Socket get_socket() { return std::move(tfd); }
};

int get_socket_error(int fd) {
  int err = 0;
  socklen_t len = sizeof(err);
  if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0) {
    return errno;
  }
  return err;
}

struct UpstreamConnection {
  Socket socket;
  Server *server{nullptr};
};

std::optional<UpstreamConnection> create_tcp_upstream() {
  std::optional<TCPserver> upstream;
  while (true) {
    auto active_server =
        server_list | std::views::filter(&Server::active_server);
    auto it =
        std::ranges::min_element(active_server, {}, &Server::active_connect);
    if (it == std::ranges::end(active_server)) {
      std::cout << "There are no available servers." << std::endl;
      return std::nullopt;
    }
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (fd < 0) {
      return std::nullopt;
    }
    Socket upstream_sock(fd);
    int res = connect(fd, reinterpret_cast<struct sockaddr *>(&it->addr),
                      sizeof(it->addr));

    if (res < 0 && errno != EINPROGRESS) {
      std::cerr << "Failed to create tcp upstream";
      it->error_count++;
      if (it->error_count >= 2) {
        std::cout << "Server " + it->server_ip
                  << " stopped communicating. Disconnecting." << std::endl;

        it->active_server = false;
        inactive_server.push_back(*it);
      }
      continue;
    }
    it->active_connect++;
    return UpstreamConnection{.socket = std::move(upstream_sock),
                              .server = &(*it)};
  }
}

void close_slot(ConnectionSlot &slot, EpollManage &epoll) noexcept {
  if (slot.socket.get() == -1) {
    return;
  }
  epoll.epoll_remove(&slot);
  if (slot.peer != nullptr) {
    if (slot.peer->peer == &slot) {
      slot.peer->peer = nullptr;
    }
    slot.peer = nullptr;
  }
  if (slot.type == SlotType::UPSTREAM && slot.server != nullptr) {
    if (slot.server->active_connect > 0) {
      slot.server->active_connect--;
    }
    slot.server = nullptr;
  }

  slot.socket = Socket(-1);

  slot.type = SlotType::UNUSED;
  slot.state = ConnState::IDLE;
  slot.out_buffer.clear();
  slot.generation++;
}
Status handle_read_event(ConnectionSlot *slot, EpollManage &epoll) {
  auto [request, status] = recvall(slot->socket.get());
  if (!request.empty() && slot->peer != nullptr) {
    slot->peer->out_buffer += request;
    //  cout << "Записано " << slot->peer->out_buffer << endl;
    epoll.epoll_enable_write(slot->peer);
  }
  if (status == Status::Error) {
    std::cout << "Отключаю" << slot->socket.get() << std::endl;
    if (slot->type == SlotType::UPSTREAM && slot->server) {
      slot->server->error_count++;
    }
    auto *peer = slot->peer;
    close_slot(*slot, epoll);
    if (peer) {
      close_slot(*peer, epoll);
    }
    return status;
  }
  if (status == Status::Disconect) {
    if (slot->type == SlotType::CLIENT) {
      auto *peer = slot->peer;
      close_slot(*slot, epoll);
      if (peer) {
        close_slot(*peer, epoll);
      }
    }
    if (slot->type == SlotType::UPSTREAM) {
      auto *peer = slot->peer;
      close_slot(*slot, epoll);

      if (peer) {
        peer->peer = nullptr;
        if (peer->out_buffer.empty()) {
          close_slot(*peer, epoll);
        } else {
          peer->state = ConnState::DRAINING;
          epoll.epoll_enable_write(peer);
        }
      }
    }
  }
  return status;
}

Status handle_write_event(ConnectionSlot *slot, EpollManage &epoll) {
  auto status = sendall(slot->socket.get(), slot->out_buffer);
  if (status == Status::Error) {
    std::cerr << "sendall error: " << strerror(errno) << std::endl;
    close_slot(*slot, epoll);
    return status;
  }
  if (slot->out_buffer.empty()) {
    epoll.epoll_disable_write(slot);
  }
  return status;
}
void handle_accept_socket(EpollManage &epoll, ConnectionSlot *slot,
                          ConectionPool &pool) {
  while (true) {
    auto [client, status] = accept_fd(slot->socket.get(), true);
    if (status != Status::Ok) {
      if (status == Status::Eagain) {
        break;
      }
      std::cerr << "accept_fd error: " << strerror(errno) << std::endl;
      break;
    }
    auto upstream = create_tcp_upstream();
    if (!upstream) {
      std::string err_503 =
          "HTTP/1.1 503 Service Unavailable\r\nContent-Length: "
          "0\r\nConnection: close\r\n\r\n";
      ::send(client.get(), err_503.data(), err_503.size(), MSG_NOSIGNAL);
      break; // client закроется деструктором ~Socket()
    }

    auto [cl_slot, up_slot] = pool.create_tunnel(
        std::move(client), std::move(upstream->socket), upstream->server);

    epoll.epoll_add_read(&cl_slot);
    epoll.epoll_add_read(&up_slot);
  }
}

void handle_timer_alarm(ConnectionSlot *slot) {
  uint64_t expirations = 0;
  read(slot->socket.get(), &expirations, sizeof(expirations));
  for (auto &s : inactive_server) {
    if (auto upstream = TCPserver::create_tcp(s.server_ip.c_str(), BACKEND_PORT,
                                              SocketMode::Connector, true)) {
      s.active_server = true;
      std::cout << "Server " << s.server_ip << "return to life" << std::endl;
    }
  }
}

void event_loop(ConnectionSlot &lis_slot, EpollManage &epoll,
                ConectionPool &pool) {
  int ep_fd = epoll.epoll_create();
  auto ep_ev = epoll.epoll_ev;

  epoll.epoll_add_read(&lis_slot);

  auto tfd = Timer(2, 2);
  auto &tfd_slot = pool.create_slot(tfd.get_socket(), SlotType::TIMER);
  epoll.epoll_add_read(&tfd_slot);

  while (true) {
    int nfds = epoll_wait(ep_fd, ep_ev, 65, -1);
    if (nfds == -1) {
      if (errno == EINTR)
        continue;
      std::cerr << "epoll_wait error: " << strerror(errno) << std::endl;
      break;
    }

    for (int i = 0; i < nfds; i++) {
      auto slot = static_cast<ConnectionSlot *>(ep_ev[i].data.ptr);
      uint32_t revents = ep_ev[i].events;
      if (slot->socket.get() == -1 || slot->state == ConnState::IDLE) {
        continue;
      }

      if (slot->type == SlotType::TIMER) {
        handle_timer_alarm(slot);
        continue;
      }
      if (slot->type == SlotType::LISTENER) {
        handle_accept_socket(epoll, slot, pool);
        continue;
      }

      if (revents & (EPOLLERR | EPOLLHUP)) {
        close_slot(*slot, epoll);
        continue;
      }
      if (revents & EPOLLIN) {
        handle_read_event(slot, epoll);
      }
      if (slot->socket.get() != -1 && (revents & EPOLLOUT)) {
        handle_write_event(slot, epoll);
      }
    }
  }
}

int main() {
  signal(SIGPIPE, SIG_IGN);
  if (!init_server_addresses()) {
    std::cerr << "The servers did not initialize." << std::endl;
    return 1;
  }

  struct rlimit rl;
  if (getrlimit(RLIMIT_NOFILE, &rl) != 0) {
    throw std::system_error(errno, std::generic_category(), "getrlimit failed");
  }

  ConectionPool connections(rl.rlim_cur);

  if (auto listener = TCPserver::create_tcp(NULL, CLIENT_PORT,
                                            SocketMode::Listener, true)) {
    auto &lis_slot =
        connections.create_slot(listener->get_socket(), SlotType::LISTENER);
    EpollManage epoll;
    std::cout << "Starting a cycle" << std::endl;
    event_loop(lis_slot, epoll, connections);
  } else {
    return 1;
  }
}
