#include <asm-generic/socket.h>
#include <cerrno>
#include <cstddef>
#include <cstring>
#include <fcntl.h>
#include <format>
#include <iostream>
#include <memory>
#include <netdb.h>
#include <netinet/in.h>
#include <optional>
#include <ostream>
#include <string>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <unordered_map>

#include "network.hpp"
using namespace std;

namespace {
unordered_map<int, unique_ptr<Connection>> sessions;
vector<int> erase_list;
} // namespace

struct Client {
  Socket socket;
  string read_buffer;
  string write_buffer;
};

void handle_read_event(Connection *conn, EpollManage &epoll,
                       const char *node_id) {
  auto [request, status] = recvall(conn->socket.get());
  cout << "принял данные от клиента: " << request << endl;
  if (!request.empty()) {
    string backend_id = format("backend-349{}", node_id);

    string body = "--- backend 3491 echo ---\n" + request + "\n";

    string response = "HTTP/1.1 200 OK\r\n"
                      "content-type: text/plain\r\n"
                      "content-length: " +
                      std::to_string(body.size()) +
                      "\r\n"
                      "connection: close\r\n"
                      "X-Backend-Id: " +
                      backend_id +
                      "\r\n"
                      "\r\n" +
                      body;
    conn->out_buffer = response;
    epoll.epoll_enable_write(conn);
  }

  if (status == Status::Disconect) {
    cout << "Клиент отключился. Вырубаю всё" << endl << endl;
    auto client_fd = conn->socket.get();
    epoll.epoll_remove(conn);
    sessions.erase(client_fd);
  }
}

void handle_write_event(Connection *conn, EpollManage &epoll) {
  auto status = sendall(conn->socket.get(), conn->out_buffer);

  if (status == Status::Error) {
    cerr << "sendall error: " << strerror(errno) << endl;
  }
  if (conn->out_buffer.empty()) {
    conn->out_buffer.clear();
    epoll.epoll_disable_write(conn);
  }
  auto client_fd = conn->socket.get();
  epoll.epoll_remove(conn);
  sessions.erase(client_fd);
}

void handle_accept_socket(Connection *conn, EpollManage &epoll) {
  while (true) {
    auto [client, status] = accept_fd(conn->socket.get(), true);
    if (status != Status::Ok) {
      if (status == Status::Eagain) {
        break;
      } else {
        cout << conn->socket.get() << endl;
        cerr << "accept_fd error: " << strerror(errno) << endl;
      }
      break;
    }
    auto session = make_unique<Connection>();
    int client_fd = client.get();
    session->socket = std::move(client);

    epoll.epoll_add_read(session.get());
    sessions[client_fd] = move(session);
  }
}

void event_loop(TCPserver &server, EpollManage &epoll, const char *node_id) {
  int ep_fd = epoll.epoll_create();
  auto ep_ev = epoll.epoll_ev;

  Connection listener_conn{.socket = server.get_socket(), .peer = nullptr};
  epoll.epoll_add_read(&listener_conn);
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

      if (conn == &listener_conn) {
        handle_accept_socket(conn, epoll);
        continue;
      }
      if (revents & EPOLLIN) {
        handle_read_event(conn, epoll, node_id);
      }
      if (revents & EPOLLOUT) {
        handle_write_event(conn, epoll);
      }
    }
  }
}

int main(int argc, char *argv[]) {
  if (!argv[1]) {
    cerr << "Non argument id" << endl;
  }
  auto node_id = argv[1];
  if (auto server =
          TCPserver::create_tcp(nullptr, "3491", SocketMode::Listener, true)) {
    cerr << node_id << " сервер запущен" << endl;
    EpollManage epoll;
    event_loop(*server, epoll, node_id);
  } else {
    cerr << "Ошибка от " << node_id << " : " << strerror(errno) << endl;
    return 1;
  }
}
