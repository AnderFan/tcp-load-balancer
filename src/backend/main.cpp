#include "network.hpp"
#include <asm-generic/socket.h>
#include <cerrno>
#include <csignal>
#include <cstddef>
#include <cstring>
#include <format>
#include <iostream>
#include <string>
#include <sys/epoll.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <system_error>
#include <unistd.h>

void close_slot(ConnectionSlot &slot, EpollManage &epoll) noexcept {
  if (slot.socket.get() == -1) {
    return;
  }
  epoll.epoll_remove(&slot);
  slot.socket = Socket(-1);
  slot.type = SlotType::UNUSED;
  slot.state = ConnState::IDLE;
  slot.out_buffer.clear();
  slot.generation++;
}

void handle_read_event(ConnectionSlot *slot, EpollManage &epoll,
                       const char *node_id) {
  auto [request, status] = recvall(slot->socket.get());
  if (!request.empty()) {
    std::string backend_id = std::format("backend-349{}", node_id);
    std::string body = "--- backend 3491 echo ---\n" + request + "\n";

    std::string response = "HTTP/1.1 200 OK\r\n"
                           "content-type: text/plain\r\n"
                           "content-length: " +
                           std::to_string(body.size()) +
                           "\r\n"
                           "connection: close\r\n"
                           "X-Backend-Id: " +
                           backend_id + "\r\n\r\n" + body;

    slot->out_buffer = std::move(response);
    epoll.epoll_enable_write(slot);
  }

  if (status == Status::Disconect || status == Status::Error) {
    close_slot(*slot, epoll);
  }
}

void handle_write_event(ConnectionSlot *slot, EpollManage &epoll) {
  auto status = sendall(slot->socket.get(), slot->out_buffer);
  if (status == Status::Error) {
    std::cerr << "sendall error: " << strerror(errno) << std::endl;
    close_slot(*slot, epoll);
    return;
  }

  if (slot->out_buffer.empty()) {
    close_slot(*slot, epoll);
  }
}

void handle_accept_socket(EpollManage &epoll, ConnectionSlot *lis_slot,
                          ConectionPool &pool) {
  while (true) {
    auto [client, status] = accept_fd(lis_slot->socket.get(), true);
    if (status != Status::Ok) {
      if (status != Status::Eagain) {
        std::cerr << "accept_fd error: " << strerror(errno) << std::endl;
      }
      break;
    }

    int client_fd = client.get();
    if (client_fd < 0 || static_cast<size_t>(client_fd) >= pool.capacity()) {
      std::cerr << "The descriptor is out of bounds for the backend pool: "
                << client_fd << std::endl;
      break;
    }

    auto &slot = pool.create_slot(std::move(client), SlotType::CLIENT);
    slot.peer = nullptr;
    slot.server = nullptr;
    slot.out_buffer.clear();

    epoll.epoll_add_read(&slot);
  }
}

void event_loop(ConnectionSlot &lis_slot, EpollManage &epoll,
                ConectionPool &pool, const char *node_id) {
  int ep_fd = epoll.epoll_create();
  auto ep_ev = epoll.epoll_ev;

  epoll.epoll_add_read(&lis_slot);

  while (true) {
    int nfds = epoll_wait(ep_fd, ep_ev, 65, -1);
    if (nfds == -1) {
      if (errno == EINTR)
        continue;
      std::cerr << "epoll_wait error: " << strerror(errno) << std::endl;
      break;
    }

    for (int i = 0; i < nfds; i++) {
      auto *slot = static_cast<ConnectionSlot *>(ep_ev[i].data.ptr);
      uint32_t revents = ep_ev[i].events;

      if (!slot || slot->socket.get() == -1 || slot->state == ConnState::IDLE) {
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
        handle_read_event(slot, epoll, node_id);
      }

      if (slot->socket.get() != -1 && (revents & EPOLLOUT)) {
        handle_write_event(slot, epoll);
      }
    }
  }
}

int main(int argc, char *argv[]) {
  signal(SIGPIPE, SIG_IGN);

  if (argc < 2 || !argv[1]) {
    std::cerr << "No argument passed node_id" << std::endl;
    return 1;
  }
  auto node_id = argv[1];

  struct rlimit rl;
  if (getrlimit(RLIMIT_NOFILE, &rl) != 0) {
    throw std::system_error(errno, std::generic_category(), "getrlimit failed");
  }

  ConectionPool connections(rl.rlim_cur);

  if (auto server =
          TCPserver::create_tcp(nullptr, "3491", SocketMode::Listener, true)) {
    std::cerr << node_id << " the server is running on port 3491" << std::endl;
    auto &lis_slot =
        connections.create_slot(server->get_socket(), SlotType::LISTENER);
    EpollManage epoll;
    event_loop(lis_slot, epoll, connections, node_id);
  } else {
    std::cerr << "Start error " << node_id << " : " << strerror(errno)
              << std::endl;
    return 1;
  }
}
