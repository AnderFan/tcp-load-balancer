#pragma once
#include <cerrno>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <memory>
#include <netdb.h>
#include <netinet/in.h>
#include <optional>
#include <string>
#include <string_view>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

enum class Status { Ok, Disconect, Error, Eagain };
struct SendData {
  std::string str;
  int fd = -1;
};
struct StringResult {
  std::string result;
  Status status;
};
struct AddrInfoDeleter {
  void operator()(addrinfo *p) const noexcept {
    if (p != nullptr)
      ::freeaddrinfo(p);
  }
};

using UniqueAddrInfo = std::unique_ptr<addrinfo, AddrInfoDeleter>;

class Socket {
private:
  int socketfd = -1;

public:
  Socket() noexcept = default;
  explicit Socket(int fd) noexcept : socketfd(fd) {}
  ~Socket() {
    if (socketfd >= 0)
      ::close(socketfd);
  }

  Socket(const Socket &) = delete;
  Socket &operator=(const Socket &) = delete;

  Socket(Socket &&other) noexcept : socketfd(other.socketfd) {
    other.socketfd = -1;
  }
  Socket &operator=(Socket &&other) noexcept {
    if (this != &other) {
      if (socketfd >= 0)
        ::close(socketfd);
      socketfd = other.socketfd;
      other.socketfd = -1;
    }
    return *this;
  }

  [[nodiscard]] int get() const noexcept { return socketfd; }
};
enum class TcpRole { Client, Upstream, Listener };

struct Session;

struct Connection {
  Socket socket;
  Connection *peer;
  std::string out_buffer;
  bool close_on_empty = false;
};

struct Server {
  std::string server_ip;
  sockaddr_in addr{};
  int active_connect = 0;
  int error_count = 0;
  bool active_server = true;
};

inline std::vector<Server> server_list{
    {"backend1"}, {"backend2"}, {"backend3"}};

struct Session {
  Connection client;
  Connection upstream;
  Server ptr_server;
  Session() {
    client.peer = &upstream;
    upstream.peer = &client;
  }
};

struct DataResult {
  Socket socket;
  Status status;
};

struct ClientSession {
  Socket client;
  Socket upstream;
  std::string write_buffer;
  std::string read_buffer;
};

enum class SocketMode { Listener, Connector };

class TCPserver {
private:
  Socket listener_fd;
  explicit TCPserver(Socket &&socket) noexcept
      : listener_fd(std::move(socket)) {}

public:
  static std::optional<TCPserver> create_tcp(const char *adress,
                                             const char *port, SocketMode mode,
                                             bool nonblock) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    if (mode == SocketMode::Listener)
      hints.ai_flags = AI_PASSIVE;

    addrinfo *raw_res{nullptr};
    if (auto status = ::getaddrinfo(adress, port, &hints, &raw_res);
        status != 0) {
      std::cerr << "getaddrinfo error: " << ::gai_strerror(status) << '\n';
      return std::nullopt;
    }
    UniqueAddrInfo res(raw_res);

    int fd = ::socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd < 0)
      return std::nullopt;

    Socket sock(fd);
    if (mode == SocketMode::Listener) {
      int yes = 1;
      if (::setsockopt(sock.get(), SOL_SOCKET, SO_REUSEADDR, &yes,
                       sizeof(yes)) < 0)
        return std::nullopt;

      if (::bind(sock.get(), res->ai_addr, res->ai_addrlen) != 0) {
        return std::nullopt;
      }
      listen(sock.get(), 128);
    }

    if (mode == SocketMode::Connector) {
      if (connect(sock.get(), res->ai_addr, res->ai_addrlen) == -1)
        return std::nullopt;
    }

    if (nonblock) {
      fcntl(sock.get(), F_SETFL, O_NONBLOCK);
    }
    return TCPserver(std::move(sock));
  }

  [[nodiscard]] int get_fd() const noexcept { return listener_fd.get(); }
  Socket get_socket() { return std::move(listener_fd); }
};

struct Connection;

class EpollManage {
private:
  int epoll_fd = -1;

  bool modify_epoll_event(Connection *client, uint32_t events, int op) {
    epoll_event ev{};
    ev.events = events | EPOLLET;
    ev.data.ptr = client;
    return ::epoll_ctl(epoll_fd, op, client->socket.get(), &ev) == 0;
  }

public:
  epoll_event epoll_ev[65];

  EpollManage() = default;
  ~EpollManage() {
    if (epoll_fd >= 0)
      ::close(epoll_fd);
  }

  EpollManage(const EpollManage &) = delete;
  EpollManage &operator=(const EpollManage &) = delete;
  EpollManage(EpollManage &&other) noexcept : epoll_fd(other.epoll_fd) {
    other.epoll_fd = -1;
  }
  EpollManage &operator=(EpollManage &&other) noexcept {
    if (this != &other) {
      if (epoll_fd >= 0)
        ::close(epoll_fd);
      epoll_fd = other.epoll_fd;
      other.epoll_fd = -1;
    }
    return *this;
  }

  int epoll_create(int size = 64) {
    epoll_fd = ::epoll_create(size);
    return epoll_fd;
  }

  bool epoll_add_write(Connection *client) {
    return modify_epoll_event(client, EPOLLOUT, EPOLL_CTL_ADD);
  }
  bool epoll_add_read(Connection *client) {
    return modify_epoll_event(client, EPOLLIN, EPOLL_CTL_ADD);
  }
  bool epoll_enable_write(Connection *client) {
    return modify_epoll_event(client, EPOLLIN | EPOLLOUT, EPOLL_CTL_MOD);
  }
  bool epoll_disable_write(Connection *client) {
    return modify_epoll_event(client, EPOLLIN, EPOLL_CTL_MOD);
  }
  bool epoll_remove(Connection *client) {
    return ::epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client->socket.get(),
                       nullptr) == 0;
  }
};

inline StringResult recvall(int fd) {
  std::string request;
  char buf[1024];
  while (true) {
    auto len_recv = ::recv(fd, buf, sizeof(buf), 0);
    if (len_recv == 0)
      return {request, Status::Disconect};
    if (len_recv < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK)
        return {request, Status::Ok};
      std::cerr << "Ошибка recvall: " << strerror(errno) << std::endl;
      return {request, Status::Error};
    }
    request.append(buf, len_recv);
    if (request.find("\n") != std::string::npos) {
      return {request, Status::Ok};
    }
  }
}
inline Status sendall(int fd, std::string &request) {
  while (!request.empty()) {
    auto n = ::send(fd, request.c_str(), request.length(), MSG_NOSIGNAL);
    if (n > 0) {
      request.erase(0, n);
    } else {
      if (n == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        return Status::Eagain;
      }
      return Status::Error;
    }
  }
  return Status::Ok;
}
inline DataResult accept_fd(int fd, bool non_block) {
  sockaddr_storage their_addr{};
  socklen_t their_addr_size = sizeof(their_addr);
  auto client_fd =
      ::accept(fd, reinterpret_cast<sockaddr *>(&their_addr), &their_addr_size);

  Socket socket(client_fd);
  if (client_fd == -1) {
    if (errno == EAGAIN || errno == EWOULDBLOCK)
      return {std::move(socket), Status::Eagain};
    return {std::move(socket), Status::Error};
  }
  if (non_block)
    ::fcntl(client_fd, F_SETFL, O_NONBLOCK);

  return {std::move(socket), Status::Ok};
}

inline SendData parse_string(std::string str) {
  std::string_view sv = str;
  size_t delim_pos = sv.find(":");
  if (delim_pos == std::string_view::npos) {
    std::cerr << "Неверная упаковка: " + str << std::endl;
    return {"-", -1};
  }
  int fd = 0;
  auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + delim_pos, fd);
  if (ec != std::errc{} || ptr != sv.data() + delim_pos) {
    std::cerr << "Неверная упаковка: " + str << std::endl;
    return {"-", -1};
  }
  std::string_view text = sv.substr(delim_pos + 1);
  if (!text.empty() && text.back() == '\n') {
    text.remove_suffix(1);
  }

  std::string result{text};
  return {result, fd};
}

// inline std::size_t sendall(int fd, const std::string &request,
//                            std::size_t total_bytes) {
//   auto bytes_left = request.size() - total_bytes;
//   while (total_bytes < request.size()) {
//     auto n =
//         ::send(fd, request.c_str() + total_bytes, bytes_left,
//         MSG_NOSIGNAL);
//     if (n == -1)
//       break;
//     total_bytes += static_cast<std::size_t>(n);
//     bytes_left -= static_cast<std::size_t>(n);
//   }
//   return bytes_left;
// }
