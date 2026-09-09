# tcp-load-balancer : High-Performance L4 TCP Load Balancer

[![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)](https://en.wikipedia.org/wiki/C%2B%2B20)
[![Linux](https://img.shields.io/badge/Platform-Linux-orange.svg)](https://www.kernel.org/)
[![I/O](https://img.shields.io/badge/I%2FO-epoll%20ET-green.svg)](https://man7.org/linux/man-pages/man7/epoll.7.html)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A simple, TCP stream load balancer written in modern C++20.

The goal of this project was to explore Linux systems programming concepts in practice: non-blocking sockets, `epoll` in edge-triggered mode
---

## Features & Implementation Details

* **Direct-Mapped Connection Pool:** Instead of using an associative container (`std::unordered_map`), active sessions are stored in a contiguous array sized to `RLIMIT_NOFILE`. Looking up a slot by its descriptor (`pool[fd]`) takes $O(1)$ time with no hash calculations or rehashing.
* **Simple Tunnel State Machine:** Pairs incoming client sockets with upstream connections, managing states (`FORWARDING`, `DRAINING`, `IDLE`).
* **Basic Health Checks via `timerfd`:** A non-blocking periodic timer checks offline backend nodes and automatically brings them back to the active pool once they respond.
* **Least Connections Balancing:** Routes new clients to the healthy backend currently handling the fewest active streams.

---

## Architecture

The balancer accepts TCP connections on port `3490` and proxies raw byte streams to one of several backend servers (listening on port `3491`).

```
                  +---------------------------------------------------+
                  |                                                    |
                  |                                                    |
[ Client ] <====> | [ Client Slot ] <--- Tunnel ---> [ Upstream Slot ] | <====> [ Backend Node ]
   (TCP)          |       fd: 5                              fd: 6     |          (3491)
                  +---------------------------------------------------+
                                            |
                                            v
                                 +---------------------+
                                 |   ConnectionPool    |
                                 |  [0] [1] ... [65535]|
                                 +---------------------+
                                            ^
                                            | (Tick every 2s)
                                 +---------------------+
                                 |   timerfd Sentinel  |
                                 +---------------------+
```

### Connection Lifecycle

Each connection pair transitions through three basic states:

```
                  accept() / connect()
                           │
                           ▼
                    [ FORWARDING ] ◄─────── Bidirectional non-blocking streaming
                           │
           Upstream EOF (recv == 0)
           with buffered client data
                           │
                           ▼
                     [ DRAINING ]  ───────► Flush pending buffer via EPOLLOUT
                           │
              Buffer empty / Socket Error
                           │
                           ▼
                        [ IDLE ]   ───────► close_slot()
```

1. **`FORWARDING`**: Active full-duplex proxying. Incoming bytes from one side are queued into the peer's buffer and flushed on `EPOLLOUT`.
2. **`DRAINING`**: If the backend finishes transmitting and closes the socket, the upstream descriptor is released immediately. The client socket remains open until all pending bytes are written out.
3. **`IDLE`**: The slot is reset and marked available for the next connection.

---

## Quick Start

The easiest way to test the project is using the included Docker Compose setup, which spins up the load balancer and 3 simple echo backends.

### Run

```bash
git clone https://github.com/anderfan/epoll-lb.git
cd tcp-load-balancer
docker compose up --build -d
```

### Test

Send a few requests through the balancer port:

```bash
curl -i http://localhost:3490
```

---

## Testing

A suite of integration tests written in Python validates basic proxying, graceful draining, and backend failover:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install pytest requests

# Run tests against the active cluster
pytest tests/ -v
```

---

## Project Limitations & Future Ideas

Since this is a learning project, there are deliberate simplifications:

* **Single-threaded:** Runs on a single event loop. Could be scaled across multiple cores using `SO_REUSEPORT` with thread-per-core workers.
* **Plain TCP Only:** Operates strictly at Layer 4; does not parse HTTP semantics, headers, or terminate TLS.
* **Buffer Management:** Buffers use `std::string`. A zero-copy pipeline using `splice(2)` or circular ring buffers would further reduce memory copies.

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE.txt) file for details.
