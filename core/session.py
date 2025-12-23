from gevent import socket
from gevent.socket import wait_read, wait_write
from gevent.pool import Group

import logging

DIR_UP = "->"
DIR_DOWN = "<-"

TCP_FASTOPEN_CONNECT = getattr(socket, "TCP_FASTOPEN_CONNECT", 30)
MSG_FASTOPEN = getattr(socket, "MSG_FASTOPEN", 0x20000000)


class Session:
    def __init__(
        self,
        client_sock: socket.socket,
        client_addr: tuple[str, int],
        remote_addr: tuple[str, int],
        family: socket.AddressFamily,
        buffer: memoryview,
        timeout: int,
    ) -> None:
        self._logger = logging.getLogger(f"{self.__class__.__name__}-{hex(id(self))}")

        self._client_sock = client_sock
        self._client_addr = client_addr
        self._remote_addr = remote_addr
        self._family = family
        self._buffer = buffer
        self._timeout = timeout

        self._client_name = f"[{self._client_addr[0]}]:{self._client_addr[1]:<5}"
        self._remote_name = f"[{self._remote_addr[0]}]:{self._remote_addr[1]:<5}"

        self._remote_sock = socket.socket(family, socket.SOCK_STREAM)
        self._tune(self._client_sock)
        self._tune(self._remote_sock, tfo=True)

    def run(self) -> None:
        label = f"{self._client_name:>48} {DIR_UP} {self._remote_name:>48}"

        with self._client_sock, self._remote_sock:
            if not self._connect():
                return

            center = len(self._buffer) // 2
            u_buf = self._buffer[:center]
            d_buf = self._buffer[center:]

            group = Group()
            group.spawn(self._relay, self._client_sock, self._remote_sock, u_buf, DIR_UP)
            group.spawn(self._relay, self._remote_sock, self._client_sock, d_buf, DIR_DOWN)

            self._log(logging.INFO, "Session started", label)
            group.join()
            self._log(logging.INFO, "Session finished", label)

    def _connect(self) -> bool:
        label = f"{self._client_name:>48} {DIR_UP} {self._remote_name:>48}"
        connected = False

        try:
            self._remote_sock.connect(self._remote_addr)

            recv = 0
            sent = 0
            try:
                wait_read(self._client_sock.fileno(), 0)
                recv = self._client_sock.recv_into(self._buffer)
            except (TimeoutError, socket.timeout):
                pass
            try:
                wait_write(self._remote_sock.fileno(), self._timeout)
                sent = self._remote_sock.sendto(self._buffer[:recv], MSG_FASTOPEN, self._remote_addr)
            except BlockingIOError:
                pass
            if sent < recv:
                wait_write(self._remote_sock.fileno(), self._timeout)
                self._remote_sock.sendall(self._buffer[sent:recv])
            if sent:
                self._log(logging.INFO, f"TFO sent {sent} bytes", label)

            connected = True
        except Exception as e:
            self._log(logging.ERROR, f"Connection error: {e}", label)

        return connected

    def _relay(self, src: socket.socket, dst: socket.socket, buffer: memoryview, dir: str) -> None:
        label = f"{self._client_name:>48} {dir} {self._remote_name:>48}"
        timeout = self._timeout

        src_fd = src.fileno()
        dst_fd = dst.fileno()

        _recv = src.recv_into
        _send = dst.send
        _wait_read = wait_read
        _wait_write = wait_write

        center = len(buffer) // 2
        r_buf = buffer[:center]
        s_buf = buffer[center:]
        r_len = 0
        s_len = 0
        eof = False

        self._log(logging.DEBUG, f"Relay started", label)
        try:
            while not eof:
                _wait_read(src_fd, timeout)
                if not (r_len := _recv(r_buf)):
                    break

                while r_len:
                    r_buf, r_len, s_buf, s_len = s_buf, 0, r_buf, r_len
                    s_pos = 0

                    while s_pos < s_len:
                        _wait_write(dst_fd, timeout)
                        if not (sent := _send(s_buf[s_pos:s_len])):
                            raise BrokenPipeError
                        s_pos += sent

                        if not eof and r_len < center:
                            try:
                                _wait_read(src_fd, 0)
                                if not (recv := _recv(r_buf[r_len:])):
                                    eof = True
                                else:
                                    r_len += recv
                            except (TimeoutError, socket.timeout):
                                pass

        except Exception as e:
            self._log(logging.ERROR, f"Relay error: {e}", label)
        finally:
            try:
                dst.shutdown(socket.SHUT_WR)
            except:
                pass
        self._log(logging.DEBUG, f"Relay finished", label)

    def _tune(self, sock: socket.socket, tfo: bool = False) -> None:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_TCP, socket.TCP_QUICKACK, 1)
        if tfo:
            sock.setsockopt(socket.SOL_TCP, TCP_FASTOPEN_CONNECT, 1)

    def _log(self, level: int, subject: str, msg: str = "") -> None:
        output = f"{subject:60} |"
        if msg:
            output += f" {msg:100} |"
        self._logger.log(level, output)
