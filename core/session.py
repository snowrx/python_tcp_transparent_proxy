import gevent
from gevent import socket, monkey
from gevent.socket import wait_read, wait_write
from gevent.pool import Group

monkey.patch_all()

import logging

DIR_UP = "->"
DIR_DOWN = "<-"


class Session:
    def __init__(self, client_sock: socket.socket, client_addr: tuple[str, int], remote_addr: tuple[str, int], family: socket.AddressFamily, buffer: memoryview, idle_timeout: int):
        self._logger = logging.getLogger(f"{self.__class__.__name__}-{hex(id(self))}")
        self._client_sock = client_sock
        self._client_addr = client_addr
        self._remote_addr = remote_addr
        self._family = family
        self._buffer = buffer
        self._idle_timeout = idle_timeout

        self._client_name = f"[{client_addr[0]}]:{client_addr[1]:<5}"
        self._remote_name = f"[{remote_addr[0]}]:{remote_addr[1]:<5}"

        self._remote_sock = socket.socket(self._family, socket.SOCK_STREAM)
        self._tune_sock(self._client_sock)
        self._tune_sock(self._remote_sock, tfo=True)

    def run(self) -> None:
        label = f"{self._client_name:>50} {DIR_UP} {self._remote_name:>50}"

        with self._client_sock, self._remote_sock:
            if not self._connect():
                return
            self._log(logging.INFO, "Connected", label)

            size = len(self._buffer) // 2
            u_buf = self._buffer[:size]
            d_buf = self._buffer[size:]
            group = Group()
            group.spawn(self._pipe, self._client_sock, self._remote_sock, u_buf, DIR_UP)
            group.spawn(self._pipe, self._remote_sock, self._client_sock, d_buf, DIR_DOWN)
            group.join()

            for sock in self._client_sock, self._remote_sock:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except:
                    pass

        self._log(logging.INFO, "Closed", label)

    def _connect(self) -> bool:
        label = f"{self._client_name:>50} {DIR_UP} {self._remote_name:>50}"
        connected = False

        try:
            self._remote_sock.connect(self._remote_addr)

            recv = 0
            sent = 0
            try:
                wait_read(self._client_sock.fileno(), 0)
                recv = self._client_sock.recv_into(self._buffer)
            except TimeoutError:
                pass
            try:
                wait_write(self._remote_sock.fileno())
                sent = self._remote_sock.sendto(self._buffer[:recv], socket.MSG_FASTOPEN, self._remote_addr)
            except BlockingIOError:
                pass
            if sent < recv:
                wait_write(self._remote_sock.fileno())
                self._remote_sock.sendall(self._buffer[sent:recv])
            if sent:
                self._log(logging.INFO, "TFO success", label)

            connected = True
        except Exception as e:
            self._log(logging.ERROR, f"Connect failed: {e}", label)

        return connected

    def _pipe(self, src_sock: socket.socket, dst_sock: socket.socket, buffer: memoryview, direction: str) -> None:
        label = f"{self._client_name:>50} {direction} {self._remote_name:>50}"
        src_fd = src_sock.fileno()
        dst_fd = dst_sock.fileno()
        idle_timeout = self._idle_timeout

        _wait_read = wait_read
        _wait_write = wait_write
        _recv = src_sock.recv_into
        _send = dst_sock.send

        size = len(buffer) // 2
        r_buf = buffer[:size]
        s_buf = buffer[size:]
        r_len = 0
        s_len = 0
        eof = False

        try:
            while not eof:
                _wait_read(src_fd, idle_timeout)
                if not (r_len := _recv(r_buf)):
                    break

                while r_len:
                    r_buf, r_len, s_buf, s_len = s_buf, 0, r_buf, r_len
                    sent = 0

                    while sent < s_len:
                        _wait_write(dst_fd, idle_timeout)
                        sent += _send(s_buf[sent:s_len])

                        if not eof and r_len < size:
                            try:
                                _wait_read(src_fd, 0)
                                if not (recv := _recv(r_buf[r_len:])):
                                    eof = True
                                else:
                                    r_len += recv
                            except TimeoutError:
                                pass

        except Exception as e:
            self._log(logging.ERROR, f"Pipe failed: {e}", label)
        finally:
            try:
                dst_sock.shutdown(socket.SHUT_WR)
            except:
                pass

    def _tune_sock(self, sock: socket.socket, tfo: bool = False) -> None:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_TCP, socket.TCP_QUICKACK, 1)
        if tfo:
            sock.setsockopt(socket.SOL_TCP, socket.TCP_FASTOPEN_CONNECT, 1)

    def _log(self, level: int, subject: str, message: str = "") -> None:
        def _print():
            gevent.idle()
            msg = f"{subject:60} |"
            if message:
                msg += f" {message:104} |"
            self._logger.log(level, msg)

        gevent.spawn(_print)
