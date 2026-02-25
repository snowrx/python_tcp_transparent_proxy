import logging

from gevent import socket
from gevent.pool import Group
from gevent.select import select
from gevent.socket import wait_read, wait_write

from core.buffer import ContinuousCircularBuffer

DIR_UP = "->"
DIR_DOWN = "<-"

TCP_FASTOPEN_CONNECT = 30
MSG_FASTOPEN = 0x20000000
TFO_BUFFER_SIZE = 63 << 10
TFO_RECV_TIMEOUT = 0.01

DEBUG = logging.DEBUG
INFO = logging.INFO
WARN = logging.WARN
ERROR = logging.ERROR
CRITICAL = logging.CRITICAL

INVALID_FD = -1


class Session:
    def __init__(
        self,
        client_sock: socket.socket,
        client_addr: tuple[str, int],
        remote_addr: tuple[str, int],
        family: socket.AddressFamily,
        buffer_size: int,
        timeout: int,
    ) -> None:
        self._logger = logging.getLogger(f"{self.__class__.__name__}-{hex(id(self))}")

        self._client_sock = client_sock
        self._client_addr = client_addr
        self._remote_addr = remote_addr
        self._family = family
        self._buffer_size = buffer_size
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

            group = Group()
            group.spawn(self._relay, self._client_sock, self._remote_sock, DIR_UP)
            group.spawn(self._relay, self._remote_sock, self._client_sock, DIR_DOWN)

            self._log(INFO, "Session started", label)
            group.join()
            self._log(INFO, "Session finished", label)

    def _connect(self) -> bool:
        label = f"{self._client_name:>48} {DIR_UP} {self._remote_name:>48}"
        connected = False

        try:
            self._remote_sock.connect(self._remote_addr)

            with memoryview(bytearray(TFO_BUFFER_SIZE)) as buffer:
                recv = 0
                sent = 0
                try:
                    wait_read(self._client_sock.fileno(), TFO_RECV_TIMEOUT)
                    if recv := self._client_sock.recv_into(buffer):
                        self._log(DEBUG, f"TFO recv {recv} bytes", label)
                except (TimeoutError, socket.timeout):
                    self._log(DEBUG, "TFO recv timeout", label)
                try:
                    wait_write(self._remote_sock.fileno(), self._timeout)
                    if sent := self._remote_sock.sendto(
                        buffer[:recv], MSG_FASTOPEN, self._remote_addr
                    ):
                        self._log(INFO, f"TFO sent {sent} bytes", label)
                except BlockingIOError:
                    if recv:
                        self._log(INFO, "TFO failed", label)
                if sent < recv:
                    wait_write(self._remote_sock.fileno(), self._timeout)
                    self._remote_sock.sendall(buffer[sent:recv])
                    self._log(INFO, f"Sent remaining {recv - sent} bytes", label)

            connected = True
        except Exception as e:
            self._log(ERROR, f"Connection error: {e}", label)

        return connected

    def _relay(self, src: socket.socket, dst: socket.socket, dir: str) -> None:
        label = f"{self._client_name:>48} {dir} {self._remote_name:>48}"
        timeout = self._timeout

        src_fd = src.fileno()
        dst_fd = dst.fileno()

        _log = self._log
        _select = select
        _recv = src.recv_into
        _send = dst.send

        eof = False

        with ContinuousCircularBuffer(self._buffer_size) as buffer:
            _log(DEBUG, "Relay started", label)
            try:
                while True:
                    if src_fd == INVALID_FD or dst_fd == INVALID_FD:
                        raise BrokenPipeError("Invalid file descriptor")

                    recv = 0
                    sent = 0
                    rv = buffer.get_readable_view()
                    wv = buffer.get_writable_view()

                    rlist = [src] if not eof and wv else []
                    wlist = [dst] if rv else []

                    if rlist or wlist:
                        r, w, _ = _select(rlist, wlist, [], timeout)
                        if not (r or w):
                            raise TimeoutError

                        if r and src in r:
                            if recv := _recv(wv):
                                buffer.advance_write(recv)
                            else:
                                eof = True

                        if w and dst in w:
                            if sent := _send(rv):
                                buffer.advance_read(sent)
                            else:
                                raise BrokenPipeError("Destination socket closed")

                        _log(DEBUG, f"{recv=:7d} {sent=:7d}", label)

                    if eof and not buffer.get_used_size():
                        break

            except Exception as e:
                try:
                    dst.close()
                except Exception:
                    pass
                _log(ERROR, f"Relay error: {e}", label)
            finally:
                try:
                    dst.shutdown(socket.SHUT_WR)
                except Exception:
                    pass
            _log(DEBUG, "Relay finished", label)

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
