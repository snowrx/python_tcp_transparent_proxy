import logging

from gevent import socket
from gevent.pool import Group
from gevent.select import select
from gevent.socket import wait_read, wait_write

DIR_UP = "->"
DIR_DOWN = "<-"

TCP_FASTOPEN_CONNECT = 30
MSG_FASTOPEN = 0x20000000
TFO_RECV_TIMEOUT = 0.01

DEFAULT_BUFFER_SIZE = 1 << 18
DEFAULT_TIMEOUT = 86400


class Session:
    def __init__(
        self,
        client_sock: socket.socket,
        client_addr: tuple[str, int],
        remote_addr: tuple[str, int],
        family: socket.AddressFamily,
        buffer: memoryview = memoryview(bytearray(DEFAULT_BUFFER_SIZE)),
        timeout: int = DEFAULT_TIMEOUT,
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
            group.spawn(
                self._relay, self._client_sock, self._remote_sock, u_buf, DIR_UP
            )
            group.spawn(
                self._relay, self._remote_sock, self._client_sock, d_buf, DIR_DOWN
            )

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
                wait_read(self._client_sock.fileno(), TFO_RECV_TIMEOUT)
                if recv := self._client_sock.recv_into(self._buffer):
                    self._log(logging.DEBUG, f"TFO recv {recv} bytes", label)
            except (TimeoutError, socket.timeout):
                self._log(logging.DEBUG, "TFO recv timeout", label)
            try:
                wait_write(self._remote_sock.fileno(), self._timeout)
                if sent := self._remote_sock.sendto(
                    self._buffer[:recv], MSG_FASTOPEN, self._remote_addr
                ):
                    self._log(logging.INFO, f"TFO sent {sent} bytes", label)
            except BlockingIOError:
                if recv:
                    self._log(logging.INFO, "TFO failed", label)
            if sent < recv:
                wait_write(self._remote_sock.fileno(), self._timeout)
                self._remote_sock.sendall(self._buffer[sent:recv])
                self._log(logging.DEBUG, f"Sent remaining {recv - sent} bytes", label)

            connected = True
        except Exception as e:
            self._log(logging.ERROR, f"Connection error: {e}", label)

        return connected

    def _relay(
        self, src: socket.socket, dst: socket.socket, buffer: memoryview, dir: str
    ) -> None:
        label = f"{self._client_name:>48} {dir} {self._remote_name:>48}"
        timeout = self._timeout

        _select = select
        _recv = src.recv_into
        _send = dst.send

        center = len(buffer) // 2
        r_buf, w_buf = buffer[:center], buffer[center:]
        r_len, w_len = 0, 0
        w_view = w_buf[:w_len]
        eof = False

        self._log(logging.DEBUG, "Relay started", label)
        try:
            while True:
                rlist = [src] if not eof and r_len < center else []
                wlist = [dst] if w_view else []

                if rlist or wlist:
                    r, w, _ = _select(rlist, wlist, [], timeout)
                    if not (r or w):
                        raise TimeoutError

                    if r and src in r:
                        if recv := _recv(r_buf[r_len:]):
                            r_len += recv
                        else:
                            eof = True

                    if w and dst in w:
                        if sent := _send(w_view):
                            w_view = w_view[sent:]
                        else:
                            raise BrokenPipeError

                if not w_view:
                    self._log(
                        logging.DEBUG,
                        f"Relay recv: {r_len:7d}, sent: {w_len:7d}, eof: {eof}",
                        label,
                    )
                    if eof and not r_len:
                        break
                    r_buf, w_buf = w_buf, r_buf
                    r_len, w_len = 0, r_len
                    w_view = w_buf[:w_len]

        except Exception as e:
            try:
                dst.close()
            except Exception:
                pass
            self._log(logging.ERROR, f"Relay error: {e}", label)
        finally:
            try:
                dst.shutdown(socket.SHUT_WR)
            except Exception:
                pass
        self._log(logging.DEBUG, "Relay finished", label)

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
