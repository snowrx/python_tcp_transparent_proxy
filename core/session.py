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

DEBUG = logging.DEBUG
INFO = logging.INFO
WARN = logging.WARN
ERROR = logging.ERROR
CRITICAL = logging.CRITICAL


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

            group = Group()
            center = self._buffer.nbytes >> 1
            up_buffer = self._buffer[:center]
            down_buffer = self._buffer[center:]
            group.spawn(
                self._relay, self._client_sock, self._remote_sock, up_buffer, DIR_UP
            )
            group.spawn(
                self._relay, self._remote_sock, self._client_sock, down_buffer, DIR_DOWN
            )

            self._log(INFO, "Session started", label)
            group.join()
            self._log(INFO, "Session finished", label)

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
                    self._log(DEBUG, f"TFO recv {recv:5d} bytes", label)
            except (TimeoutError, socket.timeout):
                self._log(DEBUG, "TFO recv timeout", label)
            try:
                wait_write(self._remote_sock.fileno(), self._timeout)
                if sent := self._remote_sock.sendto(
                    self._buffer[:recv], MSG_FASTOPEN, self._remote_addr
                ):
                    self._log(INFO, f"TFO sent {sent:5d} bytes", label)
            except BlockingIOError:
                if recv:
                    self._log(DEBUG, "TFO failed", label)
            if sent < recv:
                wait_write(self._remote_sock.fileno(), self._timeout)
                self._remote_sock.sendall(self._buffer[sent:recv])
                self._log(DEBUG, f"Sent remaining {recv - sent:5d} bytes", label)

            connected = True
        except Exception as e:
            self._log(ERROR, f"Connection error: {e}", label)

        return connected

    def _relay(
        self, src: socket.socket, dst: socket.socket, buffer: memoryview, dir: str
    ) -> None:
        label = f"{self._client_name:>48} {dir} {self._remote_name:>48}"
        timeout = self._timeout

        src_fd = src.fileno()
        dst_fd = dst.fileno()

        _log = self._log
        _select = select
        _recv = src.recv_into
        _send = dst.send

        status = "UNEXPECTED"
        eof = False

        center = buffer.nbytes >> 1
        threshold = center >> 3

        rv, wv = buffer[:center], buffer[center:]
        rl = 0
        tv = wv[:0]

        try:
            while True:
                r, s = 0, 0
                free = max(0, center - (rl + tv.nbytes))
                rlist = [src_fd] if not eof and free >= threshold else []
                wlist = [dst_fd] if tv else []

                if rlist or wlist:
                    rready, wready, _ = _select(rlist, wlist, [], timeout)

                    if not (rready or wready):
                        status = "TIMEOUT"
                        break

                    if dst_fd in wready:
                        if s := _send(tv):
                            tv = tv[s:]
                            free += s
                        else:
                            status = "DST_LOST"
                            break

                    if src_fd in rready:
                        if r := _recv(rv[rl : rl + free]):
                            _log(DEBUG, f"{free=:7d} {r=:7d}", label)
                            rl += r
                            free -= r
                        else:
                            eof = True

                if not tv:
                    if eof and not rl:
                        status = "SUCCESS"
                        break

                    rv, wv = wv, rv
                    tv = wv[:rl]
                    rl = 0

        except Exception as e:
            status = type(e).__name__

        finally:
            rv.release()
            wv.release()
            try:
                dst.shutdown(socket.SHUT_WR)
            except Exception:
                pass

        _log(INFO, f"Relay finished: {status}", label)

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
