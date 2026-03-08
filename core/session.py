import ctypes
import ctypes.util
import fcntl
import logging
import mmap
import os

from gevent import socket
from gevent.pool import Group
from gevent.select import select
from gevent.socket import wait_read, wait_write

NULL = -1

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

# === splice ===
libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
_splice = libc.splice
_splice.argtypes = [
    ctypes.c_int,
    ctypes.POINTER(ctypes.c_longlong),
    ctypes.c_int,
    ctypes.POINTER(ctypes.c_longlong),
    ctypes.c_size_t,
    ctypes.c_uint,
]
_splice.restype = ctypes.c_ssize_t

SPLICE_F_MOVE = 0x01
SPLICE_F_NONBLOCK = 0x02
S_FLAGS = SPLICE_F_MOVE | SPLICE_F_NONBLOCK
EAGAIN = 11
F_SETPIPE_SZ = 1031
PIPE_BUF_SIZE = 1 << 20
# === splice ===


class Session:
    def __init__(
        self,
        client_sock: socket.socket,
        client_addr: tuple[str, int],
        remote_addr: tuple[str, int],
        family: socket.AddressFamily,
        timeout: int,
    ) -> None:
        self._logger = logging.getLogger(f"{self.__class__.__name__}-{hex(id(self))}")

        self._client_sock = client_sock
        self._client_addr = client_addr
        self._remote_addr = remote_addr
        self._family = family
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
            group.spawn(self._zrelay, self._client_sock, self._remote_sock, DIR_UP)
            group.spawn(self._zrelay, self._remote_sock, self._client_sock, DIR_DOWN)

            self._log(INFO, "Session started", label)
            group.join()
            self._log(INFO, "Session finished", label)

    def _connect(self) -> bool:
        label = f"{self._client_name:>48} {DIR_UP} {self._remote_name:>48}"
        connected = False

        try:
            self._remote_sock.connect(self._remote_addr)

            with mmap.mmap(
                NULL, mmap.PAGESIZE, mmap.MAP_ANONYMOUS | mmap.MAP_PRIVATE
            ) as mm:
                with memoryview(mm) as mv:
                    recv = 0
                    sent = 0
                    try:
                        wait_read(self._client_sock.fileno(), TFO_RECV_TIMEOUT)
                        if recv := self._client_sock.recv_into(mv):
                            self._log(DEBUG, f"TFO recv {recv:5d} bytes", label)
                    except (TimeoutError, socket.timeout):
                        self._log(DEBUG, "TFO recv timeout", label)
                    try:
                        wait_write(self._remote_sock.fileno(), self._timeout)
                        if sent := self._remote_sock.sendto(
                            mv[:recv], MSG_FASTOPEN, self._remote_addr
                        ):
                            self._log(INFO, f"TFO sent {sent:5d} bytes", label)
                    except BlockingIOError:
                        if recv:
                            self._log(DEBUG, "TFO failed", label)
                    if sent < recv:
                        wait_write(self._remote_sock.fileno(), self._timeout)
                        self._remote_sock.sendall(mv[sent:recv])
                        self._log(
                            DEBUG, f"Sent remaining {recv - sent:5d} bytes", label
                        )

            connected = True
        except Exception as e:
            self._log(ERROR, f"Connection error: {e}", label)

        return connected

    def _zrelay(
        self, src_sock: socket.socket, dst_sock: socket.socket, dir: str
    ) -> None:
        label = f"{self._client_name:>48} {dir} {self._remote_name:>48}"

        src_fd = src_sock.fileno()
        dst_fd = dst_sock.fileno()

        p_r, p_w = os.pipe2(os.O_NONBLOCK)
        try:
            fcntl.fcntl(p_w, F_SETPIPE_SZ, PIPE_BUF_SIZE)
        except OSError:
            pass

        status = {"reason": "unknown", "errno": 0}
        is_eof = False
        pipe_has_data = False
        pipe_full = False
        _get_errno = ctypes.get_errno

        try:
            while not is_eof or pipe_has_data:
                r_watch = []
                w_watch = []

                if not is_eof and not pipe_full:
                    r_watch.append(src_fd)

                if pipe_has_data:
                    w_watch.append(dst_fd)

                rready, wready, _ = select(r_watch, w_watch, [], self._timeout)
                if not (rready or wready):
                    status = {"reason": "timeout"}
                    break

                if dst_fd in wready and pipe_has_data:
                    m = _splice(p_r, None, dst_fd, None, PIPE_BUF_SIZE, S_FLAGS)
                    if m > 0:
                        pipe_has_data = True
                    elif m < 0:
                        err = _get_errno()
                        if err == EAGAIN:
                            pipe_has_data = False
                            pipe_full = False
                        else:
                            status = {"reason": "error_dst", "errno": err}
                            break

                if src_fd in rready and not pipe_full:
                    n = _splice(src_fd, None, p_w, None, PIPE_BUF_SIZE, S_FLAGS)
                    if n > 0:
                        pipe_has_data = True
                    elif n == 0:
                        is_eof = True
                    else:
                        err = _get_errno()
                        if err == EAGAIN:
                            pipe_full = True
                        else:
                            status = {"reason": "error_src", "errno": err}
                            break

            if status["reason"] == "unknown":
                status["reason"] = "eof"

        finally:
            os.close(p_r)
            os.close(p_w)
            try:
                dst_sock.shutdown(socket.SHUT_WR)
            except OSError:
                pass

        self._log(
            INFO,
            f"ZRelay finished: {status['reason']} ({status['errno']})",
            label,
        )

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
