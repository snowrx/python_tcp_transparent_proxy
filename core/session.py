import gevent
from gevent import socket, monkey
from gevent.socket import wait_read, wait_write
from gevent.pool import Group

monkey.patch_all()

import logging
import struct
import ipaddress
import time
from functools import cache

SO_ORIGINAL_DST = 80
V4_LEN = 16
V4_FMT = "!2xH4s"
V6_LEN = 28
V6_FMT = "!2xH4x16s"

DIR_UP = "->"
DIR_DOWN = "<-"

DEFAULT_TIMEOUT = 86400


def get_original_dst(sock: socket.socket, family: socket.AddressFamily) -> tuple[str, int]:
    ip: str
    port: int

    match family:
        case socket.AF_INET:
            dst = sock.getsockopt(socket.SOL_IP, SO_ORIGINAL_DST, V4_LEN)
            port, raw_ip = struct.unpack_from(V4_FMT, dst)
            ip = socket.inet_ntop(socket.AF_INET, raw_ip)
        case socket.AF_INET6:
            dst = sock.getsockopt(socket.IPPROTO_IPV6, SO_ORIGINAL_DST, V6_LEN)
            port, raw_ip = struct.unpack_from(V6_FMT, dst)
            ip = socket.inet_ntop(socket.AF_INET6, raw_ip)
        case _:
            raise RuntimeError(f"Unsupported address family: {family}")

    return ip, port


@cache
def ipv4_mapped(addr: str) -> bool:
    return ipaddress.IPv6Address(addr).ipv4_mapped is not None


class Session:
    def __init__(self, client_sock: socket.socket, client_addr: tuple[str, int], buffer: memoryview, idle_timeout: int = DEFAULT_TIMEOUT):
        self._logger = logging.getLogger(f"{self.__class__.__name__}-{hex(id(self))}")
        self._client_sock = client_sock
        self._client_addr = client_addr
        self._buffer = buffer
        self._idle_timeout = idle_timeout

        self._client_name = f"[{self._client_addr[0]}]:{self._client_addr[1]:<5}"
        self._family = socket.AF_INET if ipv4_mapped(self._client_addr[0]) else socket.AF_INET6

        sock_addr = self._client_sock.getsockname()
        self._remote_addr = get_original_dst(self._client_sock, self._family)
        if self._remote_addr[0] == sock_addr[0] and self._remote_addr[1] == sock_addr[1]:
            raise ConnectionRefusedError("Direct connection not allowed")

        self._remote_name = f"[{self._remote_addr[0]}]:{self._remote_addr[1]:<5}"
        self._remote_sock = socket.socket(self._family, socket.SOCK_STREAM)

        self._setsockopt(self._client_sock)
        self._setsockopt(self._remote_sock, tfo=True)

    def run(self) -> None:
        self._log(logging.DEBUG, "Session started", f"{self._client_name:>50} {DIR_UP} {self._remote_name:>50}")
        started_at = time.perf_counter()

        with self._client_sock, self._remote_sock:
            if not self._connect():
                return

            connect_time = time.perf_counter() - started_at
            self._log(logging.INFO, f"Session established in {connect_time * 1000:.1f}ms", f"{self._client_name:>50} {DIR_UP} {self._remote_name:>50}")
            self._run()

        session_time = time.perf_counter() - started_at
        self._log(logging.INFO, f"Session ended in {session_time:.1f}s", f"{self._client_name:>50} {DIR_UP} {self._remote_name:>50}")

    def _connect(self) -> bool:
        success = False
        recv = 0
        sent = 0

        try:
            self._remote_sock.connect(self._remote_addr)

            try:
                wait_read(self._client_sock.fileno(), 0)
                if recv := self._client_sock.recv_into(self._buffer):
                    self._log(logging.DEBUG, f"TFO-R {recv:11} bytes", f"{self._client_name:>50} {DIR_UP} {self._remote_name:>50}")
                else:
                    self._log(logging.WARNING, "Client sent EOF before data", f"{self._client_name:>50} {DIR_UP} {self._remote_name:>50}")
            except TimeoutError:
                self._log(logging.DEBUG, "TFO-R Timeout", f"{self._client_name:>50} {DIR_UP} {self._remote_name:>50}")

            try:
                wait_write(self._remote_sock.fileno())
                if sent := self._remote_sock.sendto(self._buffer[:recv], socket.MSG_FASTOPEN, self._remote_addr):
                    self._log(logging.DEBUG, f"TFO-S {sent:4} / {recv:4} bytes", f"{self._client_name:>50} {DIR_UP} {self._remote_name:>50}")
            except BlockingIOError:
                if recv:
                    self._log(logging.DEBUG, "TFO-S Failed", f"{self._client_name:>50} {DIR_UP} {self._remote_name:>50}")

            if sent < recv:
                wait_write(self._remote_sock.fileno())
                self._remote_sock.sendall(self._buffer[sent:recv])
                self._log(logging.DEBUG, f"Sent {recv - sent:12} bytes", f"{self._client_name:>50} {DIR_UP} {self._remote_name:>50}")

            if sent:
                self._log(logging.INFO, "TFO success", f"{self._client_name:>50} {DIR_UP} {self._remote_name:>50}")

            success = True
        except Exception as e:
            self._log(logging.ERROR, f"Failed to connect: {e}", f"{self._client_name:>50} {DIR_UP} {self._remote_name:>50}")

        return success

    def _run(self) -> None:
        center = len(self._buffer) // 2
        ubuf = self._buffer[:center]
        dbuf = self._buffer[center:]
        group = Group()

        group.spawn(self._pipe, self._client_sock, self._remote_sock, ubuf, DIR_UP)
        group.spawn(self._pipe, self._remote_sock, self._client_sock, dbuf, DIR_DOWN)
        group.join()

        try:
            self._remote_sock.shutdown(socket.SHUT_RDWR)
        except:
            pass
        try:
            self._client_sock.shutdown(socket.SHUT_RDWR)
        except:
            pass

    def _pipe(self, src: socket.socket, dst: socket.socket, buf: memoryview, dir: str) -> None:
        _label = f"{self._client_name:>50} {dir} {self._remote_name:>50}"
        _idle_timeout = self._idle_timeout

        _src_fd = src.fileno()
        _dst_fd = dst.fileno()
        _wait_read = wait_read
        _wait_write = wait_write
        _recv_into = src.recv_into
        _send = dst.send

        self._log(logging.DEBUG, "Pipe started", _label)

        try:
            while 1:
                _wait_read(_src_fd, _idle_timeout)
                recv = _recv_into(buf)
                if not recv:
                    self._log(logging.DEBUG, "EOF received", _label)
                    break

                wv = buf[:recv]
                while wv:
                    _wait_write(_dst_fd, _idle_timeout)
                    sent = _send(wv)
                    if not sent:
                        self._log(logging.DEBUG, "Failed to send", _label)
                        raise BrokenPipeError
                    wv = wv[sent:]

        except ConnectionResetError:
            self._log(logging.ERROR, "Connection reset", _label)
        except TimeoutError:
            self._log(logging.ERROR, "Connection timed out", _label)
        except BrokenPipeError:
            self._log(logging.ERROR, "Broken pipe", _label)
        except Exception as e:
            self._log(logging.ERROR, f"Pipe failed: {e}", _label)
        finally:
            try:
                dst.shutdown(socket.SHUT_WR)
            except:
                pass

        self._log(logging.DEBUG, "Pipe ended", _label)

    def _setsockopt(self, sock: socket.socket, tfo: bool = False) -> None:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_TCP, socket.TCP_QUICKACK, 1)
        if tfo:
            sock.setsockopt(socket.SOL_TCP, socket.TCP_FASTOPEN_CONNECT, 1)

    def _log(self, level: int, subject: str, msg: str = "") -> None:
        txt = f"{subject:60}"
        if msg:
            txt += f" | {msg} |"
        gevent.spawn(self._logger.log, level, txt)
