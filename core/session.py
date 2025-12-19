import logging
import struct
import ipaddress
import time
from functools import cache

import gevent
from gevent import socket
from gevent.socket import wait_read, wait_write
from gevent.pool import Group


SO_ORIGINAL_DST = 80
V4_LEN = 16
V4_FMT = "!2xH4s"
V6_LEN = 28
V6_FMT = "!2xH4x16s"

DIR_UP = "->"
DIR_DOWN = "<-"

DEFAULT_TIMEOUT = 86400


def get_original_dst(sock: socket.socket, family: socket.AddressFamily):
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
        self._created = time.perf_counter()
        self._logger = logging.getLogger(f"{self.__class__.__name__}-{hex(id(self))}")
        self._buffer = buffer
        self._idle_timeout = idle_timeout if idle_timeout > 0 else DEFAULT_TIMEOUT

        self._client_sock = client_sock
        self._client_addr = client_addr
        self._cl_name = f"[{client_addr[0]}]:{client_addr[1]}"
        self._family = socket.AF_INET if ipv4_mapped(client_addr[0]) else socket.AF_INET6

        self._remote_addr = get_original_dst(client_sock, self._family)
        srvaddr = client_sock.getsockname()
        if self._remote_addr[0] == srvaddr[0] and self._remote_addr[1] == srvaddr[1]:
            raise ConnectionRefusedError("Direct connections are not allowed")
        self._rm_name = f"[{self._remote_addr[0]}]:{self._remote_addr[1]}"

        self._remote_sock = socket.socket(self._family, client_sock.type)
        self._configure_socket(self._client_sock)
        self._configure_socket(self._remote_sock, tfo=True)

    def serve(self):
        with self._client_sock, self._remote_sock:
            try:
                self._remote_sock.connect(self._remote_addr)
                self._attemt_tfo()

                center = len(self._buffer) // 2
                up_buf = self._buffer[:center]
                down_buf = self._buffer[center:]
                group = Group()
                group.spawn(self._relay, DIR_UP, self._client_sock, self._remote_sock, up_buf)
                group.spawn(self._relay, DIR_DOWN, self._remote_sock, self._client_sock, down_buf)

                start_time = (time.perf_counter() - self._created) * 1000
                self._log(logging.INFO, f"Session started in {start_time:.2f}ms", f"{self._cl_name:50} {DIR_UP} {self._rm_name:50}")
                group.join()

            except TimeoutError:
                self._log(logging.WARNING, "Connection timed out", f"{self._cl_name:50} {DIR_UP} {self._rm_name:50}")
            except ConnectionRefusedError:
                self._log(logging.WARNING, "Connection refused", f"{self._cl_name:50} {DIR_UP} {self._rm_name:50}")
            except Exception as e:
                self._log(logging.ERROR, f"Unexpected in session: {e}", f"{self._cl_name:50} {DIR_UP} {self._rm_name:50}")

        session_time = time.perf_counter() - self._created
        self._log(logging.INFO, f"Session ended in {session_time:.2f}s", f"{self._cl_name:50} {DIR_UP} {self._rm_name:50}")

    def _relay(self, direction: str, src: socket.socket, dst: socket.socket, buf: memoryview):
        center = len(buf) // 2
        rcvbuf = buf[:center]
        sndbuf = buf[center:]
        rcvlen = 0
        sndlen = 0
        eof = False

        try:
            self._log(logging.DEBUG, "Starting relay", f"{self._cl_name:50} {direction} {self._rm_name:50}")

            while not eof:
                wait_read(src.fileno(), self._idle_timeout)
                if not (rcvlen := src.recv_into(rcvbuf)):
                    eof = True
                    break

                dst.setsockopt(socket.SOL_TCP, socket.TCP_CORK, 1)
                while rcvlen:
                    (rcvbuf, rcvlen), (sndbuf, sndlen) = (sndbuf, 0), (rcvbuf, rcvlen)

                    snd = gevent.spawn(self._sendto, dst, sndbuf[:sndlen])
                    gevent.sleep(0)

                    try:
                        wait_read(src.fileno(), 0)
                        if not (rcvlen := src.recv_into(rcvbuf)):
                            eof = True
                            break
                    except TimeoutError:
                        pass

                    if not snd.get():
                        raise BrokenPipeError(f"Failed to send {sndlen} bytes")
                dst.setsockopt(socket.SOL_TCP, socket.TCP_CORK, 0)

        except BrokenPipeError as e:
            self._log(logging.WARNING, f"{e}", f"{self._cl_name:50} {direction} {self._rm_name:50}")
        except ConnectionResetError:
            self._log(logging.WARNING, "Connection reset", f"{self._cl_name:50} {direction} {self._rm_name:50}")
        except TimeoutError:
            self._log(logging.WARNING, "Connection timed out", f"{self._cl_name:50} {direction} {self._rm_name:50}")
        except Exception as e:
            self._log(logging.ERROR, f"Unexpected in relay: {e}", f"{self._cl_name:50} {direction} {self._rm_name:50}")
        finally:
            try:
                dst.setsockopt(socket.SOL_TCP, socket.TCP_CORK, 0)
                dst.shutdown(socket.SHUT_WR)
            except:
                pass
            self._log(logging.DEBUG, "Relay stopped", f"{self._cl_name:50} {direction} {self._rm_name:50}")

    def _configure_socket(self, sock: socket.socket, tfo: bool = False):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_TCP, socket.TCP_QUICKACK, 1)
        if tfo:
            sock.setsockopt(socket.SOL_TCP, socket.TCP_FASTOPEN_CONNECT, 1)

    def _sendto(self, sock: socket.socket, buf: memoryview) -> bool:
        try:
            wait_write(sock.fileno())
            sock.sendall(buf)
            return True
        except Exception as e:
            self._log(logging.DEBUG, f"Failed to send: {e}", f"{self._cl_name:50} {DIR_UP} {self._rm_name:50}")
            return False

    def _attemt_tfo(self):
        recv = 0
        sent = 0

        try:
            wait_read(self._client_sock.fileno(), 0)
            if recv := self._client_sock.recv_into(self._buffer):
                self._log(logging.DEBUG, f"TFO-R {recv} bytes", f"{self._cl_name:50} {DIR_UP} {self._rm_name:50}")
            else:
                self._log(logging.WARNING, "Client sent EOF with no data", f"{self._cl_name:50} {DIR_UP} {self._rm_name:50}")
        except TimeoutError:
            self._log(logging.DEBUG, f"TFO-R timeout", f"{self._cl_name:50} {DIR_UP} {self._rm_name:50}")

        try:
            if sent := self._remote_sock.sendto(self._buffer[:recv], socket.MSG_FASTOPEN, self._remote_addr):
                self._log(logging.DEBUG, f"TFO-T {sent} / {recv} bytes", f"{self._cl_name:50} {DIR_UP} {self._rm_name:50}")
        except BlockingIOError:
            if recv:
                self._log(logging.DEBUG, f"TFO-T failed", f"{self._cl_name:50} {DIR_UP} {self._rm_name:50}")

        wait_write(self._remote_sock.fileno())
        if sent < recv:
            self._remote_sock.sendall(self._buffer[sent:recv])
            self._log(logging.DEBUG, f"Sent remaining {recv - sent} bytes", f"{self._cl_name:50} {DIR_UP} {self._rm_name:50}")

    def _log(self, level: int, subject: str, msg: str = ""):
        txt = f"{subject:60}"
        if msg:
            txt += f" | {msg}"
        gevent.spawn(self._logger.log, level, txt)
