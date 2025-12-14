import gevent
import gevent.monkey
from gevent import socket
from gevent.pool import Group
from gevent.server import StreamServer
from gevent.socket import wait_read, wait_write, timeout
from gevent.lock import RLock
from gevent.queue import SimpleQueue

gevent.monkey.patch_all()

import logging
import time
import gc
import os
import struct
import ipaddress
from concurrent.futures import ProcessPoolExecutor
from contextlib import contextmanager

LOG_LEVEL = logging.INFO

PORT = 8081

# 0: auto, 1: single process, >1: use process pool
NUM_WORKERS = 4
BUFFER_SIZE = 500 << 10

IDLE_TIMEOUT = 7200
SEND_TIMEOUT = 10


class BufferPool:
    def __init__(self, buffer_size: int, max_pool: int = 100, prealloc: bool = True, clear_on_release: bool = True):
        self._buffer_size = buffer_size
        self._lock = RLock()
        self._buffer_pool: SimpleQueue[memoryview] = SimpleQueue(max_pool)
        self._known = 0
        if prealloc:
            for _ in range(max_pool):
                self._buffer_pool.put(memoryview(bytearray(buffer_size)))
                self._known += 1
        self._clear_on_release = clear_on_release
        self._logger = logging.getLogger(f"BufferPool-{hex(id(self))}")

    @contextmanager
    def use(self):
        buffer = self.acquire()
        try:
            yield buffer
        finally:
            # self.release(buffer)
            gevent.spawn(self.release, buffer)

    def acquire(self) -> memoryview:
        with self._lock:
            if self._buffer_pool.empty():
                self._known += 1
                self._logger.debug(f"Allocated new buffer, known: {self._known}, in pool: {self._buffer_pool.qsize()}")
                return memoryview(bytearray(self._buffer_size))
            self._logger.debug(f"Reusing buffer, known: {self._known}, in pool: {self._buffer_pool.qsize()}")
            return self._buffer_pool.get()

    def release(self, buffer: memoryview):
        gevent.idle()
        with self._lock:
            if self._buffer_pool.full():
                buffer.release()
                self._known -= 1
                self._logger.debug(f"Discarded buffer, known: {self._known}, in pool: {self._buffer_pool.qsize()}")
                return
            if self._clear_on_release:
                buffer[:] = b"\0" * self._buffer_size
            self._buffer_pool.put(buffer)
            self._logger.debug(f"Released buffer, known: {self._known}, in pool: {self._buffer_pool.qsize()}")


class Session:
    _SO_ORIGINAL_DST = 80
    _V4_LEN = 16
    _V4_FMT = "!2xH4s"
    _V6_LEN = 28
    _V6_FMT = "!2xH4x16s"
    _DIR_UP = "->"
    _DIR_DOWN = "<-"

    def __init__(self, client_sock: socket.socket, client_addr: tuple[str, int], buffer_pool: BufferPool, accepted_at: float):
        self._accepted_at = accepted_at
        self._buffer_pool = buffer_pool
        self._name = f"Session-{hex(id(self))}"
        self._logger = logging.getLogger(self._name)
        self._cl_name = f"[{client_addr[0]}]:{client_addr[1]}"
        ip = ipaddress.IPv6Address(client_addr[0])
        self._family = socket.AF_INET if ip.ipv4_mapped else socket.AF_INET6

        self._remote_addr = self._get_orig_dst(client_sock, self._family)
        srv_addr = client_sock.getsockname()
        if self._remote_addr[0] == srv_addr[0] and self._remote_addr[1] == srv_addr[1]:
            raise ConnectionRefusedError("Direct connections are not allowed")
        self._rm_name = f"[{self._remote_addr[0]}]:{self._remote_addr[1]}"

        self._client_sock = client_sock
        self._remote_sock = socket.socket(self._family, client_sock.type)

        self._configure_socket(client_sock)
        self._configure_socket(self._remote_sock, tfo=True)

    def serve(self):
        with self._remote_sock:
            try:
                self._remote_sock.connect(self._remote_addr)

                with self._buffer_pool.use() as buffer:
                    recv = 0
                    sent = 0

                    try:
                        wait_read(self._client_sock.fileno(), 0)
                        if recv := self._client_sock.recv_into(buffer):
                            self._log(logging.DEBUG, f"TFO-R {recv:17} bytes", f"{self._cl_name:50} {self._DIR_UP} {self._rm_name:50}")
                        else:
                            self._log(logging.WARNING, "Client sent EOF with no data", f"{self._cl_name:50} {self._DIR_UP} {self._rm_name:50}")
                    except timeout:
                        self._log(logging.DEBUG, f"TFO-R timeout", f"{self._cl_name:50} {self._DIR_UP} {self._rm_name:50}")

                    try:
                        if sent := self._remote_sock.sendto(buffer[:recv], socket.MSG_FASTOPEN, self._remote_addr):
                            self._log(logging.DEBUG, f"TFO-T {sent:7} /{recv:8} bytes", f"{self._cl_name:50} {self._DIR_UP} {self._rm_name:50}")
                    except BlockingIOError:
                        if recv:
                            self._log(logging.DEBUG, f"TFO-T failed", f"{self._cl_name:50} {self._DIR_UP} {self._rm_name:50}")

                    wait_write(self._remote_sock.fileno(), SEND_TIMEOUT)
                    if sent < recv:
                        self._remote_sock.sendall(buffer[sent:recv])
                        self._log(logging.DEBUG, f"Sent remaining {recv - sent:8} bytes", f"{self._cl_name:50} {self._DIR_UP} {self._rm_name:50}")

                open_time = time.perf_counter() - self._accepted_at
                self._log(logging.INFO, f"Established ({open_time * 1000:.2f}ms)", f"{self._cl_name:50} {self._DIR_UP} {self._rm_name:50}")
                group = Group()
                group.spawn(self._relay, self._DIR_DOWN, self._remote_sock, self._client_sock)
                group.spawn(self._relay, self._DIR_UP, self._client_sock, self._remote_sock)
                group.join()
            except Exception as e:
                self._log(logging.ERROR, f"Serve failed: {e}", f"{self._cl_name:50} {self._DIR_UP} {self._rm_name:50}")
            finally:
                run_time = time.perf_counter() - self._accepted_at
                self._log(logging.INFO, f"Closed ({run_time:.2f}s)", f"{self._cl_name:50} {self._DIR_UP} {self._rm_name:50}")

    def _relay(self, direction: str, src: socket.socket, dst: socket.socket):
        def sendall(buf: memoryview) -> bool:
            sent = False
            try:
                wait_write(dst.fileno(), SEND_TIMEOUT)
                dst.sendall(buf)
                sent = True
            except:
                pass
            return sent

        with self._buffer_pool.use() as buffer:
            center = BUFFER_SIZE // 2
            rcvbuf = buffer[:center]
            sndbuf = buffer[center:]
            rcvlen = 0
            sndlen = 0
            eof = False

            try:
                while not eof:
                    wait_read(src.fileno(), IDLE_TIMEOUT)
                    if not (rcvlen := src.recv_into(rcvbuf)):
                        eof = True
                        break

                    dst.setsockopt(socket.SOL_TCP, socket.TCP_CORK, 1)
                    while rcvlen:
                        (rcvbuf, rcvlen), (sndbuf, sndlen) = (sndbuf, sndlen), (rcvbuf, rcvlen)

                        snd = gevent.spawn(sendall, sndbuf[:sndlen])
                        try:
                            wait_read(src.fileno(), 0)
                            gevent.idle()
                            if not (rcvlen := src.recv_into(rcvbuf)):
                                eof = True
                        except timeout:
                            pass

                        if not snd.get():
                            raise ConnectionError(f"Failed to send {sndlen} bytes")
                        sndlen = 0
                    dst.setsockopt(socket.SOL_TCP, socket.TCP_CORK, 0)

                self._log(logging.DEBUG, "EOF", f"{self._cl_name:50} {direction} {self._rm_name:50}")
            except timeout:
                self._log(logging.DEBUG, "Idle timeout", f"{self._cl_name:50} {direction} {self._rm_name:50}")
            except ConnectionResetError:
                self._log(logging.DEBUG, "Connection reset", f"{self._cl_name:50} {direction} {self._rm_name:50}")
            except BrokenPipeError:
                self._log(logging.DEBUG, "Broken pipe", f"{self._cl_name:50} {direction} {self._rm_name:50}")
            except ConnectionError as e:
                self._log(logging.DEBUG, f"{e}", f"{self._cl_name:50} {direction} {self._rm_name:50}")
            except OSError:
                self._log(logging.DEBUG, "Socket error", f"{self._cl_name:50} {direction} {self._rm_name:50}")
            except Exception as e:
                self._log(logging.ERROR, f"Relay failed: {e}", f"{self._cl_name:50} {direction} {self._rm_name:50}")
            finally:
                try:
                    dst.setsockopt(socket.SOL_TCP, socket.TCP_CORK, 0)
                    dst.shutdown(socket.SHUT_WR)
                except:
                    pass
                del rcvbuf, sndbuf

    def _get_orig_dst(self, sock: socket.socket, family: socket.AddressFamily):
        ip: str = ""
        port: int = 0
        match family:
            case socket.AF_INET:
                dst = sock.getsockopt(socket.SOL_IP, self._SO_ORIGINAL_DST, self._V4_LEN)
                port, raw_ip = struct.unpack_from(self._V4_FMT, dst)
                ip = socket.inet_ntop(socket.AF_INET, raw_ip)
            case socket.AF_INET6:
                dst = sock.getsockopt(socket.IPPROTO_IPV6, self._SO_ORIGINAL_DST, self._V6_LEN)
                port, raw_ip = struct.unpack_from(self._V6_FMT, dst)
                ip = socket.inet_ntop(socket.AF_INET6, raw_ip)
            case _:
                raise RuntimeError(f"Unknown socket family: {family}")
        return ip, port

    def _configure_socket(self, sock: socket.socket, tfo: bool = False):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_TCP, socket.TCP_QUICKACK, 1)
        if tfo:
            sock.setsockopt(socket.SOL_TCP, socket.TCP_FASTOPEN_CONNECT, 1)

    def _log(self, level: int, subject: str, msg: str = ""):
        txt = f"{subject:50}"
        if msg:
            txt += f" | {msg}"
        self._logger.log(level, txt)


class ProxyServer:
    def __init__(self, worker_id: int):
        self._name = f"Server-{worker_id}"
        self._logger = logging.getLogger(self._name)
        self._buffer_pool = BufferPool(BUFFER_SIZE)

    def run(self):
        self._log(logging.INFO, "Starting server")
        with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
            sock.bind(("", PORT))
            sock.listen(socket.SOMAXCONN)
            self._log(logging.INFO, f"Listening on *:{PORT}")
            server = StreamServer(sock, self._accept_client)
            server.serve_forever()

    def _accept_client(self, client_sock: socket.socket, client_addr: tuple[str, int]):
        with client_sock:
            accepted_at = time.perf_counter()
            cl_name = f"[{client_addr[0]}]:{client_addr[1]}"
            self._log(logging.DEBUG, "Accepted", cl_name)
            try:
                session = Session(client_sock, client_addr, self._buffer_pool, accepted_at)
                session.serve()
            except ConnectionRefusedError as e:
                self._log(logging.WARNING, f"{e}", cl_name)
            except Exception as e:
                self._log(logging.ERROR, f"Session failed: {e}", cl_name)

    def _log(self, level: int, subject: str, msg: str = ""):
        txt = f"{subject:50}"
        if msg:
            txt += f" | {msg}"
        self._logger.log(level, txt)


def main(worker_id: int = 0):
    server = ProxyServer(worker_id)
    gevent.spawn(server.run).join()


if __name__ == "__main__":
    gc.collect()
    if os.getenv("DEBUG"):
        LOG_LEVEL = logging.DEBUG
        gc.set_debug(gc.DEBUG_STATS)
    logging.basicConfig(level=LOG_LEVEL, format="%(name)-25s | %(levelname)-10s | %(message)s")

    w = NUM_WORKERS if NUM_WORKERS > 0 else os.cpu_count() or 1
    logging.info(f"Starting {w} workers")
    if w > 1:
        with ProcessPoolExecutor(w) as pool:
            pool.map(main, range(w))
    else:
        main()
