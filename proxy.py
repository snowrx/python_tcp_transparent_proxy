import gevent
import gevent.monkey
from gevent import socket
from gevent.pool import Group
from gevent.server import StreamServer
from gevent.socket import wait_read, wait_write, timeout

gevent.monkey.patch_all()

import logging
import time
import gc
import os
import struct
from concurrent.futures import ProcessPoolExecutor

LOG_LEVEL = logging.INFO

PORT = 8081
FAMILY = [socket.AF_INET, socket.AF_INET6]

NUM_WORKERS = 4
BUFFER_SIZE = 1 << 18
IDLE_TIMEOUT = 43200
SEND_TIMEOUT = 10

TFO_MSS = 1200
TFO_TIMEOUT = 0


class Session:
    _SO_ORIGINAL_DST = 80
    _SOL_IPV6 = 41
    _V4_LEN = 16
    _V4_FMT = "!2xH4s"
    _V6_LEN = 28
    _V6_FMT = "!2xH4x16s"
    _DIR_UP = "->"
    _DIR_DOWN = "<-"

    def __init__(self, client_sock: socket.socket, client_addr: tuple[str, int], accepted_at: float):
        self._accepted_at = accepted_at
        self._name = f"Session-{hex(id(self))}"
        self._logger = logging.getLogger(self._name)
        self._cl_name = f"[{client_addr[0]}]:{client_addr[1]}"

        self._remote_addr = self._get_orig_dst(client_sock)
        srv_addr = client_sock.getsockname()
        if self._remote_addr[0] == srv_addr[0] and self._remote_addr[1] == srv_addr[1]:
            raise ConnectionRefusedError("Direct connections are not allowed")
        self._rm_name = f"[{self._remote_addr[0]}]:{self._remote_addr[1]}"

        self._client_sock = client_sock
        self._remote_sock = socket.socket(client_sock.family, client_sock.type)

        self._configure_socket(client_sock)
        self._configure_socket(self._remote_sock, tfo=True)

        creation_time = time.perf_counter() - self._accepted_at
        self._log(logging.DEBUG, f"Created ({creation_time * 1000:.2f}ms)", f"{self._cl_name:50} {self._DIR_UP} {self._rm_name:50}")

    def serve(self):
        try:
            self._remote_sock.connect(self._remote_addr)

            buffer = memoryview(bytearray(TFO_MSS))
            recv = 0
            sent = 0
            try:
                wait_read(self._client_sock.fileno(), TFO_TIMEOUT)
                if recv := self._client_sock.recv_into(buffer):
                    self._log(logging.DEBUG, f"TFO-R {recv:17} bytes", f"{self._cl_name:50} {self._DIR_UP} {self._rm_name:50}")
                else:
                    raise ConnectionAbortedError("Empty client connection")
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
            del buffer

            open_time = time.perf_counter() - self._accepted_at
            self._log(logging.INFO, f"Established ({open_time * 1000:.2f}ms)", f"{self._cl_name:50} {self._DIR_UP} {self._rm_name:50}")
            group = Group()
            group.spawn(self._relay, self._DIR_DOWN, self._remote_sock, self._client_sock)
            group.spawn(self._relay, self._DIR_UP, self._client_sock, self._remote_sock)
            group.join()
        except Exception as e:
            self._log(logging.ERROR, f"Serve failed: {e}", f"{self._cl_name:50} {self._DIR_UP} {self._rm_name:50}")
        finally:
            try:
                self._remote_sock.shutdown(socket.SHUT_RDWR)
                self._remote_sock.close()
            except OSError:
                pass
            try:
                self._client_sock.shutdown(socket.SHUT_RDWR)
                self._client_sock.close()
            except OSError:
                pass
            run_time = time.perf_counter() - self._accepted_at
            self._log(logging.INFO, f"Closed ({run_time:.2f}s)", f"{self._cl_name:50} {self._DIR_UP} {self._rm_name:50}")

    def _relay(self, direction: str, src: socket.socket, dst: socket.socket):
        buffer = memoryview(bytearray(BUFFER_SIZE << 1))
        rbuf = buffer[:BUFFER_SIZE]
        wbuf = buffer[BUFFER_SIZE:]
        rlen = 0
        wlen = 0

        def sendall(buf: memoryview):
            try:
                ts = time.perf_counter()
                wait_write(dst.fileno(), SEND_TIMEOUT)
                dst.sendall(buf)
                if (latency := (time.perf_counter() - ts) * 1000) >= 200:
                    self._log(logging.DEBUG, f"High latency {latency:.2f}ms", f"{self._cl_name:50} {direction} {self._rm_name:50}")
                return True
            except:
                return False

        try:
            self._log(logging.DEBUG, f"Relay started", f"{self._cl_name:50} {direction} {self._rm_name:50}")

            while True:
                wait_read(src.fileno(), IDLE_TIMEOUT)
                if not (rlen := src.recv_into(rbuf)):
                    break

                dst.setsockopt(socket.SOL_TCP, socket.TCP_CORK, 1)
                while rlen:
                    rbuf, rlen, wbuf, wlen = wbuf, wlen, rbuf, rlen

                    g = gevent.spawn(sendall, wbuf[:wlen])
                    try:
                        wait_read(src.fileno(), 0)
                        gevent.idle()
                        if not (rlen := src.recv_into(rbuf)):
                            break
                    except timeout:
                        pass

                    if not g.get():
                        raise ConnectionResetError("Remote connection closed")
                    wlen = 0
                dst.setsockopt(socket.SOL_TCP, socket.TCP_CORK, 0)

            self._log(logging.DEBUG, f"EOF received", f"{self._cl_name:50} {direction} {self._rm_name:50}")
        except timeout:
            self._log(logging.DEBUG, f"Idle timeout", f"{self._cl_name:50} {direction} {self._rm_name:50}")
        except ConnectionResetError:
            self._log(logging.DEBUG, f"Connection reset", f"{self._cl_name:50} {direction} {self._rm_name:50}")
        except BrokenPipeError:
            self._log(logging.DEBUG, f"Broken pipe", f"{self._cl_name:50} {direction} {self._rm_name:50}")
        except OSError:
            self._log(logging.DEBUG, f"OS error", f"{self._cl_name:50} {direction} {self._rm_name:50}")
        finally:
            try:
                dst.setsockopt(socket.SOL_TCP, socket.TCP_CORK, 0)
                dst.shutdown(socket.SHUT_WR)
            except OSError:
                pass
            del buffer, rbuf, wbuf, rlen, wlen
            self._log(logging.DEBUG, f"Relay finished", f"{self._cl_name:50} {direction} {self._rm_name:50}")

    def _get_orig_dst(self, sock: socket.socket):
        ip: str = ""
        port: int = 0
        match sock.family:
            case socket.AF_INET:
                dst = sock.getsockopt(socket.SOL_IP, self._SO_ORIGINAL_DST, self._V4_LEN)
                port, raw_ip = struct.unpack_from(self._V4_FMT, dst)
                ip = socket.inet_ntop(socket.AF_INET, raw_ip)
            case socket.AF_INET6:
                dst = sock.getsockopt(self._SOL_IPV6, self._SO_ORIGINAL_DST, self._V6_LEN)
                port, raw_ip = struct.unpack_from(self._V6_FMT, dst)
                ip = socket.inet_ntop(socket.AF_INET6, raw_ip)
            case _:
                raise RuntimeError(f"Unknown socket family: {sock.family}")
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

    def run(self):
        self._log(logging.INFO, "Starting server")
        listener_pool = Group()
        listener_pool.map(self._launch_listener, FAMILY)
        listener_pool.join()

    def _launch_listener(self, family: socket.AddressFamily):
        sock = socket.socket(family, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind(("", PORT))
        sock.listen(socket.SOMAXCONN)
        addr = sock.getsockname()
        self._log(logging.INFO, f"Listening on [{addr[0]}]:{addr[1]}")
        StreamServer(sock, self._accept_client).serve_forever()

    def _accept_client(self, client_sock: socket.socket, client_addr: tuple[str, int]):
        accepted_at = time.perf_counter()
        cl_name = f"[{client_addr[0]}]:{client_addr[1]}"
        self._log(logging.DEBUG, "Accepted", cl_name)
        try:
            session = Session(client_sock, client_addr, accepted_at)
            session.serve()
        except ConnectionRefusedError as e:
            self._log(logging.WARNING, f"{e}", cl_name)
        except Exception as e:
            self._log(logging.ERROR, f"Session failed: {e}", cl_name)
            try:
                client_sock.shutdown(socket.SHUT_RDWR)
                client_sock.close()
            except OSError:
                pass

    def _log(self, level: int, subject: str, msg: str = ""):
        txt = f"{subject:50}"
        if msg:
            txt += f" | {msg}"
        self._logger.log(level, txt)


def main(worker_id: int = 0):
    server = ProxyServer(worker_id)
    gevent.spawn(server.run).join()


if __name__ == "__main__":
    gc.set_threshold(3000)
    gc.collect()
    gc.freeze()
    if os.getenv("DEBUG"):
        LOG_LEVEL = logging.DEBUG
        gc.set_debug(gc.DEBUG_STATS)
    logging.basicConfig(level=LOG_LEVEL, format="%(name)-25s | %(levelname)-10s | %(message)s")

    with ProcessPoolExecutor(NUM_WORKERS) as pool:
        pool.map(main, range(NUM_WORKERS))
