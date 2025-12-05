import gevent
import gevent.monkey
from gevent import socket
from gevent.pool import Group
from gevent.server import StreamServer
from gevent.socket import wait_read, wait_write, timeout

gevent.monkey.patch_all()

import gc
import logging
import os
import struct
from concurrent.futures import ProcessPoolExecutor


LOG_LEVEL = logging.INFO

PORT = 8081
FAMILY = [socket.AF_INET, socket.AF_INET6]

NUM_WORKERS = 4
BUFFER_SIZE = 1 << 20
IDLE_TIMEOUT = 43200

TFO_MSS = 1200
TFO_TIMEOUT = 0.01


class Session:
    _client_sock: socket.socket
    _client_addr: tuple[str, int]
    _remote_sock: socket.socket
    _remote_addr: tuple[str, int]
    _cl_str: str
    _rm_str: str
    _logger: logging.Logger
    _SO_ORIGINAL_DST = 80
    _SOL_IPV6 = 41
    _V4_LEN = 16
    _V4_FMT = "!2xH4s"
    _V6_LEN = 28
    _V6_FMT = "!2xH4x16s"

    def __init__(self, client_sock: socket.socket, client_addr: tuple[str, int]):
        self._remote_addr = self._get_orig_dst(client_sock)
        srv_addr = client_sock.getsockname()
        if self._remote_addr[0] == srv_addr[0] and self._remote_addr[1] == srv_addr[1]:
            raise ConnectionRefusedError("Connections to the proxy server itself are not allowed")

        self._client_sock = client_sock
        self._client_addr = client_addr
        self._configure_socket(client_sock)

        self._remote_sock = socket.socket(client_sock.family, client_sock.type)
        self._configure_socket(self._remote_sock)
        self._remote_sock.setsockopt(socket.SOL_TCP, socket.TCP_FASTOPEN_CONNECT, 1)

        cl_str = f"[{self._client_addr[0]}]:{self._client_addr[1]}"
        rm_str = f"[{self._remote_addr[0]}]:{self._remote_addr[1]}"
        self._cl_str = cl_str
        self._rm_str = rm_str

        session_id = f"Session[{self._client_sock.fileno()}]"
        self._logger = logging.getLogger(session_id)
        self._log(logging.DEBUG, "->", "Session Created")

    def serve(self):
        try:
            buffer = memoryview(bytearray(TFO_MSS))
            recv = 0
            sent = 0
            try:
                wait_read(self._client_sock.fileno(), TFO_TIMEOUT)
                if recv := self._client_sock.recv_into(buffer):
                    self._log(logging.DEBUG, "->", f"Received initial {recv} bytes")
                else:
                    raise ConnectionAbortedError("Connection closed by client before data was sent")
            except timeout:
                self._log(logging.DEBUG, "->", "Client did not send data")

            self._remote_sock.connect(self._remote_addr)

            try:
                if sent := self._remote_sock.sendto(buffer[:recv], socket.MSG_FASTOPEN, self._remote_addr):
                    self._log(logging.DEBUG, "->", f"Sent initial {sent} bytes (TFO)")
            except BlockingIOError:
                self._log(logging.DEBUG, "->", "TFO not supported/failed")

            wait_write(self._remote_sock.fileno(), IDLE_TIMEOUT)
            if sent < recv:
                self._remote_sock.sendall(buffer[sent:recv])
                self._log(logging.DEBUG, "->", f"Sent remaining {recv - sent} bytes")
            del buffer

            self._log(logging.INFO, "->", "Connected")
            group = Group()
            group.spawn(self._relay, "->", self._client_sock, self._remote_sock)
            group.spawn(self._relay, "<-", self._remote_sock, self._client_sock)
            group.join()
        except Exception as e:
            self._log(logging.ERROR, "->", f"Session Failed: {e}")
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
            self._log(logging.INFO, "->", "Session Closed")

    def _relay(self, direction: str, src: socket.socket, dst: socket.socket):
        eof = False
        buffer = memoryview(bytearray(BUFFER_SIZE << 1))
        rbuf = buffer[:BUFFER_SIZE]
        wbuf = buffer[BUFFER_SIZE:]
        rlen = 0
        wlen = 0

        try:
            while True:
                wait_read(src.fileno(), IDLE_TIMEOUT)
                if rlen := src.recv_into(rbuf):
                    pass
                else:
                    eof = True

                dst.setsockopt(socket.SOL_TCP, socket.TCP_CORK, 1)
                while rlen:
                    rbuf, rlen, wbuf, wlen = wbuf, wlen, rbuf, rlen
                    sent = 0
                    while sent < wlen:
                        sent += dst.send(wbuf[sent:wlen])

                        if not eof and not rlen:
                            try:
                                wait_read(src.fileno(), 0)
                                if rlen := src.recv_into(rbuf):
                                    pass
                                else:
                                    eof = True
                            except timeout:
                                pass
                    wlen = 0
                dst.setsockopt(socket.SOL_TCP, socket.TCP_CORK, 0)

                if eof and not rlen:
                    break
        except timeout:
            pass
        except ConnectionResetError:
            pass
        except BrokenPipeError:
            pass
        except OSError:
            pass
        finally:
            try:
                dst.setsockopt(socket.SOL_TCP, socket.TCP_CORK, 0)
                dst.shutdown(socket.SHUT_WR)
            except OSError:
                pass
            del buffer, rbuf, wbuf, rlen, wlen
            self._log(logging.INFO, direction, "Relay Finished")

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

    def _configure_socket(self, sock: socket.socket):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        sock.setsockopt(socket.SOL_TCP, socket.TCP_QUICKACK, 1)

    def _log(self, level: int, direction: str, message: str):
        """Formats and logs a message with consistent structure."""
        subject = f"{self._cl_str:<48s} {direction} {self._rm_str:<48s}"
        log_msg = f"{subject:<99s} | {message}"
        self._logger.log(level, log_msg)


class ProxyServer:
    _logger: logging.Logger

    def __init__(self, worker_id: int):
        self._logger = logging.getLogger(f"ProxyServer[{worker_id}]")

    def _log(self, level: int, subject: str, direction: str, detail: str, message: str):
        """Formats and logs a server message with consistent structure."""
        # Mimic Session's log format for perfect alignment.
        log_subject = f"{subject:<48s} {direction} {detail:<48s}"
        log_msg = f"{log_subject:<99s} | {message}"
        self._logger.log(level, log_msg)

    def _accept_client(self, client_sock: socket.socket, client_addr: tuple[str, int]):
        cl_str = f"[{client_addr[0]}]:{client_addr[1]}"
        self._log(logging.DEBUG, cl_str, "::", "Accepted Client", "OK")
        try:
            session = Session(client_sock, client_addr)
            session.serve()
        except ConnectionRefusedError as e:
            self._log(logging.WARNING, cl_str, "::", "Refused Client", str(e))
        except Exception as e:
            self._log(logging.ERROR, cl_str, "::", "Client Error", str(e))
            try:
                client_sock.shutdown(socket.SHUT_RDWR)
                client_sock.close()
            except OSError:
                pass

    def _launch_listener(self, family: socket.AddressFamily):
        sock = socket.socket(family, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind(("", PORT))
        sock.listen(socket.SOMAXCONN)
        addr = sock.getsockname()
        self._log(logging.INFO, "Listening", "::", f"on [{addr[0]}]:{addr[1]}", "Ready")
        StreamServer(sock, self._accept_client).serve_forever()

    def run(self):
        self._log(logging.INFO, "Worker", "::", "Process", "Starting")
        listener_pool = Group()
        listener_pool.map(self._launch_listener, FAMILY)
        listener_pool.join()
        self._log(logging.INFO, "Worker", "::", "Process", "Stopped")


def main(worker_id: int):
    server = ProxyServer(worker_id)
    gevent.spawn(server.run).join()


if __name__ == "__main__":
    if os.getenv("DEBUG"):
        LOG_LEVEL = logging.DEBUG
        gc.set_debug(gc.DEBUG_STATS)
    logging.basicConfig(level=LOG_LEVEL, format="%(name)-20s [%(levelname)-8s] %(message)s")

    with ProcessPoolExecutor(NUM_WORKERS) as pool:
        pool.map(main, range(NUM_WORKERS))
