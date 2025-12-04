import os
import struct
import multiprocessing
import logging
import gc

import gevent
from gevent import socket
from gevent.socket import wait_read, wait_write, timeout
from gevent.server import StreamServer
from gevent.pool import Group

LOG_LEVEL = logging.INFO

PORT = 8081
FAMILY = [socket.AF_INET, socket.AF_INET6]

NUM_WORKERS = 4
BUFFER_SIZE = 1 << 20

IDLE_TIMEOUT = 43200
TFO_TIMEOUT = 10 / 1000


class Session:
    _client_sock: socket.socket
    _client_addr: tuple[str, int]
    _remote_sock: socket.socket
    _remote_addr: tuple[str, int]
    _up_label: str
    _down_label: str

    def __init__(self, client_sock: socket.socket, client_addr: tuple[str, int]):
        self._remote_addr = self._get_orig_dst(client_sock)
        srv_addr = client_sock.getsockname()
        if self._remote_addr[0] == srv_addr[0] and self._remote_addr[1] == srv_addr[1]:
            client_sock.shutdown(socket.SHUT_RDWR)
            client_sock.close()
            logging.error(f"[{client_addr[0]}]:{client_addr[1]} Refused direct connection")
            return

        self._client_sock = client_sock
        self._client_addr = client_addr
        client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        client_sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        client_sock.setsockopt(socket.SOL_TCP, socket.TCP_QUICKACK, 1)

        self._remote_sock = socket.socket(client_sock.family, client_sock.type)
        self._remote_sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        self._remote_sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        self._remote_sock.setsockopt(socket.SOL_TCP, socket.TCP_QUICKACK, 1)
        self._remote_sock.setsockopt(socket.SOL_TCP, socket.TCP_FASTOPEN_CONNECT, 1)

        cl_str = f"[{self._client_addr[0]}]:{self._client_addr[1]}"
        rm_str = f"[{self._remote_addr[0]}]:{self._remote_addr[1]}"
        self._up_label = f"{cl_str} -> {rm_str}"
        self._down_label = f"{cl_str} <- {rm_str}"

        logging.debug(f"{self._up_label} Session created")

    def serve(self):
        try:
            self._remote_sock.connect(self._remote_addr)

            buffer = memoryview(bytearray(BUFFER_SIZE))
            recv = 0
            sent = 0
            try:
                wait_read(self._client_sock.fileno(), TFO_TIMEOUT)
                if recv := self._client_sock.recv_into(buffer):
                    logging.debug(f"{self._up_label} Received initial {recv} bytes")
                else:
                    raise ConnectionAbortedError("Client closed connection before send data")
            except timeout:
                logging.debug(f"{self._up_label} Client did not send data before timeout")

            try:
                if sent := self._remote_sock.sendto(buffer[:recv], socket.MSG_FASTOPEN, self._remote_addr):
                    logging.debug(f"{self._up_label} Sent initial {sent} bytes")
            except BlockingIOError:
                logging.debug(f"{self._up_label} Failed to TFO (no cookie/support)")

            wait_write(self._remote_sock.fileno(), IDLE_TIMEOUT)
            if sent < recv:
                self._remote_sock.sendall(buffer[sent:recv])
                logging.debug(f"{self._up_label} Sent remaining {recv - sent} bytes")
            del buffer

            logging.info(f"{self._up_label} Connected")
            group = Group()
            group.spawn(self._relay, self._up_label, self._client_sock, self._remote_sock)
            group.spawn(self._relay, self._down_label, self._remote_sock, self._client_sock)
            group.join()
        except Exception as e:
            logging.error(f"{self._up_label} Error: {e}")
        finally:
            try:
                self._remote_sock.shutdown(socket.SHUT_RDWR)
                self._remote_sock.close()
            except:
                pass
            try:
                self._client_sock.shutdown(socket.SHUT_RDWR)
                self._client_sock.close()
            except:
                pass
            logging.info(f"{self._up_label} Closed")

    def _relay(self, label: str, src: socket.socket, dst: socket.socket):
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
            except:
                pass
            del buffer, rbuf, wbuf, rlen, wlen
            logging.info(f"{label} EOF")

    _SO_ORIGINAL_DST = 80
    _SOL_IPV6 = 41
    _V4_LEN = 16
    _V4_FMT = "!2xH4s"
    _V6_LEN = 28
    _V6_FMT = "!2xH4x16s"

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


class ProxyServer:
    _session_pool: Group

    def __init__(self):
        self._session_pool = Group()

    def _accept_client(self, client_sock: socket.socket, client_addr: tuple[str, int]):
        logging.debug(f"Accepted client from [{client_addr[0]}]:{client_addr[1]}")
        try:
            self._session_pool.spawn(Session(client_sock, client_addr).serve).join()
        except Exception as e:
            logging.error(f"[{client_addr[0]}]:{client_addr[1]} Session error: {e}")
        finally:
            try:
                client_sock.shutdown(socket.SHUT_RDWR)
                client_sock.close()
            except:
                pass

    def _launch_listener(self, family: socket.AddressFamily):
        sock = socket.socket(family, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind(("", PORT))
        sock.listen(socket.SOMAXCONN)
        addr = sock.getsockname()
        logging.info(f"Listening on [{addr[0]}]:{addr[1]}")
        gevent.spawn(StreamServer(sock, self._accept_client).serve_forever).join()

    def run(self):
        logging.info("Starting proxy server")
        listener_pool = Group()
        listener_pool.map(self._launch_listener, FAMILY)
        listener_pool.join()
        logging.info("Proxy server stopped")


def main(*_):
    gevent.spawn(ProxyServer().run).join()


if __name__ == "__main__":
    if os.getenv("DEBUG"):
        LOG_LEVEL = logging.DEBUG
        gc.set_debug(gc.DEBUG_STATS)
    logging.basicConfig(level=LOG_LEVEL)

    with multiprocessing.Pool(NUM_WORKERS) as pool:
        pool.map(main, range(NUM_WORKERS))
