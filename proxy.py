import os
import struct
import multiprocessing
import logging

import gevent
from gevent import socket
from gevent.socket import wait_read, wait_write
from gevent.server import StreamServer
from gevent.pool import Group

LOG_LEVEL = logging.INFO

PORT = 8081
FAMILY = [socket.AF_INET, socket.AF_INET6]

BUFFER_SIZE = 1 << 14

IDLE_TIMEOUT = 43200
FAST_TIMEOUT = 1 / 1000
TFO_TIMEOUT = 10 / 1000


class Session:
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
            raise ConnectionRefusedError("The client tried to connect directly to the proxy")

        self._client_sock = client_sock
        self._client_addr = client_addr
        self._remote_sock = socket.socket(client_sock.family, client_sock.type)
        self._up_label = f"[{client_addr[0]}]:{client_addr[1]} -> [{self._remote_addr[0]}]:{self._remote_addr[1]}"
        self._down_label = f"[{client_addr[0]}]:{client_addr[1]} <- [{self._remote_addr[0]}]:{self._remote_addr[1]}"

        try:
            client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            client_sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
            client_sock.setsockopt(socket.SOL_TCP, socket.TCP_QUICKACK, 1)
        except AttributeError:
            pass

        try:
            self._remote_sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            self._remote_sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
            self._remote_sock.setsockopt(socket.SOL_TCP, socket.TCP_QUICKACK, 1)
            self._remote_sock.setsockopt(socket.SOL_TCP, socket.TCP_FASTOPEN_CONNECT, 1)
        except AttributeError:
            pass

        logging.debug(f"{self._up_label} Session created")

    def _forward_data(self, label: str, src: socket.socket, dst: socket.socket):
        try:
            eof = False
            wait_read(src.fileno(), IDLE_TIMEOUT)
            while buffer := src.recv(BUFFER_SIZE):
                dst.setsockopt(socket.SOL_TCP, socket.TCP_CORK, 1)
                while buffer:
                    # send chunk
                    wait_write(dst.fileno())
                    sent = dst.send(buffer)
                    buffer = bytes(memoryview(buffer)[sent:])

                    # try fill more
                    if not eof and len(buffer) < BUFFER_SIZE:
                        try:
                            wait_read(src.fileno(), FAST_TIMEOUT)
                            if next_buffer := src.recv(BUFFER_SIZE - len(buffer)):
                                buffer += next_buffer
                            else:
                                eof = True
                        except TimeoutError:
                            pass
                dst.setsockopt(socket.SOL_TCP, socket.TCP_CORK, 0)
                if eof:
                    break
                wait_read(src.fileno(), IDLE_TIMEOUT)
            logging.debug(f"{label} EOF")
        except TimeoutError:
            logging.debug(f"{label} Timeout")
        except ConnectionResetError:
            logging.debug(f"{label} Connection reset by peer")
        except Exception as e:
            logging.error(f"{label} Unexpected error: {e}")
        finally:
            try:
                dst.shutdown(socket.SHUT_WR)
            except:
                pass

    def serve(self):
        self._remote_sock.connect(self._remote_addr)

        buffer = bytes()
        try:
            wait_read(self._client_sock.fileno(), TFO_TIMEOUT)
            if buffer := self._client_sock.recv(BUFFER_SIZE):
                logging.debug(f"{self._up_label} Received TFO {len(buffer)} bytes")
                sent = self._remote_sock.sendto(buffer, socket.MSG_FASTOPEN, self._remote_addr)
                buffer = bytes(memoryview(buffer)[sent:])
                logging.debug(f"{self._up_label} Sent TFO {sent} bytes")
            else:
                raise ConnectionAbortedError("Client closed connection before any data was sent")
        except (TimeoutError, BlockingIOError) as e:
            logging.debug(f"{self._up_label} TFO failed: {e}")

        if buffer:
            wait_write(self._remote_sock.fileno())
            self._remote_sock.sendall(buffer)
            logging.debug(f"{self._up_label} Sent remaining {len(buffer)} bytes")
        del buffer

        logging.info(f"{self._up_label} Session established")
        gevent.joinall(
            [
                gevent.spawn(self._forward_data, self._up_label, self._client_sock, self._remote_sock),
                gevent.spawn(self._forward_data, self._down_label, self._remote_sock, self._client_sock),
            ]
        )
        logging.info(f"{self._up_label} Session closed")


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
    logging.basicConfig(level=LOG_LEVEL)

    w = multiprocessing.cpu_count()
    with multiprocessing.Pool(w) as p:
        p.map(main, range(w))

    logging.shutdown()
