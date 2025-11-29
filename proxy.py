import logging
import struct
import multiprocessing
import gevent
from gevent import socket
from gevent.socket import wait_read, wait_write
from gevent.server import StreamServer
from gevent.pool import Group

LOG_LEVEL = logging.DEBUG

PORT = 8081
FAMILY = [socket.AF_INET, socket.AF_INET6]

BUFFER_SIZE = 1 << 20

IDLE_TIMEOUT = 43200
FAST_TIMEOUT = 1 / 1000


class Session:
    _SO_ORIGINAL_DST = 80
    _SOL_IPV6 = 41
    _V4_LEN = 16
    _V4_FMT = "!2xH4s"
    _V6_LEN = 28
    _V6_FMT = "!2xH4x16s"

    client_sock: socket.socket
    proxy_sock: socket.socket
    dst_addr: tuple[str, int]
    up_label: str
    down_label: str

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

    def __init__(self, client_sock: socket.socket, client_addr: tuple[str, int]):
        self.dst_addr = self._get_orig_dst(client_sock)
        srv_addr = client_sock.getsockname()
        if self.dst_addr[0] == srv_addr[0] and self.dst_addr[1] == srv_addr[1]:
            raise ConnectionRefusedError(f"The client tried to connect directly to the server.")

        self.client_sock = client_sock
        try:
            self.client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            self.client_sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
            self.client_sock.setsockopt(socket.SOL_TCP, socket.TCP_QUICKACK, 1)
        except Exception as e:
            logging.debug(f"Failed to set client socket options: {e}")

        self.proxy_sock = socket.socket(client_sock.family, client_sock.type)
        try:
            self.proxy_sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            self.proxy_sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
            self.proxy_sock.setsockopt(socket.SOL_TCP, socket.TCP_QUICKACK, 1)
            self.proxy_sock.setsockopt(socket.SOL_TCP, socket.TCP_FASTOPEN_CONNECT, 1)
        except Exception as e:
            logging.debug(f"Failed to set proxy socket options: {e}")

        self.up_label = f"[{client_addr[0]}]:{client_addr[1]} -> [{self.dst_addr[0]}]:{self.dst_addr[1]}"
        self.down_label = f"[{client_addr[0]}]:{client_addr[1]} <- [{self.dst_addr[0]}]:{self.dst_addr[1]}"
        logging.debug(f"Initialized Session {self.up_label}")

    def __del__(self):
        try:
            self.proxy_sock.shutdown(socket.SHUT_RDWR)
            self.proxy_sock.close()
        except Exception:
            pass
        try:
            self.client_sock.shutdown(socket.SHUT_RDWR)
            self.client_sock.close()
        except Exception:
            pass

    def _relay(self, label: str, src: socket.socket, dst: socket.socket):
        logging.debug(f"Starting relay {label}")
        try:
            eof = False
            wait_read(src.fileno(), IDLE_TIMEOUT)
            while buffer := src.recv(BUFFER_SIZE):
                dst.setsockopt(socket.SOL_TCP, socket.TCP_CORK, 1)
                while buffer:
                    wait_write(dst.fileno())
                    sent = dst.send(buffer)
                    buffer = bytes(memoryview(buffer)[sent:])
                    if not eof and len(buffer) < BUFFER_SIZE:
                        try:
                            wait_read(src.fileno(), FAST_TIMEOUT)
                            if next_buffer := src.recv(BUFFER_SIZE - len(buffer)):
                                buffer += next_buffer
                            else:
                                eof = True
                        except (BlockingIOError, TimeoutError):
                            pass
                dst.setsockopt(socket.SOL_TCP, socket.TCP_CORK, 0)
                if eof:
                    break
                wait_read(src.fileno(), IDLE_TIMEOUT)
        except (ConnectionResetError, BrokenPipeError, TimeoutError) as e:
            logging.debug(f"Failed to relay {label}: {e}")
        except Exception as e:
            logging.error(f"Failed to relay {label}: {e}")
        finally:
            try:
                dst.shutdown(socket.SHUT_WR)
            except Exception:
                pass
        logging.debug(f"Closed relay {label}")

    def serve(self):
        buffer = bytes()
        sent = 0

        self.proxy_sock.connect(self.dst_addr)

        try:
            wait_read(self.client_sock.fileno(), FAST_TIMEOUT)
            if buffer := self.client_sock.recv(BUFFER_SIZE):
                wait_write(self.proxy_sock.fileno())
                sent = self.proxy_sock.sendto(buffer, socket.MSG_FASTOPEN, self.dst_addr)
                buffer = bytes(memoryview(buffer)[sent:])
            else:
                raise ConnectionAbortedError("The client disconnected before sending any data.")
        except (BlockingIOError, TimeoutError) as e:
            logging.debug(f"Failed to TFO {self.up_label}: {e}")

        if buffer:
            wait_write(self.proxy_sock.fileno())
            self.proxy_sock.sendall(buffer)
            logging.debug(f"Sent remaining {len(buffer)} bytes to {self.up_label}")

        if sent:
            logging.info(f"Connected {self.up_label} with TFO {sent} bytes")
        else:
            logging.info(f"Connected {self.up_label}")

        gevent.joinall(
            [
                gevent.spawn(self._relay, self.up_label, self.client_sock, self.proxy_sock),
                gevent.spawn(self._relay, self.down_label, self.proxy_sock, self.client_sock),
            ]
        )
        logging.info(f"Disconnected {self.up_label}")


class ProxyServer:
    _group: Group

    def __init__(self):
        self._group = Group()

    def _accept(self, client_sock: socket.socket, client_addr: tuple[str, int]):
        try:
            self._group.spawn(Session(client_sock, client_addr).serve).join()
        except Exception as e:
            logging.error(f"Failed to establish session for {client_addr}: {e}")
            try:
                client_sock.shutdown(socket.SHUT_RDWR)
                client_sock.close()
            except Exception:
                pass

    def _start(self, family: socket.AddressFamily):
        sock = socket.socket(family, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind(("", PORT))
        sock.listen(socket.SOMAXCONN)
        addr = sock.getsockname()
        logging.info(f"Listening on [{addr[0]}]:{addr[1]}")
        self._group.spawn(StreamServer(sock, self._accept).serve_forever).join()

    def run(self):
        self._group.map(self._start, FAMILY)
        self._group.join()


def entry_worker(*_):
    gevent.spawn(ProxyServer().run).join()


if __name__ == "__main__":
    logging.basicConfig(level=LOG_LEVEL)
    w = multiprocessing.cpu_count()
    with multiprocessing.Pool(w) as p:
        p.map(entry_worker, range(w))
