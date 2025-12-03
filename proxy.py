import os
import struct
import multiprocessing
import logging

import gevent
from gevent import socket
from gevent.socket import wait_read, wait_write, timeout
from gevent.server import StreamServer
from gevent.pool import Group

from modules.circular import RingBuffer

LOG_LEVEL = logging.INFO

PORT = 8081
FAMILY = [socket.AF_INET, socket.AF_INET6]

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

            buffer: bytes = b""
            try:
                wait_read(self._client_sock.fileno(), TFO_TIMEOUT)
                if buffer := self._client_sock.recv(BUFFER_SIZE):
                    logging.debug(f"{self._up_label} Received initial {len(buffer)} bytes")
                else:
                    raise ConnectionAbortedError("Client closed connection before send data")
            except timeout:
                logging.debug(f"{self._up_label} Client did not send data before timeout")

            try:
                if sent := self._remote_sock.sendto(buffer, socket.MSG_FASTOPEN, self._remote_addr):
                    buffer = bytes(memoryview(buffer)[sent:])
                    logging.debug(f"{self._up_label} Sent initial {sent} bytes")
            except BlockingIOError:
                logging.debug(f"{self._up_label} Failed to TFO (no cookie/support)")

            wait_write(self._remote_sock.fileno(), IDLE_TIMEOUT)
            if buffer:
                self._remote_sock.sendall(buffer)
                logging.debug(f"{self._up_label} Sent remaining {len(buffer)} bytes")

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
        try:
            with RingBuffer(BUFFER_SIZE) as rb:
                eof = False
                while True:
                    if len(rb) < BUFFER_SIZE and (wbuf := rb.get_writable_buffer()):
                        wait_read(src.fileno(), IDLE_TIMEOUT)
                        if recv := src.recv_into(wbuf):
                            rb.advance_write(recv)
                        else:
                            eof = True

                    if len(rb):
                        dst.setsockopt(socket.SOL_TCP, socket.TCP_CORK, 1)
                        while len(rb):
                            if rbuf := rb.get_readable_buffer():
                                wait_write(dst.fileno(), IDLE_TIMEOUT)
                                if sent := dst.send(rbuf):
                                    rb.advance_read(sent)

                            if not eof and (wbuf := rb.get_writable_buffer()):
                                try:
                                    wait_read(src.fileno(), 0)
                                    if recv := src.recv_into(wbuf):
                                        rb.advance_write(recv)
                                    else:
                                        eof = True
                                except timeout:
                                    pass
                        dst.setsockopt(socket.SOL_TCP, socket.TCP_CORK, 0)

                    if eof and not len(rb):
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
                dst.shutdown(socket.SHUT_WR)
            except:
                pass
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
    logging.basicConfig(level=LOG_LEVEL)

    w = multiprocessing.cpu_count()
    with multiprocessing.Pool(w) as p:
        p.map(main, range(w))

    logging.shutdown()
