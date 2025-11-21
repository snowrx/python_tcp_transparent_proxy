import logging
import struct
import os
import time
import gevent
from gevent import socket
from gevent.socket import wait_read, wait_write
from gevent.server import StreamServer
from gevent.pool import Group
from gevent.threadpool import ThreadPool

LOG_LEVEL = logging.INFO
PORT = 8081
IDLE_TIMEOUT = 43200
FAST_TIMEOUT = 1 / 1000
BUFFER_SIZE = 1 << 16
BUFFER_SCALE = 2
TFO_MSS = 1220


class ProxyServer:
    _SO_ORIGINAL_DST = 80
    _SOL_IPV6 = 41
    _V4_LEN = 16
    _V4_FMT = "!2xH4s"
    _V6_LEN = 28
    _V6_FMT = "!2xH4x16s"
    _FAMILY = [socket.AF_INET, socket.AF_INET6]

    def __init__(self):
        self._group = Group()

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
                raise Exception(f"Unknown socket family: {sock.family}")
        return ip, port

    def _relay(self, label: str, src: socket.socket, dst: socket.socket):
        logging.debug(f"Starting relay {label}")
        total_bytes = 0
        buffer_size = BUFFER_SIZE
        try:
            eof = False
            wait_read(src.fileno(), IDLE_TIMEOUT)
            while buffer := src.recv(buffer_size):
                dst.setsockopt(socket.SOL_TCP, socket.TCP_CORK, 1)
                while buffer:
                    wait_write(dst.fileno())
                    sent = dst.send(buffer)
                    buffer = bytes(memoryview(buffer)[sent:])
                    total_bytes += sent
                    if (s := sent * BUFFER_SCALE) > buffer_size:
                        buffer_size = s
                        logging.debug(f"Buffer extended to {buffer_size} bytes for {label}")
                    if not eof and len(buffer) < buffer_size:
                        try:
                            wait_read(src.fileno(), FAST_TIMEOUT)
                            if next_buffer := src.recv(buffer_size - len(buffer)):
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
        logging.debug(f"Closed relay {label} total {total_bytes} bytes")

    def _accept(self, client_sock: socket.socket, client_addr: tuple[str, int]):
        prepare_start = time.perf_counter()
        try:
            client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            client_sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
            client_sock.setsockopt(socket.SOL_TCP, socket.TCP_QUICKACK, 1)
            srv_addr = client_sock.getsockname()
            dst_addr = self._get_orig_dst(client_sock)
            if dst_addr[0] == srv_addr[0] and dst_addr[1] == srv_addr[1]:
                client_sock.shutdown(socket.SHUT_RDWR)
                client_sock.close()
                logging.error(f"Attempt to connect to the proxy server itself from [{client_addr[0]}]:{client_addr[1]}")
                return
            up_label = f"[{client_addr[0]}]:{client_addr[1]} -> [{dst_addr[0]}]:{dst_addr[1]}"
            down_label = f"[{client_addr[0]}]:{client_addr[1]} <- [{dst_addr[0]}]:{dst_addr[1]}"
        except Exception as e:
            client_sock.shutdown(socket.SHUT_RDWR)
            client_sock.close()
            logging.error(f"Failed to get original destination for client [{client_addr[0]}]:{client_addr[1]}: {e}")
            return

        try:
            proxy_sock = socket.socket(client_sock.family, client_sock.type)
            proxy_sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            proxy_sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
            proxy_sock.setsockopt(socket.SOL_TCP, socket.TCP_QUICKACK, 1)
            proxy_sock.setsockopt(socket.SOL_TCP, socket.TCP_FASTOPEN_CONNECT, 1)
            proxy_sock.connect(dst_addr)
            buffer = bytes()
            sent = 0
            try:
                wait_read(client_sock.fileno(), FAST_TIMEOUT)
                if buffer := client_sock.recv(TFO_MSS):
                    wait_write(proxy_sock.fileno())
                    sent = proxy_sock.sendto(buffer, socket.MSG_FASTOPEN, dst_addr)
                    buffer = bytes(memoryview(buffer)[sent:])
                else:
                    raise Exception("Client disconnected before sending any data")
            except (BlockingIOError, TimeoutError) as e:
                logging.debug(f"Failed to TFO {up_label}: {e}")
            if buffer:
                wait_write(proxy_sock.fileno())
                proxy_sock.sendall(buffer)
                logging.debug(f"Sent remaining {len(buffer)} bytes to {up_label}")
            prepare_time = time.perf_counter() - prepare_start
            if sent:
                logging.info(f"Connected {up_label} with TFO {sent} bytes ({prepare_time * 1000:.2f}ms)")
            else:
                logging.info(f"Connected {up_label} ({prepare_time * 1000:.2f}ms)")
        except Exception as e:
            client_sock.shutdown(socket.SHUT_RDWR)
            client_sock.close()
            logging.error(f"Failed to connect {up_label}: {e}")
            return

        try:
            j = [
                self._group.spawn(self._relay, up_label, client_sock, proxy_sock),
                self._group.spawn(self._relay, down_label, proxy_sock, client_sock),
            ]
            gevent.joinall(j)
        except Exception as e:
            logging.error(f"Failed to proxy {up_label}: {e}")
        finally:
            try:
                proxy_sock.shutdown(socket.SHUT_RDWR)
                proxy_sock.close()
            except Exception:
                pass
            try:
                client_sock.shutdown(socket.SHUT_RDWR)
                client_sock.close()
            except Exception:
                pass
            logging.info(f"Disconnected {up_label}")

    def _listen(self, family: socket.AddressFamily = socket.AF_INET):
        sock = socket.socket(family, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind(("", PORT))
        sock.listen(socket.SOMAXCONN)
        addr = sock.getsockname()
        logging.info(f"Listening on [{addr[0]}]:{addr[1]}")
        server = StreamServer(sock, self._accept)
        self._group.spawn(server.serve_forever).join()

    def run(self, *_):
        try:
            self._group.map(self._listen, self._FAMILY)
            self._group.join()
        except Exception as e:
            logging.error(f"Failed to start proxy server: {e}")
            self._group.kill()


if __name__ == "__main__":
    if os.getenv("DEBUG"):
        LOG_LEVEL = logging.DEBUG
    logging.basicConfig(level=LOG_LEVEL)
    c = os.cpu_count() or 1
    thread_pool = ThreadPool(c)
    try:
        thread_pool.map(ProxyServer().run, range(c))
        thread_pool.join()
    except KeyboardInterrupt:
        thread_pool.kill()
