import gevent
from gevent import socket
from gevent.socket import wait_read, wait_write
from gevent.server import StreamServer
from gevent.pool import Group
from gevent.threadpool import ThreadPool

import os
import logging
import struct
import time

LOG_LEVEL = logging.INFO
PORT = 8081
BUFFER_SIZE = 1 << 20
IDLE_TIMEOUT = 43200
FAST_TIMEOUT = 1 / 1000
ENABLE_EXPERIMENTAL = False


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

    def get_original_dst(self, sock: socket.socket):
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

    def _transfer(self, label: str, src_sock: socket.socket, dst_sock: socket.socket):
        logging.debug(f"Start transfer {label}")
        eof = False

        try:
            wait_read(src_sock.fileno(), IDLE_TIMEOUT)
            while buffer := src_sock.recv(BUFFER_SIZE):
                if ENABLE_EXPERIMENTAL:
                    dst_sock.setsockopt(socket.SOL_TCP, socket.TCP_CORK, 1)
                    logging.debug(f"{label:100s} Set cork")

                while buffer:
                    write_start = time.perf_counter()
                    wait_write(dst_sock.fileno())
                    sent = dst_sock.send(buffer)
                    buffer = bytes(memoryview(buffer)[sent:])
                    logging.debug(f"{label:100s} Sent {sent:7d}, remaining {len(buffer):7d}")

                    if not eof and len(buffer) < BUFFER_SIZE:
                        try:
                            wait_read(src_sock.fileno(), FAST_TIMEOUT)
                            if deficit := src_sock.recv(BUFFER_SIZE - len(buffer)):
                                buffer += deficit
                            else:
                                eof = True
                        except (BlockingIOError, TimeoutError):
                            pass
                        except (ConnectionResetError, OSError):
                            eof = True
                    if (write_time := time.perf_counter() - write_start) >= 0.1:
                        logging.warning(f"Writing to {label} took {write_time * 1000:.2f} ms")

                if ENABLE_EXPERIMENTAL:
                    dst_sock.setsockopt(socket.SOL_TCP, socket.TCP_CORK, 0)
                    logging.debug(f"{label:100s} Unset cork")

                if eof:
                    break
                wait_read(src_sock.fileno(), IDLE_TIMEOUT)
        except (ConnectionResetError, OSError) as e:
            logging.debug(f"Failed to transfer {label}: {e}")
        except Exception as e:
            logging.error(f"Failed to transfer {label}: {e}")
        finally:
            try:
                dst_sock.shutdown(socket.SHUT_WR)
            except:
                pass

        logging.debug(f"End transfer {label}")

    def _accept(self, client_sock: socket.socket, client_addr: tuple[str, int]):
        logging.debug(f"New connection from [{client_addr[0]}]:{client_addr[1]}")
        prepare_start = time.perf_counter()

        try:
            client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            if ENABLE_EXPERIMENTAL:
                client_sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
                client_sock.setsockopt(socket.SOL_TCP, socket.TCP_QUICKACK, 1)
            srv_addr = client_sock.getsockname()
            dst_addr = self.get_original_dst(client_sock)

            if dst_addr[0] == srv_addr[0] and dst_addr[1] == srv_addr[1]:
                client_sock.shutdown(socket.SHUT_RDWR)
                client_sock.close()
                logging.error(f"Blocked direct connection from [{client_addr[0]}]:{client_addr[1]}")
                return

            up_label = f"[{client_addr[0]}]:{client_addr[1]} -> [{dst_addr[0]}]:{dst_addr[1]}"
            down_label = f"[{client_addr[0]}]:{client_addr[1]} <- [{dst_addr[0]}]:{dst_addr[1]}"
        except Exception as e:
            client_sock.shutdown(socket.SHUT_RDWR)
            client_sock.close()
            logging.error(f"Failed to prepare connection for [{client_addr[0]}]:{client_addr[1]}: {e}")
            return

        try:
            proxy_sock = socket.socket(client_sock.family, client_sock.type)
            proxy_sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            proxy_sock.setsockopt(socket.SOL_TCP, socket.TCP_FASTOPEN_CONNECT, 1)
            if ENABLE_EXPERIMENTAL:
                proxy_sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
                proxy_sock.setsockopt(socket.SOL_TCP, socket.TCP_QUICKACK, 1)
                proxy_sock.setsockopt(socket.SOL_TCP, socket.TCP_DEFER_ACCEPT, 1)

            connected = False
            buffer = bytes()
            try:
                wait_read(client_sock.fileno(), FAST_TIMEOUT)
                if buffer := client_sock.recv(BUFFER_SIZE):
                    sent = proxy_sock.sendto(buffer, socket.MSG_FASTOPEN, dst_addr)
                    buffer = bytes(memoryview(buffer)[sent:])
                    connected = True
                    logging.info(f"Connected {up_label} with TCP Fast Open (sent {sent} bytes)")
                else:
                    raise Exception("Client closed connection before sending any data")
            except (BlockingIOError, TimeoutError):
                pass

            if not connected:
                proxy_sock.connect(dst_addr)
                connected = True
                logging.info(f"Connected {up_label}")

            if buffer:
                wait_write(proxy_sock.fileno())
                proxy_sock.sendall(buffer)
                logging.debug(f"Sent remaining {len(buffer)} bytes for {up_label}")
        except Exception as e:
            client_sock.shutdown(socket.SHUT_RDWR)
            client_sock.close()
            logging.error(f"Failed to connect {up_label}: {e}")
            return

        prepare_time = time.perf_counter() - prepare_start
        logging.debug(f"Prepared {up_label} in {prepare_time * 1000:.2f} ms")

        try:
            t = [
                self._group.spawn(self._transfer, down_label, proxy_sock, client_sock),
                self._group.spawn(self._transfer, up_label, client_sock, proxy_sock),
            ]
            gevent.joinall(t)
        except Exception as e:
            logging.error(f"Failed to proxy {up_label}: {e}")

        try:
            proxy_sock.shutdown(socket.SHUT_RDWR)
            proxy_sock.close()
        except:
            pass

        try:
            client_sock.shutdown(socket.SHUT_RDWR)
            client_sock.close()
        except:
            pass

        logging.info(f"Closed {up_label}")

    def _listen(self, family: socket.AddressFamily = socket.AF_INET):
        try:
            sock = socket.socket(family, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            sock.bind(("", PORT))
            sock.listen(socket.SOMAXCONN)
            addr = sock.getsockname()
            logging.info(f"Listening on [{addr[0]}]:{addr[1]}")

            server = StreamServer(sock, self._accept)
            self._group.spawn(server.serve_forever).join()
        except Exception as e:
            logging.critical(f"Server error: {e}")

    def run(self):
        self._group.map(self._listen, self._FAMILY)
        self._group.join()


def run(*_):
    ProxyServer().run()


if __name__ == "__main__":
    if os.getenv("DEBUG"):
        LOG_LEVEL = logging.DEBUG
    logging.basicConfig(level=LOG_LEVEL)
    logging.critical(f"Log level: {logging._levelToName[LOG_LEVEL]}")

    if os.getenv("EXPERIMENTAL"):
        ENABLE_EXPERIMENTAL = True
        logging.info("Experimental features are enabled")

    try:
        cpu_count = os.cpu_count() or 1
        thread_pool = ThreadPool(cpu_count)
        thread_pool.map(run, range(cpu_count))
        thread_pool.join()
    except KeyboardInterrupt:
        pass
