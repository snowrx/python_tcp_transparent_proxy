import logging
import struct

import gevent
from gevent import socket
from gevent.server import StreamServer
from gevent.socket import wait_read, wait_write

LOG_LEVEL = logging.DEBUG
PORT = 8081
CONN_LIFE = 86400
LIMIT = 1 << 27
TFO = True


class Server:
    _SO_ORIGINAL_DST = 80
    _SOL_IPV6 = 41
    _V4_LEN = 16
    _V4_FMT = "!2xH4s"
    _V6_LEN = 28
    _V6_FMT = "!2xH4x16s"

    def _get_original_dst(self, sock: socket.socket):
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

    def _forward(self, label: str, src_sock: socket.socket, dst_sock: socket.socket):
        try:
            dst_sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            dst_sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
            dst_sock.setsockopt(socket.SOL_TCP, socket.TCP_QUICKACK, 1)
            wait_read(src_sock.fileno())
            while data := src_sock.recv(LIMIT):
                while data:
                    wait_write(dst_sock.fileno())
                    sent = dst_sock.send(data)
                    data = bytes(memoryview(data)[sent:])
                wait_read(src_sock.fileno())
        except Exception as e:
            logging.error(f"Failed to forward {label}: {e}")
        finally:
            if dst_sock.fileno() != -1:
                try:
                    dst_sock.close()
                except:
                    pass

    def _accept(self, client_sock: socket.socket, client_addr: tuple[str, int]):
        logging.debug(f"Accepted from [{client_addr[0]}]:{client_addr[1]}")
        try:
            srv_addr = client_sock.getsockname()
            dst_addr = self._get_original_dst(client_sock)
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
            logging.error(f"Failed to get original destination for [{client_addr[0]}]:{client_addr[1]}: {e}")
            return

        try:
            proxy_sock = socket.socket(client_sock.family, client_sock.type)
            proxy_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            proxy_sock.bind(("", client_addr[1]))
            if TFO:
                data = bytes()
                try:
                    proxy_sock.setsockopt(socket.SOL_TCP, socket.TCP_FASTOPEN_CONNECT, 1)
                    proxy_sock.setsockopt(socket.SOL_TCP, socket.TCP_FASTOPEN_NO_COOKIE, 1)
                    wait_write(proxy_sock.fileno())
                    wait_read(client_sock.fileno(), timeout=0.01)
                    data = client_sock.recv(LIMIT)
                    tfo_size = len(data)
                    while data:
                        wait_write(proxy_sock.fileno())
                        sent = proxy_sock.sendto(data, socket.MSG_FASTOPEN, dst_addr)
                        data = bytes(memoryview(data)[sent:])
                    logging.debug(f"Sent {tfo_size} bytes with TFO {up_label}")
                except Exception as e:
                    logging.debug(f"Failed to send with TFO {up_label}: {e}")
                finally:
                    if data:
                        wait_write(proxy_sock.fileno())
                        proxy_sock.sendall(data)
                        logging.debug(f"Retried to send {len(data)} bytes without TFO {up_label}")
            proxy_sock.connect(dst_addr)
        except Exception as e:
            client_sock.shutdown(socket.SHUT_RDWR)
            client_sock.close()
            logging.error(f"Failed to connect {up_label}: {e}")
            return

        logging.info(f"Connected {up_label}")
        try:
            j = [
                gevent.spawn(self._forward, up_label, client_sock, proxy_sock),
                gevent.spawn(self._forward, down_label, proxy_sock, client_sock),
            ]
            gevent.joinall(j, timeout=CONN_LIFE)
        except Exception as e:
            logging.error(f"Failed to forward {up_label}: {e}")
        finally:
            if proxy_sock.fileno() != -1:
                try:
                    proxy_sock.shutdown(socket.SHUT_RDWR)
                    proxy_sock.close()
                except:
                    pass
            if client_sock.fileno() != -1:
                try:
                    client_sock.shutdown(socket.SHUT_RDWR)
                    client_sock.close()
                except:
                    pass
        logging.info(f"Disconnected {up_label}")

    def run(self):
        try:
            srv_socks = [
                socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                socket.socket(socket.AF_INET6, socket.SOCK_STREAM),
            ]
            for sock in srv_socks:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
                sock.setsockopt(socket.SOL_TCP, socket.TCP_FASTOPEN, socket.SOMAXCONN)
                sock.bind(("", PORT))
                sock.listen(socket.SOMAXCONN)
                addr = sock.getsockname()
                logging.info(f"Listening on [{addr[0]}]:{addr[1]}")
            j = [gevent.spawn(StreamServer(sock, self._accept).serve_forever) for sock in srv_socks]
            gevent.joinall(j)
        except Exception as e:
            logging.critical(f"Failed to start proxy: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=LOG_LEVEL)
    try:
        Server().run()
    except KeyboardInterrupt:
        pass
