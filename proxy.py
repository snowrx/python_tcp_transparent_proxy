import logging
import struct

import gevent
from gevent import socket
from gevent.socket import wait_read, wait_write
from gevent.server import StreamServer
from gevent.threadpool import ThreadPool

# + config
LOG_LEVEL = logging.INFO
PORT = 8081
POOL_SIZE = 4
BUFFER_SIZE = 1 << 20
IDLE_TIMEOUT = 7200
FAST_TIMEOUT = 1e-3
# - config

# + constant
SO_ORIGINAL_DST = 80
SOL_IPV6 = 41
V4_LEN = 16
V4_FMT = "!2xH4s"
V6_LEN = 28
V6_FMT = "!2xH4x16s"
FAMILY = [socket.AF_INET, socket.AF_INET6]
# - constant


def get_original_dst(sock: socket.socket):
    ip: str = ""
    port: int = 0
    match sock.family:
        case socket.AF_INET:
            dst = sock.getsockopt(socket.SOL_IP, SO_ORIGINAL_DST, V4_LEN)
            port, raw_ip = struct.unpack_from(V4_FMT, dst)
            ip = socket.inet_ntop(socket.AF_INET, raw_ip)
        case socket.AF_INET6:
            dst = sock.getsockopt(SOL_IPV6, SO_ORIGINAL_DST, V6_LEN)
            port, raw_ip = struct.unpack_from(V6_FMT, dst)
            ip = socket.inet_ntop(socket.AF_INET6, raw_ip)
        case _:
            raise Exception(f"Unknown socket family: {sock.family}")
    return ip, port


def transfer(label: str, src_sock: socket.socket, dst_sock: socket.socket):
    logging.debug(f"Start transfer {label}")

    try:
        dst_sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        dst_sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        dst_sock.setsockopt(socket.SOL_TCP, socket.TCP_QUICKACK, 1)
        eof = False
        buffer = bytearray()
        wait_read(src_sock.fileno(), IDLE_TIMEOUT)
        while data := src_sock.recv(BUFFER_SIZE):
            buffer.extend(data)
            while buffer:
                wait_write(dst_sock.fileno())
                sent = dst_sock.send(buffer)
                del buffer[:sent]
                if not eof and len(buffer) < BUFFER_SIZE:
                    try:
                        wait_read(src_sock.fileno(), FAST_TIMEOUT)
                        if data := src_sock.recv(BUFFER_SIZE - len(buffer)):
                            buffer.extend(data)
                        else:
                            eof = True
                    except (BlockingIOError, TimeoutError):
                        pass
            if eof:
                break
            wait_read(src_sock.fileno(), IDLE_TIMEOUT)
    except Exception as e:
        logging.error(f"Failed to transfer {label}: {type(e).__name__}")
        logging.debug(e)
    finally:
        try:
            dst_sock.shutdown(socket.SHUT_RDWR)
            dst_sock.close()
        except:
            pass


def accept(client_sock: socket.socket, client_addr: tuple[str, int]):
    logging.debug(f"New connection from [{client_addr[0]}]:{client_addr[1]}")

    # + prepare
    try:
        srv_addr = client_sock.getsockname()
        dst_addr = get_original_dst(client_sock)
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
        logging.error(f"Failed to get original destination for [{client_addr[0]}]:{client_addr[1]}: {type(e).__name__}")
        logging.debug(e)
        return
    # - prepare

    # + open
    try:
        connected = False
        proxy_sock = socket.socket(client_sock.family, client_sock.type)
        proxy_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        proxy_sock.bind(("", client_addr[1]))
        data = bytes()
        try:
            proxy_sock.setsockopt(socket.SOL_TCP, socket.TCP_FASTOPEN_CONNECT, 1)
            wait_write(proxy_sock.fileno(), FAST_TIMEOUT)
            wait_read(client_sock.fileno(), FAST_TIMEOUT)
            if data := client_sock.recv(BUFFER_SIZE):
                size = len(data)
                while data:
                    wait_write(proxy_sock.fileno())
                    sent = proxy_sock.sendto(data, socket.MSG_FASTOPEN, dst_addr)
                    data = bytes(memoryview(data)[sent:])
                connected = True
                logging.info(f"Connected {up_label} with TFO {size} bytes")
        except (BlockingIOError, TimeoutError) as e:
            logging.debug(f"Failed to send TFO {up_label}: {type(e).__name__}")
        if not connected:
            proxy_sock.connect(dst_addr)
            connected = True
            logging.info(f"Connected {up_label}")
            if data:
                wait_write(proxy_sock.fileno())
                proxy_sock.sendall(data)
                logging.debug(f"Unsent {len(data)} bytes was sent to {up_label}")
    except Exception as e:
        client_sock.shutdown(socket.SHUT_RDWR)
        client_sock.close()
        logging.error(f"Failed to connect {up_label}: {type(e).__name__}")
        logging.debug(e)
        return
    # - open

    # + proxy
    try:
        gevent.joinall(
            [
                gevent.spawn(transfer, up_label, client_sock, proxy_sock),
                gevent.spawn(transfer, down_label, proxy_sock, client_sock),
            ]
        )
    except Exception as e:
        logging.error(f"Failed to proxy {up_label}: {type(e).__name__}")
        logging.debug(e)
    # - proxy

    # + close
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
    # - close

    logging.info(f"Closed {up_label}")


def run(*_):
    try:
        socks = [socket.socket(family, socket.SOCK_STREAM) for family in FAMILY]
        for sock in socks:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            sock.bind(("", PORT))
            sock.listen(socket.SOMAXCONN)
            addr = sock.getsockname()
            logging.info(f"Listening on [{addr[0]}]:{addr[1]}")
        gevent.joinall([gevent.spawn(StreamServer(sock, accept).serve_forever) for sock in socks])
    except Exception as e:
        logging.critical(f"Failed to start server: {type(e).__name__}")
        logging.debug(e)


if __name__ == "__main__":
    logging.basicConfig(level=LOG_LEVEL)
    try:
        pool = ThreadPool(POOL_SIZE)
        pool.map(run, range(POOL_SIZE))
    except KeyboardInterrupt:
        pass
