import os
import gevent
from gevent import socket
from gevent.socket import wait_read, wait_write
from gevent.server import StreamServer
from gevent.pool import Group
from gevent import monkey

monkey.patch_all()

import logging
import struct

LOG_LEVEL = logging.INFO
PORT = 8081
BUFFER_SIZE = 1 << 20
IDLE_TIMEOUT = 43200
FAST_TIMEOUT = 1e-3

SO_ORIGINAL_DST = 80
SOL_IPV6 = 41
V4_LEN = 16
V4_FMT = "!2xH4s"
V6_LEN = 28
V6_FMT = "!2xH4x16s"
FAMILY = [socket.AF_INET, socket.AF_INET6]

global_group = Group()


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
    global_group.spawn(logging.debug, f"Start transfer {label}")
    eof = False

    try:
        wait_read(src_sock.fileno(), IDLE_TIMEOUT)
        while buffer := src_sock.recv(BUFFER_SIZE):
            while buffer:
                wait_write(dst_sock.fileno())
                sent = dst_sock.send(buffer)
                buffer = bytes(memoryview(buffer)[sent:])

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
            if eof:
                break
            wait_read(src_sock.fileno(), IDLE_TIMEOUT)
    except (ConnectionResetError, OSError) as e:
        global_group.spawn(logging.debug, f"Failed to transfer {label}: {e}")
    except Exception as e:
        global_group.spawn(logging.error, f"Failed to transfer {label}: {e}")
    finally:
        try:
            dst_sock.shutdown(socket.SHUT_WR)
        except:
            pass

    global_group.spawn(logging.debug, f"End transfer {label}")


def accept(client_sock: socket.socket, client_addr: tuple[str, int]):
    global_group.spawn(logging.debug, f"New connection from [{client_addr[0]}]:{client_addr[1]}")

    try:
        client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        srv_addr = client_sock.getsockname()
        dst_addr = get_original_dst(client_sock)

        if dst_addr[0] == srv_addr[0] and dst_addr[1] == srv_addr[1]:
            client_sock.shutdown(socket.SHUT_RDWR)
            client_sock.close()
            global_group.spawn(logging.error, f"Blocked direct connection from [{client_addr[0]}]:{client_addr[1]}")
            return

        up_label = f"[{client_addr[0]}]:{client_addr[1]} -> [{dst_addr[0]}]:{dst_addr[1]}"
        down_label = f"[{client_addr[0]}]:{client_addr[1]} <- [{dst_addr[0]}]:{dst_addr[1]}"
    except Exception as e:
        client_sock.shutdown(socket.SHUT_RDWR)
        client_sock.close()
        global_group.spawn(logging.error, f"Failed to prepare connection for [{client_addr[0]}]:{client_addr[1]}: {e}")
        return

    try:
        proxy_sock = socket.socket(client_sock.family, client_sock.type)
        proxy_sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        proxy_sock.setsockopt(socket.SOL_TCP, socket.TCP_FASTOPEN_CONNECT, 1)

        connected = False
        buffer = bytes()
        try:
            wait_read(client_sock.fileno(), FAST_TIMEOUT)
            if buffer := client_sock.recv(BUFFER_SIZE):
                sent = proxy_sock.sendto(buffer, socket.MSG_FASTOPEN, dst_addr)
                buffer = bytes(memoryview(buffer)[sent:])
                connected = True
                global_group.spawn(logging.info, f"Connected {up_label} with TCP Fast Open (sent {sent} bytes)")
            else:
                raise Exception("Client closed connection before sending any data")
        except (BlockingIOError, TimeoutError):
            pass

        if not connected:
            proxy_sock.connect(dst_addr)
            connected = True
            global_group.spawn(logging.info, f"Connected {up_label}")

        if buffer:
            wait_write(proxy_sock.fileno())
            proxy_sock.sendall(buffer)
    except Exception as e:
        client_sock.shutdown(socket.SHUT_RDWR)
        client_sock.close()
        global_group.spawn(logging.error, f"Failed to connect {up_label}: {e}")
        return

    try:
        t = [
            global_group.spawn(transfer, down_label, proxy_sock, client_sock),
            global_group.spawn(transfer, up_label, client_sock, proxy_sock),
        ]
        gevent.joinall(t)
    except Exception as e:
        global_group.spawn(logging.error, f"Failed to proxy {up_label}: {e}")

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

    global_group.spawn(logging.info, f"Closed {up_label}")


def run(family: socket.AddressFamily = socket.AF_INET):
    try:
        sock = socket.socket(family, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind(("", PORT))
        sock.listen(socket.SOMAXCONN)
        addr = sock.getsockname()
        global_group.spawn(logging.info, f"Listening on [{addr[0]}]:{addr[1]}")

        server = StreamServer(sock, accept)
        global_group.spawn(server.serve_forever).join()
    except Exception as e:
        global_group.spawn(logging.critical, f"Server error: {e}")


if __name__ == "__main__":
    if os.getenv("DEBUG"):
        LOG_LEVEL = logging.DEBUG
    logging.basicConfig(level=LOG_LEVEL)
    try:
        global_group.map(run, FAMILY)
        global_group.join()
    except KeyboardInterrupt:
        pass
