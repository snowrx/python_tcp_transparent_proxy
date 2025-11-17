import logging
import struct
import gc

import gevent
from gevent import socket
from gevent.socket import wait_read, wait_write
from gevent.server import StreamServer

LOG_LEVEL = logging.INFO
PORT = 8081
BUFFER_SIZE = 1 << 16
BUFFER_SCALE = 2
IDLE_TIMEOUT = 43200
FAST_TIMEOUT = 1e-3

SO_ORIGINAL_DST = 80
SOL_IPV6 = 41
V4_LEN = 16
V4_FMT = "!2xH4s"
V6_LEN = 28
V6_FMT = "!2xH4x16s"
FAMILY = [socket.AF_INET, socket.AF_INET6]


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
    eof = False
    buffer = bytearray()
    buffer_size = BUFFER_SIZE
    max_sent = 0

    try:
        wait_read(src_sock.fileno(), IDLE_TIMEOUT)
        while recv := src_sock.recv(buffer_size):
            buffer.extend(recv)

            while buffer:
                wait_write(dst_sock.fileno())
                sent = dst_sock.send(buffer)
                del buffer[:sent]

                if sent > max_sent:
                    max_sent = sent
                    if (s := max_sent * BUFFER_SCALE) > buffer_size:
                        buffer_size = s
                        logging.debug(f"{buffer_size=} {label}")

                if not eof and len(buffer) < buffer_size:
                    try:
                        wait_read(src_sock.fileno(), FAST_TIMEOUT)
                        if recv := src_sock.recv(buffer_size - len(buffer)):
                            buffer.extend(recv)
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
        logging.debug(f"Failed to transfer {label}: {e}")
    except Exception as e:
        logging.error(f"Failed to transfer {label}: {e}")
    finally:
        try:
            dst_sock.shutdown(socket.SHUT_WR)
        except:
            pass

    logging.debug(f"End transfer {label}")


def accept(client_sock: socket.socket, client_addr: tuple[str, int]):
    logging.debug(f"New connection from [{client_addr[0]}]:{client_addr[1]}")

    try:
        client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
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
        logging.error(f"Failed to prepare connection [{client_addr[0]}]:{client_addr[1]}: {e}")
        return

    try:
        proxy_sock = socket.socket(client_sock.family, client_sock.type)
        proxy_sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        proxy_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        proxy_sock.setsockopt(socket.SOL_TCP, socket.TCP_FASTOPEN_CONNECT, 1)
        proxy_sock.bind(("", client_addr[1]))
        connected = False
        buffer = bytearray()

        try:
            wait_read(client_sock.fileno(), FAST_TIMEOUT)
            if recv := client_sock.recv(client_sock.getsockopt(socket.SOL_TCP, socket.TCP_MAXSEG)):
                buffer.extend(recv)
                wait_write(proxy_sock.fileno())
                sent = proxy_sock.sendto(buffer, socket.MSG_FASTOPEN, dst_addr)
                del buffer[:sent]
                connected = True
                logging.info(f"Connected {up_label} with TFO {sent} bytes")
            else:
                raise EOFError("Client closed before sending data")
        except (BlockingIOError, TimeoutError, OSError) as e:
            logging.debug(f"Failed to TFO {up_label}: {e}")

        if not connected:
            proxy_sock.connect(dst_addr)
            connected = True
            logging.info(f"Connected {up_label}")

        if buffer:
            wait_write(proxy_sock.fileno())
            proxy_sock.sendall(buffer)
            logging.debug(f"Unsent {len(buffer)} bytes was sent to {up_label}")
    except Exception as e:
        client_sock.shutdown(socket.SHUT_RDWR)
        client_sock.close()
        logging.error(f"Failed to connect {up_label}: {e}")
        return

    try:
        gevent.joinall(
            [
                gevent.spawn(transfer, down_label, proxy_sock, client_sock),
                gevent.spawn(transfer, up_label, client_sock, proxy_sock),
            ]
        )
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
        logging.critical(f"Server error: {e}")


if __name__ == "__main__":
    gc.collect()
    gc.set_threshold(3000)
    gc.set_debug(gc.DEBUG_STATS)
    logging.basicConfig(level=LOG_LEVEL)

    try:
        run()
    except KeyboardInterrupt:
        pass
