import logging
import mmap
import struct

import gevent
from gevent import socket
from gevent.server import StreamServer


LOG = logging.DEBUG
PORT = 8081
LIMIT = 1 << 20


class util:
    _SO_ORIGINAL_DST = 80
    _SOL_IPV6 = 41
    _V4_LEN = 16
    _V6_LEN = 28

    @staticmethod
    def get_original_dst(so: socket.socket, v4: bool = True):
        ip: str
        port: int
        if v4:
            dst = so.getsockopt(socket.SOL_IP, util._SO_ORIGINAL_DST, util._V4_LEN)
            port, raw_ip = struct.unpack_from("!2xH4s", dst)
            ip = socket.inet_ntop(socket.AF_INET, raw_ip)
        else:
            dst = so.getsockopt(util._SOL_IPV6, util._SO_ORIGINAL_DST, util._V6_LEN)
            port, raw_ip = struct.unpack_from("!2xH4x16s", dst)
            ip = socket.inet_ntop(socket.AF_INET6, raw_ip)
        return ip, port


def forward(src: socket.socket, dst: socket.socket):
    try:
        dst.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        dst.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        with mmap.mmap(src.fileno(), LIMIT, mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS) as buf:
            buf.madvise(mmap.MADV_HUGEPAGE | mmap.MADV_SEQUENTIAL)
            while l := src.recv_into(buf, LIMIT):
                dst.sendall(buf[:l])
                buf.seek(0)
    except Exception as e:
        logging.error(f"Error in forward: {e}")
    finally:
        dst.close()


def handle(sock: socket.socket, addr: tuple):
    v4 = "." in addr[0]
    dst = util.get_original_dst(sock, v4)
    try:
        peer = socket.create_connection(dst)
    except Exception as e:
        logging.error(f"Failed to connect to [{dst[0]}]:{dst[1]}: {e}")
        sock.close()
        return

    logging.info(f"Established connection [{addr[0]}]:{addr[1]} <=> [{dst[0]}]:{dst[1]}")
    _ = gevent.joinall([gevent.spawn(forward, sock, peer), gevent.spawn(forward, peer, sock)])
    logging.info(f"Connection closed [{addr[0]}]:{addr[1]} <=> [{dst[0]}]:{dst[1]}")


def main():
    logging.basicConfig(level=LOG)
    server = StreamServer(("", PORT), handle)
    logging.info(f"Starting transparent proxy on port {PORT}")
    server.serve_forever()


if __name__ == "__main__":
    main()
