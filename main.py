import logging
import os

from gevent import socket

from core.buffer_pool import BufferPool
from core.server import Server
from core.session import Session
from core.util import get_original_dst, ipv4_mapped

LOG_LEVEL = logging.INFO
LOG_FORMAT = "%(name)-30s | %(levelname)-10s | %(message)s"

PORT = 8081

BUFFER_SIZE = 1 << 20
POOL_SIZE = 100


def main() -> None:
    buffer_pool = BufferPool(BUFFER_SIZE, POOL_SIZE)

    def handler(client_sock: socket.socket, client_addr: tuple[str, int]) -> None:
        with client_sock:
            family = socket.AF_INET if ipv4_mapped(client_addr[0]) else socket.AF_INET6
            remote_addr = get_original_dst(client_sock, family)
            sockname = client_sock.getsockname()
            if remote_addr == sockname:
                return

            with buffer_pool.borrow() as buffer:
                Session(client_sock, client_addr, remote_addr, family, buffer).run()

    server = Server(PORT, handler)
    server.serve_forever()


if __name__ == "__main__":
    if os.getenv("DEBUG"):
        LOG_LEVEL = logging.DEBUG
    logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)

    main()
