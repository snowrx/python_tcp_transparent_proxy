import gevent
from gevent import socket, monkey

from core import server

monkey.patch_all()

import logging
import os
import gc

from core.server import Server
from core.session import Session
from core.buffer_pool import BufferPool
from core.util import get_original_dst, ipv4_mapped

LOG_LEVEL = logging.INFO
LOG_FORMAT = "%(name)-30s | %(levelname)-10s | %(message)s"

PORT = 8081
IDLE_TIMEOUT = 7200

BUFFER_SIZE = 1 << 21
POOL_SIZE = 100


def main():
    buffer_pool = BufferPool(BUFFER_SIZE, POOL_SIZE)

    def handler(client_sock: socket.socket, client_addr: tuple[str, int]) -> None:
        with client_sock:
            try:
                family = socket.AF_INET if ipv4_mapped(client_addr[0]) else socket.AF_INET6
                remote_addr = get_original_dst(client_sock, family)
                srv_name = client_sock.getsockname()
                if remote_addr[0] == srv_name[0] and remote_addr[1] == srv_name[1]:
                    return

                with buffer_pool.buffer() as buffer:
                    session = Session(client_sock, client_addr, remote_addr, family, buffer, IDLE_TIMEOUT)
                    session.run()

            except Exception as e:
                logging.error(f"Session failed: {e}")

    server = Server(PORT, handler)
    server.serve_forever()


if __name__ == "__main__":
    if os.getenv("DEBUG"):
        LOG_LEVEL = logging.DEBUG
        gc.set_debug(gc.DEBUG_STATS)
    logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
    gc.set_threshold(10000)

    main()
