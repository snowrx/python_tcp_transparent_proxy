import gevent
from gevent import socket
from gevent import monkey

monkey.patch_all()

import logging
import os
from concurrent.futures import ProcessPoolExecutor

from core.buffer_pool import BufferPool
from core.server import Server
from core.session import Session


LOG_LEVEL = logging.INFO
PORT = 8081
NUM_WORKERS = 4
IDLE_TIMEOUT = 86400
BUFFER_SIZE = 1 << 21


def main():
    buffer_pool = BufferPool(BUFFER_SIZE)

    def on_accepted(client_sock: socket.socket, client_addr: tuple[str, int]):
        with buffer_pool.borrow() as buffer:
            session = Session(client_sock, client_addr, buffer, IDLE_TIMEOUT)
            session.serve()

    server = Server(PORT, on_accepted)
    gevent.spawn(server.serve_forever).join()


if __name__ == "__main__":
    if os.getenv("DEBUG"):
        LOG_LEVEL = logging.DEBUG
    logging.basicConfig(level=LOG_LEVEL, format="%(name)-25s | %(levelname)-10s | %(message)s")

    if NUM_WORKERS > 0:
        with ProcessPoolExecutor(NUM_WORKERS) as pool:
            for _ in range(NUM_WORKERS):
                pool.submit(main)
    else:
        main()
