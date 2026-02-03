import logging
import os
from concurrent.futures import ProcessPoolExecutor

from gevent import socket

from core.buffer import BufferAllocator
from core.server import Server
from core.session import Session
from core.util import get_original_dst, ipv4_mapped

LOG_LEVEL = logging.INFO
LOG_FORMAT = "%(name)-30s | %(levelname)-10s | %(message)s"

PORT = 8081
BUFFER_SIZE = 1 << 22
MAX_CONNECTIONS = 1 << 8


def main() -> None:
    buffer_allocator = BufferAllocator(BUFFER_SIZE, MAX_CONNECTIONS)

    def handler(client_sock: socket.socket, client_addr: tuple[str, int]) -> None:
        with client_sock:
            family = socket.AF_INET if ipv4_mapped(client_addr[0]) else socket.AF_INET6
            remote_addr = get_original_dst(client_sock, family)
            sockname = client_sock.getsockname()
            if remote_addr == sockname:
                return

            try:
                with buffer_allocator.managed() as buffer:
                    Session(client_sock, client_addr, remote_addr, family, buffer).run()
            except Exception as e:
                logging.error(f"Error in session: {e}")

    server = Server(PORT, handler)
    server.serve_forever()


if __name__ == "__main__":
    if os.getenv("DEBUG"):
        LOG_LEVEL = logging.DEBUG
    logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)

    cpu_count = os.cpu_count() or 1
    if sub_count := cpu_count - 1:
        with ProcessPoolExecutor(sub_count) as executor:
            for _ in range(sub_count):
                executor.submit(main)
            main()
    else:
        main()
