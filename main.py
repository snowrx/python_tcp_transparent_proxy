import logging
import os
from concurrent.futures import ProcessPoolExecutor

from gevent import socket
from gevent.server import StreamServer

from core.buffer import BufferManager
from core.session import Session
from core.util import get_original_dst, ipv4_mapped

LOG_LEVEL = logging.INFO
LOG_FORMAT = "%(name)-30s | %(levelname)-10s | %(message)s"

PORT = 8081
TIMEOUT = 7200
BUFFER_SIZE = 1 << 20
POOL_SIZE = 100


def worker_main(fd: int) -> None:
    buffer_manager = BufferManager(BUFFER_SIZE, POOL_SIZE)

    def handler(client_sock: socket.socket, client_addr: tuple[str, int]) -> None:
        with client_sock:
            family = socket.AF_INET if ipv4_mapped(client_addr[0]) else socket.AF_INET6
            remote_addr = get_original_dst(client_sock, family)
            sockname = client_sock.getsockname()
            if remote_addr == sockname:
                return

            with buffer_manager.acquire() as buffer:
                Session(
                    client_sock, client_addr, remote_addr, family, buffer, TIMEOUT
                ).run()

    listener = socket.fromfd(fd, socket.AF_INET6, socket.SOCK_STREAM)
    server = StreamServer(listener, handler)
    server.serve_forever()


def main() -> None:
    with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        listener.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        listener.bind(("", PORT))
        listener.listen(socket.SOMAXCONN)

        fd = listener.fileno()
        cores = len(os.sched_getaffinity(0))
        if sub := cores - 1:
            with ProcessPoolExecutor(sub) as executor:
                for _ in range(sub):
                    executor.submit(worker_main, fd)
                worker_main(fd)
        else:
            worker_main(fd)


if __name__ == "__main__":
    if os.getenv("DEBUG"):
        LOG_LEVEL = logging.DEBUG
    logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)

    main()
