import logging
import os
import multiprocessing

from gevent import socket
from gevent.server import StreamServer

from core.session import Session
from core.util import get_original_dst, ipv4_mapped

LOG_LEVEL = logging.INFO
LOG_FORMAT = "%(name)-30s | %(levelname)-10s | %(message)s"

PORT = 8081
BUFFER_SIZE = 1 << 20
TIMEOUT = 7200


def worker_main(fd: int) -> None:
    def handler(client_sock: socket.socket, client_addr: tuple[str, int]) -> None:
        with client_sock:
            family = socket.AF_INET if ipv4_mapped(client_addr[0]) else socket.AF_INET6
            remote_addr = get_original_dst(client_sock, family)
            sockname = client_sock.getsockname()
            if remote_addr == sockname:
                return

            Session(client_sock, client_addr, remote_addr, family, BUFFER_SIZE, TIMEOUT).run()

    listener = socket.fromfd(fd, socket.AF_INET6, socket.SOCK_STREAM)
    server = StreamServer(listener, handler)
    server.serve_forever()


def main() -> None:
    with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        listener.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        listener.bind(("", PORT))
        listener.listen(socket.SOMAXCONN)

        process = multiprocessing.Process(target=worker_main, args=(listener.fileno(),), daemon=True)
        try:
            process.start()
            worker_main(listener.fileno())
        except KeyboardInterrupt:
            print("Stopping...")
        finally:
            if process.is_alive():
                process.terminate()
                process.join()


if __name__ == "__main__":
    if os.getenv("DEBUG"):
        LOG_LEVEL = logging.DEBUG
    logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)

    main()
