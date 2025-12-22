from gevent import socket, monkey
from gevent.server import StreamServer

monkey.patch_all()

import logging


class Server:
    def __init__(self, port: int, handler) -> None:
        self._logger = logging.getLogger(f"{self.__class__.__name__}")
        self._port = port

        sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        sock.bind(("", port))
        sock.listen(socket.SOMAXCONN)
        self._server = StreamServer(sock, handler)

    def serve_forever(self) -> None:
        self._logger.info(f"Listening on port {self._port}")
        self._server.serve_forever()
