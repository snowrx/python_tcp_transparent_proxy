import logging

from gevent import socket
from gevent.server import StreamServer

BACKLOG = 10


class Server:
    def __init__(self, port: int, handler) -> None:
        self._logger = logging.getLogger(f"{self.__class__.__name__}-{hex(id(self))}")
        self._port = port

        listener = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        listener.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        listener.bind(("", self._port))
        listener.listen(BACKLOG)

        self._server = StreamServer(listener, handler)

    def serve_forever(self) -> None:
        self._logger.info(f"Listening on port {self._port}")
        self._server.serve_forever()
