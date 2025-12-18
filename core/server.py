import logging

from gevent import socket
from gevent.server import StreamServer


class Server:
    def __init__(self, port: int, on_accepted):
        self._port = port
        self._logger = logging.getLogger(self.__class__.__name__)
        self._server = self._create_server(on_accepted)

    def serve_forever(self):
        self._logger.info(f"Listening on *:{self._port}")
        with self._server:
            self._server.serve_forever()

    def _create_server(self, on_accepted):
        sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        sock.bind(("", self._port))
        sock.listen(socket.SOMAXCONN)
        return StreamServer(sock, on_accepted)
