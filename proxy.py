import logging
import struct

import trio

LOG_LEVEL = logging.DEBUG
PORT = 8081
IDLE_TIMEOUT = 3600
CLOSE_WAIT = 60


class Utility:
    _SO_ORIGINAL_DST = 80
    _SOL_IPV6 = 41
    _V4_LEN = 16
    _V4_FMT = "!2xH4s"
    _V6_LEN = 28
    _V6_FMT = "!2xH4x16s"

    @staticmethod
    def get_original_dst(stream: trio.SocketStream, v4: bool):
        ip: str
        port: int
        if v4:
            dst = stream.getsockopt(trio.socket.SOL_IP, Utility._SO_ORIGINAL_DST, Utility._V4_LEN)
            port, raw_ip = struct.unpack_from(Utility._V4_FMT, dst)
            ip = trio.socket.inet_ntop(trio.socket.AF_INET, raw_ip)
        else:
            dst = stream.getsockopt(Utility._SOL_IPV6, Utility._SO_ORIGINAL_DST, Utility._V6_LEN)
            port, raw_ip = struct.unpack_from(Utility._V6_FMT, dst)
            ip = trio.socket.inet_ntop(trio.socket.AF_INET6, raw_ip)
        return ip, port


class Proxy:
    def __init__(self, client_stream: trio.SocketStream, proxy_stream: trio.SocketStream):
        self._client_stream = client_stream
        self._proxy_stream = proxy_stream
        c = client_stream.socket.getpeername()
        p = proxy_stream.socket.getpeername()
        self._up = f"[{c[0]}]:{c[1]} -> [{p[0]}]:{p[1]}"
        self._down = f"[{c[0]}]:{c[1]} <- [{p[0]}]:{p[1]}"

    async def _proxy(self, nursery: trio.Nursery, flow: str, src: trio.SocketStream, dst: trio.SocketStream):
        try:
            dst.setsockopt(trio.socket.SOL_SOCKET, trio.socket.SO_KEEPALIVE, 1)
            async for chunk in src:
                nursery.cancel_scope.relative_deadline = IDLE_TIMEOUT
                await dst.send_all(chunk)
        except (trio.ClosedResourceError, trio.BrokenResourceError) as e:
            logging.debug(f"{type(e).__name__} {flow}: {e}")
            nursery.cancel_scope.cancel(type(e).__name__)
        except Exception as e:
            logging.error(f"{type(e).__name__} {flow}: {e}")
            nursery.cancel_scope.cancel(type(e).__name__)
        finally:
            logging.debug(f"Closing {flow}")
            nursery.cancel_scope.relative_deadline = CLOSE_WAIT

    async def run(self):
        logging.info(f"Start proxy {self._up}")
        async with self._client_stream, self._proxy_stream:
            async with trio.open_nursery() as nursery:
                nursery.cancel_scope.relative_deadline = IDLE_TIMEOUT
                nursery.start_soon(self._proxy, nursery, self._up, self._client_stream, self._proxy_stream)
                nursery.start_soon(self._proxy, nursery, self._down, self._proxy_stream, self._client_stream)
        logging.info(f"End proxy {self._up}")


class Server:
    async def _accept_client(self, client_stream: trio.SocketStream):
        try:
            peername = client_stream.socket.getpeername()
            v4 = "." in peername[0]
            dstname = Utility.get_original_dst(client_stream, v4)
            proxy_stream = await trio.open_tcp_stream(*dstname)
            proxy = Proxy(client_stream, proxy_stream)
            await proxy.run()
        except Exception as e:
            logging.error(f"{type(e).__name__}: {e}")

    async def serve(self):
        logging.info(f"Listening on port {PORT}")
        try:
            await trio.serve_tcp(self._accept_client, PORT)
        except Exception as e:
            logging.error(f"{type(e).__name__}: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=LOG_LEVEL)
    trio.run(Server().serve)
