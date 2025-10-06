import asyncio
import socket
import struct
import logging
from sys import maxsize

import uvloop

LOG_LEVEL = logging.DEBUG
PORT = 8081
IDLE_TIMEOUT = 3600


class Server:
    _SO_ORIGINAL_DST = 80
    _SOL_IPV6 = 41
    _V4_LEN = 16
    _V4_FMT = "!2xH4s"
    _V6_LEN = 28
    _V6_FMT = "!2xH4x16s"

    def _get_original_dst(self, writer: asyncio.StreamWriter):
        ip: str = ""
        port: int = 0
        sock: socket.socket = writer.get_extra_info("socket")
        match sock.family:
            case socket.AF_INET:
                dst = sock.getsockopt(socket.SOL_IP, self._SO_ORIGINAL_DST, self._V4_LEN)
                port, raw_ip = struct.unpack_from(self._V4_FMT, dst)
                ip = socket.inet_ntop(socket.AF_INET, raw_ip)
            case socket.AF_INET6:
                dst = sock.getsockopt(self._SOL_IPV6, self._SO_ORIGINAL_DST, self._V6_LEN)
                port, raw_ip = struct.unpack_from(self._V6_FMT, dst)
                ip = socket.inet_ntop(socket.AF_INET6, raw_ip)
            case _:
                raise Exception(f"Unknown socket family: {sock.family}")
        return ip, port

    async def _stream(self, flow: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            sock: socket.socket = writer.get_extra_info("socket")
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            sock.setsockopt(socket.SOL_TCP, socket.TCP_QUICKACK, 1)
            while data := await asyncio.wait_for(reader.read(maxsize), IDLE_TIMEOUT):
                await writer.drain()
                writer.write(data)
            writer.write_eof()
            await writer.drain()
        except Exception as e:
            logging.debug(f"{type(e).__name__} {flow} {e}")
        finally:
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()

    async def _accept(self, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter):
        try:
            peername = client_writer.get_extra_info("peername")
            dstname = self._get_original_dst(client_writer)
            up_flow = f"[{peername[0]}]:{peername[1]} -> [{dstname[0]}]:{dstname[1]}"
            down_flow = f"[{peername[0]}]:{peername[1]} <- [{dstname[0]}]:{dstname[1]}"
        except Exception as e:
            logging.error(f"Failed to get original destination: {e}")
            client_writer.transport.abort()
            client_writer.close()
            await client_writer.wait_closed()
            return

        try:
            proxy_reader, proxy_writer = await asyncio.open_connection(*dstname)
        except Exception as e:
            logging.error(f"Failed to connect {up_flow}: {e}")
            client_writer.transport.abort()
            client_writer.close()
            await client_writer.wait_closed()
            return

        logging.info(f"Established {up_flow}")
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self._stream(up_flow, client_reader, proxy_writer))
                tg.create_task(self._stream(down_flow, proxy_reader, client_writer))
        except Exception as e:
            logging.error(f"{type(e).__name__} {up_flow} {e}")
        logging.info(f"Closed {up_flow}")

    async def run(self):
        self._server = await asyncio.start_server(self._accept, port=PORT)
        logging.info(f"Listening on {PORT}")
        async with self._server:
            await self._server.serve_forever()


def main():
    try:
        uvloop.run(Server().run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    logging.basicConfig(level=LOG_LEVEL)
    main()
    logging.shutdown()
