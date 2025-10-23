import asyncio
import socket
import struct
import logging
import gc

import uvloop

LOG_LEVEL = logging.DEBUG
PORT = 8081
CONN_LIFE = 86400
CHUNK_SIZE = 1 << 14


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
            mss = sock.getsockopt(socket.SOL_TCP, socket.TCP_MAXSEG)
            writer.transport.set_write_buffer_limits(mss, mss)
            async with asyncio.timeout(CONN_LIFE):
                while v := memoryview(await reader.read(CHUNK_SIZE)):
                    await writer.drain()
                    writer.write(v)
        except Exception as e:
            logging.error(f"{type(e).__name__} {flow} {e}")
        finally:
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
            logging.debug(f"Closed flow {flow}")

    async def _accept(self, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter):
        try:
            peername = client_writer.get_extra_info("peername")
            dstname = self._get_original_dst(client_writer)
            up_flow = f"[{peername[0]}]:{peername[1]} -> [{dstname[0]}]:{dstname[1]}"
            down_flow = f"[{peername[0]}]:{peername[1]} <- [{dstname[0]}]:{dstname[1]}"
        except Exception as e:
            logging.error(f"Failed to get original destination: {type(e).__name__} {e}")
            client_writer.transport.abort()
            return

        try:
            proxy_reader, proxy_writer = await asyncio.open_connection(*dstname)
        except Exception as e:
            logging.error(f"Failed to connect {up_flow}: {type(e).__name__} {e}")
            client_writer.transport.abort()
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
        server = await asyncio.start_server(self._accept, port=PORT)
        logging.info(f"Listening on port {PORT}")
        async with server:
            await server.serve_forever()


async def main():
    logging.basicConfig(level=LOG_LEVEL)
    await Server().run()
    logging.info("Server stopped")


if __name__ == "__main__":
    gc.set_threshold(3000)
    gc.set_debug(gc.DEBUG_STATS)
    uvloop.run(main())
