import asyncio
from concurrent.futures import ProcessPoolExecutor
import os
import socket
import struct
import logging
import gc
import time

import uvloop

LOG_LEVEL = logging.DEBUG
PORT = 8081
CONN_LIFE = 86400
CHUNK_SIZE = 64000


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
            writer.transport.set_write_buffer_limits(CHUNK_SIZE, CHUNK_SIZE)
            async with asyncio.timeout(CONN_LIFE):
                while v := memoryview(await reader.read(CHUNK_SIZE)):
                    t = time.perf_counter()
                    if self._last_reader is reader:
                        await asyncio.sleep(0)
                        if self._last_reader is not reader:
                            logging.debug(f"Yielded {flow}")
                    self._last_reader = reader
                    await writer.drain()
                    writer.write(v)
                    if (latency := (time.perf_counter() - t) * 1000) >= 100:
                        logging.warning(f"High latency {flow}: {latency:.2f}ms")
        except Exception as e:
            logging.error(f"{type(e).__name__} {flow} {e}")
        finally:
            if not writer.is_closing():
                writer.write_eof()
                await writer.drain()
                writer.close()
                await writer.wait_closed()
            logging.debug(f"Closed flow {flow}")

    async def _accept(self, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter):
        try:
            sockname = client_writer.get_extra_info("sockname")
            peername = client_writer.get_extra_info("peername")
            dstname = self._get_original_dst(client_writer)
            if dstname[0] == sockname[0] and dstname[1] == sockname[1]:
                client_writer.transport.abort()
                logging.error(f"[{peername[0]}]:{peername[1]} Attempted to connect directly to the server")
                return
            up_flow = f"[{peername[0]}]:{peername[1]} -> [{dstname[0]}]:{dstname[1]}"
            down_flow = f"[{peername[0]}]:{peername[1]} <- [{dstname[0]}]:{dstname[1]}"
        except Exception as e:
            client_writer.transport.abort()
            logging.error(f"Failed to get original destination: {type(e).__name__} {e}")
            return

        try:
            proxy_reader, proxy_writer = await asyncio.open_connection(*dstname)
        except Exception as e:
            client_writer.transport.abort()
            logging.error(f"Failed to connect {up_flow}: {type(e).__name__} {e}")
            return

        logging.info(f"Established {up_flow}")
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self._stream(up_flow, client_reader, proxy_writer))
                tg.create_task(self._stream(down_flow, proxy_reader, client_writer))
        except ExceptionGroup as eg:
            for e in eg.exceptions:
                logging.error(f"{type(e).__name__} {up_flow} {e}")
        finally:
            proxy_writer.transport.abort()
            client_writer.transport.abort()
        logging.info(f"Closed {up_flow}")

    async def run(self):
        self._last_reader = None
        server = await asyncio.start_server(self._accept, port=PORT, reuse_port=True)
        async with server:
            for sock in server.sockets:
                sockname = sock.getsockname()
                logging.info(f"Listening on [{sockname[0]}]:{sockname[1]}")
            await asyncio.create_task(server.serve_forever())


def main(*_):
    uvloop.run(Server().run())


if __name__ == "__main__":
    logging.basicConfig(level=LOG_LEVEL)
    gc.set_threshold(10000)
    gc.collect()
    gc.set_debug(gc.DEBUG_STATS)
    cpu_count = os.cpu_count() or 1
    with ProcessPoolExecutor(cpu_count) as executor:
        executor.map(main, range(cpu_count))
