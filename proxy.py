from concurrent.futures import ThreadPoolExecutor
import asyncio
import gc
import logging
import socket
import struct

import uvloop

LOG_LEVEL = logging.DEBUG
LOG_INTERVAL = 60
PORT = 8081
TIMEOUT = 3600 * 6
LIMIT = 1 << 18
WORKERS = 4


class Utility:
    _SO_ORIGINAL_DST = 80
    _SOL_IPV6 = 41
    _V4_LEN = 16
    _V6_LEN = 28

    @staticmethod
    def get_original_dst(so: socket.socket, v4: bool):
        ip: str
        port: int
        if v4:
            dst = so.getsockopt(socket.SOL_IP, Utility._SO_ORIGINAL_DST, Utility._V4_LEN)
            port, raw_ip = struct.unpack_from("!2xH4s", dst)
            ip = socket.inet_ntop(socket.AF_INET, raw_ip)
        else:
            dst = so.getsockopt(Utility._SOL_IPV6, Utility._SO_ORIGINAL_DST, Utility._V6_LEN)
            port, raw_ip = struct.unpack_from("!2xH4x16s", dst)
            ip = socket.inet_ntop(socket.AF_INET6, raw_ip)
        return ip, port


class Server:
    async def _connection(self, flow: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            so: socket.socket = writer.get_extra_info("socket")
            so.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            async with asyncio.timeout(TIMEOUT):
                while data := await reader.read(LIMIT):
                    await writer.drain()
                    writer.write(data)
        except Exception as e:
            logging.error(f"[{self._id}] {type(e).__name__} {flow}: {e}")
        finally:
            if not writer.is_closing():
                try:
                    writer.write_eof()
                    await writer.drain()
                    writer.close()
                    await writer.wait_closed()
                except Exception as e:
                    logging.debug(f"[{self._id}] {type(e).__name__} {flow}: {e}")

    async def _handler(self, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter):
        try:
            peername = client_writer.get_extra_info("peername")
            so: socket.socket = client_writer.get_extra_info("socket")
            v4 = "." in peername[0]
            dst = Utility.get_original_dst(so, v4)
            write_flow = f"[{peername[0]}]:{peername[1]} → [{dst[0]}]:{dst[1]}"
            read_flow = f"[{peername[0]}]:{peername[1]} ← [{dst[0]}]:{dst[1]}"
        except Exception as e:
            logging.error(f"[{self._id}] Failed to gather connection info: {e}")
            client_writer.transport.abort()
            client_writer.close()
            await client_writer.wait_closed()
            return

        try:
            proxy_reader, proxy_writer = await asyncio.open_connection(*dst, limit=LIMIT)
        except Exception as e:
            logging.error(f"[{self._id}] Failed to connect {write_flow}: {e}")
            client_writer.transport.abort()
            client_writer.close()
            await client_writer.wait_closed()
            return

        logging.info(f"[{self._id}] Established {write_flow}")
        start = self._loop.time()
        self._active_connections[write_flow] = start
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self._connection(write_flow, client_reader, proxy_writer))
                tg.create_task(self._connection(read_flow, proxy_reader, client_writer))
        except* Exception as e:
            logging.error(f"[{self._id}] Error in task group for {write_flow}: {e.exceptions}")
        finally:
            logging.info(f"[{self._id}] Closed {write_flow} {self._loop.time() - start:.2f}s")
        del self._active_connections[write_flow]

        if (now := self._loop.time()) - self._ts > LOG_INTERVAL:
            self._ts = now
            logging.debug(f"[{self._id}] * Active connections: {len(self._active_connections)}")
            if self._active_connections:
                for flow, ts in self._active_connections.items():
                    logging.debug(f"[{self._id}] * {flow}: {now - ts:.2f}s")

    async def serve(self, id: int = 0):
        self._id = id
        self._loop = asyncio.get_running_loop()
        self._loop.set_task_factory(asyncio.eager_task_factory)
        self._active_connections: dict[str, float] = {}
        self._ts = self._loop.time()
        server = await asyncio.start_server(self._handler, port=PORT, reuse_port=True, limit=LIMIT)
        logging.info(f"[{self._id}] Listening on port {PORT}")
        async with server:
            await server.serve_forever()


def run_server(id: int = 0):
    try:
        uvloop.run(Server().serve(id))
    except Exception as e:
        logging.critical(f"Worker {id} stopped due to an unhandled exception: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=LOG_LEVEL)
    gc.set_threshold(10000)
    gc.set_debug(gc.DEBUG_STATS)
    with ThreadPoolExecutor(WORKERS) as pool:
        pool.map(run_server, range(WORKERS))
    logging.shutdown()
