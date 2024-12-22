from concurrent.futures import ProcessPoolExecutor
from enum import IntEnum
import asyncio
import logging
import socket
import struct
import time

PORT = 8081
LIFETIME = 86400
WORKERS = 4


class Listener:
    _SO_ORIGINAL_DST = 80
    _SOL_IPV6 = 41
    _V4_LEN = 16
    _V6_LEN = 28

    _pid = 0

    def run(self, pid=0):
        async def _server():
            server = await asyncio.start_server(self._client, port=PORT, reuse_port=True)
            async with server:
                await server.serve_forever()
            for t in asyncio.all_tasks():
                t.cancel()

        self._pid = pid
        asyncio.run(_server())

    async def _client(self, cr: asyncio.StreamReader, cw: asyncio.StreamWriter):
        prepare_start = time.perf_counter()
        src = cw.get_extra_info("peername")
        srv = cw.get_extra_info("sockname")
        soc = cw.get_extra_info("socket")
        is_ipv4 = "." in src[0]
        dst = self._get_original_dst(soc, is_ipv4)
        w_label = f"{src[0]}@{src[1]}->{dst[0]}@{dst[1]}"
        r_label = f"{dst[0]}@{dst[1]}->{src[0]}@{src[1]}"

        if dst[0] == srv[0] and dst[1] == srv[1]:
            logging.warning(f"[{self._pid}] {w_label} Blocked loopback")
            await writer_close(cw)
            return

        try:
            pr, pw = await asyncio.open_connection(host=dst[0], port=dst[1])
        except:
            logging.warning(f"[{self._pid}] {w_label} Connection failed")
            await writer_close(cw)
            return

        reader = Connector(self._pid, r_label, pr, cw)
        writer = Connector(self._pid, w_label, cr, pw)
        prepare_time = round((time.perf_counter() - prepare_start) * 1000)
        logging.info(f"[{self._pid}] {w_label} Established {prepare_time=}ms")

        proxy_start = time.perf_counter()
        async with asyncio.TaskGroup() as tg:
            _ = (tg.create_task(reader.proxy()), tg.create_task(writer.proxy()))
        proxy_time = round(time.perf_counter() - proxy_start)
        logging.info(f"[{self._pid}] {w_label} Closed {proxy_time=}s")

    def _get_original_dst(self, so: socket.socket, is_ipv4=True):
        if is_ipv4:
            dst = so.getsockopt(socket.SOL_IP, self._SO_ORIGINAL_DST, self._V4_LEN)
            port, raw_ip = struct.unpack_from("!2xH4s", dst)
            ip = socket.inet_ntop(socket.AF_INET, raw_ip)
        else:
            dst = so.getsockopt(self._SOL_IPV6, self._SO_ORIGINAL_DST, self._V6_LEN)
            port, raw_ip = struct.unpack_from("!2xH4x16s", dst)
            ip = socket.inet_ntop(socket.AF_INET6, raw_ip)
        return ip, port


class Connector:
    _pid: int
    _label: str
    _r: asyncio.StreamReader
    _w: asyncio.StreamWriter

    def __init__(self, pid: int, label: str, r: asyncio.StreamReader, w: asyncio.StreamWriter):
        self._pid = pid
        self._label = label
        self._r = r
        self._w = w

    async def proxy(self):
        total_bytes = 0

        try:
            s: socket.socket = self._w.get_extra_info("socket")
            s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, True)
            mss = s.getsockopt(socket.SOL_TCP, socket.TCP_MAXSEG)
            self._w.transport.set_write_buffer_limits(mss)
            logging.debug(f"[{self._pid}] {self._label} {mss=}")

            async with asyncio.timeout(LIFETIME):
                while data := await self._r.read(mss):
                    write_start = time.perf_counter()
                    self._w.write(memoryview(data))
                    total_bytes += len(data)
                    await self._w.drain()
                    write_time = round((time.perf_counter() - write_start) * 1000)
                    if write_time > 100:
                        logging.warning(f"[{self._pid}] {self._label} Slow write {write_time=}ms")

            logging.debug(f"[{self._pid}] {self._label} EOF")
            self._w.write_eof()
            await self._w.drain()
            self._r.feed_eof()

        except Exception as err:
            logging.debug(f"[{self._pid}] {self._label} Error: {err=}")

        finally:
            await writer_close(self._w)

        logging.debug(f"[{self._pid}] {self._label} Closed {total_bytes=}")


async def writer_close(writer: asyncio.StreamWriter):
    try:
        if not writer.is_closing():
            writer.close()
            await writer.wait_closed()
    except Exception as err:
        logging.debug(f"Error in close: {err=}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.debug(f"{PORT=}, {LIFETIME=}, {WORKERS=}")
    with ProcessPoolExecutor(WORKERS) as executor:
        executor.map(Listener().run, range(WORKERS))
