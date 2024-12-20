from concurrent.futures import ProcessPoolExecutor
from enum import IntEnum
import asyncio
import logging
import socket
import struct
import time

PORT = 8081
LIFETIME = 86400
CLOSE_WAIT = 60
LIMIT = 2**18
FAMILY = [socket.AF_INET, socket.AF_INET6]


class Listener:
    _SO_ORIGINAL_DST = 80
    _SOL_IPV6 = 41
    _V4_LEN = 16
    _V6_LEN = 28
    _CID_ROTATE = 1000000

    _pid = 0
    _cid = 0

    def run(self, pid=0, family=socket.AF_UNSPEC):
        async def _server():
            server = await asyncio.start_server(self._client, port=PORT, reuse_port=True, limit=LIMIT, family=family)
            async with server:
                await server.serve_forever()
            for t in asyncio.all_tasks():
                t.cancel()

        self._pid = pid
        asyncio.run(_server())

    async def _client(self, cr: asyncio.StreamReader, cw: asyncio.StreamWriter):
        prepare_start = time.perf_counter()
        cid = self._cid
        self._cid = (self._cid + 1) % self._CID_ROTATE
        src = cw.get_extra_info("peername")
        srv = cw.get_extra_info("sockname")
        soc = cw.get_extra_info("socket")
        is_ipv4 = "." in src[0]
        dst = self._get_original_dst(soc, is_ipv4)

        if dst[0] == srv[0] and dst[1] == srv[1]:
            logging.error(f"[{self._pid}:{cid}] Blocked direct access from {src[0]}@{src[1]}")
            await writer_close(cw)
            return

        try:
            pr, pw = await asyncio.open_connection(host=dst[0], port=dst[1], limit=LIMIT)
        except:
            logging.warning(f"[{self._pid}:{cid}] Failed proxy {src[0]}@{src[1]} <> {dst[0]}@{dst[1]}")
            await writer_close(cw)
            return

        reader = Connector(self._pid, cid, flow_dir.R, pr, cw)
        writer = Connector(self._pid, cid, flow_dir.W, cr, pw)
        prepare_time = round((time.perf_counter() - prepare_start) * 1000)
        logging.info(f"[{self._pid}:{cid}] Established proxy {src[0]}@{src[1]} <> {dst[0]}@{dst[1]} ({prepare_time=}ms)")

        proxy_start = time.perf_counter()
        async with asyncio.TaskGroup() as tg:
            _ = (tg.create_task(reader.proxy()), tg.create_task(writer.proxy()))
        proxy_time = round(time.perf_counter() - proxy_start)
        logging.info(f"[{self._pid}:{cid}] Closed proxy {src[0]}@{src[1]} <> {dst[0]}@{dst[1]} ({proxy_time=}s)")

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
    _flow_id: str
    _r: asyncio.StreamReader
    _w: asyncio.StreamWriter

    def __init__(self, pid: int, cid: int, fid: int, r: asyncio.StreamReader, w: asyncio.StreamWriter):
        self._flow_id = f"{pid}:{cid}:{flow_dir(fid).name}"
        self._r = r
        self._w = w

    async def proxy(self):
        try:
            s: socket.socket = self._w.get_extra_info("socket")
            s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, True)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, CLOSE_WAIT))
            self._w.transport.set_write_buffer_limits(0)

            async with asyncio.timeout(LIFETIME):
                while data := await self._r.read(LIMIT):
                    write_start = time.perf_counter()
                    self._w.write(memoryview(data))
                    await self._w.drain()
                    write_time = round((time.perf_counter() - write_start) * 1000)
                    if write_time > 100:
                        logging.warning(f"[{self._flow_id}] Slow write {write_time=}ms")

            logging.debug(f"[{self._flow_id}] EOF")
            self._w.write_eof()
            await self._w.drain()
            self._r.feed_eof()

        except Exception as err:
            logging.debug(f"[{self._flow_id}] Error in loop: {err=}")

        finally:
            await writer_close(self._w)

        logging.debug(f"[{self._flow_id}] Closed")


class flow_dir(IntEnum):
    R = 0
    W = 1


async def writer_close(writer: asyncio.StreamWriter):
    try:
        if not writer.is_closing():
            writer.close()
            await writer.wait_closed()
    except Exception as err:
        logging.debug(f"Error in close: {err=}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logging.debug(f"{PORT=}, {LIFETIME=}, {LIMIT=}")
    with ProcessPoolExecutor(len(FAMILY)) as ex:
        h = [ex.submit(Listener().run, pid, FAMILY[pid]) for pid in range(len(FAMILY))]
