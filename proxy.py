from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import asyncio
import logging
import os
import socket
import struct
import time

PORT = 8081
TIMEOUT = 86400

_COMMON_PAGESIZE = 2**12


class Listener:
    _SO_ORIGINAL_DST = 80
    _SOL_IPV6 = 41
    _V4_LEN = 16
    _V6_LEN = 28
    _CID_ROTATE = 1000000
    _FAMILY = [socket.AF_INET, socket.AF_INET6]

    _pid = 0
    _cid = 0

    def __init__(self, pid: int):
        self._pid = pid

    def run(self):
        async def _server(family=socket.AF_UNSPEC):
            server = await asyncio.start_server(self._client, port=PORT, reuse_port=True, limit=_COMMON_PAGESIZE, family=family)
            for so in server.sockets:
                so.setsockopt(socket.SOL_TCP, socket.TCP_DEFER_ACCEPT, True)
            async with server:
                await server.serve_forever()
            for t in asyncio.all_tasks():
                t.cancel()

        def _run_thread(family=socket.AF_UNSPEC):
            asyncio.run(_server(family))

        with ThreadPoolExecutor(len(self._FAMILY)) as tex:
            tex.map(_run_thread, self._FAMILY)

    async def _client(self, cr: asyncio.StreamReader, cw: asyncio.StreamWriter):
        cid = self._cid
        self._cid = (cid + 1) % self._CID_ROTATE
        src = cw.get_extra_info("peername")
        srv = cw.get_extra_info("sockname")
        soc = cw.get_extra_info("socket")
        is_ipv4 = "." in src[0]
        dst = self._get_original_dst(soc, is_ipv4)

        if dst[0] == srv[0] and dst[1] == srv[1]:
            logging.error(f"[{self._pid}:{cid}] Blocked direct access from {src[0]}@{src[1]}")
            try:
                cw.transport.abort()
                cw.close()
                await cw.wait_closed()
            except:
                pass
            return

        try:
            open_start = time.perf_counter()
            pr, pw = await asyncio.open_connection(host=dst[0], port=dst[1], limit=_COMMON_PAGESIZE)
            open_delay = time.perf_counter() - open_start
        except:
            try:
                cw.transport.abort()
                cw.close()
                await cw.wait_closed()
            except:
                pass
            logging.warning(f"[{self._pid}:{cid}] Failed proxy in {src[0]}@{src[1]} <> {dst[0]}@{dst[1]}")
            return

        logging.info(f"[{self._pid}:{cid}] Open proxy in {src[0]}@{src[1]} <> {dst[0]}@{dst[1]} ({round(open_delay * 1000)}ms)")
        barrier = asyncio.Barrier(2)
        lc = Connector(self._pid, cid, 0, barrier, cr, pw)
        pc = Connector(self._pid, cid, 1, barrier, pr, cw)
        proxy_start = time.perf_counter()
        async with asyncio.TaskGroup() as tg:
            r0 = tg.create_task(lc.proxy())
            r1 = tg.create_task(pc.proxy())
        proxy_duration = time.perf_counter() - proxy_start
        logging.info(f"[{self._pid}:{cid}] Close proxy in {src[0]}@{src[1]} ({r0.result()}) <> {dst[0]}@{dst[1]} ({r1.result()}) in {round(proxy_duration)}s")

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
    _cid: int
    _fid: int
    _barrier: asyncio.Barrier
    _r: asyncio.StreamReader
    _w: asyncio.StreamWriter

    def __init__(self, pid: int, cid: int, fid: int, barrier: asyncio.Barrier, r: asyncio.StreamReader, w: asyncio.StreamWriter):
        self._pid = pid
        self._cid = cid
        self._fid = fid
        self._barrier = barrier
        self._r = r
        self._w = w

    async def proxy(self):
        code = 0
        try:
            s: socket.socket = self._w.get_extra_info("socket")
            s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, True)
            self._w.transport.set_write_buffer_limits(_COMMON_PAGESIZE)
            async with asyncio.timeout(TIMEOUT):
                while data := await self._r.read(_COMMON_PAGESIZE):
                    self._w.write(memoryview(data))
                    await self._w.drain()
            self._r.feed_eof()
            logging.debug(f"[{self._pid}:{self._cid}:{self._fid}] EOF")
        except Exception as err:
            logging.debug(f"[{self._pid}:{self._cid}:{self._fid}] error in loop: {err=}")
            code |= 0b1
        finally:
            # before wait
            if not self._w.is_closing():
                try:
                    self._w.write_eof()
                    await self._w.drain()
                except:
                    code |= 0b10
            # wait
            await self._barrier.wait()
            # after wait
            if not self._w.is_closing():
                try:
                    self._w.close()
                    await self._w.wait_closed()
                except:
                    code |= 0b100
        return code


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    try:
        workers = len(os.sched_getaffinity(0))
    except:
        workers = os.cpu_count() or 1
    logging.debug(f"{PORT=}, {TIMEOUT=}, {workers=}")
    with ProcessPoolExecutor(workers) as pex:
        listeners = [Listener(pid) for pid in range(workers)]
        pf = [pex.submit(l.run) for l in listeners]
