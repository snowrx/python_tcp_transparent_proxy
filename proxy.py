from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
import asyncio
import gc
import logging
import socket
import struct
import time

PORT = 8081
LIFETIME = 86400
LIMIT = 1 << 20


@dataclass(order=True)
class ticket:
    ts: int = field(default_factory=time.monotonic_ns, compare=True)
    event: asyncio.Event = field(default_factory=asyncio.Event, compare=False)


class proxy:
    _SO_ORIGINAL_DST = 80
    _SOL_IPV6 = 41
    _V4_LEN = 16
    _V6_LEN = 28

    _pq: asyncio.PriorityQueue[ticket] = asyncio.PriorityQueue()

    def get_original_dst(self, so: socket.socket, is_ipv4: bool = True):
        if is_ipv4:
            dst = so.getsockopt(socket.SOL_IP, self._SO_ORIGINAL_DST, self._V4_LEN)
            port, raw_ip = struct.unpack_from("!2xH4s", dst)
            ip = socket.inet_ntop(socket.AF_INET, raw_ip)
        else:
            dst = so.getsockopt(self._SOL_IPV6, self._SO_ORIGINAL_DST, self._V6_LEN)
            port, raw_ip = struct.unpack_from("!2xH4x16s", dst)
            ip = socket.inet_ntop(socket.AF_INET6, raw_ip)
        return ip, port

    async def writer_close(self, w: asyncio.StreamWriter):
        try:
            if not w.is_closing():
                w.close()
                await w.wait_closed()
        except Exception as err:
            logging.error(f"Failed to close writer: {err}")

    async def proxy(self, label: str, r: asyncio.StreamReader, w: asyncio.StreamWriter):
        status = "start"
        try:
            s: socket.socket = w.get_extra_info("socket")
            s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, True)
            w.transport.set_write_buffer_limits(0)

            status = "read"
            async with asyncio.timeout(LIFETIME):
                t = ticket()
                while not w.is_closing() and (data := await r.read(LIMIT)):
                    status = "write"
                    await self._pq.put(t)
                    await t.event.wait()
                    t.event.clear()
                    w.write(memoryview(data))
                    await w.drain()
                    status = "read"

            if not w.is_closing():
                status = "write_eof"
                w.write_eof()
                await w.drain()

            status = "feed_eof"
            r.feed_eof()
            logging.debug(f"EOF {label}")
            status = "wait_closed"

        except Exception as err:
            logging.error(f"Failed to {status} {label}: {err}")
            w.transport.abort()
            await self.writer_close(w)

    async def client(self, cr: asyncio.StreamReader, cw: asyncio.StreamWriter):
        soc: socket.socket = cw.get_extra_info("socket")
        srv: tuple[str, int] = cw.get_extra_info("sockname")
        src: tuple[str, int] = cw.get_extra_info("peername")
        is_ipv4 = "." in src[0]

        try:
            dst: tuple[str, int] = await asyncio.to_thread(self.get_original_dst, soc, is_ipv4)
        except Exception as err:
            logging.error(f"Failed to get original destination: {err}")
            await self.writer_close(cw)
            return
        w_label = f"{src[0]}@{src[1]} -> {dst[0]}@{dst[1]}"
        r_label = f"{src[0]}@{src[1]} <- {dst[0]}@{dst[1]}"

        if dst[0] == srv[0] and dst[1] == srv[1]:
            logging.error(f"Blocked direct connection {w_label}")
            cw.transport.abort()
            await self.writer_close(cw)
            return

        try:
            pr, pw = await asyncio.open_connection(host=dst[0], port=dst[1], limit=LIMIT)
        except Exception as err:
            logging.error(f"Failed to open connection {w_label}: {err}")
            cw.transport.abort()
            await self.writer_close(cw)
            return

        logging.info(f"Connection established {w_label}")
        proxy_start = time.monotonic()
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self.proxy(r_label, pr, cw))
            tg.create_task(self.proxy(w_label, cr, pw))
        await self.writer_close(pw)
        await self.writer_close(cw)
        proxy_time = round(time.monotonic() - proxy_start)
        logging.info(f"Connection closed {w_label} in {proxy_time} seconds")

    async def server(self):
        server = await asyncio.start_server(self.client, port=PORT, limit=LIMIT)
        async with server:
            await server.serve_forever()
        for t in asyncio.all_tasks():
            t.cancel()

    async def gatekeeper(self):
        while True:
            t = await self._pq.get()
            t.ts = time.monotonic_ns()
            t.event.set()
            self._pq.task_done()
            await asyncio.sleep(0)

    async def launch(self):
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self.server())
            tg.create_task(self.gatekeeper())

    def run(self):
        logging.info(f"Proxy server started on port {PORT}")
        asyncio.run(self.launch())


if __name__ == "__main__":
    gc.collect()
    gc.freeze()
    gc.set_threshold(10000)
    gc.set_debug(gc.DEBUG_STATS)
    logging.basicConfig(level=logging.DEBUG)
    with ThreadPoolExecutor() as tp:
        tp.submit(proxy().run)
