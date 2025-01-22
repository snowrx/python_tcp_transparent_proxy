import asyncio
import logging
import os
import socket
import struct
import time

PORT = 8081
LIFETIME = 14400


class proxy:
    _LIMIT = 2**14
    _SO_ORIGINAL_DST = 80
    _SOL_IPV6 = 41
    _V4_LEN = 16
    _V6_LEN = 28

    def get_original_dst(self, so: socket.socket, is_ipv4=True):
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
            await asyncio.to_thread(logging.debug, f"{type(err).__name__} in writer_close")

    async def proxy(self, label: str, r: asyncio.StreamReader, w: asyncio.StreamWriter):
        status = "setsockopt"
        try:
            s: socket.socket = w.get_extra_info("socket")
            s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, True)
            w.transport.set_write_buffer_limits(self._LIMIT, self._LIMIT)

            status = "read"
            async with asyncio.timeout(LIFETIME):
                while data := await r.read(self._LIMIT):
                    status = "write"
                    w.write(data)
                    await w.drain()
                    status = "read"

            status = "write_eof"
            r.feed_eof()
            w.write_eof()
            await w.drain()
            await asyncio.to_thread(logging.debug, f"EOF {label}")

        except Exception as err:
            await asyncio.to_thread(logging.debug, f"{type(err).__name__} in {status} {label}")
            await self.writer_close(w)

    async def client(self, cr: asyncio.StreamReader, cw: asyncio.StreamWriter):
        soc: socket.socket = cw.get_extra_info("socket")
        srv: tuple[str, int] = cw.get_extra_info("sockname")
        src: tuple[str, int] = cw.get_extra_info("peername")
        is_ipv4 = "." in src[0]

        dst: tuple[str, int] = await asyncio.to_thread(self.get_original_dst, soc, is_ipv4)
        w_label = f"{src[0]}@{src[1]} ▶ {dst[0]}@{dst[1]}"
        r_label = f"{src[0]}@{src[1]} ◀ {dst[0]}@{dst[1]}"

        if dst[0] == srv[0] and dst[1] == srv[1]:
            await asyncio.to_thread(logging.warning, f"Blocked {w_label}")
            await self.writer_close(cw)
            return

        try:
            open_start = time.perf_counter_ns()
            pr, pw = await asyncio.open_connection(host=dst[0], port=dst[1])
            open_time = round((time.perf_counter_ns() - open_start) * 1e-6)
        except Exception as err:
            await asyncio.to_thread(logging.warning, f"Failed by {type(err).__name__} {w_label}")
            await self.writer_close(cw)
            return

        await asyncio.to_thread(logging.info, f"Established {open_time}ms {w_label}")
        proxy_start = time.time()
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self.proxy(r_label, pr, cw))
            tg.create_task(self.proxy(w_label, cr, pw))
        await self.writer_close(pw)
        await self.writer_close(cw)
        proxy_time = round(time.time() - proxy_start)
        await asyncio.to_thread(logging.info, f"Closed {proxy_time}s {w_label}")

    async def server(self):
        server = await asyncio.start_server(self.client, port=PORT)
        async with server:
            await server.serve_forever()
        for t in asyncio.all_tasks():
            t.cancel()

    def run(self):
        logging.info(f"Listening {PORT=}")
        asyncio.run(self.server())


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    try:
        os.sched_setaffinity(0, sorted(os.sched_getaffinity(0))[-4:])
    except:
        pass
    proxy().run()
