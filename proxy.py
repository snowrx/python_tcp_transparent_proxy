import asyncio
import logging
import socket
import struct
import time

PORT = 8081
LIFETIME = 43200


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
        except* Exception as err:
            logging.debug(f"{err.exceptions}")

    async def write(self, w: asyncio.StreamWriter, data: bytes):
        w.write(data)
        await w.drain()

    async def proxy(self, label: str, r: asyncio.StreamReader, w: asyncio.StreamWriter):
        status = "setsockopt"
        try:
            s: socket.socket = w.get_extra_info("socket")
            s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, True)

            status = "read"
            async with asyncio.timeout(LIFETIME):
                while (data := await r.read(self._LIMIT)) and not w.is_closing():
                    status = "write"
                    await asyncio.create_task(self.write(w, data))
                    status = "read"

            status = "write_eof"
            r.feed_eof()
            w.write_eof()
            await w.drain()
            logging.debug(f"EOF {label}")

        except* Exception as err:
            logging.debug(f"Error in {status} {label} {err.exceptions}")
            await self.writer_close(w)

    async def client(self, cr: asyncio.StreamReader, cw: asyncio.StreamWriter):
        soc: socket.socket = cw.get_extra_info("socket")
        srv: tuple[str, int] = cw.get_extra_info("sockname")
        src: tuple[str, int] = cw.get_extra_info("peername")
        is_ipv4 = "." in src[0]

        dst: tuple[str, int] = await asyncio.to_thread(self.get_original_dst, soc, is_ipv4)
        w_label = f"{src[0]}@{src[1]} > {dst[0]}@{dst[1]}"
        r_label = f"{src[0]}@{src[1]} < {dst[0]}@{dst[1]}"

        if dst[0] == srv[0] and dst[1] == srv[1]:
            logging.warning(f"Blocked {w_label}")
            cw.write(b"HTTP/1.1 403 Forbidden\r\n\r\n")
            cw.write_eof()
            await cw.drain()
            await self.writer_close(cw)
            return

        try:
            pr, pw = await asyncio.open_connection(host=dst[0], port=dst[1])
        except:
            logging.warning(f"Failed {w_label}")
            cw.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
            cw.write_eof()
            await cw.drain()
            await self.writer_close(cw)
            return

        logging.info(f"Established {w_label}")
        proxy_start = time.perf_counter()
        async with asyncio.TaskGroup() as tg:
            r = tg.create_task(self.proxy(r_label, pr, cw))
            w = tg.create_task(self.proxy(w_label, cr, pw))
        await self.writer_close(pw)
        await self.writer_close(cw)
        proxy_time = round(time.perf_counter() - proxy_start)
        logging.info(f"Closed {proxy_time}s {w_label}")

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
    proxy().run()
