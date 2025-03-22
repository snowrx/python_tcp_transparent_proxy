from concurrent.futures import ThreadPoolExecutor
import asyncio
import gc
import logging
import socket
import struct
import time

PORT = 8081
READ_TIMEOUT = 3600
LOOKAHEAD = 1 << 24


class proxy:
    _DEFAULT_LIMIT = 1 << 16
    _SO_ORIGINAL_DST = 80
    _SOL_IPV6 = 41
    _V4_LEN = 16
    _V6_LEN = 28

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

    async def writer_close(self, w: asyncio.StreamWriter, abort: bool = False):
        try:
            if not w.is_closing():
                if abort:
                    w.transport.abort()
                w.close()
                await w.wait_closed()
        except Exception as err:
            logging.error(f"Failed to close writer: {type(err).__name__}")

    async def read_data(self, label: str, r: asyncio.StreamReader):
        try:
            async with asyncio.timeout(READ_TIMEOUT):
                return await r.read(self._DEFAULT_LIMIT)
        except Exception as err:
            logging.error(f"Error in read task: {type(err).__name__}, {label}")
            return None

    async def proxy(self, label: str, r: asyncio.StreamReader, w: asyncio.StreamWriter):
        read = asyncio.create_task(self.read_data(label, r))
        try:
            s: socket.socket = w.get_extra_info("socket")
            s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, True)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 10))
            w.transport.set_write_buffer_limits(LOOKAHEAD, LOOKAHEAD)

            await asyncio.sleep(0)
            logging.debug(f"Start channel: {label}")
            while not w.is_closing() and (data := await read):
                read = asyncio.create_task(self.read_data(label, r))
                w.write(data)
                del data
                await w.drain()

            if not w.is_closing():
                w.write_eof()
                await w.drain()

            r.feed_eof()
            logging.debug(f"Finished channel: {label}")

        except Exception as err:
            logging.error(f"Error in channel: {type(err).__name__}, {label}")
            read.cancel()
            await self.writer_close(w, True)

    async def client(self, from_client: asyncio.StreamReader, to_client: asyncio.StreamWriter):
        soc: socket.socket = to_client.get_extra_info("socket")
        srv: tuple[str, int] = to_client.get_extra_info("sockname")
        src: tuple[str, int] = to_client.get_extra_info("peername")
        is_ipv4 = "." in src[0]

        try:
            dst: tuple[str, int] = await asyncio.to_thread(self.get_original_dst, soc, is_ipv4)
        except Exception as err:
            logging.error(f"Failed to get original destination: {type(err).__name__}, {src[0]}@{src[1]}")
            await self.writer_close(to_client, True)
            return

        w_label = f"{src[0]}@{src[1]} -> {dst[0]}@{dst[1]}"
        r_label = f"{src[0]}@{src[1]} <- {dst[0]}@{dst[1]}"

        if dst[0] == srv[0] and dst[1] == srv[1]:
            logging.error(f"Refused to connect: {w_label}")
            await self.writer_close(to_client, True)
            return

        try:
            from_remote, to_remote = await asyncio.open_connection(host=dst[0], port=dst[1], limit=self._DEFAULT_LIMIT)
        except Exception as err:
            logging.error(f"Failed to open connection: {type(err).__name__}, {w_label}")
            await self.writer_close(to_client, True)
            return

        logging.info(f"Established: {w_label}")
        proxy_start = time.monotonic()
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self.proxy(w_label, from_client, to_remote))
            tg.create_task(self.proxy(r_label, from_remote, to_client))
        await self.writer_close(to_remote)
        await self.writer_close(to_client)
        proxy_time = round(time.monotonic() - proxy_start)
        logging.info(f"Closed: {w_label}, {proxy_time}s")

    async def server(self):
        server = await asyncio.start_server(self.client, port=PORT, limit=self._DEFAULT_LIMIT)
        async with server:
            logging.info(f"Listening on port {PORT}")
            await server.serve_forever()
        for t in asyncio.all_tasks():
            t.cancel()

    async def launch(self):
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self.server())

    def run(self):
        asyncio.run(self.launch())


if __name__ == "__main__":
    gc.set_debug(gc.DEBUG_STATS)
    gc.set_threshold(10000)
    logging.basicConfig(level=logging.INFO)
    with ThreadPoolExecutor(1) as t:
        t.submit(proxy().run)
