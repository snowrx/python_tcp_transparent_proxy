from concurrent.futures import ProcessPoolExecutor
import asyncio
import gc
import logging
import os
import socket
import struct
import time

PORT = 8081
TIMEOUT = 3600
LOW_WATERMARK = 1 << 12


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

    async def writer_close(self, w: asyncio.StreamWriter):
        try:
            if not w.is_closing():
                w.close()
                await w.wait_closed()
        except Exception as err:
            logging.error(f"Failed to close writer: {type(err).__name__}")
        return

    async def read(self, r: asyncio.StreamReader):
        try:
            async with asyncio.timeout(TIMEOUT):
                return await r.read(self._DEFAULT_LIMIT)
        except Exception as err:
            logging.error(f"Failed to read: {type(err).__name__}")
            return b""

    async def proxy(self, label: str, r: asyncio.StreamReader, w: asyncio.StreamWriter):
        try:
            read = asyncio.create_task(self.read(r))
            s: socket.socket = w.get_extra_info("socket")
            s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, True)
            w.transport.set_write_buffer_limits(LOW_WATERMARK, LOW_WATERMARK)
            await asyncio.sleep(0)

            while not w.is_closing() and (mv := memoryview(await read)):
                read = asyncio.create_task(self.read(r))
                w.write(mv)
                await w.drain()

            if not w.is_closing():
                w.write_eof()
                await w.drain()

        except Exception as err:
            logging.error(f"Error in channel: {label}, {type(err).__name__}, {err}")
            w.transport.abort()
        return

    async def client(self, from_client: asyncio.StreamReader, to_client: asyncio.StreamWriter):
        soc: socket.socket = to_client.get_extra_info("socket")
        srv: tuple[str, int] = to_client.get_extra_info("sockname")
        src: tuple[str, int] = to_client.get_extra_info("peername")
        is_ipv4 = "." in src[0]

        try:
            dst: tuple[str, int] = await asyncio.to_thread(self.get_original_dst, soc, is_ipv4)
        except Exception as err:
            logging.error(f"Failed to get original destination: {type(err).__name__}, {src[0]}@{src[1]}")
            to_client.transport.abort()
            await self.writer_close(to_client)
            return

        w_label = f"{src[0]}@{src[1]} -> {dst[0]}@{dst[1]}"
        r_label = f"{src[0]}@{src[1]} <- {dst[0]}@{dst[1]}"

        if dst[0] == srv[0] and dst[1] == srv[1]:
            logging.error(f"Loopback detected: {w_label}")
            to_client.transport.abort()
            await self.writer_close(to_client)
            return

        try:
            from_remote, to_remote = await asyncio.open_connection(host=dst[0], port=dst[1])
        except Exception as err:
            logging.error(f"Failed to connect: {w_label}, {type(err).__name__}, {err}")
            to_client.transport.abort()
            await self.writer_close(to_client)
            return

        logging.info(f"Established: {w_label}")
        proxy_start = time.monotonic()
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self.proxy(w_label, from_client, to_remote))
            tg.create_task(self.proxy(r_label, from_remote, to_client))
        await self.writer_close(to_remote)
        await self.writer_close(to_client)
        proxy_time = time.monotonic() - proxy_start
        logging.info(f"Closed: {w_label}, {proxy_time:.1f}s")
        del from_remote, to_remote, from_client, to_client
        return

    async def start_server(self):
        server = await asyncio.start_server(self.client, port=PORT, reuse_port=True)
        async with server:
            logging.info(f"Listening on port {PORT}")
            await server.serve_forever()
        return

    def run(self, pid: int = 0):
        logging.debug(f"Process {pid} started")
        asyncio.run(self.start_server())


if __name__ == "__main__":
    gc.collect()
    gc.freeze()
    gc.set_threshold(10000)
    gc.set_debug(gc.DEBUG_STATS)
    logging.basicConfig(level=logging.DEBUG)
    workers = len(os.sched_getaffinity(0))
    with ProcessPoolExecutor(workers) as pool:
        pool.map(proxy().run, range(workers))
