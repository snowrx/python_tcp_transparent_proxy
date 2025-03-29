from concurrent.futures import ThreadPoolExecutor
import asyncio
import gc
import logging
import socket
import struct
import time

PORT = 8081
CONNECTION_LIFETIME = 86400
LOOKAHEAD = 1 << 20


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

    async def proxy(self, label: str, r: asyncio.StreamReader, w: asyncio.StreamWriter):
        read = asyncio.create_task(r.read(self._DEFAULT_LIMIT))

        try:
            s: socket.socket = w.get_extra_info("socket")
            s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, True)
            w.transport.set_write_buffer_limits(LOOKAHEAD, LOOKAHEAD)
            await asyncio.sleep(0)

            async with asyncio.timeout(CONNECTION_LIFETIME):
                while not w.is_closing() and (data := await read):
                    w.write(data)
                    del data
                    del read
                    read = asyncio.create_task(r.read(self._DEFAULT_LIMIT))
                    await w.drain()

            if not w.is_closing():
                w.write_eof()
                await w.drain()

        except Exception as err:
            logging.error(f"Error in channel: {type(err).__name__}, {label}")
            await self.writer_close(w)

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
            await self.writer_close(to_client)
            return

        w_label = f"{src[0]}@{src[1]} -> {dst[0]}@{dst[1]}"
        r_label = f"{src[0]}@{src[1]} <- {dst[0]}@{dst[1]}"

        if dst[0] == srv[0] and dst[1] == srv[1]:
            logging.error(f"Refused to connect: {w_label}")
            await self.writer_close(to_client)
            return

        try:
            from_remote, to_remote = await asyncio.open_connection(host=dst[0], port=dst[1], limit=self._DEFAULT_LIMIT)
        except Exception as err:
            logging.error(f"Failed to open connection: {type(err).__name__}, {w_label}")
            await self.writer_close(to_client)
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
        del from_remote, to_remote, from_client, to_client
        return

    async def server(self):
        server = await asyncio.start_server(self.client, port=PORT, limit=self._DEFAULT_LIMIT)
        async with server:
            logging.info(f"Listening on port {PORT}")
            await server.serve_forever()
        return

    async def launch(self):
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self.server())
        return

    def run(self):
        asyncio.run(self.launch())
        return


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    gc.set_threshold(3000)
    gc.collect()
    gc.set_debug(gc.DEBUG_STATS)
    with ThreadPoolExecutor() as t:
        t.submit(proxy().run)
