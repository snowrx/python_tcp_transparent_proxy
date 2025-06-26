from concurrent.futures import ThreadPoolExecutor
import asyncio
import gc
import logging
import os
import socket
import struct

PORT = 8081
LIFETIME = 86400
LIMIT = 1 << 18


class channel:
    def __init__(self, pid: int, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, label: str):
        self._pid: int = pid
        self._reader: asyncio.StreamReader = reader
        self._writer: asyncio.StreamWriter = writer
        self._label: str = label

    async def streaming(self):
        while not self._writer.is_closing() and (v := memoryview(await self._reader.read(LIMIT))):
            await self._writer.drain()
            self._writer.write(v)
        if not self._writer.is_closing():
            self._writer.write_eof()
            await self._writer.drain()

    async def transfer(self):
        try:
            so: socket.socket = self._writer.get_extra_info("socket")
            so.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, True)
            self._writer.transport.set_write_buffer_limits(LIMIT, LIMIT)
            async with asyncio.timeout(LIFETIME):
                await self.streaming()
        except Exception as err:
            logging.error(f"[{self._pid}] Error in channel: {self._label}, {err}")
        finally:
            if not self._writer.is_closing():
                try:
                    self._writer.close()
                    await self._writer.wait_closed()
                except Exception as err:
                    logging.debug(f"[{self._pid}] Failed to close writer: {self._label}, {err}")


class server:
    _SO_ORIGINAL_DST = 80
    _SOL_IPV6 = 41
    _V4_LEN = 16
    _V6_LEN = 28

    def __init__(self, pid: int):
        self._pid: int = pid
        self._fid: int = 0
        gc.set_debug(gc.DEBUG_STATS)

    def get_original_dst(self, so: socket.socket):
        ip: str
        port: int
        match so.family:
            case socket.AF_INET:
                dst = so.getsockopt(socket.SOL_IP, self._SO_ORIGINAL_DST, self._V4_LEN)
                port, raw_ip = struct.unpack_from("!2xH4s", dst)
                ip = socket.inet_ntop(socket.AF_INET, raw_ip)
            case socket.AF_INET6:
                dst = so.getsockopt(self._SOL_IPV6, self._SO_ORIGINAL_DST, self._V6_LEN)
                port, raw_ip = struct.unpack_from("!2xH4x16s", dst)
                ip = socket.inet_ntop(socket.AF_INET6, raw_ip)
            case _:
                raise ValueError(f"Unsupported address family: {so.family}")
        return ip, port

    async def accept(self, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter):
        try:
            orig: tuple[str, int] = self.get_original_dst(client_writer.get_extra_info("socket"))
        except Exception as err:
            logging.error(f"[{self._pid}] Failed to get original destination: {err}")
            client_writer.close()
            await client_writer.wait_closed()
            return

        peername: tuple[str, int] = client_writer.get_extra_info("peername")
        sockname: tuple[str, int] = client_writer.get_extra_info("sockname")
        read_label = f"{orig[0]}@{orig[1]} -> {peername[0]}@{peername[1]}"
        write_label = f"{peername[0]}@{peername[1]} -> {orig[0]}@{orig[1]}"

        if orig[0] == sockname[0] and orig[1] == sockname[1]:
            logging.error(f"[{self._pid}] Blocked loopback connection: {write_label}")
            client_writer.close()
            await client_writer.wait_closed()
            return

        try:
            logging.debug(f"[{self._pid}] calling: {write_label}")
            orig_reader, orig_writer = await asyncio.open_connection(orig[0], orig[1], limit=LIMIT)
        except Exception as err:
            logging.error(f"[{self._pid}] Failed to connect: {write_label}, {err}")
            client_writer.close()
            await client_writer.wait_closed()
            return

        logging.info(f"[{self._pid}] Established connection: {write_label}")
        read_channel = channel(self._pid, orig_reader, client_writer, read_label)
        write_channel = channel(self._pid, client_reader, orig_writer, write_label)
        async with asyncio.TaskGroup() as tg:
            tg.create_task(read_channel.transfer())
            tg.create_task(write_channel.transfer())
        logging.info(f"[{self._pid}] Closed connection: {write_label}")

    async def start_server(self):
        loop = asyncio.get_running_loop()
        loop.set_task_factory(asyncio.eager_task_factory)
        loop.set_default_executor(ThreadPoolExecutor(1))
        server = await asyncio.start_server(self.accept, port=PORT, limit=LIMIT, reuse_port=True)
        logging.info(f"[{self._pid}] Listening on port {PORT}")
        async with server:
            await server.serve_forever()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    worker_count = len(os.sched_getaffinity(0))
    with ThreadPoolExecutor(worker_count) as executor:
        executor.map(asyncio.run, [server(i).start_server() for i in range(worker_count)])
    logging.shutdown()
