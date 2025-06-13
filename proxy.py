import asyncio
import gc
import logging
import socket
import struct

PORT = 8081
LIMIT = 1 << 18
IDLE_TIMEOUT = 600


class channel:
    _reader: asyncio.StreamReader
    _writer: asyncio.StreamWriter
    _label: str

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, label: str):
        self._reader = reader
        self._writer = writer
        self._label = label

    async def read(self):
        async with asyncio.timeout(IDLE_TIMEOUT):
            return await self._reader.read(LIMIT)

    async def streaming(self):
        reader_task = asyncio.create_task(self.read())
        while data := await reader_task:
            self._writer.write(data)
            reader_task = asyncio.create_task(self.read())
            await self._writer.drain()
        self._writer.write_eof()
        await self._writer.drain()

    async def transfer(self):
        try:
            so: socket.socket = self._writer.get_extra_info("socket")
            so.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, True)
            self._writer.transport.set_write_buffer_limits(LIMIT, LIMIT)
            await asyncio.create_task(self.streaming())
        except Exception as err:
            logging.error(f"Error in channel: {self._label}: {err}")
            self._writer.transport.abort()
        finally:
            if not self._writer.is_closing():
                try:
                    self._writer.close()
                    await self._writer.wait_closed()
                except Exception as err:
                    logging.debug(f"Failed to close writer: {self._label}: {err}")


class server:
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

    async def accept(self, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter):
        so: socket.socket = client_writer.get_extra_info("socket")
        peername: tuple[str, int] = client_writer.get_extra_info("peername")
        sockname: tuple[str, int] = client_writer.get_extra_info("sockname")
        orig: tuple[str, int] = self.get_original_dst(so, "." in peername[0])
        read_label = f"{orig[0]}@{orig[1]} -> {peername[0]}@{peername[1]}"
        write_label = f"{peername[0]}@{peername[1]} -> {orig[0]}@{orig[1]}"

        if orig[0] == sockname[0] and orig[1] == sockname[1]:
            logging.error(f"Blocked loopback connection: {write_label}")
            client_writer.transport.abort()
            client_writer.close()
            await client_writer.wait_closed()
            return

        try:
            orig_reader, orig_writer = await asyncio.open_connection(orig[0], orig[1], limit=LIMIT)
        except Exception as err:
            logging.error(f"Failed to connect: {write_label}: {err}")
            client_writer.transport.abort()
            client_writer.close()
            await client_writer.wait_closed()
            return

        logging.info(f"Established connection: {write_label}")
        read_channel = channel(orig_reader, client_writer, read_label)
        write_channel = channel(client_reader, orig_writer, write_label)
        async with asyncio.TaskGroup() as tg:
            tg.create_task(read_channel.transfer())
            tg.create_task(write_channel.transfer())
        logging.info(f"Closed connection: {write_label}")

    async def start_server(self):
        server = await asyncio.start_server(self.accept, port=PORT, limit=LIMIT)
        logging.info(f"Listening on port {PORT}")
        async with server:
            await server.serve_forever()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    gc.set_threshold(10000)
    gc.collect()
    gc.freeze()
    gc.set_debug(gc.DEBUG_STATS)
    asyncio.run(server().start_server())
