import asyncio
import logging
import socket
import struct
import gc

import uvloop

LOG_LEVEL = logging.DEBUG
PORT = 8081
MSS = 64000


class Utility:
    _SO_ORIGINAL_DST = 80
    _SOL_IPV6 = 41
    _V4_LEN = 16
    _V6_LEN = 28

    @staticmethod
    def get_original_dst(so: socket.socket, v4: bool):
        ip: str
        port: int
        if v4:
            dst = so.getsockopt(socket.SOL_IP, Utility._SO_ORIGINAL_DST, Utility._V4_LEN)
            port, raw_ip = struct.unpack_from("!2xH4s", dst)
            ip = socket.inet_ntop(socket.AF_INET, raw_ip)
        else:
            dst = so.getsockopt(Utility._SOL_IPV6, Utility._SO_ORIGINAL_DST, Utility._V6_LEN)
            port, raw_ip = struct.unpack_from("!2xH4x16s", dst)
            ip = socket.inet_ntop(socket.AF_INET6, raw_ip)
        return ip, port


class Buffer:
    _DEFAULT_LIMIT = 1 << 20

    def __init__(self, limit: int = _DEFAULT_LIMIT):
        if limit <= 0:
            raise ValueError("Limit cannot be <= 0")
        self._buffer = bytearray()
        self._limit = limit
        self._cond = asyncio.Condition()
        self._eof = False
        self._abort = False

    def __len__(self):
        return len(self._buffer)

    def readable(self):
        return len(self._buffer) > 0 or self._eof or self._abort

    def writable(self):
        return len(self._buffer) < self._limit or self._abort

    def at_eof(self):
        return (self._eof and not self._buffer) or self._abort

    async def write_eof(self):
        async with self._cond:
            self._eof = True
            self._cond.notify_all()

    async def wait_closed(self):
        async with self._cond:
            await self._cond.wait_for(self.at_eof)
            self._cond.notify_all()

    async def abort(self):
        async with self._cond:
            self._abort = True
            self._cond.notify_all()

    async def read(self, n: int = -1) -> bytes:
        if n == 0:
            return b""
        async with self._cond:
            await self._cond.wait_for(self.readable)
            if self._abort:
                raise ConnectionResetError("Connection aborted")
            if n < 0 or n > len(self._buffer):
                n = len(self._buffer)
            data = bytes(memoryview(self._buffer)[:n])
            del self._buffer[:n]
            self._cond.notify_all()
            return data

    async def write(self, data: bytes):
        if not data:
            return
        async with self._cond:
            await self._cond.wait_for(self.writable)
            if self._abort:
                raise ConnectionResetError("Connection aborted")
            self._buffer.extend(data)
            self._cond.notify_all()


class Proxy:
    async def _feeder(self, flow: str, reader: asyncio.StreamReader, buffer: Buffer):
        try:
            while data := await reader.read(MSS):
                await buffer.write(data)
        except Exception as e:
            logging.debug(f"Error in feeder {flow}: {e}")
            await buffer.abort()
        finally:
            await buffer.write_eof()
            await buffer.wait_closed()

    async def _writer(self, flow: str, buffer: Buffer, writer: asyncio.StreamWriter):
        try:
            while data := await buffer.read(MSS):
                await writer.drain()
                writer.write(data)
        except Exception as e:
            logging.debug(f"Error in writer {flow}: {e}")
            await buffer.abort()
            writer.transport.abort()
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception as e:
                logging.debug(f"Error in closing writer {flow}: {e}")

    async def _transport(self, flow: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            s: socket.socket = writer.get_extra_info("socket")
            s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            buffer = Buffer()
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self._feeder(flow, reader, buffer))
                tg.create_task(self._writer(flow, buffer, writer))
        except Exception as e:
            logging.debug(f"Error in transport {flow}: {e}")

    async def _handle_client(self, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter):
        try:
            peername = client_writer.get_extra_info("peername")
            logging.debug(f"New connection from [{peername[0]}]:{peername[1]}")
            is_v4 = "." in peername[0]
            s: socket.socket = client_writer.get_extra_info("socket")
            dst = Utility.get_original_dst(s, is_v4)
            writer_label = f"[{peername[0]}]:{peername[1]} → [{dst[0]}]:{dst[1]}"
            reader_label = f"[{peername[0]}]:{peername[1]} ← [{dst[0]}]:{dst[1]}"
        except Exception as e:
            logging.error(f"Failed to gather connection info: {e}")
            client_writer.transport.abort()
            client_writer.close()
            await client_writer.wait_closed()
            return

        try:
            proxy_reader, proxy_writer = await asyncio.open_connection(*dst)
        except Exception as e:
            logging.error(f"Failed to connect {writer_label}: {e}")
            client_writer.transport.abort()
            client_writer.close()
            await client_writer.wait_closed()
            return

        logging.info(f"Established {writer_label}")
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._transport(writer_label, client_reader, proxy_writer))
            tg.create_task(self._transport(reader_label, proxy_reader, client_writer))
        logging.info(f"Closed {writer_label}")

    async def run(self):
        self._loop = asyncio.get_running_loop()
        self._loop.set_task_factory(asyncio.eager_task_factory)
        server = await asyncio.start_server(self._handle_client, port=PORT)
        logging.info(f"Listening on port {PORT}")
        async with server:
            await server.serve_forever()


if __name__ == "__main__":
    logging.basicConfig(level=LOG_LEVEL)
    gc.set_debug(gc.DEBUG_STATS)
    gc.set_threshold(10000)
    try:
        uvloop.run(Proxy().run())
    except KeyboardInterrupt:
        logging.info("Shutting down")
    logging.shutdown()
