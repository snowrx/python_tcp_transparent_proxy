import asyncio
import logging
import socket
import struct

import uvloop


LOG = logging.DEBUG
PORT = 8081
LIMIT = 1 << 20


class util:
    _SO_ORIGINAL_DST = 80
    _SOL_IPV6 = 41
    _V4_LEN = 16
    _V6_LEN = 28

    @staticmethod
    def get_original_dst(so: socket.socket, v4: bool = True):
        ip: str
        port: int
        if v4:
            dst = so.getsockopt(socket.SOL_IP, util._SO_ORIGINAL_DST, util._V4_LEN)
            port, raw_ip = struct.unpack_from("!2xH4s", dst)
            ip = socket.inet_ntop(socket.AF_INET, raw_ip)
        else:
            dst = so.getsockopt(util._SOL_IPV6, util._SO_ORIGINAL_DST, util._V6_LEN)
            port, raw_ip = struct.unpack_from("!2xH4x16s", dst)
            ip = socket.inet_ntop(socket.AF_INET6, raw_ip)
        return ip, port


class proxy:
    async def transfer(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            so: socket.socket = writer.get_extra_info("socket")
            so.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            while not writer.is_closing() and (v := memoryview(await reader.read(LIMIT))):
                await writer.drain()
                writer.write(v)
        except Exception as e:
            logging.error(f"transfer: {type(e).__name__}: {e}")
        finally:
            if not writer.is_closing():
                try:
                    writer.write_eof()
                    await writer.drain()
                    writer.close()
                    await writer.wait_closed()
                except Exception as e:
                    logging.error(f"close: {type(e).__name__}: {e}")

    async def accept(self, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter):
        try:
            client: tuple[str, int] = client_writer.get_extra_info("peername")
            v4 = "." in client[0]
            so: socket.socket = client_writer.get_extra_info("socket")
            dest: tuple[str, int] = util.get_original_dst(so, v4)
            label = f"[{client[0]}]:{client[1]} <-> [{dest[0]}]:{dest[1]}"
            proxy_reader, proxy_writer = await asyncio.open_connection(*dest, limit=LIMIT)
        except Exception as e:
            logging.error(f"accept: {type(e).__name__}: {e}")
            client_writer.close()
            await client_writer.wait_closed()
            return

        logging.info(f"accept: {label}")
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self.transfer(client_reader, proxy_writer))
            tg.create_task(self.transfer(proxy_reader, client_writer))
        logging.info(f"close: {label}")

    async def run(self):
        asyncio.get_running_loop().set_task_factory(asyncio.eager_task_factory)
        server = await asyncio.start_server(self.accept, port=PORT, limit=LIMIT)
        logging.info(f"listen: {PORT=}")
        async with server:
            await server.serve_forever()


if __name__ == "__main__":
    logging.basicConfig(level=LOG)
    uvloop.run(proxy().run())
    logging.shutdown()
