import asyncio
import logging
import socket
import struct

import uvloop

LOG = logging.DEBUG
PORT = 8081
LIFETIME = 86400
LIMIT = 1 << 18


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
    async def streaming(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            so: socket.socket = writer.get_extra_info("socket")
            so.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            writer.transport.set_write_buffer_limits(LIMIT, LIMIT)
            async with asyncio.timeout(LIFETIME):
                rt = asyncio.create_task(reader.read(LIMIT))
                await asyncio.sleep(0)
                while data := await rt:
                    rt = asyncio.create_task(reader.read(LIMIT))
                    await writer.drain()
                    writer.write(data)
        except Exception as e:
            logging.error(f"streaming: {type(e).__name__}: {e}")
        finally:
            try:
                writer.write_eof()
                await writer.drain()
                writer.close()
                await writer.wait_closed()
            except:
                pass

    async def accept(self, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter):
        try:
            so: socket.socket = client_writer.get_extra_info("socket")
            peer: tuple[str, int] = client_writer.get_extra_info("peername")
            v4 = "." in peer[0]
            orig: tuple[str, int] = util.get_original_dst(so, v4)
        except Exception as e:
            logging.error(f"get_original_dst: {type(e).__name__}: {e}")
            client_writer.close()
            await client_writer.wait_closed()
            return

        try:
            proxy_reader, proxy_writer = await asyncio.open_connection(*orig, limit=LIMIT)
        except Exception as e:
            logging.error(f"open_connection: {type(e).__name__}: {e}")
            client_writer.close()
            await client_writer.wait_closed()
            return

        logging.info(f"open [{peer[0]}]:{peer[1]} <-> [{orig[0]}]:{orig[1]}")
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self.streaming(client_reader, proxy_writer))
            tg.create_task(self.streaming(proxy_reader, client_writer))
        logging.info(f"close [{peer[0]}]:{peer[1]} <-> [{orig[0]}]:{orig[1]}")

    async def run(self):
        asyncio.get_running_loop().set_task_factory(asyncio.eager_task_factory)
        server = await asyncio.start_server(self.accept, port=PORT, limit=LIMIT)
        logging.info(f"listening on port {PORT}")
        async with server:
            await server.serve_forever()


if __name__ == "__main__":
    logging.basicConfig(level=LOG)
    uvloop.run(proxy().run())
