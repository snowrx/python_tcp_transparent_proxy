import asyncio
import logging
import socket
import struct

import uvloop

LOG = logging.DEBUG
PORT = 8081
LIFETIME = 86400
MSS = 1 << 16
WRITE_BUFFER_LIMIT = 1 << 30


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
    async def relay(self, flag: asyncio.Event, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            so: socket.socket = writer.get_extra_info("socket")
            so.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            writer.transport.set_write_buffer_limits(WRITE_BUFFER_LIMIT, WRITE_BUFFER_LIMIT)
            async with asyncio.timeout(LIFETIME):
                while not flag.is_set() and (data := await reader.read(MSS)):
                    await writer.drain()
                    writer.write(data)
            logging.debug("EOF")
        except Exception as e:
            logging.error(f"Error in relay: {type(e).__name__}: {e}")
        finally:
            flag.set()
            if not writer.is_closing():
                writer.write_eof()
                await writer.drain()
                writer.close()
                await writer.wait_closed()

    async def accept(self, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter):
        try:
            so: socket.socket = client_writer.get_extra_info("socket")
            peer: tuple[str, int] = client_writer.get_extra_info("peername")
            v4 = "." in peer[0]
            orig: tuple[str, int] = util.get_original_dst(so, v4)
            label: str = f"[{peer[0]}]:{peer[1]} -> [{orig[0]}]:{orig[1]}"
        except Exception as e:
            logging.error(f"Failed to get original destination: {type(e).__name__}: {e}")
            client_writer.close()
            await client_writer.wait_closed()
            return

        try:
            proxy_reader, proxy_writer = await asyncio.open_connection(*orig)
        except Exception as e:
            logging.error(f"Failed to connect to {label}: {type(e).__name__}: {e}")
            client_writer.close()
            await client_writer.wait_closed()
            return

        flag = asyncio.Event()
        logging.info(f"Connected {label}")
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self.relay(flag, proxy_reader, client_writer))
            tg.create_task(self.relay(flag, client_reader, proxy_writer))
        logging.info(f"Disconnected {label}")

    async def run(self):
        asyncio.get_running_loop().set_task_factory(asyncio.eager_task_factory)
        server = await asyncio.start_server(self.accept, port=PORT)
        logging.info(f"listening on port {PORT}")
        async with server:
            await server.serve_forever()


if __name__ == "__main__":
    logging.basicConfig(level=LOG)
    uvloop.run(proxy().run())
