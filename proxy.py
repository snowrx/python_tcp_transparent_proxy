import asyncio
import logging
import socket
import struct

import uvloop


LOG = logging.DEBUG
PORT = 8081
MSS = 1 << 14


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
    async def _transport(self, label: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            so: socket.socket = writer.get_extra_info("socket")
            so.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            while not writer.is_closing() and (data := await reader.read(MSS)):
                await writer.drain()
                writer.write(data)
        except Exception as e:
            logging.error(f"Failed to transport {label}: {e}")
        finally:
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()

    async def _handle_client(self, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter):
        try:
            so: socket.socket = client_writer.get_extra_info("socket")
            peer = client_writer.get_extra_info("peername")
            v4 = "." in peer[0]
            dest = util.get_original_dst(so, v4)
            w_label = f"[{peer[0]}]:{peer[1]} -> [{dest[0]}]:{dest[1]}"
            r_label = f"[{peer[0]}]:{peer[1]} <- [{dest[0]}]:{dest[1]}"
        except Exception as e:
            logging.error(f"Failed to get original destination: {e}")
            client_writer.close()
            await client_writer.wait_closed()
            return

        try:
            proxy_reader, proxy_writer = await asyncio.open_connection(*dest)
        except Exception as e:
            logging.error(f"Failed to connect {w_label}: {e}")
            client_writer.close()
            await client_writer.wait_closed()
            return

        logging.info(f"Connected {w_label}")
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._transport(w_label, client_reader, proxy_writer))
            tg.create_task(self._transport(r_label, proxy_reader, client_writer))
        logging.info(f"Disconnected {w_label}")

    async def _start_server(self):
        server = await asyncio.start_server(self._handle_client, port=PORT)
        async with server:
            await server.serve_forever()

    async def run(self):
        self._loop = asyncio.get_running_loop()
        self._loop.set_task_factory(asyncio.eager_task_factory)
        await self._start_server()


if __name__ == "__main__":
    logging.basicConfig(level=LOG)
    uvloop.run(proxy().run())
    logging.shutdown()
