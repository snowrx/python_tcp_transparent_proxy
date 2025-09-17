import asyncio
import logging
import socket
import struct

import uvloop

LOG = logging.DEBUG
PORT = 8081
MSS = 64000


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
            so.setsockopt(socket.SOL_TCP, socket.TCP_QUICKACK, 1)
            writer.transport.set_write_buffer_limits(MSS)
            while not writer.is_closing() and (data := await reader.read(MSS)):
                await writer.drain()
                writer.write(data)
        except Exception as e:
            logging.error(f"Failed to transport {label}: {e}")
        finally:
            if not writer.is_closing():
                writer.write_eof()
                await writer.drain()
                writer.close()
                await writer.wait_closed()

    async def _accept(self, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter):
        try:
            peername = client_writer.get_extra_info("peername")
            so: socket.socket = client_writer.get_extra_info("socket")
            v4 = "." in peername[0]
            origname = util.get_original_dst(so, v4)
            read_label = f"[{peername[0]}]:{peername[1]} ← [{origname[0]}]:{origname[1]}"
            write_label = f"[{peername[0]}]:{peername[1]} → [{origname[0]}]:{origname[1]}"
        except Exception as e:
            logging.error(f"Failed to prepare: {e}")
            client_writer.close()
            await client_writer.wait_closed()
            return

        try:
            proxy_reader, proxy_writer = await asyncio.open_connection(*origname)
        except Exception as e:
            logging.error(f"Failed to connect {write_label}")
            client_writer.close()
            await client_writer.wait_closed()
            return

        logging.info(f"Established {write_label}")
        start = self._loop.time()
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._transport(read_label, proxy_reader, client_writer))
            tg.create_task(self._transport(write_label, client_reader, proxy_writer))
        end = self._loop.time()
        logging.info(f"Closed {write_label} ({end - start:.2f}s)")

    async def run(self):
        self._loop = asyncio.get_running_loop()
        self._loop.set_task_factory(asyncio.eager_task_factory)
        server = await asyncio.start_server(self._accept, port=PORT)
        async with server:
            logging.info(f"Listening on {PORT}")
            await server.serve_forever()


if __name__ == "__main__":
    logging.basicConfig(level=LOG)
    try:
        uvloop.run(proxy().run())
    except KeyboardInterrupt:
        logging.info("Shutting down.")
    except Exception as e:
        logging.error(f"Unhandled exception: {e}")
    logging.shutdown()
