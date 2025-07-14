import asyncio
import logging
import socket
import struct

import uvloop

LOG = logging.DEBUG
PORT = 8081
LIFETIME = 86400
BUFFER = 1 << 14


class util:
    _SO_ORIGINAL_DST = 80
    _SOL_IPV6 = 41
    _V4_LEN = 16
    _V6_LEN = 28

    @staticmethod
    def get_original_dst(so: socket.socket):
        ip: str
        port: int
        match so.family:
            case socket.AF_INET:
                dst = so.getsockopt(socket.SOL_IP, util._SO_ORIGINAL_DST, util._V4_LEN)
                port, raw_ip = struct.unpack_from("!2xH4s", dst)
                ip = socket.inet_ntop(socket.AF_INET, raw_ip)
            case socket.AF_INET6:
                dst = so.getsockopt(util._SOL_IPV6, util._SO_ORIGINAL_DST, util._V6_LEN)
                port, raw_ip = struct.unpack_from("!2xH4x16s", dst)
                ip = socket.inet_ntop(socket.AF_INET6, raw_ip)
            case _:
                raise ValueError(f"Unsupported address family: {so.family}")
        return ip, port


class coupler:
    def __init__(self, label: str, cr: asyncio.StreamReader, cw: asyncio.StreamWriter, pr: asyncio.StreamReader, pw: asyncio.StreamWriter):
        self._label: str = label
        self._cr: asyncio.StreamReader = cr
        self._cw: asyncio.StreamWriter = cw
        self._pr: asyncio.StreamReader = pr
        self._pw: asyncio.StreamWriter = pw

    def status(self):
        h = self._cw.is_closing() ^ self._pw.is_closing()
        f = self._cw.is_closing() & self._pw.is_closing()
        return f << 1 | h

    async def run(self):
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._streaming(self._cr, self._pw))
            tg.create_task(self._streaming(self._pr, self._cw))

    async def _streaming(self, r: asyncio.StreamReader, w: asyncio.StreamWriter):
        try:
            so: socket.socket = w.get_extra_info("socket")
            so.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, True)
            w.transport.set_write_buffer_limits(BUFFER, BUFFER)
            async with asyncio.timeout(LIFETIME):
                while not self.status() and (b := await r.read(BUFFER)):
                    await w.drain()
                    w.write(b)
        except Exception as err:
            logging.error(f"Failed to stream: {self._label}, {err}")
        finally:
            logging.debug(f"Closing: {self._label} status={self.status()}")
            try:
                w.close()
                await w.wait_closed()
            except:
                pass


class server:
    async def accept(self, cr: asyncio.StreamReader, cw: asyncio.StreamWriter):
        try:
            dst: tuple[str, int] = util.get_original_dst(cw.get_extra_info("socket"))
        except Exception as err:
            logging.error(f"Failed to get original destination: {err}")
            cw.transport.abort()
            cw.close()
            await cw.wait_closed()
            return

        peer: tuple[str, int] = cw.get_extra_info("peername")
        sock: tuple[str, int] = cw.get_extra_info("sockname")
        label = f"{peer[0]}@{peer[1]} <-> {dst[0]}@{dst[1]}"

        if dst[0] == sock[0] and dst[1] == sock[1]:
            logging.error(f"Invalid destination: {label}")
            cw.transport.abort()
            cw.close()
            await cw.wait_closed()
            return

        try:
            pr, pw = await asyncio.open_connection(dst[0], dst[1], limit=BUFFER)
        except Exception as err:
            logging.error(f"Failed to open connection: {label}, {err}")
            cw.transport.abort()
            cw.close()
            await cw.wait_closed()
            return

        logging.info(f"Connected: {label}")
        await coupler(label, cr, cw, pr, pw).run()
        logging.info(f"Disconnected: {label}")

    async def start_server(self):
        server = await asyncio.start_server(self.accept, port=PORT, limit=BUFFER, reuse_port=True)
        logging.info(f"Listening on port {PORT}")
        async with server:
            await server.serve_forever()


if __name__ == "__main__":
    logging.basicConfig(level=LOG)
    uvloop.run(server().start_server())
    logging.shutdown()
