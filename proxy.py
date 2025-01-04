from concurrent.futures import ProcessPoolExecutor
import asyncio
import logging
import os
import socket
import struct
import time

_DEFAULT_LIMIT = 2**16

PORT = 8081
LIFETIME = 86400


class Listener:
    _SO_ORIGINAL_DST = 80
    _SOL_IPV6 = 41
    _V4_LEN = 16
    _V6_LEN = 28

    _live = 0

    def run(self, cpu=None):
        async def _server():
            server = await asyncio.start_server(self._client, port=PORT, reuse_port=True)
            async with server:
                await server.serve_forever()
            for t in asyncio.all_tasks():
                t.cancel()
                logging.debug(f"Cancelled {t.get_name()}")

        logging.info(f"Listening {PORT=}")
        if cpu is not None:
            os.sched_setaffinity(0, {cpu})
            logging.info(f"CPU affinity {cpu=}")
        asyncio.run(_server())

    async def _client(self, cr: asyncio.StreamReader, cw: asyncio.StreamWriter):
        src = cw.get_extra_info("peername")
        srv = cw.get_extra_info("sockname")
        soc = cw.get_extra_info("socket")
        is_ipv4 = "." in src[0]
        dst = self._get_original_dst(soc, is_ipv4)
        w_label = f"{src[0]}@{src[1]} > {dst[0]}@{dst[1]}"
        r_label = f"{dst[0]}@{dst[1]} > {src[0]}@{src[1]}"

        if dst[0] == srv[0] and dst[1] == srv[1]:
            logging.warning(f"Blocked {w_label}")
            cw.write(b"HTTP/1.1 403 Forbidden\r\n\r\n")
            cw.write_eof()
            await cw.drain()
            await writer_close(cw)
            return

        try:
            pr, pw = await asyncio.open_connection(host=dst[0], port=dst[1])
        except:
            logging.warning(f"Failed {w_label}")
            cw.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
            cw.write_eof()
            await cw.drain()
            await writer_close(cw)
            return

        self._live += 1
        writer = Channel(w_label, cr, pw)
        reader = Channel(r_label, pr, cw)
        logging.info(f"Established {w_label}")
        logging.debug(f"live={self._live}")

        proxy_start = time.perf_counter()
        async with asyncio.TaskGroup() as tg:
            _ = (tg.create_task(writer.run()), tg.create_task(reader.run()))
        proxy_time = round(time.perf_counter() - proxy_start)
        self._live -= 1
        logging.info(f"Closed {proxy_time}s {w_label}")
        logging.debug(f"live={self._live}")

    def _get_original_dst(self, so: socket.socket, is_ipv4=True):
        if is_ipv4:
            dst = so.getsockopt(socket.SOL_IP, self._SO_ORIGINAL_DST, self._V4_LEN)
            port, raw_ip = struct.unpack_from("!2xH4s", dst)
            ip = socket.inet_ntop(socket.AF_INET, raw_ip)
        else:
            dst = so.getsockopt(self._SOL_IPV6, self._SO_ORIGINAL_DST, self._V6_LEN)
            port, raw_ip = struct.unpack_from("!2xH4x16s", dst)
            ip = socket.inet_ntop(socket.AF_INET6, raw_ip)
        return ip, port


class Channel:
    _label: str
    _r: asyncio.StreamReader
    _w: asyncio.StreamWriter

    def __init__(self, label: str, r: asyncio.StreamReader, w: asyncio.StreamWriter):
        self._label = label
        self._r = r
        self._w = w

    async def run(self):
        try:
            s: socket.socket = self._w.get_extra_info("socket")
            s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, True)

            async with asyncio.timeout(LIFETIME):
                while data := await self._r.read(_DEFAULT_LIMIT):
                    self._w.write(data)
                    await self._w.drain()

                self._r.feed_eof()
                self._w.write_eof()
                await self._w.drain()
                logging.debug(f"EOF {self._label}")

        except Exception as err:
            logging.debug(f"Error {err} {self._label}")

        finally:
            await writer_close(self._w)
            logging.debug(f"Closed channel {self._label}")


async def writer_close(writer: asyncio.StreamWriter):
    try:
        if not writer.is_closing():
            writer.close()
            await writer.wait_closed()
    except Exception as err:
        logging.debug(f"Error {err}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    cpu_list = os.sched_getaffinity(0)
    with ProcessPoolExecutor(len(cpu_list)) as executor:
        executor.map(Listener().run, cpu_list)
