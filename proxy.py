from concurrent.futures import ProcessPoolExecutor
import asyncio
import logging
import socket
import struct
import time

PORT = 8081
LIFETIME = 86400
WORKERS = 1
READ_LIMIT = 2**18
WRITE_LIMIT = 2**10


class Listener:
    _SO_ORIGINAL_DST = 80
    _SOL_IPV6 = 41
    _V4_LEN = 16
    _V6_LEN = 28

    _pid = 0

    def run(self, pid=0):
        async def _server():
            server = await asyncio.start_server(self._client, port=PORT, reuse_port=True, limit=READ_LIMIT)
            async with server:
                await server.serve_forever()
            for t in asyncio.all_tasks():
                t.cancel()
                logging.debug(f"[{self._pid}] Cancelled {t.get_name()}")

        self._pid = pid
        logging.info(f"[{self._pid}] Listening {PORT=}")
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
            logging.warning(f"[{self._pid}] Blocked {w_label}")
            await writer_close(cw)
            return

        try:
            pr, pw = await asyncio.open_connection(host=dst[0], port=dst[1], limit=READ_LIMIT)
        except:
            logging.warning(f"[{self._pid}] Connection failed {w_label}")
            await writer_close(cw)
            return

        barrier = asyncio.Barrier(2)
        writer = Channel(self._pid, w_label, cr, pw, barrier)
        reader = Channel(self._pid, r_label, pr, cw, barrier)
        logging.info(f"[{self._pid}] Established {w_label}")

        proxy_start = time.perf_counter()
        async with asyncio.TaskGroup() as tg:
            _ = (tg.create_task(writer.run()), tg.create_task(reader.run()))
        proxy_time = round(time.perf_counter() - proxy_start)
        logging.info(f"[{self._pid}] Closed {w_label} {proxy_time}s")

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
    _pid: int
    _label: str
    _r: asyncio.StreamReader
    _w: asyncio.StreamWriter
    _barrier: asyncio.Barrier

    def __init__(self, pid: int, label: str, r: asyncio.StreamReader, w: asyncio.StreamWriter, barrier: asyncio.Barrier):
        self._pid = pid
        self._label = label
        self._r = r
        self._w = w
        self._barrier = barrier

    async def run(self):
        try:
            s: socket.socket = self._w.get_extra_info("socket")
            s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, True)
            self._w.transport.set_write_buffer_limits(WRITE_LIMIT)

            async with asyncio.timeout(LIFETIME):
                while data := await self._r.read(READ_LIMIT):
                    write_start = time.perf_counter()
                    self._w.write(data)
                    await self._w.drain()
                    write_time = round((time.perf_counter() - write_start) * 1000)
                    if write_time > 100:
                        logging.warning(f"[{self._pid}] Slow write {self._label} {write_time}ms")
                logging.debug(f"[{self._pid}] EOF {self._label}")
                self._w.write_eof()
                await self._w.drain()
                self._r.feed_eof()
                await self._barrier.wait()

        except Exception as err:
            logging.debug(f"[{self._pid}] Error {self._label} {err}")
            await self._barrier.abort()

        finally:
            await writer_close(self._w)

        logging.debug(f"[{self._pid}] Closed channel {self._label}")


async def writer_close(writer: asyncio.StreamWriter):
    try:
        if not writer.is_closing():
            writer.close()
            await writer.wait_closed()
    except Exception as err:
        logging.debug(f"Error {err}")


def main():
    logging.basicConfig(level=logging.DEBUG)
    if WORKERS > 1:
        with ProcessPoolExecutor(WORKERS) as executor:
            executor.map(Listener().run, range(WORKERS))
    else:
        Listener().run()


if __name__ == "__main__":
    main()
