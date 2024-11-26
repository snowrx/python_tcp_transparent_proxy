from concurrent.futures import ProcessPoolExecutor
import asyncio
import logging
import os
import socket
import struct
import time


PORT = 8081
TIMEOUT = 86400
READ_LIMIT = 0x40000
WRITE_LIMIT = 0x8000000

SO_ORIGINAL_DST = 80
SOL_IPV6 = 41
V4_LEN = 16
V6_LEN = 28
CID_ROTATE = 1000000


class v:
    pid = 0
    cid = 0


def get_original_dst(so: socket.socket, is_ipv4=True):
    if is_ipv4:
        dst = so.getsockopt(socket.SOL_IP, SO_ORIGINAL_DST, V4_LEN)
        port, raw_ip = struct.unpack_from("!2xH4s", dst)
        ip = socket.inet_ntop(socket.AF_INET, raw_ip)
    else:
        dst = so.getsockopt(SOL_IPV6, SO_ORIGINAL_DST, V6_LEN)
        port, raw_ip = struct.unpack_from("!2xH4x16s", dst)
        ip = socket.inet_ntop(socket.AF_INET6, raw_ip)
    return ip, port


async def proxy(cid: int, fid: int, barrier: asyncio.Barrier, r: asyncio.StreamReader, w: asyncio.StreamWriter):
    code = 0
    try:
        s: socket.socket = w.get_extra_info("socket")
        s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, True)
        w.transport.set_write_buffer_limits(WRITE_LIMIT)
        async with asyncio.timeout(TIMEOUT):
            while data := await r.read(READ_LIMIT):
                w.write(memoryview(data))
                await w.drain()
        r.feed_eof()
        logging.debug(f"[{v.pid}:{cid}:{fid}] EOF")
    except Exception as err:
        logging.debug(f"[{v.pid}:{cid}:{fid}] error in loop: {err=}, {type(err)=}")
        code |= 0b1
    finally:
        # before wait
        if not w.is_closing():
            try:
                w.write_eof()
                await w.drain()
            except:
                code |= 0b10
        # wait
        await barrier.wait()
        # after wait
        if not w.is_closing():
            try:
                w.close()
                await w.wait_closed()
            except:
                code |= 0b100
    return code


async def client(cr: asyncio.StreamReader, cw: asyncio.StreamWriter):
    cid = v.cid
    v.cid = (v.cid + 1) % CID_ROTATE
    src = cw.get_extra_info("peername")
    srv = cw.get_extra_info("sockname")
    soc = cw.get_extra_info("socket")
    is_ipv4 = "." in src[0]
    dst = get_original_dst(soc, is_ipv4)

    if dst[0] == srv[0] and dst[1] == srv[1]:
        logging.error(f"[{v.pid}:{cid}] Blocked direct access from {src[0]}@{src[1]}")
        try:
            cw.transport.abort()
            cw.close()
            await cw.wait_closed()
        except:
            pass
        return

    try:
        open_start = time.perf_counter()
        pr, pw = await asyncio.open_connection(host=dst[0], port=dst[1], limit=READ_LIMIT)
        open_delay = time.perf_counter() - open_start
    except:
        try:
            cw.transport.abort()
            cw.close()
            await cw.wait_closed()
        except:
            pass
        logging.warning(f"[{v.pid}:{cid}] Failed proxy in {src[0]}@{src[1]} <> {dst[0]}@{dst[1]}")
        return

    logging.info(f"[{v.pid}:{cid}] Open proxy in {src[0]}@{src[1]} <> {dst[0]}@{dst[1]} ({round(open_delay * 1000)}ms)")
    barrier = asyncio.Barrier(2)
    proxy_start = time.perf_counter()
    async with asyncio.TaskGroup() as tg:
        r0 = tg.create_task(proxy(cid, 0, barrier, cr, pw))
        r1 = tg.create_task(proxy(cid, 1, barrier, pr, cw))
    proxy_duration = time.perf_counter() - proxy_start
    logging.info(f"[{v.pid}:{cid}] Close proxy in {src[0]}@{src[1]} ({r0.result()}) <> {dst[0]}@{dst[1]} ({r1.result()}) in {round(proxy_duration)}s")


def run(pid):
    async def server():
        v.pid = pid
        server = await asyncio.start_server(client, port=PORT, reuse_port=True, limit=READ_LIMIT)
        async with server:
            await server.serve_forever()
        for t in asyncio.all_tasks():
            t.cancel()

    asyncio.run(server())


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    try:
        workers = len(os.sched_getaffinity(0))
    except:
        workers = os.cpu_count() or 1
    logging.debug(f"{PORT=}, {TIMEOUT=}, {READ_LIMIT=}, {WRITE_LIMIT=}, {workers=}")
    with ProcessPoolExecutor(workers) as ex:
        ex.map(run, range(workers))
