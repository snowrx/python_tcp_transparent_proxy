from concurrent.futures import ProcessPoolExecutor
import asyncio
import logging
import multiprocessing
import socket
import struct
import time


class config:
    port: int = 8081
    timeout: int = 3660
    limit: int = 1 << 20


class consts:
    SO_ORIGINAL_DST = 80
    SOL_IPV6 = 41
    V4_LEN = 16
    V6_LEN = 28


def get_original_dst(so: socket.socket, is_ipv4=True):
    if is_ipv4:
        dst = so.getsockopt(socket.SOL_IP, consts.SO_ORIGINAL_DST, consts.V4_LEN)
        port, raw_ip = struct.unpack_from("!2xH4s", dst)
        ip = socket.inet_ntop(socket.AF_INET, raw_ip)
    else:
        dst = so.getsockopt(consts.SOL_IPV6, consts.SO_ORIGINAL_DST, consts.V6_LEN)
        port, raw_ip = struct.unpack_from("!2xH4x16s", dst)
        ip = socket.inet_ntop(socket.AF_INET6, raw_ip)
    return ip, port


async def proxy(event: asyncio.Event, r: asyncio.StreamReader, w: asyncio.StreamWriter):
    code = 0
    try:
        s: socket.socket = w.get_extra_info("socket")
        s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, True)
        while not event.is_set() and (data := await asyncio.wait_for(r.read(config.limit), config.timeout)):
            w.write(data)
            del data
            await w.drain()
        r.feed_eof()
    except Exception as ex:
        logging.debug(ex)
        code |= 0b1
    finally:
        if not w.is_closing() and w.can_write_eof():
            try:
                w.write_eof()
                await w.drain()
                w.close()
                await w.wait_closed()
            except Exception as ex:
                logging.debug(ex)
                code |= 0b10
        event.set()
    return code


async def client(cr: asyncio.StreamReader, cw: asyncio.StreamWriter):
    c = cw.get_extra_info("peername")
    sn = cw.get_extra_info("sockname")
    is_ipv4 = "." in c[0]
    r = get_original_dst(cw.get_extra_info("socket"), is_ipv4)
    if r[0] == sn[0] and r[1] == sn[1]:
        logging.error(f"Blocked direct access from {c[0]}@{c[1]}")
        try:
            cw.close()
            await cw.wait_closed()
        except:
            pass
        return
    try:
        open_start = time.perf_counter()
        pr, pw = await asyncio.open_connection(host=r[0], port=r[1], limit=config.limit)
        open_finish = time.perf_counter()
        open_delay = open_finish - open_start
    except Exception:
        try:
            cw.close()
            await cw.wait_closed()
        except:
            pass
        logging.warning(f"Failed proxy in {c[0]}@{c[1]} <> {r[0]}@{r[1]}")
        return

    logging.info(f"Open proxy in {c[0]}@{c[1]} <> {r[0]}@{r[1]} ({round(open_delay * 1000)}ms)")
    event = asyncio.Event()
    proxy_start = time.perf_counter()
    codes = await asyncio.gather(proxy(event, cr, pw), proxy(event, pr, cw))
    proxy_end = time.perf_counter()
    proxy_duration = proxy_end - proxy_start
    logging.info(f"Close proxy in {c[0]}@{c[1]} ({codes[0]}) <> {r[0]}@{r[1]} ({codes[1]}) in {round(proxy_duration)}s")


def run(_):
    async def server():
        server = await asyncio.start_server(client, port=config.port, reuse_port=True, limit=config.limit)
        async with server:
            await server.serve_forever()

    asyncio.run(server())


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    nproc = multiprocessing.cpu_count() << 1
    logging.debug(f"{config.port=}, {config.timeout=}, {config.limit=}, {nproc=}")
    with ProcessPoolExecutor(nproc) as ex:
        ex.map(run, range(nproc))
