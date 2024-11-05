from concurrent.futures import ProcessPoolExecutor
import asyncio
import logging
import multiprocessing
import socket
import struct
import time


class config:
    port = 8081
    timeout = 3660
    limit = 1 << 18
    cid_rotate = 1000000


class consts:
    SO_ORIGINAL_DST = 80
    SOL_IPV6 = 41
    V4_LEN = 16
    V6_LEN = 28


class v:
    pid = 0
    cid = 0
    active = 0


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


async def proxy(cid: int, fid: int, r_state: asyncio.Event, w_state: asyncio.Event, r: asyncio.StreamReader, w: asyncio.StreamWriter):
    code = 0
    try:
        s: socket.socket = w.get_extra_info("socket")
        s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, True)
        logging.debug(f"[{v.pid}:{cid}:{fid}] start loop")
        while data := await asyncio.wait_for(r.read(config.limit), config.timeout):
            w.write(data)
            await w.drain()
        r.feed_eof()
        logging.debug(f"[{v.pid}:{cid}:{fid}] feed eof")
    except asyncio.TimeoutError:
        logging.debug(f"[{v.pid}:{cid}:{fid}] read timeout")
        code |= 0b1
    except Exception as ex:
        logging.debug(f"[{v.pid}:{cid}:{fid}] error in loop: {ex}")
        code |= 0b1
    finally:
        r_state.set()
        if not w.is_closing():
            logging.debug(f"[{v.pid}:{cid}:{fid}] write eof")
            try:
                w.write_eof()
                await w.drain()
            except Exception as ex:
                logging.debug(f"[{v.pid}:{cid}:{fid}] error in write eof: {ex}")
                code |= 0b10
        if not w_state.is_set():
            logging.debug(f"[{v.pid}:{cid}:{fid}] waiting other side")
        await w_state.wait()
        if not w.is_closing():
            logging.debug(f"[{v.pid}:{cid}:{fid}] closing")
            try:
                w.close()
                await w.wait_closed()
            except Exception as ex:
                logging.debug(f"[{v.pid}:{cid}:{fid}] error in close: {ex}")
                code |= 0b10
    return code


async def client(cr: asyncio.StreamReader, cw: asyncio.StreamWriter):
    cid = v.cid
    v.cid = (v.cid + 1) % config.cid_rotate
    c = cw.get_extra_info("peername")
    sn = cw.get_extra_info("sockname")
    is_ipv4 = "." in c[0]
    r = get_original_dst(cw.get_extra_info("socket"), is_ipv4)

    if r[0] == sn[0] and r[1] == sn[1]:
        logging.error(f"[{v.pid}:{cid}] Blocked direct access from {c[0]}@{c[1]}")
        try:
            cw.close()
            await cw.wait_closed()
        except:
            pass
        return

    try:
        open_start = time.perf_counter()
        pr, pw = await asyncio.open_connection(host=r[0], port=r[1], limit=config.limit)
        open_delay = time.perf_counter() - open_start
    except Exception as ex:
        logging.debug(f"[{v.pid}:{cid}] error in open: {ex}")
        try:
            cw.close()
            await cw.wait_closed()
        except:
            pass
        logging.warning(f"[{v.pid}:{cid}] Failed proxy in {c[0]}@{c[1]} <> {r[0]}@{r[1]}")
        return

    logging.info(f"[{v.pid}:{cid}] Open proxy in {c[0]}@{c[1]} <> {r[0]}@{r[1]} ({round(open_delay * 1000)}ms)")
    logging.debug(f"[{v.pid}:{cid}] active: {v.active} -> {v.active + 1}")
    v.active += 1
    proxy_start = time.perf_counter()
    c_state = asyncio.Event()
    p_state = asyncio.Event()
    codes = await asyncio.gather(proxy(cid, 0, c_state, p_state, cr, pw), proxy(cid, 1, p_state, c_state, pr, cw))
    proxy_duration = time.perf_counter() - proxy_start
    logging.info(f"[{v.pid}:{cid}] Close proxy in {c[0]}@{c[1]} ({codes[0]}) <> {r[0]}@{r[1]} ({codes[1]}) in {round(proxy_duration)}s")
    v.active -= 1
    logging.debug(f"[{v.pid}:{cid}] active: {v.active + 1} -> {v.active}")


def run(pid):
    async def server():
        v.pid = pid
        server = await asyncio.start_server(client, port=config.port, reuse_port=True, backlog=3, limit=config.limit)
        async with server:
            await server.serve_forever()

    asyncio.run(server())


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    nproc = multiprocessing.cpu_count()
    logging.debug(f"{config.port=}, {config.timeout=}, {config.limit=}, {nproc=}")
    with ProcessPoolExecutor(nproc) as ex:
        ex.map(run, range(nproc))
