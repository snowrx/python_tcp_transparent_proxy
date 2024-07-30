from concurrent.futures import ProcessPoolExecutor
from time import time
import asyncio
import logging
import multiprocessing
import socket
import struct
import configparser


class config:
    port: int = 8081
    timeout: int = 3660
    limit: int = 1 << 20
    conf: str = "proxy.conf"


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


async def proxy(r: asyncio.StreamReader, w: asyncio.StreamWriter):
    code = 0b0000
    try:
        s: socket.socket = w.get_extra_info("socket")
        s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, True)
        while not w.is_closing() and (data := await asyncio.wait_for(r.read(config.limit), config.timeout)):
            w.write(data)
            del data
            await w.drain()
    except asyncio.TimeoutError:
        code |= 0b0010
    except Exception as ex:
        code |= 0b0001
        logging.debug(ex)
    finally:
        if r.at_eof():
            try:
                r.feed_eof()
            except:
                code |= 0b0100
        if not w.is_closing():
            try:
                w.write_eof()
                await w.drain()
                w.close()
                await w.wait_closed()
            except:
                code |= 0b1000
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
        pr, pw = await asyncio.open_connection(host=r[0], port=r[1], limit=config.limit)
    except:
        try:
            cw.close()
            await cw.wait_closed()
        except:
            pass
        logging.warning(f"Failed proxy in {c[0]}@{c[1]} <-> {r[0]}@{r[1]}")
        return

    logging.info(f"Open proxy in {c[0]}@{c[1]} <-> {r[0]}@{r[1]}")
    start = time()
    codes = await asyncio.gather(proxy(cr, pw), proxy(pr, cw))
    end = time()
    duration = end - start
    logging.info(f"Close proxy in {c[0]}@{c[1]} ({codes[0]}) <-> {r[0]}@{r[1]} ({codes[1]}) in {round(duration)}s")


async def server():
    server = await asyncio.start_server(client, port=config.port, reuse_port=True, limit=config.limit)
    async with server:
        await server.serve_forever()


def run(_):
    asyncio.run(server())


logging.basicConfig(level=logging.DEBUG)
nproc = multiprocessing.cpu_count()
conf = configparser.ConfigParser()
conf.read(config.conf)
if "DEFAULT" in conf:
    if "port" in conf["DEFAULT"]:
        config.port = int(conf["DEFAULT"]["port"])
    if "timeout" in conf["DEFAULT"]:
        config.timeout = int(conf["DEFAULT"]["timeout"])
    if "limit" in conf["DEFAULT"]:
        config.limit = int(conf["DEFAULT"]["limit"])
    if "nproc" in conf["DEFAULT"]:
        nproc = int(conf["DEFAULT"]["nproc"])

logging.debug(f"{config.port=}, {config.timeout=}, {config.limit=}, {nproc=}")
with ProcessPoolExecutor(nproc) as ex:
    ex.map(run, range(nproc))
