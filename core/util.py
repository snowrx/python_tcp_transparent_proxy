from gevent import socket
import struct
import ipaddress
from functools import cache

SO_ORIGINAL_DST = 80
V4_LEN = 16
V4_FMT = "!2xH4s"
V6_LEN = 28
V6_FMT = "!2xH4x16s"


def get_original_dst(sock: socket.socket, family: socket.AddressFamily) -> tuple[str, int]:
    ip: str
    port: int

    match family:
        case socket.AF_INET:
            dst = sock.getsockopt(socket.SOL_IP, SO_ORIGINAL_DST, V4_LEN)
            port, raw_ip = struct.unpack_from(V4_FMT, dst)
            ip = socket.inet_ntop(socket.AF_INET, raw_ip)
        case socket.AF_INET6:
            dst = sock.getsockopt(socket.IPPROTO_IPV6, SO_ORIGINAL_DST, V6_LEN)
            port, raw_ip = struct.unpack_from(V6_FMT, dst)
            ip = socket.inet_ntop(socket.AF_INET6, raw_ip)
        case _:
            raise RuntimeError(f"Unsupported address family: {family}")

    return ip, port


@cache
def ipv4_mapped(addr: str) -> bool:
    return ipaddress.IPv6Address(addr).ipv4_mapped is not None
