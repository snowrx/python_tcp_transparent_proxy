import logging
import struct
import sys

import gevent
from gevent import socket
from gevent.socket import wait_read, wait_write
from gevent.server import StreamServer
from gevent.threadpool import ThreadPool

LOG_LEVEL = logging.INFO
PORT = 8081
POOL_SIZE = 4
BUFFER_SIZE = 1 << 20
IDLE_TIMEOUT = 43200
FAST_TIMEOUT = 1e-3

SO_ORIGINAL_DST = 80
SOL_IPV6 = 41
V4_LEN = 16
V4_FMT = "!2xH4s"
V6_LEN = 28
V6_FMT = "!2xH4x16s"
FAMILY = [socket.AF_INET, socket.AF_INET6]


def get_original_dst(sock: socket.socket):
    """
    LinuxのSO_ORIGINAL_DSTソケットオプションを使用して、
    透過プロキシされた接続の元の宛先アドレスとポートを取得します。

    :param sock: クライアント接続のソケットオブジェクト。
    :return: 元の宛先の(IP, ポート)のタプル。
    """
    ip: str = ""
    port: int = 0

    match sock.family:
        case socket.AF_INET:
            dst = sock.getsockopt(socket.SOL_IP, SO_ORIGINAL_DST, V4_LEN)
            port, raw_ip = struct.unpack_from(V4_FMT, dst)
            ip = socket.inet_ntop(socket.AF_INET, raw_ip)
        case socket.AF_INET6:
            dst = sock.getsockopt(SOL_IPV6, SO_ORIGINAL_DST, V6_LEN)
            port, raw_ip = struct.unpack_from(V6_FMT, dst)
            ip = socket.inet_ntop(socket.AF_INET6, raw_ip)
        case _:
            raise Exception(f"Unknown socket family: {sock.family}")

    return ip, port


def transfer(label: str, src_sock: socket.socket, dst_sock: socket.socket):
    """
    一方のソケットからもう一方のソケットへデータを転送します。
    geventを使用して、ノンブロッキングI/Oを効率的に処理します。
    データ転送が完了するか、アイドルタイムアウトに達すると終了します。

    :param label: ログ出力用の転送方向を示すラベル。
    :param src_sock: データ受信元のソケット。
    :param dst_sock: データ送信先のソケット。
    """
    logging.debug(f"Start transfer {label}")

    try:
        eof = False
        wait_read(src_sock.fileno(), IDLE_TIMEOUT)

        while buffer := bytearray(src_sock.recv(BUFFER_SIZE)):
            while buffer:
                wait_write(dst_sock.fileno())
                sent = dst_sock.send(buffer)
                del buffer[:sent]
                if not eof and len(buffer) < BUFFER_SIZE:
                    try:
                        wait_read(src_sock.fileno(), FAST_TIMEOUT)
                        if data := src_sock.recv(BUFFER_SIZE - len(buffer)):
                            buffer.extend(data)
                        else:
                            eof = True
                    except (BlockingIOError, TimeoutError):
                        pass
            if eof:
                break

            wait_read(src_sock.fileno(), IDLE_TIMEOUT)
        eof = True

    except Exception as e:
        logging.debug(f"Failed to transfer {label}: {e}")

    finally:
        try:
            dst_sock.shutdown(socket.SHUT_RDWR)
        except:
            pass

    logging.debug(f"End transfer {label}")


def accept(client_sock: socket.socket, client_addr: tuple[str, int]):
    """
    StreamServerによって新しい接続ごとに呼び出されるハンドラ関数。
    元の宛先を取得し、そこへの新しい接続を確立して、
    クライアントと宛先サーバー間の双方向のデータ転送を開始します。

    :param client_sock: 新しいクライアント接続のソケットオブジェクト。
    :param client_addr: クライアントの(IP, ポート)のタプル。
    """
    logging.debug(f"New connection from [{client_addr[0]}]:{client_addr[1]}")

    try:
        client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

        srv_addr = client_sock.getsockname()
        dst_addr = get_original_dst(client_sock)
        if dst_addr[0] == srv_addr[0] and dst_addr[1] == srv_addr[1]:
            client_sock.shutdown(socket.SHUT_RDWR)
            client_sock.close()
            logging.error(f"Blocked direct connection from [{client_addr[0]}]:{client_addr[1]}")
            return

        up_label = f"[{client_addr[0]}]:{client_addr[1]} -> [{dst_addr[0]}]:{dst_addr[1]}"
        down_label = f"[{client_addr[0]}]:{client_addr[1]} <- [{dst_addr[0]}]:{dst_addr[1]}"
    except Exception as e:
        client_sock.shutdown(socket.SHUT_RDWR)
        client_sock.close()
        logging.error(f"Failed to prepare connection [{client_addr[0]}]:{client_addr[1]}: {e}")
        return

    try:
        proxy_sock = socket.socket(client_sock.family, client_sock.type)
        proxy_sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        proxy_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        proxy_sock.setsockopt(socket.SOL_TCP, socket.TCP_FASTOPEN_CONNECT, 1)
        proxy_sock.bind(("", client_addr[1]))

        connected = False
        buffer = bytearray()
        try:
            wait_read(client_sock.fileno(), FAST_TIMEOUT)
            if buffer := bytearray(client_sock.recv(BUFFER_SIZE)):
                wait_write(proxy_sock.fileno())
                sent = proxy_sock.sendto(buffer, socket.MSG_FASTOPEN, dst_addr)
                del buffer[:sent]
                connected = True
                logging.info(f"Connected {up_label} with TFO, sent {sent} bytes")
        except (BlockingIOError, TimeoutError, OSError) as e:
            logging.debug(f"Failed to TFO {up_label}: {e}")

        if not connected:
            proxy_sock.connect(dst_addr)
            connected = True
            logging.info(f"Connected {up_label}")

        if buffer:
            wait_write(proxy_sock.fileno())
            proxy_sock.sendall(buffer)
            logging.debug(f"Unsent {len(buffer)} bytes was sent to {up_label}")
            buffer.clear()

    except Exception as e:
        client_sock.shutdown(socket.SHUT_RDWR)
        client_sock.close()
        logging.error(f"Failed to connect {up_label}: {e}")
        return

    try:
        gevent.joinall(
            [
                gevent.spawn(transfer, down_label, proxy_sock, client_sock),
                gevent.spawn(transfer, up_label, client_sock, proxy_sock),
            ]
        )
    except Exception as e:
        logging.error(f"Failed to proxy {up_label}: {e}")

    try:
        proxy_sock.shutdown(socket.SHUT_RDWR)
        proxy_sock.close()
    except:
        pass

    try:
        client_sock.shutdown(socket.SHUT_RDWR)
        client_sock.close()
    except:
        pass

    logging.info(f"Closed {up_label}")


def run(*_):
    """
    プロキシサーバーの単一インスタンスを起動します。
    IPv4とIPv6の両方でリッスンソケットを作成し、
    geventのStreamServerを使用して着信接続を待ち受けます。
    SO_REUSEPORTにより、複数のインスタンスを同じポートで実行できます。
    """
    try:
        socks = [socket.socket(family, socket.SOCK_STREAM) for family in FAMILY]
        for sock in socks:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            sock.bind(("", PORT))
            sock.listen(socket.SOMAXCONN)
            addr = sock.getsockname()
            logging.info(f"Listening on [{addr[0]}]:{addr[1]}")

        gevent.joinall([gevent.spawn(StreamServer(sock, accept).serve_forever) for sock in socks])
    except Exception as e:
        logging.critical(f"Server error: {e}")


if __name__ == "__main__":
    for arg in sys.argv:
        match arg:
            case "-v":
                LOG_LEVEL = logging.DEBUG

    logging.basicConfig(level=LOG_LEVEL)

    try:
        pool = ThreadPool(POOL_SIZE)
        pool.map(run, range(POOL_SIZE))
        pool.join()
    except KeyboardInterrupt:
        pass
