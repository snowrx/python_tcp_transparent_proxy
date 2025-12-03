from gevent import monkey

monkey.patch_all()

import socket
import gevent
from gevent.socket import wait_read, wait_write, timeout


class Session:
    def __init__(self, client_sock: socket.socket, client_addr: tuple[str, int], remote_addr: tuple[str, int]):
        self._created_at = gevent.get_hub().loop.now()
        self._client_sock = client_sock
        self._client_addr = client_addr
        self._remote_addr = remote_addr

        self._remote_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._remote_sock.setsockopt(socket.SOL_TCP, socket.TCP_FASTOPEN_CONNECT, 1)

        self._up_label = f"[{client_addr[0]}]:{client_addr[1]} -> [{remote_addr[0]}]:{remote_addr[1]}"
        self._down_label = f"[{client_addr[0]}]:{client_addr[1]} <- [{remote_addr[0]}]:{remote_addr[1]}"

    def _start_bidirectional_forwarding(self):
        def _forward_data(label: str, src: socket.socket, dst: socket.socket):
            try:
                while data := src.recv(4096):
                    dst.sendall(data)
            except Exception as e:
                raise
            finally:
                dst.shutdown(socket.SHUT_WR)

        # --- 双方向のデータ転送 ---
        greenlets = [
            gevent.spawn(_forward_data, self._up_label, self._client_sock, self._remote_sock),
            gevent.spawn(_forward_data, self._down_label, self._remote_sock, self._client_sock),
        ]
        gevent.joinall(greenlets, raise_error=True)
        self._client_sock.close()
        self._remote_sock.close()

    def serve(self):
        # --- TFO用の初期データ受信 ---
        TFO_TIMEOUT = 10 / 1000
        buffer_to_send = b""
        try:
            wait_read(self._client_sock.fileno(), timeout=TFO_TIMEOUT)
            buffer_to_send = self._client_sock.recv(4096)
            if not buffer_to_send:
                return
        except timeout:
            pass
        except Exception as e:
            return

        # --- TFO接続試行とフォールバック ---
        try:
            sent = self._remote_sock.sendto(buffer_to_send, socket.MSG_FASTOPEN, self._remote_addr)
            buffer_to_send = buffer_to_send[sent:]
        except BlockingIOError:
            pass
        except Exception as e:
            return

        # --- 接続確立の待機と残データ送信 ---
        wait_write(self._remote_sock.fileno())
        if buffer_to_send:
            self._remote_sock.sendall(buffer_to_send)

        self._start_bidirectional_forwarding()
