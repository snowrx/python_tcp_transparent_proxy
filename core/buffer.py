"""
書き込みビューの連続性を保証する循環バッファ。

parameter
    capacity: 論理容量
    head: 書き込み先頭
    tail: 読み出し先頭
    marker: 書き込み折り返し位置
function
    get_writable_view(): 書き込みビューの取得
    get_readable_view(): 読み出しビューの取得
    advance_write(): 書き込みポインタを進める
    advance_read(): 読み出しポインタを進める
    get_used_size(): 使用済みバッファサイズ
"""


class ContinuousCircularBuffer:
    def __init__(self, capacity: int):
        if capacity <= 0:
            raise ValueError("capacity must be greater than 0")
        self._capacity = capacity
        self._buffer = memoryview(bytearray(2 * capacity))
        self._head = 0
        self._tail = 0
        self._marker = 0

    def get_writable_view(self) -> memoryview:
        # 空き容量取得
        if available_size := self._capacity - self.get_used_size():
            return self._buffer[self._head : self._head + available_size]

        # 空きがない場合空を返す
        return self._buffer[0:0]

    def get_readable_view(self) -> memoryview:
        # 使用済み容量取得
        if available_size := self.get_used_size():
            # マーカーあり
            if self._marker:
                return self._buffer[self._tail : self._marker]
            # マーカーなし
            else:
                return self._buffer[self._tail : self._tail + available_size]

        # 使用済みがない場合空を返す
        return self._buffer[0:0]

    def advance_write(self, nbytes: int):
        # 進行可能確認
        if nbytes < 0:
            raise ValueError("nbytes must be greater than 0")
        if nbytes > self._capacity - self.get_used_size():
            raise ValueError("nbytes is greater than the writable size")

        # 書き込み位置進行
        self._head += nbytes

        # 折り返し判定
        if self._head >= self._capacity:
            # マーカーセット
            self._marker = self._head
            # ヘッダー折り返し
            self._head = 0

    def advance_read(self, nbytes: int):
        # 進行可能確認
        if nbytes < 0:
            raise ValueError("nbytes must be greater than 0")
        if nbytes > self.get_used_size():
            raise ValueError("nbytes is greater than the readable size")

        # 読み出し位置進行
        self._tail += nbytes

        # 折り返し判定
        if self._marker and self._tail >= self._marker:
            # マーカークリア
            self._marker = 0
            # テール折り返し
            self._tail = 0

    def get_used_size(self) -> int:
        # 使用済み容量取得
        if self._marker:
            # マーカーあり
            return (self._marker - self._tail) + self._head
        # マーカーなし
        return self._head - self._tail

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._buffer is not None:
            self._buffer.release()
            self._buffer = None
