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

import mmap

NULL = -1
MAP_FLAGS = mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS
MADV_FLAGS = mmap.MADV_HUGEPAGE
HUGEPAGE_THRESHOLD = 1 << 19


class ContinuousCircularBuffer:
    def __init__(self, capacity: int):
        if capacity <= 0:
            raise ValueError("capacity must be greater than 0")
        self._capacity = capacity
        # 物理領域を2倍確保して、論理的な連続性を保証する
        self._mm = mmap.mmap(NULL, capacity << 1, flags=MAP_FLAGS)
        if self._capacity >= HUGEPAGE_THRESHOLD:
            try:
                self._mm.madvise(MADV_FLAGS)
            except OSError:
                pass
        self._buffer = memoryview(self._mm)
        self._head = 0
        self._tail = 0
        self._marker = 0

    def get_writable_view(self) -> memoryview:
        free_size = self._capacity - self.get_used_size()
        return self._buffer[self._head : self._head + free_size]

    def get_readable_view(self) -> memoryview:
        used_size = self.get_used_size()
        if self._marker:
            end = min(self._marker, self._tail + used_size)
        else:
            end = self._tail + used_size
        return self._buffer[self._tail : end]

    def advance_write(self, nbytes: int):
        free_size = self._capacity - self.get_used_size()
        if not (0 <= nbytes <= free_size):
            raise ValueError("Invalid range")

        self._head += nbytes

        if self._head >= self._capacity:
            self._marker = self._head
            self._head = 0

    def advance_read(self, nbytes: int):
        used_size = self.get_used_size()
        if not (0 <= nbytes <= used_size):
            raise ValueError("Invalid range")

        self._tail += nbytes

        if self._marker and self._tail >= self._marker:
            self._marker = 0
            self._tail = 0

    def get_used_size(self) -> int:
        if self._marker:
            return (self._marker - self._tail) + self._head
        return self._head - self._tail

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __del__(self):
        self.close()

    def close(self):
        if hasattr(self, "_buffer"):
            self._buffer.release()
            del self._buffer
        if hasattr(self, "_mm"):
            self._mm.close()
            del self._mm
