import logging
from contextlib import contextmanager

from gevent.lock import BoundedSemaphore
from gevent.queue import SimpleQueue

DEFAULT_CHUNK_SIZE = 1 << 20
DEFAULT_POOL_SIZE = 1 << 8
TEMP_ID = -1


class BufferPool:
    def __init__(
        self,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        pool_size: int = DEFAULT_POOL_SIZE,
    ):
        self._logger = logging.getLogger(f"{__class__.__name__}-{hex(id(self))}")
        self._chunk_size = chunk_size if chunk_size > 0 else DEFAULT_CHUNK_SIZE
        self._pool_size = pool_size if pool_size > 0 else DEFAULT_POOL_SIZE
        self._lock = BoundedSemaphore()
        self._memory = memoryview(bytearray(self._chunk_size * self._pool_size))
        self._free_list = SimpleQueue()
        for i in range(self._pool_size):
            self._free_list.put(i)

    @contextmanager
    def borrow(self):
        idx, buf = self._alloc()
        try:
            yield buf
        finally:
            self._free(idx, buf)

    def _alloc(self) -> tuple[int, memoryview]:
        with self._lock:
            if self._free_list.empty():
                self._logger.warning("Allocating temporary buffer")
                return TEMP_ID, memoryview(bytearray(self._chunk_size))
            else:
                idx = self._free_list.get()
                head = idx * self._chunk_size
                return idx, self._memory[head : head + self._chunk_size]

    def _free(self, idx: int, buf: memoryview):
        with self._lock:
            if idx == TEMP_ID:
                self._logger.warning("Freeing temporary buffer")
                buf.release()
            else:
                self._free_list.put(idx)
                buf[:] = b"\0" * self._chunk_size
