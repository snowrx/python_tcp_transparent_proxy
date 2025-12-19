from contextlib import contextmanager
import logging

import gevent
from gevent.lock import BoundedSemaphore

DEFAULT_BUFFER_SIZE = 1 << 20
DEFAULT_POOL_SIZE = 100
TEMP_INDEX = -1


class BufferPool:
    def __init__(self, buffer_size: int = DEFAULT_BUFFER_SIZE, pool_size: int = DEFAULT_POOL_SIZE):
        self._logger = logging.getLogger(f"{self.__class__.__name__}-{hex(id(self))}")
        self._lock = BoundedSemaphore()

        self._buffer_size = buffer_size if buffer_size > 0 else DEFAULT_BUFFER_SIZE
        self._pool_size = pool_size if pool_size > 0 else DEFAULT_POOL_SIZE

        self._bedrock = memoryview(bytearray(self._buffer_size * self._pool_size))
        self._free_list = list(range(self._pool_size))
        self._temp_count = 0
        self._eraser = bytes(self._buffer_size)

    @contextmanager
    def borrow(self):
        idx, buffer = self._acquire()
        try:
            yield buffer
        finally:
            gevent.spawn(self._release, idx, buffer)

    def _acquire(self) -> tuple[int, memoryview]:
        with self._lock:
            if not self._free_list:
                self._temp_count += 1
                self._logger.warning(f"No free buffers, allocating temporary buffer {self._temp_count}")
                return TEMP_INDEX, memoryview(bytearray(self._buffer_size))
            else:
                idx = self._free_list.pop(0)
                buffer = self._bedrock[idx * self._buffer_size : (idx + 1) * self._buffer_size]
                self._logger.debug(f"Acquired buffer {idx}")
                return idx, buffer

    def _release(self, idx: int, buffer: memoryview):
        gevent.idle()
        with self._lock:
            if idx is TEMP_INDEX:
                self._temp_count -= 1
                buffer.release()
                self._logger.warning(f"Released temporary buffer, {self._temp_count} remaining")
            else:
                buffer[:] = self._eraser
                self._free_list.append(idx)
                self._logger.debug(f"Released buffer {idx}")
