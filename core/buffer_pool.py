from contextlib import contextmanager
import logging

import gevent
from gevent.lock import BoundedSemaphore
from gevent.queue import SimpleQueue

DEFAULT_BUFFER_SIZE = 1 << 20
DEFAULT_POOL_SIZE = 100


class BufferPool:
    def __init__(self, buffer_size: int = DEFAULT_BUFFER_SIZE, pool_size: int = DEFAULT_POOL_SIZE):
        self._logger = logging.getLogger(f"{self.__class__.__name__}-{hex(id(self))}")
        self._lock = BoundedSemaphore()

        self._buffer_size = buffer_size if buffer_size > 0 else DEFAULT_BUFFER_SIZE
        self._pool_size = pool_size if pool_size > 0 else DEFAULT_POOL_SIZE

        self._pool: SimpleQueue[memoryview] = SimpleQueue(self._pool_size)
        self._eraser = bytes(self._buffer_size)

    @contextmanager
    def borrow(self):
        buffer = self._acquire()
        try:
            yield buffer
        finally:
            gevent.spawn(self._release, buffer)

    def _acquire(self) -> memoryview:
        with self._lock:
            if self._pool.empty():
                self._logger.info("Creating new buffer")
                return memoryview(bytearray(self._buffer_size))
            else:
                return memoryview(self._pool.get())

    def _release(self, buffer: memoryview) -> None:
        gevent.idle()
        with self._lock:
            if self._pool.full():
                self._logger.info("Dropping buffer")
                buffer.release()
            else:
                buffer[:] = self._eraser
                self._pool.put(buffer)
