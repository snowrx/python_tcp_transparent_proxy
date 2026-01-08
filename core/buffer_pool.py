import logging
from contextlib import contextmanager

from gevent.lock import BoundedSemaphore
from gevent.queue import SimpleQueue

DEFAULT_CHUNK_SIZE = 1 << 20
DEFAULT_POOL_SIZE = 100


class BufferPool:
    def __init__(
        self, chunk_size: int = DEFAULT_CHUNK_SIZE, pool_size: int = DEFAULT_POOL_SIZE
    ):
        self._logger = logging.getLogger(f"{__class__.__name__}-{hex(id(self))}")
        self._chunk_size = chunk_size if chunk_size > 0 else DEFAULT_CHUNK_SIZE
        self._pool_size = pool_size if pool_size > 0 else DEFAULT_POOL_SIZE
        self._pool: SimpleQueue[memoryview] = SimpleQueue(self._pool_size)
        self._semaphore = BoundedSemaphore()

    @contextmanager
    def borrow(self):
        buffer = self.acquire()
        try:
            yield buffer
        finally:
            self.release(buffer)

    def acquire(self) -> memoryview:
        with self._semaphore:
            if self._pool.empty():
                self._logger.debug("Creating new buffer")
                return memoryview(bytearray(self._chunk_size))
            self._logger.debug("Acquiring buffer from pool")
            return self._pool.get()

    def release(self, buffer: memoryview) -> None:
        with self._semaphore:
            if self._pool.full():
                self._logger.debug("Buffer pool is full, discarding buffer")
                buffer.release()
            else:
                self._logger.debug("Releasing buffer back to pool")
                self._pool.put(buffer)
