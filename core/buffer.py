from contextlib import contextmanager

from gevent.lock import BoundedSemaphore
from gevent.queue import SimpleQueue


class BufferManager:
    def __init__(self, buffer_size: int, pool_size: int):
        self._buffer_size = buffer_size
        self._pool_size = pool_size
        self._pool: SimpleQueue[memoryview] = SimpleQueue(self._pool_size)
        self._semaphore = BoundedSemaphore()

    def get(self) -> memoryview:
        with self._semaphore:
            if self._pool.empty():
                return memoryview(bytearray(self._buffer_size))
            return self._pool.get()

    def put(self, item: memoryview):
        with self._semaphore:
            if self._pool.full():
                item.release()
                return
            self._pool.put(item)

    @contextmanager
    def acquire(self):
        item = self.get()
        try:
            yield item
        finally:
            self.put(item)
