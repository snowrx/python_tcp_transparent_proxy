from contextlib import contextmanager

import gevent
from gevent.queue import SimpleQueue
from gevent.lock import BoundedSemaphore

DEFAULT_BUFFER_SIZE = 1 << 20
DEFAULT_POOL_SIZE = 100


class BufferPool:
    def __init__(self, buffer_size: int = DEFAULT_BUFFER_SIZE, pool_size: int = DEFAULT_POOL_SIZE, pre_fill: bool = True):
        self._buffer_size = buffer_size if buffer_size > 0 else DEFAULT_BUFFER_SIZE
        self._pool_size = pool_size if pool_size > 0 else DEFAULT_POOL_SIZE
        self._pool = SimpleQueue(self._pool_size)
        self._eraser = bytes(self._buffer_size)
        self._bsem = BoundedSemaphore()
        self._known = 0
        if pre_fill:
            self._pre_fill()

    @contextmanager
    def borrow(self):
        buffer = self.acquire()
        try:
            yield buffer
        finally:
            gevent.spawn(self.release, buffer)

    def acquire(self) -> memoryview:
        with self._bsem:
            if self._pool.empty():
                self._pool.put(memoryview(bytearray(self._buffer_size)))
                self._known += 1
            return self._pool.get()

    def release(self, buffer: memoryview):
        with self._bsem:
            if self._pool.full():
                buffer.release()
                self._known -= 1
            else:
                buffer[:] = self._eraser
                self._pool.put(buffer)

    def _pre_fill(self):
        with self._bsem:
            for _ in range(self._pool_size):
                self._pool.put(memoryview(bytearray(self._buffer_size)))
                self._known += 1
