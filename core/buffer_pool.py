from gevent import monkey
from gevent.lock import BoundedSemaphore
from gevent.queue import SimpleQueue

monkey.patch_all()

from contextlib import contextmanager


class BufferPool:
    def __init__(self, page_size: int, count: int) -> None:
        self._page_size = page_size
        self._count = count
        self._lock = BoundedSemaphore()
        self._free = SimpleQueue()
        for idx in range(count):
            self._free.put(idx)
        self._memory = memoryview(bytearray(page_size * count))
        self._overcomitted = 0
        self._eraser = bytes(page_size)

    @contextmanager
    def buffer(self):
        idx, buf = self._acquire()
        try:
            yield buf
        finally:
            self._release(idx, buf)

    def _acquire(self) -> tuple[int | None, memoryview]:
        with self._lock:
            if self._free.empty():
                self._overcomitted += 1
                return None, memoryview(bytearray(self._page_size))

            idx = self._free.get()
            head = idx * self._page_size
            tail = head + self._page_size
            return idx, self._memory[head:tail]

    def _release(self, idx: int | None, buf: memoryview) -> None:
        with self._lock:
            if idx is None:
                self._overcomitted -= 1
                buf.release()
                return

            buf[:] = self._eraser
            self._free.put(idx)
