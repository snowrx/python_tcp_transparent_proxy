from gevent.queue import SimpleQueue
from gevent.lock import BoundedSemaphore

import logging
from contextlib import contextmanager


class BufferPool:
    def __init__(self, page_size: int, page_count: int):
        self._logger = logging.getLogger(f"{self.__class__.__name__}-{hex(id(self))}")
        self._page_size = page_size
        self._page_count = page_count
        self._lock = BoundedSemaphore()
        self._free_pages = SimpleQueue()
        for i in range(page_count):
            self._free_pages.put(i)
        self._memory = memoryview(bytearray(self._page_size * self._page_count))
        self._eraser = bytes(self._page_size)
        self._overcommit = 0

    @contextmanager
    def acquire(self):
        page_id, buffer = self._acquire()
        try:
            yield buffer
        finally:
            self._release(page_id, buffer)

    def _acquire(self) -> tuple[int | None, memoryview]:
        with self._lock:
            if self._free_pages.empty():
                self._overcommit += 1
                self._logger.warning(f"overcommit: {self._overcommit}")
                return None, memoryview(bytearray(self._page_size))
            else:
                page_id = self._free_pages.get()
                return page_id, self._memory[page_id * self._page_size : (page_id + 1) * self._page_size]

    def _release(self, page_id: int | None, buffer: memoryview) -> None:
        with self._lock:
            if page_id is None:
                buffer.release()
                self._overcommit -= 1
                self._logger.debug(f"overcommit: {self._overcommit}")
            else:
                buffer[:] = self._eraser
                self._free_pages.put(page_id)
