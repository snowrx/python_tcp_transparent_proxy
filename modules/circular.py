from gevent.lock import RLock

NULL_BUFFER = memoryview(bytearray(0))


class RingBuffer:
    _buffer: memoryview
    _size: int
    _allocated: int
    _head: int
    _tail: int
    _mark: int
    _closed: bool
    _lock: RLock

    def __init__(self, size: int):
        self._size = size
        self._allocated = 2 * size
        self._buffer = memoryview(bytearray(self._allocated))
        self._head = 0
        self._tail = 0
        self._mark = 0
        self._closed = False
        self._lock = RLock()

    def get_readable_buffer(self) -> memoryview:
        with self._lock:
            if self._head == self._tail:
                readable_size = 0
            elif self._head > self._tail:
                readable_size = self._head - self._tail
            else:
                readable_size = self._mark - self._tail
            return self._buffer[self._tail : self._tail + readable_size]

    def get_writable_buffer(self) -> memoryview:
        with self._lock:
            if self._closed:
                return NULL_BUFFER
            # Use len(self) which is now thread-safe to get the correct available size.
            available_size = self._size - len(self)
            return self._buffer[self._head : self._head + available_size]

    def advance_read(self, size: int):
        with self._lock:
            if size > len(self):
                raise ValueError("Cannot advance read beyond the current buffer size")
            self._tail += size
            if self._tail == self._mark:
                self._tail = 0
                self._mark = 0

    def advance_write(self, size: int):
        with self._lock:
            if size > self._size - len(self):
                raise ValueError("Cannot advance write beyond the available buffer space")
            self._head += size
            if self._head > self._size:
                self._mark = self._head
                self._head = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __len__(self):
        with self._lock:
            if self._mark == 0:  # Not wrapped
                return self._head - self._tail
            else:  # Wrapped
                return (self._mark - self._tail) + self._head

    @property
    def closed(self):
        return self._closed

    def close(self):
        with self._lock:
            if not self._closed:
                self._closed = True
                # Release memory and reset state safely
                self._buffer = NULL_BUFFER
                self._size = 0
                self._allocated = 0
                self._head = 0
                self._tail = 0
                self._mark = 0
