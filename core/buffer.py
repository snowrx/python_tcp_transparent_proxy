class ContinuousCircularBuffer:
    def __init__(self, capacity: int):
        if capacity <= 0:
            raise ValueError("capacity must be greater than 0")
        self._capacity = capacity
        self._buffer = memoryview(bytearray(capacity << 1))
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
