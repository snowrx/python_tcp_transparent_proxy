from threading import Lock


class ZeroCopyRingBuffer:
    _MAX_SIZE: int = 1 << 30

    _lock: Lock
    _buffer: memoryview
    _size: int
    _allocated: int
    _wptr: int
    _rptr: int
    _end_marker: int
    _closed: bool

    def __init__(self, size: int) -> None:
        assert 0 < size <= self._MAX_SIZE

        self._lock = Lock()
        self._size = size
        self._allocated = 2 * size
        self._buffer = memoryview(bytearray(self._allocated))
        self._wptr = 0
        self._rptr = 0
        self._end_marker = 0
        self._closed = False

    def _status_validation(self) -> None:
        if self._closed:
            raise RuntimeError("Buffer closed")

    def __repr__(self) -> str:
        self._status_validation()
        msg = f"size={self._size}\n"
        msg += f"alloc={self._allocated}\n"
        msg += f"wptr={self._wptr}\n"
        msg += f"rptr={self._rptr}\n"
        msg += f"emkr={self._end_marker}\n"
        return msg

    def readable(self) -> int:
        self._status_validation()
        if self._rptr < self._wptr:
            return self._wptr - self._rptr
        else:
            return self._end_marker - self._rptr + self._wptr

    def commit_write(self, size: int) -> None:
        with self._lock:
            self._status_validation()
            assert 0 <= size <= max(0, self._size - self.readable())
            self._wptr += size
            if self._wptr > self._size:
                self._end_marker = self._wptr
                self._wptr = 0

    def commit_read(self, size: int) -> None:
        with self._lock:
            self._status_validation()
            if self._end_marker:
                assert 0 <= size <= (self._end_marker - self._rptr)
                self._rptr += size
                if self._rptr == self._end_marker:
                    self._end_marker = 0
                    self._rptr = 0
            else:
                assert 0 <= size <= (self._wptr - self._rptr)
                self._rptr += size

    def get_writable_buffer(self) -> memoryview:
        with self._lock:
            self._status_validation()
            size = max(0, self._size - self.readable())
            return self._buffer[self._wptr : self._wptr + size]

    def get_readable_buffer(self) -> memoryview:
        with self._lock:
            self._status_validation()
            if self._end_marker:
                return self._buffer[self._rptr : self._end_marker]
            else:
                return self._buffer[self._rptr : self._wptr]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        with self._lock:
            self._buffer = memoryview(b"")
            self._size = 0
            self._allocated = 0
            self._wptr = 0
            self._rptr = 0
            self._end_marker = 0
            self._closed = True
        return False


if __name__ == "__main__":
    C = 10
    W = 9
    R = 8

    with ZeroCopyRingBuffer(C) as rb:
        print("init")
        print(rb)

        data = bytearray(b"AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz")
        send = data.copy()
        recv = bytearray()

        eof = False
        while True:
            if wbuf := rb.get_writable_buffer():
                l = min(W, len(send), len(wbuf))
                wbuf[:l] = send[:l]
                rb.commit_write(l)
                del send[:l]
                print(f"write {l}")
                if not send:
                    eof = True
                    print("eof")
                print(rb)

            if rbuf := rb.get_readable_buffer():
                l = min(R, len(rbuf))
                recv += rbuf[:l]
                rb.commit_read(l)
                print(f"read {l}")
                print(rb)

            if eof and not rb.readable():
                break

        print(data)
        print(recv)
        print(recv == data)
