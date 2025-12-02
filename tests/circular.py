from random import randint
import unittest


class CircularBuffer:
    _MAX_CAPACITY = 1 << 30

    _buffer: memoryview
    _capacity: int
    _physical_size: int
    _wptr: int
    _rptr: int
    _readable: int
    _end_marker: int
    _is_cleaned_up: bool

    def __init__(self, capacity: int):
        if not (0 < capacity <= self._MAX_CAPACITY):
            raise ValueError(f"Invalid capacity: {capacity}, valid: 0 < capacity <= {self._MAX_CAPACITY}")

        self._capacity = capacity
        self._physical_size = capacity * 2
        self._buffer = memoryview(bytearray(self._physical_size))

        self._wptr = 0
        self._rptr = 0
        self._readable = 0
        self._end_marker = 0
        self._is_cleaned_up = False

    def __repr__(self) -> str:
        if self._is_cleaned_up:
            return "CircularBuffer(CLEANED UP)"

        msg = f"{self._capacity=}\n"
        msg += f"{self._physical_size=}\n"
        msg += f"{self._wptr=}\n"
        msg += f"{self._rptr=}\n"
        msg += f"{self._readable=}\n"
        msg += f"{self._end_marker=}\n"
        return msg

    def _check_cleaned_up(self):
        if self._is_cleaned_up:
            raise RuntimeError("Buffer is closed (via context manager) and cannot be used.")

    @property
    def free(self) -> int:
        self._check_cleaned_up()
        return self._capacity - self._readable

    @property
    def is_empty(self) -> bool:
        self._check_cleaned_up()
        return self._readable == 0

    def get_writable_buffer(self) -> memoryview:
        self._check_cleaned_up()
        return self._buffer[self._wptr : self._wptr + self.free]

    def advance_write(self, n: int):
        self._check_cleaned_up()
        if not (0 <= n <= self.free):
            raise ValueError(f"Invalid write size: {n}")

        self._wptr += n
        self._readable += n
        if self._wptr >= self._capacity:
            self._end_marker = self._wptr
            self._wptr = 0

    def get_readable_buffer(self) -> memoryview:
        self._check_cleaned_up()
        if self._readable == 0:
            return memoryview(b"")
        if self._rptr < self._wptr:
            return self._buffer[self._rptr : self._wptr]
        else:
            return self._buffer[self._rptr : self._end_marker]

    def advance_read(self, n: int):
        self._check_cleaned_up()
        if not (0 < n <= self._readable):
            raise ValueError(f"Invalid read size: {n}. Readable data: {self._readable}")

        if self._rptr < self._wptr:
            max_chunk = self._wptr - self._rptr
        else:
            max_chunk = self._end_marker - self._rptr

        if n > max_chunk:
            raise ValueError(f"Advance size {n} exceeds the returned continuous chunk size ({max_chunk}).")

        self._rptr += n
        self._readable -= n
        if self._rptr == self._end_marker:
            self._end_marker = 0
            self._rptr = 0

    # --- コンテキストマネージャ実装 ---

    def __enter__(self):
        self._check_cleaned_up()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._buffer = memoryview(b"")
        self._wptr = 0
        self._rptr = 0
        self._readable = 0
        self._end_marker = 0
        self._capacity = 0
        self._is_cleaned_up = True
        return False


# -----------------------------------------------------------
# UNIT TEST CLASS
# -----------------------------------------------------------


class CircularBufferSecurityTest(unittest.TestCase):

    def test_initial_state_and_invalid_capacity(self):
        with self.assertRaises(ValueError):
            CircularBuffer(0)
        with self.assertRaises(ValueError):
            CircularBuffer(-10)

        buffer = CircularBuffer(10)
        self.assertEqual(buffer.free, 10)
        self.assertTrue(buffer.is_empty)
        self.assertFalse(buffer._is_cleaned_up)

    def test_context_manager_cleanup_and_guard(self):
        buffer = CircularBuffer(10)

        with buffer:
            buffer.advance_write(5)
            self.assertEqual(buffer._readable, 5)

        self.assertTrue(buffer._is_cleaned_up, "Cleanup flag was not set to True.")
        self.assertEqual(buffer._capacity, 0, "Capacity was not reset.")
        self.assertEqual(len(buffer._buffer), 0, "Buffer was not reset to empty memoryview.")

        with self.assertRaises(RuntimeError, msg="Closed buffer allowed advance_write"):
            buffer.advance_write(1)

        with self.assertRaises(RuntimeError, msg="Closed buffer allowed free access"):
            _ = buffer.free

    def test_full_cycle_integrity_with_random_io(self):
        capacity = 10
        buffer = CircularBuffer(capacity)
        data = bytearray(b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")
        send = data.copy()
        out = bytearray()

        with buffer:
            eof = False
            while True:
                if send:
                    wbuf = buffer.get_writable_buffer()
                    l = min(randint(5, capacity), len(send), len(wbuf))
                    if l > 0:
                        wbuf[:l] = send[:l]
                        buffer.advance_write(l)
                        del send[:l]
                else:
                    eof = True

                if rbuf := buffer.get_readable_buffer():
                    l = min(randint(3, 8), len(rbuf))
                    if l > 0:
                        out += rbuf[:l]
                        buffer.advance_read(l)

                if eof and buffer.is_empty:
                    break

        self.assertEqual(data, out, "Random I/O resulted in data corruption or loss.")
        self.assertTrue(buffer._is_cleaned_up)


# -----------------------------------------------------------
# テスト実行ブロック
# -----------------------------------------------------------

if __name__ == "__main__":
    print("--- CircularBuffer Unittest 開始 ---")
    unittest.main(argv=["first-arg-is-ignored"], exit=False)
