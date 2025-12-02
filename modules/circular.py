from random import randint
import unittest
import time


class CircularBuffer:
    """
    ゼロコピーのI/Oを可能にするための円形バッファクラス。
    バッファリング領域と物理領域を分離し、データコピーを最小化する。
    """

    _MAX_CAPACITY: int = 1 << 30

    _buffer: memoryview
    _capacity: int
    _physical_size: int
    _wptr: int
    _rptr: int
    _readable: int
    _end_marker: int
    _is_cleaned_up: bool

    def __init__(self, capacity: int):
        """
        CircularBufferの初期化。

        Args:
            capacity (int): バッファの論理的な最大容量。

        Raises:
            ValueError: 容量が不正な場合。
        """
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
        return (
            f"self._capacity={self._capacity}\n"
            f"self._physical_size={self._physical_size}\n"
            f"self._wptr={self._wptr}\n"
            f"self._rptr={self._rptr}\n"
            f"self._readable={self._readable}\n"
            f"self._end_marker={self._end_marker}\n"
        )

    def _check_cleaned_up(self):
        """
        バッファがクローズされているかチェックし、クローズされていればRuntimeErrorを送出する。
        """
        if self._is_cleaned_up:
            raise RuntimeError("Buffer is closed (via context manager) and cannot be used.")

    @property
    def free(self) -> int:
        """バッファの空き容量を返す。"""
        self._check_cleaned_up()
        return self._capacity - self._readable

    @property
    def is_empty(self) -> bool:
        """バッファが空かどうかを返す。"""
        self._check_cleaned_up()
        return self._readable == 0

    def get_writable_buffer(self) -> memoryview:
        """
        書き込み可能な連続したメモリビューを返す。
        """
        self._check_cleaned_up()
        return self._buffer[self._wptr : self._wptr + self.free]

    def advance_write(self, n: int):
        """
        書き込みポインタをnバイト進める。

        Args:
            n (int): 書き込んだバイト数。

        Raises:
            ValueError: 書き込みサイズが不正な場合。
        """
        self._check_cleaned_up()
        if not (0 <= n <= self.free):
            raise ValueError(f"Invalid write size: {n}")

        self._wptr += n
        self._readable += n
        if self._wptr >= self._capacity:
            self._end_marker = self._wptr
            self._wptr = 0

    def get_readable_buffer(self) -> memoryview:
        """
        読み取り可能な連続したメモリビューを返す（ゼロコピー）。
        """
        self._check_cleaned_up()
        if self._readable == 0:
            return memoryview(b"")
        if self._rptr < self._wptr:
            return self._buffer[self._rptr : self._wptr]
        else:
            return self._buffer[self._rptr : self._end_marker]

    def advance_read(self, n: int):
        """
        読み取りポインタをnバイト進める（データを消費する）。

        Args:
            n (int): 読み取り、消費したバイト数。

        Raises:
            ValueError: 読み取りサイズが不正な場合、または連続したチャンクサイズを超えている場合。
        """
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

    def __enter__(self):
        """コンテキストマネージャ開始時にインスタンスを返す。"""
        self._check_cleaned_up()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """コンテキストマネージャ終了時にバッファをクリーンアップする。"""
        self._buffer = memoryview(b"")
        self._wptr = 0
        self._rptr = 0
        self._readable = 0
        self._end_marker = 0
        self._capacity = 0
        self._is_cleaned_up = True
        return False


# -----------------------------------------------------------


class CircularBufferSecurityTest(unittest.TestCase):
    """
    CircularBufferのセキュリティ、整合性、スループットを検証するテストクラス。
    """

    CHUNK_SIZE: int = 4096
    TEST_RUNS: int = 3

    def test_initial_state_and_invalid_capacity(self):
        """初期状態と不正な容量のチェック。"""
        with self.assertRaises(ValueError):
            CircularBuffer(0)
        with self.assertRaises(ValueError):
            CircularBuffer(-10)

        buffer: CircularBuffer = CircularBuffer(10)
        self.assertEqual(buffer.free, 10)
        self.assertTrue(buffer.is_empty)
        self.assertFalse(buffer._is_cleaned_up)

    def test_context_manager_cleanup_and_guard(self):
        """コンテキストマネージャによるクリーンアップと使用ガードのチェック。"""
        buffer: CircularBuffer = CircularBuffer(10)

        with buffer:
            buffer.advance_write(5)
            self.assertEqual(buffer._readable, 5)

        self.assertTrue(buffer._is_cleaned_up)
        self.assertEqual(buffer._capacity, 0)
        self.assertEqual(len(buffer._buffer), 0)

        with self.assertRaises(RuntimeError):
            buffer.advance_write(1)

        with self.assertRaises(RuntimeError):
            _ = buffer.free

    def test_data_integrity_full_cycle(self):
        """ランダムな読み書きによるデータ整合性の検証。"""
        capacity: int = 10
        buffer: CircularBuffer = CircularBuffer(capacity)
        data: bytearray = bytearray(b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")
        send: bytearray = data.copy()
        out: bytearray = bytearray()

        with buffer:
            eof: bool = False
            while True:
                if send:
                    wbuf: memoryview = buffer.get_writable_buffer()
                    l: int = min(randint(1, capacity), len(send), len(wbuf))
                    if l > 0:
                        wbuf[:l] = send[:l]
                        buffer.advance_write(l)
                        del send[:l]
                else:
                    eof = True

                rbuf: memoryview = buffer.get_readable_buffer()
                if len(rbuf) > 0:
                    l: int = min(randint(1, 8), len(rbuf))
                    if l > 0:
                        out += rbuf[:l]
                        buffer.advance_read(l)

                if eof and buffer.is_empty:
                    break

        self.assertEqual(data, out, "Random I/O resulted in data corruption or loss.")
        self.assertTrue(buffer._is_cleaned_up)

    def measure_io_speed(self, DATA_SIZE: int, do_copy: bool) -> tuple:
        """
        指定されたデータサイズとコピー有無フラグに基づいてスループットを測定する。

        Args:
            DATA_SIZE (int): 測定に使用する総データサイズ。
            do_copy (bool): 読み取り時に明示的なデータコピーを行うかどうかのフラグ。

        Returns:
            tuple: (平均処理時間 [float], 平均スループットMB/s [float], 総データ長 [int])
        """

        CHUNK_SIZE: int = self.CHUNK_SIZE
        TEST_RUNS: int = self.TEST_RUNS

        base_data: bytearray = bytearray(b"A" * CHUNK_SIZE * (DATA_SIZE // CHUNK_SIZE))
        data_len: int = len(base_data)

        output_buffer: bytearray = bytearray(data_len)
        output_mv: memoryview = memoryview(output_buffer)
        out_ptr: int = 0

        def run_single_io_test():
            nonlocal out_ptr
            buffer: CircularBuffer = CircularBuffer(CHUNK_SIZE * 2)
            temp_data: memoryview = memoryview(base_data)
            out_ptr = 0

            while temp_data or buffer._readable > 0:
                if temp_data:
                    wbuf: memoryview = buffer.get_writable_buffer()
                    write_len: int = min(CHUNK_SIZE, len(temp_data), len(wbuf))
                    if write_len > 0:
                        wbuf[:write_len] = temp_data[:write_len]
                        buffer.advance_write(write_len)
                        temp_data = temp_data[write_len:]

                rbuf: memoryview = buffer.get_readable_buffer()
                if len(rbuf) > 0:
                    read_len: int = min(CHUNK_SIZE, len(rbuf))
                    if read_len > 0:
                        if do_copy:
                            output_mv[out_ptr : out_ptr + read_len] = rbuf[:read_len]
                            out_ptr += read_len

                        buffer.advance_read(read_len)

        io_times: list = []
        for _ in range(TEST_RUNS):
            start: float = time.time()
            run_single_io_test()
            io_times.append(time.time() - start)

        avg_io_time: float = sum(io_times) / TEST_RUNS
        throughput_mbps: float = (data_len / 1024.0 / 1024.0) / avg_io_time

        return avg_io_time, throughput_mbps, data_len

    def test_io_throughput(self):
        """複数のデータサイズとコピー有無シナリオでスループットを測定し、結果を出力する。"""

        DATA_SIZES: dict = {"1MB": 1 * 1024 * 1024, "10MB": 10 * 1024 * 1024, "100MB": 100 * 1024 * 1024}

        print("\n--- I/O スループット測定開始 (関数呼び出し形式) ---")

        for copy_name, do_copy in [("ゼロコピー", False), ("コピーあり", True)]:
            print(f"\n[シナリオ: {copy_name}]")

            for name, DATA_SIZE in DATA_SIZES.items():

                avg_time: float
                throughput_mbps: float
                data_len: int
                avg_time, throughput_mbps, data_len = self.measure_io_speed(DATA_SIZE, do_copy)

                print(f"  --- 測定結果 ({name}) ---")
                print(f"  総データ量: {data_len/1024.0/1024.0:.0f} MB")
                print(f"  平均処理時間: {avg_time:.4f}秒")
                print(f"  平均スループット: {throughput_mbps:.2f} MB/秒")

                if name == "100MB":
                    self.assertGreater(
                        throughput_mbps, 50, f"I/O throughput for {name} ({copy_name}) is too low (below 50 MB/s)."
                    )


# -----------------------------------------------------------

if __name__ == "__main__":
    unittest.main(argv=["first-arg-is-ignored"], exit=False)
