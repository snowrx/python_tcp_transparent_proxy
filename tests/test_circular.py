from gevent import monkey

monkey.patch_all()

import unittest
import sys
import os
from gevent import spawn, joinall, sleep
from gevent.event import Event
from gevent.lock import Semaphore

# Add project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from modules.circular import RingBuffer


class TestBasicOperations(unittest.TestCase):
    """基本操作を検証するテスト"""

    def test_initialization(self):
        """バッファが正しく初期化されることをテストする"""
        buf = RingBuffer(1024)
        self.assertEqual(buf._size, 1024)
        self.assertEqual(buf._allocated, 2048)
        self.assertEqual(len(buf), 0)
        self.assertFalse(buf.closed)
        buf.close()

    def test_simple_write_and_read(self):
        """単純な書き込みと読み出しをテストする"""
        buf = RingBuffer(1024)
        write_buf = buf.get_writable_buffer()
        data_to_write = b"hello world"
        write_buf[: len(data_to_write)] = data_to_write
        buf.advance_write(len(data_to_write))
        self.assertEqual(len(buf), 11)

        read_buf = buf.get_readable_buffer()
        self.assertEqual(len(read_buf), 11)
        self.assertEqual(read_buf, data_to_write)

        buf.advance_read(len(read_buf))
        self.assertEqual(len(buf), 0)
        buf.close()

    def test_close(self):
        """close()メソッドをテストする"""
        buf = RingBuffer(100)
        buf.close()
        self.assertTrue(buf.closed)
        self.assertEqual(len(buf), 0)
        self.assertEqual(buf._size, 0)
        self.assertEqual(buf._allocated, 0)
        self.assertEqual(len(buf.get_writable_buffer()), 0)

    def test_context_manager(self):
        """コンテキストマネージャとして使用した際にclose()が呼ばれることをテストする"""
        with RingBuffer(100) as buf:
            pass
        self.assertTrue(buf.closed)


class TestBoundaryConditions(unittest.TestCase):
    """境界条件を検証するテスト"""

    def test_fill_buffer_exactly(self):
        """バッファがちょうど満杯になるまで書き込む"""
        buf = RingBuffer(100)
        write_buf = buf.get_writable_buffer()
        data = b"a" * 100
        write_buf[:100] = data
        buf.advance_write(100)
        self.assertEqual(len(buf), 100)
        self.assertEqual(len(buf.get_writable_buffer()), 0)
        self.assertEqual(buf.get_readable_buffer(), data)
        buf.close()

    def test_write_over_capacity_raises_error(self):
        """利用可能な容量を超えて書き込もうとするとValueErrorが発生する"""
        buf = RingBuffer(10)
        with self.assertRaises(ValueError):
            buf.advance_write(11)
        buf.close()

    def test_read_over_capacity_raises_error(self):
        """バッファ内のデータ量を超えて読み出そうとするとValueErrorが発生する"""
        buf = RingBuffer(10)
        buf.get_writable_buffer()[:5] = b"hello"
        buf.advance_write(5)
        with self.assertRaises(ValueError):
            buf.advance_read(6)
        buf.close()

    def test_wrap_around_logic(self):
        """ラップアラウンド（循環）のコアロジックをテストする"""
        buf = RingBuffer(100)
        buf.get_writable_buffer()[:70] = b"a" * 70
        buf.advance_write(70)
        buf.advance_read(50)
        self.assertEqual(len(buf), 20)
        self.assertEqual(buf._tail, 50)

        write_buf = buf.get_writable_buffer()
        self.assertEqual(len(write_buf), 80)
        data_b = b"b" * 80
        write_buf[:80] = data_b
        buf.advance_write(80)

        self.assertEqual(buf._head, 0)
        self.assertEqual(buf._mark, 150)
        self.assertEqual(len(buf), 100)

        read_buf = buf.get_readable_buffer()
        self.assertEqual(len(read_buf), 100)
        expected_data = (b"a" * 20) + data_b
        self.assertEqual(read_buf, expected_data)

        buf.advance_read(100)
        self.assertEqual(buf._tail, 0)
        self.assertEqual(buf._mark, 0)
        self.assertEqual(len(buf), 0)
        buf.close()

    def test_wrap_around_read_in_two_parts(self):
        """ラップアラウンドしたデータを2回に分けて読み出す"""
        buf = RingBuffer(100)
        buf.get_writable_buffer()[:80] = b"a" * 80
        buf.advance_write(80)
        buf.advance_read(60)
        buf.get_writable_buffer()[:50] = b"b" * 50
        buf.advance_write(50)
        self.assertEqual(len(buf), 70)

        read_buf1 = buf.get_readable_buffer()
        self.assertEqual(read_buf1[:20], b"a" * 20)
        buf.advance_read(20)
        self.assertEqual(len(buf), 50)
        self.assertEqual(buf._tail, 80)

        read_buf2 = buf.get_readable_buffer()
        self.assertEqual(read_buf2, b"b" * 50)
        buf.advance_read(50)
        self.assertEqual(len(buf), 0)
        self.assertEqual(buf._tail, 0)
        self.assertEqual(buf._mark, 0)
        buf.close()


class TestHighLoad(unittest.TestCase):
    """geventグリーンレットによる高負荷テスト"""

    def test_multithreaded_integrity(self):
        """複数のプロデューサーとコンシューマーによるデータ完全性テスト"""
        NUM_PRODUCERS = 4
        NUM_CONSUMERS = 4
        RUNS = 5

        for i in range(RUNS):
            with self.subTest(run=i):
                buffer_size = 1024 * 16
                data_per_producer = 1024 * 128
                total_data_to_transfer = data_per_producer * NUM_PRODUCERS
                buf = RingBuffer(buffer_size)

                error_detected = Event()
                producers_finished_count = 0
                counter_lock = Semaphore()
                total_bytes_produced = 0
                total_bytes_consumed = 0

                def producer(thread_id: int):
                    nonlocal total_bytes_produced, producers_finished_count
                    bytes_produced = 0
                    seq = 0
                    while bytes_produced < data_per_producer and not error_detected.is_set():
                        writable_buf = buf.get_writable_buffer()
                        if not writable_buf:
                            sleep(0)  # gevent.sleep(0)
                            continue
                        chunk_size = min(len(writable_buf) // 2, 512) * 2
                        if chunk_size == 0:
                            sleep(0)  # gevent.sleep(0)
                            continue
                        for i in range(0, chunk_size, 2):
                            writable_buf[i] = thread_id
                            writable_buf[i + 1] = seq
                            seq = (seq + 1) % 256
                        buf.advance_write(chunk_size)
                        bytes_produced += chunk_size
                    with counter_lock:
                        total_bytes_produced += bytes_produced
                        producers_finished_count += 1

                def consumer():
                    nonlocal total_bytes_consumed
                    expected_sequences = {}
                    while True:
                        with counter_lock:
                            all_producers_done = producers_finished_count == NUM_PRODUCERS
                        if all_producers_done and len(buf) == 0:
                            break
                        readable_buf = buf.get_readable_buffer()
                        read_len = len(readable_buf) - (len(readable_buf) % 2)
                        if read_len == 0:
                            sleep(0)  # gevent.sleep(0)
                            continue
                        for i in range(0, read_len, 2):
                            thread_id, seq = readable_buf[i], readable_buf[i + 1]
                            next_seq = expected_sequences.setdefault(thread_id, 0)
                            if seq != next_seq:
                                error_detected.set()
                                return
                            expected_sequences[thread_id] = (seq + 1) % 256
                        buf.advance_read(read_len)
                        with counter_lock:
                            total_bytes_consumed += read_len

                greenlets = [spawn(producer, i) for i in range(NUM_PRODUCERS)]  # gevent.spawn
                greenlets += [spawn(consumer) for _ in range(NUM_CONSUMERS)]  # gevent.spawn
                joinall(greenlets, timeout=15)  # gevent.joinall

                self.assertFalse(error_detected.is_set(), "Data corruption was detected.")
                self.assertGreaterEqual(total_bytes_produced, total_data_to_transfer)
                self.assertEqual(total_bytes_produced, total_bytes_consumed)
                buf.close()


class TestThroughput(unittest.TestCase):
    """スループット（性能）を測定するテスト"""

    def test_throughput(self):
        """一定時間におけるデータの処理能力を測定する"""
        TEST_DURATION_SECONDS = 5
        BUFFER_SIZE = 1024 * 1024 * 1  # 1MBにバッファサイズを増加
        buf = RingBuffer(BUFFER_SIZE)
        stop_event = Event()
        bytes_transferred = 0
        counter_lock = Semaphore()

        def producer():
            data_chunk = b"\x01" * BUFFER_SIZE
            while not stop_event.is_set():
                writable_buf = buf.get_writable_buffer()
                if not writable_buf:
                    sleep(0)  # gevent.sleep(0)
                    continue
                write_size = len(writable_buf)
                writable_buf[:write_size] = data_chunk[:write_size]
                buf.advance_write(write_size)

        def consumer():
            nonlocal bytes_transferred
            while not stop_event.is_set():
                readable_buf = buf.get_readable_buffer()
                if not readable_buf:
                    sleep(0)  # gevent.sleep(0)
                    continue
                read_len = len(readable_buf)
                buf.advance_read(read_len)
                with counter_lock:
                    bytes_transferred += read_len

        producer_greenlet = spawn(producer)
        consumer_greenlet = spawn(consumer)

        sleep(TEST_DURATION_SECONDS)  # gevent.sleep
        stop_event.set()

        # Allow threads to finish their current loop
        producer_greenlet.join(timeout=1)
        consumer_greenlet.join(timeout=1)

        throughput_mb_per_sec = (bytes_transferred / (1024 * 1024)) / TEST_DURATION_SECONDS
        print(f"\nThroughput: {throughput_mb_per_sec:.2f} MB/s")
        self.assertGreater(throughput_mb_per_sec, 0)
        buf.close()


if __name__ == "__main__":
    unittest.main()
