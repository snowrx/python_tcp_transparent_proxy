import unittest
from core.buffer import ContinuousCircularBuffer


class TestContinuousCircularBuffer(unittest.TestCase):
    def test_basic_read_write(self):
        buf = ContinuousCircularBuffer(10)
        with buf.get_writable_view() as write_view:
            data = b"hello"
            write_view[: len(data)] = data
            buf.advance_write(len(data))

        with buf.get_readable_view() as read_view:
            self.assertEqual(read_view.tobytes(), b"hello")
            buf.advance_read(len(data))
        self.assertEqual(buf.get_used_size(), 0)
        buf.close()

    def test_buffer_wrap(self):
        capacity = 10
        buf = ContinuousCircularBuffer(capacity)

        # Fill up to near capacity
        data1 = b"01234567"
        with buf.get_writable_view() as write_view:
            write_view[: len(data1)] = data1
            buf.advance_write(len(data1))

        # Read some back
        buf.advance_read(4)  # remains "4567" at [4:8]

        # Write more to cause wrap
        data2 = b"89AB"
        with buf.get_writable_view() as write_view:
            # head=8. capacity=10. available=6. head+available=14.
            write_view[: len(data2)] = data2
            buf.advance_write(len(data2))
            # head becomes 12. head >= 10. marker=12, head=0.

        self.assertEqual(buf._marker, 12)
        self.assertEqual(buf._head, 0)

        # Read from tail=4 to marker=12
        with buf.get_readable_view() as read_view:
            # In this implementation, get_readable_view returns tail to marker if marker exists.
            # tail=4, marker=12. So it returns [4:12].
            # Content at [4:8] is "4567", [8:12] is "89AB".
            self.assertEqual(read_view.tobytes(), b"456789AB")

        buf.advance_read(8)  # tail becomes 12. tail >= marker. marker=0, tail=0.
        self.assertEqual(buf._tail, 0)
        self.assertEqual(buf.get_used_size(), 0)
        buf.close()

    def test_simultaneous_view_operations(self):
        """
        Verify that getting a readable view and a writable view at the same time
        works as expected, especially when they overlap or represent the same physical memory.
        """
        buf = ContinuousCircularBuffer(10)

        # 1. Get initial views
        with buf.get_writable_view() as write_view, buf.get_readable_view() as read_view:
            self.assertEqual(len(read_view), 0)
            self.assertEqual(len(write_view), 10)

            # 2. Write via view WITHOUT calling advance_write yet
            data = b"ABCDE"
            write_view[: len(data)] = data

            # 3. Even though we wrote to the memory, get_readable_view() relies on get_used_size()
            # which depends on _head and _tail. So new call to get_readable_view should still be empty.
            with buf.get_readable_view() as temp_read_view:
                self.assertEqual(len(temp_read_view), 0)

            # 4. Advance write. Now it should be readable.
            buf.advance_write(len(data))
            with buf.get_readable_view() as new_read_view:
                self.assertEqual(new_read_view.tobytes(), b"ABCDE")

            # 5. Simultaneous write/read simulation
            # Already have data "ABCDE" at [0:5]. head=5, tail=0.
            # Get writable view for the rest.
            with buf.get_writable_view() as write_view_2:  # length 5, starts at index 5
                self.assertEqual(len(write_view_2), 5)

                # Get readable view for existing data
                with buf.get_readable_view() as read_view_2:  # length 5, starts at index 0
                    self.assertEqual(read_view_2.tobytes(), b"ABCDE")

                    # Write to write_view_2 while read_view_2 is active
                    write_view_2[0:2] = b"FG"

                    # Verify read_view_2 is still intact (doesn't change because they don't overlap in memory)
                    self.assertEqual(read_view_2.tobytes(), b"ABCDE")

            # Advance and check
            buf.advance_write(2)
            self.assertEqual(buf.get_used_size(), 7)
            with buf.get_readable_view() as final_read_view:
                self.assertEqual(final_read_view.tobytes(), b"ABCDEFG")
        buf.close()

    def test_held_views_sequential_ops(self):
        """
        Verify: (Get rv & wv -> Operate on rv -> Operate on wv) x 2
        """
        buf = ContinuousCircularBuffer(10)
        # Setup initial data
        with buf.get_writable_view() as w:
            w[0:3] = b"XYZ"
            buf.advance_write(3)

        # --- Iteration 1 ---
        # 1. Get both views
        with buf.get_readable_view() as rv1, buf.get_writable_view() as wv1:
            # 2. Operate on rv (e.g. read)
            self.assertEqual(rv1.tobytes(), b"XYZ")

            # 3. Operate on wv (e.g. write)
            wv1[0:2] = b"12"

            # Advance both
            buf.advance_read(3)
            buf.advance_write(2)

            self.assertEqual(buf.get_used_size(), 2)
            with buf.get_readable_view() as temp_rv:
                self.assertEqual(temp_rv.tobytes(), b"12")

        # --- Iteration 2 ---
        # 1. Get both views again
        with buf.get_readable_view() as rv2, buf.get_writable_view() as wv2:
            # 2. Operate on rv
            self.assertEqual(rv2.tobytes(), b"12")

            # 3. Operate on wv
            wv2[0:3] = b"345"

            # Advance both
            buf.advance_read(2)
            buf.advance_write(3)

            self.assertEqual(buf.get_used_size(), 3)
            with buf.get_readable_view() as final_rv:
                self.assertEqual(final_rv.tobytes(), b"345")
        buf.close()


if __name__ == "__main__":
    unittest.main()
