import asyncio


class AsyncBytesBuffer:

    _DEFAULT_LIMIT = 1 << 20

    def __init__(self, limit: int = _DEFAULT_LIMIT):
        self._buffer = bytearray()
        self._limit = limit
        self._readable = asyncio.Event()
        self._writable = asyncio.Event()
        self._closed = False

    def __len__(self):
        if self._closed:
            raise BufferError("Buffer closed")
        return len(self._buffer)

    def is_closed(self):
        return self._closed

    def close(self):
        self._closed = True
        self._readable.set()
        self._writable.set()
        self._buffer.clear()

    async def read(self, n: int = _DEFAULT_LIMIT):
        assert n > 0

        if self._buffer:
            self._readable.set()
        else:
            await self._readable.wait()

        if self._closed:
            return b""

        data = bytes(memoryview(self._buffer)[:n])
        del self._buffer[:n]

        if not self._buffer:
            self._readable.clear()

        if len(self._buffer) < self._limit:
            self._writable.set()

        return data

    async def write(self, data: bytes):
        assert data

        if not self._buffer:
            self._writable.set()
        else:
            await self._writable.wait()

        if self._closed:
            raise BufferError("Buffer closed")

        self._buffer.extend(data)
        self._readable.set()

        if len(self._buffer) >= self._limit:
            self._writable.clear()
