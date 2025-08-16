import asyncio


class AsyncBytesBuffer:
    _DEFAULT_LIMIT = 1 << 30

    def __init__(self, limit: int = _DEFAULT_LIMIT):
        self._limit = limit
        self._buffer = bytearray()
        self._readable = asyncio.Event()
        self._writable = asyncio.Event()
        self._writable.set()
        self._closed = False

    def __len__(self):
        return len(self._buffer)

    def close(self):
        self._closed = True
        self._readable.set()
        self._writable.set()
        self._buffer.clear()

    async def read(self, n: int = _DEFAULT_LIMIT):
        if n <= 0:
            raise ValueError("n must be positive")

        if not self._buffer:
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
        if self._buffer:
            await self._writable.wait()

        if self._closed:
            return

        self._buffer.extend(data)
        self._readable.set()

        if len(self._buffer) >= self._limit:
            self._writable.clear()
