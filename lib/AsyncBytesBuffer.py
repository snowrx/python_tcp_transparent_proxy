import asyncio


class AsyncBytesBuffer:
    _DEFAULT_LIMIT = 1 << 24

    def __init__(self, limit: int = _DEFAULT_LIMIT):
        self._limit = limit
        self._buffer = bytearray()
        self._readable = asyncio.Event()
        self._writable = asyncio.Event()
        self._writable.set()
        self._eof = False

    async def write(self, data: bytes):
        if self._eof:
            raise EOFError("Buffer is closed for writing")
        await self._writable.wait()
        self._buffer.extend(data)
        self._readable.set()
        if len(self._buffer) >= self._limit:
            self._writable.clear()

    def write_eof(self):
        self._eof = True
        self._readable.set()

    async def read(self, n: int = _DEFAULT_LIMIT):
        if not self._eof:
            await self._readable.wait()
        if not self._buffer and self._eof:
            return b""
        data = bytes(memoryview(self._buffer)[:n])
        del self._buffer[:n]
        if not self._buffer:
            self._readable.clear()
        if len(self._buffer) < self._limit:
            self._writable.set()
        return data

    def close(self):
        self.write_eof()
        self._buffer.clear()
