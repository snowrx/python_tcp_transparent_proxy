import asyncio


class AsyncBytesBuffer:
    _DEFAULT_LIMIT = 1 << 16

    def __init__(self, limit: int = _DEFAULT_LIMIT):
        self._limit = limit
        self._buffer = bytearray()
        self._cond = asyncio.Condition()
        self._eof = False

    def __len__(self):
        return len(self._buffer)

    def at_eof(self):
        return self._eof and not self._buffer

    def readable(self):
        return self._eof or len(self._buffer) > 0

    def writable(self):
        return not self._eof and len(self._buffer) < self._limit

    async def write(self, data: bytes):
        if self._eof:
            raise EOFError("Buffer is already closed.")
        async with self._cond:
            await self._cond.wait_for(self.writable)
            self._buffer.extend(data)
            self._cond.notify_all()

    async def read(self, n: int = -1):
        async with self._cond:
            await self._cond.wait_for(self.readable)
            if n == 0 or self.at_eof():
                return b""
            if n < 0:
                n = self._limit
            data = bytes(memoryview(self._buffer)[:n])
            del self._buffer[:n]
            self._cond.notify_all()
            return data

    async def close(self):
        async with self._cond:
            self._eof = True
            self._cond.notify_all()
            await self._cond.wait_for(self.at_eof)
