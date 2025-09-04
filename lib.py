import asyncio


class buffer:
    _DEFAULT_LIMIT = 1 << 16

    def __init__(self, limit: int = _DEFAULT_LIMIT):
        self._buffer = bytearray()
        self._cond = asyncio.Condition()
        self._limit = limit
        self._eof = False
        self._abort = False

    def __del__(self):
        del self._buffer

    def __len__(self):
        return len(self._buffer)

    @property
    def limit(self):
        return self._limit

    @limit.setter
    def limit(self, limit: int):
        self._limit = limit

    def at_eof(self):
        return (self._eof and not self._buffer) or self._abort

    def readable(self):
        return len(self._buffer) > 0 or self._eof

    def writable(self):
        return len(self._buffer) < self._limit or self._eof

    async def read(self, n: int = -1):
        if n == 0:
            return b""
        async with self._cond:
            await self._cond.wait_for(self.readable)
            if self._eof:
                return b""
            if n < 0:
                n = self._limit
            data = bytes(memoryview(self._buffer)[:n])
            del self._buffer[:n]
            self._cond.notify_all()
            return data

    async def write(self, data: bytes):
        if not data:
            return
        async with self._cond:
            await self._cond.wait_for(self.writable)
            if self._eof:
                raise EOFError("buffer is closed")
            self._buffer.extend(data)
            self._cond.notify_all()

    async def write_eof(self):
        async with self._cond:
            self._eof = True
            self._cond.notify_all()

    async def wait_closed(self):
        async with self._cond:
            await self._cond.wait_for(self.at_eof)

    async def abort(self):
        async with self._cond:
            self._abort = True
            self._cond.notify_all()
