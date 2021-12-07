from typing import List, Any


class Buffer:
    _max_size: int = 1000

    def __init__(self, max_size: int = 1000):
        self._max_size = max_size
        self._buffer: List[Any] = []

    def __getitem__(self, index):
        return self._buffer[index]

    def add(self, item: Any):
        self._buffer.append(item)

    def pop(self) -> Any:
        return self._buffer.pop()

    def flush_buffer(self) -> Any:
        buff_copy = self._buffer.copy()
        self.reset_buffer()
        return buff_copy

    def reset_buffer(self):
        self._buffer = []

    @property
    def is_empty(self) -> bool:
        return len(self._buffer) == 0

    @property
    def is_full(self) -> bool:
        return len(self._buffer) >= self._max_size

    @property
    def buffer(self) -> List[Any]:
        return self._buffer

    def extend(self, items: List):
        self.buffer.extend(items)

    @property
    def size(self):
        return len(self.buffer)
