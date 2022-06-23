from typing import Any

from connectors.base import Serializer


class DefaultSerializer(Serializer):
    def serialize(self, data: Any) -> str:
        return str(data)

    def deserialize(self, data: str) -> str:
        return data
