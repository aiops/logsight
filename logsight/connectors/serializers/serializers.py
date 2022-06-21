from abc import abstractmethod
from dataclasses import asdict
from typing import Any

import ujson

from analytics_core.logs import LogBatch, LogsightLog
from connectors.serializers import Serializer


class LogBatchSerializer(Serializer):
    """
    Interface for serialization of LogBatch objects.
    """

    @abstractmethod
    def serialize(self, data: LogBatch) -> Any:
        raise NotImplementedError

    @abstractmethod
    def deserialize(self, data: Any) -> LogBatch:
        raise NotImplementedError


class JSONStringSerializer(LogBatchSerializer):
    """
    Serializer class for transforming JSON strings to LogBatch
    """

    def serialize(self, data: LogBatch) -> str:
        """
        The serialize function takes a LogBatch object and returns a JSON string.
        The JSON string is the serialized representation of the LogBatch object.

        Args:
            data:LogBatch: Pass the data that is to be serialized

        Returns:
            A JSON string
        """
        return ujson.dumps(asdict(data))

    def deserialize(self, data: Any) -> LogBatch:
        """
        The deserialize function takes a JSON string and returns an instance of the LogBatch class.

        Args:
            data:string: Pass the data to be deserialized

        Returns:
            A LogBatch object
        """
        d = ujson.loads(data)
        return LogBatch(id=d.get('id'), index=d.get('index'), logs=[LogsightLog(**log) for log in d['logs']],
                        metadata=d.get('metadata', dict()))
