from dataclasses import asdict
from typing import Any, Dict

import ujson

from analytics_core.logs import LogBatch, LogsightLog
from connectors.serializers import Serializer


class JSONSerializer(Serializer):
    """
    Serializer class for transforming JSON strings to LogBatch
    """

    def serialize(self, data: Dict) -> str:
        """
        The serialize function takes a LogBatch object and returns a JSON string.
        The JSON string is the serialized representation of the LogBatch object.

        Args:
            data:LogBatch: Pass the data to be serialized

        Returns:
            A JSON string
        """
        return ujson.dumps(data)

    def deserialize(self, data: str) -> Dict:
        """
        The deserialize function takes a JSON string and returns an instance of the LogBatch class.

        Args:
            data:string: Pass the data to be deserialized

        Returns:
            A LogBatch object
        """
        return ujson.loads(data)


class LogBatchSerializer(Serializer):
    """
    Interface for serialization of LogBatch objects.
    """

    def serialize(self, data: LogBatch) -> str:
        """
        The serialize function takes a LogBatch object and returns a JSON string.
        Args:
            data:LogBatch: Pass the data that is to be serialized

        Returns:
            A dictionary

        """
        return ujson.dumps(asdict(data))

    def deserialize(self, data: Any) -> LogBatch:
        """
        The deserialize function takes a string and returns the corresponding
        LogBatch object. It is used to convert data from JSON format into a LogBatch
        object, which can then be used for further processing.

        Args:
            data:Any: Deserialize the data into a LogBatch object

        Returns:
            A LogBatch object
        """
        d = ujson.loads(data)
        return LogBatch(id=d.get('id'), index=d.get('index'), logs=[LogsightLog(**log) for log in d['logs']],
                        metadata=d.get('metadata', dict()))
