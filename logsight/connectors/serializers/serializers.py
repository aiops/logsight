import json
from dataclasses import asdict
from typing import Dict

import ujson

from analytics_core.logs import LogBatch, LogsightLog
from connectors.serializers import Serializer


class DictSerializer(Serializer):
    """
    This class transforms a string or a dictionary and returns a dictionary
    """

    def deserialize(self, data: bytes) -> Dict:
        """
        The deserialize function takes a bytes object and returns a dictionary.
        Args:
            data:bytes: Deserialize the data into a dictionary

        Returns:
            A dictionary
        """
        return json.loads(data.decode('utf-8'))

    def serialize(self, data: Dict) -> bytes:
        """
        The serialize function takes a dictionary of data and returns a byte string.
        Args:
            data:Dict: Pass the data that is to be serialized

        Returns:
            A bytes object
        """
        return json.dumps(data).encode('utf-8')


class LogBatchSerializer(Serializer):
    """
    Transformer class for transforming data to LogBatch
    """

    def serialize(self, data: LogBatch) -> bytes:
        """
        The serialize function takes a LogBatch object and returns a byte string.
        The byte string is the serialized representation of the LogBatch object.

        Args:
            data:LogBatch: Pass the data to be serialized

        Returns:
            A bytes object
        """
        return json.dumps(asdict(data)).encode('utf-8')

    def deserialize(self, data: bytes) -> LogBatch:
        """
        The deserialize function takes a byte string and returns an instance of the LogBatch class.

        Args:
            data:bytes: Pass the data to be deserialized

        Returns:
            A LogBatch object
        """
        d = ujson.loads(data.decode('utf-8'))
        return LogBatch(id=d.get('id'), index=d.get('index'), logs=[LogsightLog(**log) for log in d['logs']],
                        metadata=d.get('metadata', dict()))
