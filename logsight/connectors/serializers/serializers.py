from dataclasses import asdict

import ujson

from analytics_core.logs import LogBatch, LogsightLog
from connectors.serializers.base import LogBatchSerializer


class JSONSerializer(LogBatchSerializer):
    """
    Transformer class for transforming data to LogBatch
    """

    def serialize(self, data: LogBatch) -> str:
        """
        The serialize function takes a LogBatch object and returns a byte string.
        The byte string is the serialized representation of the LogBatch object.

        Args:
            data:LogBatch: Pass the data that is to be serialized

        Returns:
            A bytes object
        """
        return ujson.dumps(asdict(data))

    def deserialize(self, data: Any) -> LogBatch:
        """
        The deserialize function takes a byte string and returns an instance of the LogBatch class.

        Args:
            data:bytes: Pass the data to be deserialized

        Returns:
            A LogBatch object
        """
        d = ujson.loads(data)
        return LogBatch(id=d.get('id'), index=d.get('index'), logs=[LogsightLog(**log) for log in d['logs']],
                        metadata=d.get('metadata', dict()))
