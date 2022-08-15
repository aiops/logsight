from dataclasses import asdict
from typing import Any

import ujson

from logsight.analytics_core.logs import LogBatch, LogsightLog
from logsight.connectors.base.serializer import Serializer


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

    def deserialize(self, data: str) -> LogBatch:
        """
        The deserialize function takes a string and returns the corresponding
        LogBatch object. It is used to convert data from JSON format into a LogBatch
        object, which can then be used for further processing.

        Args:
            data:str: Deserialize the data into a LogBatch object

        Returns:
            A LogBatch object
        """
        d = ujson.loads(data)
        return LogBatch(id=d.get('id'), index=d.get('index'), logs=[LogsightLog(**log) for log in d['logs']],
                        metadata=d.get('metadata', dict()))
