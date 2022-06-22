from typing import Dict

import ujson

from connectors.base.serializer import Serializer


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
