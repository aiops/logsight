from typing import Dict

import ujson

from logsight.connectors.base.serializer import Serializer


class JSONSerializer(Serializer):
    """
    Serializer class for transforming JSON strings to dictionary
    """

    def serialize(self, data: Dict) -> str:
        """
        The serialize function takes a Dict object and returns a JSON string.
        The JSON string is the serialized representation of the Dict object.

        Args:
            data:LogBatch: Pass the data to be serialized

        Returns:
            A JSON string
        """
        return ujson.dumps(data)

    def deserialize(self, data: str) -> Dict:
        """
        The deserialize function takes a JSON string and returns a dict.

        Args:
            data:string: Pass the data to be deserialized

        Returns:
            A dict object
        """
        return ujson.loads(data)
