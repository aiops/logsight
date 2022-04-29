import json
from typing import Dict, Union

from dacite import from_dict

from analytics_core.logs import LogBatch, LogEvent, LogsightLog
from connectors.transformers import Transformer


class DictTransformer(Transformer):
    """
    This class transforms a string or a dictionary and returns a dictionary
    """

    def transform(self, data: Union[str, Dict]) -> Dict:
        """
        This function takes in a string or a dictionary and returns a dictionary

        :param data: The data to be transformed
        :type data: Union[str, Dict]
        """
        if isinstance(data, str):
            return json.loads(data)
        return data


class LogBatchTransformer(Transformer):
    """
    Transformer class for transforming data to LogBatch
    """

    def transform(self, data: Union[str, Dict]) -> LogBatch:
        """
        It takes a string or a dictionary, and returns a LogBatch object

        :param data: The data to be transformed
        :type data: Union[str, Dict]
        :return: A LogBatch object
        """
        if isinstance(data, str):
            data = json.loads(data)
        return from_dict(data_class=LogBatch, data=data)


class FileToLogBatchTransformer(Transformer):
    """
    Transformer class for transforming data to LogBatch
    """

    def transform(self, data: Union[str, Dict]) -> LogBatch:
        """
        It takes a string or a dictionary, and returns a LogBatch object

        :param data: The data to be transformed
        :type data: Union[str, Dict]
        :return: A LogBatch object
        """
        if isinstance(data, str):
            data = [data]
        return LogBatch(list(map(LogsightLog, map(LogEvent, data))))
