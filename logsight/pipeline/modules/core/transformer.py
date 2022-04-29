from abc import abstractmethod
from typing import Callable

from analytics_core.logs import LogBatch, LogsightLog


class DataTransformer:
    """ Abstract class which performs data transformations on LogsightLog.
        Every Implementation requires implementing _get_transform_function where the transformation function is provided."""
    _transform_function: Callable[[LogsightLog], LogsightLog] = None

    def __init__(self):
        self._transform_function = self._get_transform_function()

    @abstractmethod
    def _get_transform_function(self) -> Callable[[LogsightLog], LogsightLog]:
        raise NotImplementedError

    def transform(self, data: LogBatch) -> LogBatch:
        """
        The transform function takes a LogBatch object as input and returns a transformed
        LogBatch object. The transform function is applied to each batch before it is
        submitted to the output queue. It can be used for filtering or modifying the logs in
        the batch.
        Args:
            data (LogBatch): The data to be transformed.
        Returns:
            LogBatch:  A LogBatch object with the logs transformed by the _transform_function
        """
        data.logs = list(map(self._transform_function, data.logs))
        return data
