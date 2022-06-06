from abc import ABC, abstractmethod

from analytics_core.logs import LogBatch


class DataTransformer(ABC):
    """ Abstract class which performs data transformations on LogsightLog.
        Every Implementation requires implementing _get_transform_function where the transformation function is provided."""

    @abstractmethod
    def transform(self, data: LogBatch) -> LogBatch:
        """
        The transform function takes a LogBatch object as input and returns a transformed
        LogBatch object. The transform function is applied to each batch before it is
        submitted to the output queue. It can be used for filtering or modifying the logs in
        the batch.
        Args:
            data (LogBatch): The data to be transformed.
        Returns:
            LogBatch:  A LogBatch object with the transformed logs
        """
        raise NotImplementedError
