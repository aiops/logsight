import logging

from analytics_core.logs import LogBatch
from common.utils.helpers import to_flat_dict
from connectors.sinks import Sink
from pipeline.modules.core.module import ConnectableModule

logger = logging.getLogger("logsight." + __name__)


class DataStoreModule(ConnectableModule):
    """
    Module for storing the data using a connector.
    """

    def __init__(self, connector: Sink, store_metadata: bool = False,
                 store_logs: bool = False):
        super().__init__(connector)
        self.store_metadata = store_metadata
        self.store_logs = store_logs
        if not (self.store_logs or self.store_metadata):
            logger.warning("Not storing any information, please specify whether to store logs or metadata from batch.")

    def process(self, batch: LogBatch) -> LogBatch:
        """
        The process function is called for every batch of logs that is received.
        Its purpose is to send the log data using a connector and store it in a format that can be
        queried later.  The function should return the log batch if no errors are encountered.
        Args:
        data (LogBatch): Pass the log batch object to the process function
        Returns:
             LogBatch: The log batch, so we can use it in the next function
        """
        if self.store_logs:
            processed = [to_flat_dict(log) for log in batch.logs]
            self.connector.send(processed, target=batch.index)
        if self.store_metadata:
            self.connector.send(batch.metadata)
        print(batch)
        return batch
