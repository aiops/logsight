import logging

from analytics_core.logs import LogBatch
from common.utils.helpers import to_flat_dict
from configs.global_vars import PIPELINE_INDEX_EXT
from pipeline.modules.core.module import ConnectableModule

logger = logging.getLogger("logsight." + __name__)


class DataStoreModule(ConnectableModule):
    """
    Module for storing the data using a connector.
    """

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
        processed = [to_flat_dict(log) for log in batch.logs]
        self.connector.send(processed, target="_".join([batch.index, PIPELINE_INDEX_EXT]))
        return batch
