import logging
from typing import Union

from analytics_core.logs import LogBatch
from common.utils.helpers import to_flat_dict
from configs.global_vars import PIPELINE_INDEX_EXT
from connectors import ConnectableConnector, Sink, Source
from pipeline.modules.core.module import ConnectableModule

logger = logging.getLogger("logsight." + __name__)


class BatchMetadataStoreModule(ConnectableModule):
    """
    Module for storing the data using a connector.
    """

    def __init__(self, connector: Union[Source, Sink, ConnectableConnector], index_ext: str = "log_agg",
                 field: str = None):
        super().__init__(connector)
        self.index_ext = index_ext
        self.field = field

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
        to_send = batch.metadata[self.field] if self.field else batch.metadata
        self.connector.send(to_send, target="_".join([batch.index, self.index_ext]))
        return batch


class LogStoreModule(ConnectableModule):
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


class TemplateStoreModule(ConnectableModule):
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
        templates = {log.metadata.get('template', "") for log in batch.logs}
        processed = [{"template": template} for template in templates]
        self.connector.send(processed, target="_".join([batch.index, "templates"]))
        return batch
