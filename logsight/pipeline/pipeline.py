import logging
import threading
import uuid
from typing import Dict, Optional

from connectors import Connector, Source
from connectors.sources.source import ConnectionSource
from .modules.core import Module
from .modules.core.module import ConnectableModule

logger = logging.getLogger("logsight." + __name__)


class Pipeline:
    """ A pipeline is a collection of modules that are connected together..."""
    _id = uuid.uuid4()

    def __init__(self, modules: Dict[str, Module], input_module: Module, data_source: Source,
                 control_source: Optional[ConnectionSource] = None, metadata: Optional[Dict] = None):
        self.control_source = control_source
        self.data_source = data_source
        self.input_module = input_module
        self.modules = modules
        self.metadata = metadata

    def run(self):
        """
        Run the pipeline. The pipeline and its modules connect to external endpoints before the pipeline starts
        receiving messages from the data source.
        """
        self._connect()
        self._start_receiving()

    def _connect(self):
        """
        The function connects the data source, control source, and modules
        """
        # connect data source
        if isinstance(self.data_source, Connector):
            self.data_source.connect()
        # connect control source
        if self.control_source:
            self.control_source.connect()
        # connect modules
        for module in self.modules.values():
            if isinstance(module, ConnectableModule):
                module.connector.connect()

    def _start_receiving(self):
        """
        It starts a thread that listens for control messages, and then it loops over the data source, receiving messages and
        passing them to the input module
        """
        internal = threading.Thread(
            name=str(self), target=self._start_control_listener, daemon=True
        )
        internal.start()
        while self.data_source.has_next():
            message = self.data_source.receive_message()
            self.input_module.handle(message)

    def _start_control_listener(self):
        """
        The function starts a thread that listens for control messages from the control source
        """
        logger.info("Pipeline is ready to receive control messages.")
        while self.control_source.has_next():
            msg = self.control_source.receive_message()
            logger.debug(f"Pipeline received control message: {msg}")
            self._process_control_message(msg)
        logger.debug("Control message receiving thread terminated.")

    @staticmethod
    def _process_control_message(msg):
        print(msg)

    def __repr__(self):
        return f"Pipeline ({self._id})"

    @property
    def id(self):
        return self._id
