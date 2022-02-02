import logging
import threading
import time
from copy import deepcopy
from typing import Any, Optional

from connectors.sources import Source
from modules.core import AbstractHandler, Module
from modules.core.module import ControlModule

logger = logging.getLogger("logsight." + __name__)


class InputModule(ControlModule, AbstractHandler):
    module_name = "input_module"

    def __init__(self, control_source=None, control_sink=None, data_source: Source = None, app_settings=None):
        ControlModule.__init__(self, control_source, control_sink)
        AbstractHandler.__init__(self)

        self.data_source = data_source

    def handle(self, request: Any) -> Optional[str]:
        return super().handle(request)

    def start(self, ctx: dict):
        ctx["module"] = self.module_name

        AbstractHandler.start(self, ctx)
        # Connect sources
        ControlModule.connect(self)
        self.data_source.connect()

        internal = threading.Thread(name=self.module_name + "IntSrc", target=self._start_control_listener,
                                    daemon=True)
        internal.start()

        # This is a multiprocessing.Event which notifies the parend process that the connection was established
        while self.data_source.has_next():
            request = self.data_source.receive_message()
            # if request=="END"
            self.handle(request)

    def _start_control_listener(self):
        if self.control_source is None:
            return
        while self.control_source.has_next():
            logger.debug("Waiting for message")
            msg = self.control_source.receive_message()
            self.process_control_message(msg)
        logger.debug("Thread ended.")

    def flush(self, request: Any) -> Optional[str]:
        logger.debug("Flushing")
        return super().flush(request)

    def _process_data(self, data: Any) -> Optional[Any]:
        """Not used for input"""
        pass

    def process_control_message(self, message):
        if message['message'] == "END":
            done = self.flush(None)
            result = {"message": "done"}
            self.control_sink.send(result)

    def to_json(self):
        d = super().to_json()
        d.update({"source": self.data_source.to_json()})
        return d
