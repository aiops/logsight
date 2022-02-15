import json
import logging
import threading
from typing import Any, Optional, Type

from dacite import from_dict

from connectors.sources import Source
from modules.core import AbstractHandler
from modules.core.module import ControlModule
from modules.input.control_io import ControlRequest, InputControlOperations, ControlReply, ControlReplyFail, \
    ControlReplySuccess, TControlReply, ControlReplyValidationFail
from utils.helpers import DataClassJSONEncoder

logger = logging.getLogger("logsight." + __name__)


class InputModule(ControlModule, AbstractHandler):
    module_name = "input_module"

    def __init__(self, control_source=None, control_sink=None, data_source: Source = None, app_settings=None):
        ControlModule.__init__(self, control_source, control_sink)
        AbstractHandler.__init__(self)

        self.data_source = data_source

        # Due to parallel processing of control messages, a lock is needed to synchronize access to the flush_controller
        self.rlock = threading.RLock()
        self.flush_controller: Optional[FlushController] = None

    def handle(self, request: Any) -> Optional[str]:
        return super().handle(request)

    def start(self, ctx: dict):
        ctx["module"] = self.module_name

        AbstractHandler.start(self, ctx)
        # Connect sources
        self.data_source.connect()
        # connect controller
        ControlModule.connect(self)

        internal = threading.Thread(
            name=self.module_name + "IntSrc", target=self._start_control_listener, daemon=True
        )
        internal.start()

        # This is a multiprocessing.Event which notifies the parend process that the connection was established
        while self.data_source.has_next():
            request = self.data_source.receive_message()
            self.handle(request)
            if self.flush_controller and "orderCounter" in request:
                self.flush_controller.check_flush(request["orderCounter"])

    def _start_control_listener(self):
        if self.control_source is None:
            return
        while self.control_source.has_next():
            logger.info("Waiting for control messages...")
            msg = self.control_source.receive_message()
            logger.info(f"Received message: {msg}")
            self._process_control_message(msg)
        logger.debug("Thread ended.")

    def flush(self, request: Any) -> Optional[str]:
        logger.debug("Flushing input module")
        try:
            super().flush(request)
        except Exception as e:
            logger.error(f"Flushing {self.flush_controller.to_json()} failed. Reason: {e}")
            self._send_control_reply(to_control_reply(ControlReplyFail, self.flush_controller, str(e)))
            self.flush_controller = None
            return
        self._send_control_reply(to_control_reply(ControlReplySuccess, self.flush_controller, "Flush success"))
        self.flush_controller = None

    def _process_data(self, data: Any) -> Optional[Any]:
        """Not used for input"""
        pass

    def _process_control_message(self, message):
        try:
            input_control_message = from_dict(data_class=ControlRequest, data=message)
        except Exception as e:
            logger.error(f"Failed to deserialize input control message {message}. Reason: {e}")
            self._send_control_reply(ControlReplyValidationFail(description=str(e)))
            return

        if input_control_message.operation == InputControlOperations.FLUSH:
            # Simply overwrite previous.
            # TODO: Runtime conditions
            self.flush_controller = FlushController(
                input_module=self,
                receipt_id=input_control_message.id,
                order_counter=input_control_message.orderCounter,
                logs_count=input_control_message.logsCount
            )

    def _send_control_reply(self, reply: ControlReply):
        try:
            self.control_sink.send(json.dumps(reply, cls=DataClassJSONEncoder))
        except Exception as e:
            logger.error(f"Failed to send input control reply {reply}. Reason: {e}")

    def to_json(self):
        d = super().to_json()
        d.update({"source": self.data_source.to_json()})
        return d


class FlushController:

    def __init__(self, input_module: InputModule, receipt_id: str, order_counter: int, logs_count: int):
        self.input_module = input_module
        self.receipt_id = receipt_id
        self.order_counter = order_counter
        self.logs_count = logs_count
        self.current_logs_counter = 0

    def check_flush(self, order_counter):
        self.current_logs_counter += 1
        if order_counter > self.order_counter:
            return self.input_module.flush(None)
        elif order_counter == self.order_counter and self.current_logs_counter == self.logs_count:
            return self.input_module.flush(None)

    def to_json(self):
        return {
            "receipt_id": self.receipt_id,
            "order_counter": self.order_counter,
            "num_messages": self.logs_count,
            "message_counter": self.current_logs_counter
        }


def to_control_reply(reply_class: Type[TControlReply], flush_controller: FlushController, description: str) -> TControlReply:
    return reply_class(
        id=flush_controller.receipt_id,
        orderCounter=flush_controller.order_counter,
        logsCount=flush_controller.logs_count,
        currentLogsCount=flush_controller.current_logs_counter,
        description=description
    )
