from __future__ import annotations

import json
import logging
import threading
from typing import Any, Optional, Type, Callable

from dacite import from_dict, Config

from connectors.sources import Source
from modules.core import AbstractHandler
from modules.core.module import ControlModule, StatefulControlModule, Subject, SubjectObserver
from modules.input.control_io import ControlRequest, InputControlOperations, FlushReply, FlushReplyFail, \
    FlushReplySuccess, TFlushReply, FlushReplyValidationError
from utils.helpers import DataClassJSONEncoder

logger = logging.getLogger("logsight." + __name__)


class InputModuleState(Subject):
    def __init__(self, order_num: int, logs_counter: int):
        super().__init__()
        self.order_num = order_num
        self.logs_counter = logs_counter

    @staticmethod
    def from_request_and_state(request, prev_state: InputModuleState) -> Optional[InputModuleState]:
        if "orderCounter" not in request:
            return None
        order_counter = request["orderCounter"]
        if not prev_state or order_counter > prev_state.order_num:
            return InputModuleState(order_counter, 1)
        else:
            return InputModuleState(order_counter, prev_state.logs_counter + 1)


class InputModule(StatefulControlModule, AbstractHandler):
    module_name = "input_module"

    def __init__(self, control_source=None, control_sink=None, data_source: Source = None, app_settings=None):
        StatefulControlModule.__init__(self, control_source, control_sink)
        AbstractHandler.__init__(self)
        self.data_source = data_source

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
        while self.data_source.has_next():
            request = self.data_source.receive_message()
            # TODO NEEDS REVISION @PETAR
            if request['source'] == "FILE" or request['source'] == "SAMPLE":
                request['message'] = request['message']['message']
            else:
                request['message'] = json.dumps(request['message'])  #
            self.handle(request)
            self._update_state(request)

    def _update_state(self, request):
        state = InputModuleState.from_request_and_state(request, self.state)
        # self.notify() will be called when the state is set
        self.state = state

    def _start_control_listener(self):
        if self.control_source is None:
            return
        logger.info("Input module is ready to receive control messages.")
        while self.control_source.has_next():
            msg = self.control_source.receive_message()
            logger.debug(f"Input module received control message: {msg}")
            self._process_control_message(msg)
        logger.debug("Control message receiving thread terminated.")

    def flush(self, request: Any) -> Optional[str]:
        logger.debug("Flushing input module")
        return super().flush(request)

    def on_flush_callback(self, state: InputModuleState, observer: InputModuleFlushStateObserver):
        logger.debug(f"Executing flush callback for state: {state}")
        self.detach(observer)
        try:
            self.flush(None)
        except Exception as e:
            logger.error(f"Failed to execute flush request {observer.flush_request}. Reason: {e}")
            self._send_flush_failed_reply(state, observer, f"Failed to execute flush request {observer.flush_request}. Reason: {e}")
            return

        logger.debug(f"Flush successful. Sending success reply.")
        self._send_success_reply(state, observer, "Flush success")

    def _send_flush_failed_reply(self, state: InputModuleState, observer: InputModuleFlushStateObserver, msg: str):
        flush_reply = to_flush_reply(FlushReplyFail, observer.flush_request, state, msg)
        self._send_control_reply(flush_reply)

    def _send_success_reply(self, state: InputModuleState, observer: InputModuleFlushStateObserver, msg: str):
        flush_reply = to_flush_reply(FlushReplySuccess, observer.flush_request, state, msg)
        self._send_control_reply(flush_reply)

    def _send_request_validation_error_reply(self, msg: str):
        flush_reply = FlushReplyValidationError(description=msg)
        self._send_control_reply(flush_reply)

    def _process_data(self, data: Any) -> Optional[Any]:
        """Not used for input"""
        pass

    def _process_control_message(self, message):
        try:
            control_request = from_dict(data_class=ControlRequest, data=message, config=Config(cast=[InputControlOperations]))
            logger.debug(f"{self.module_name} received a control message: {control_request}")
        except Exception as e:
            logger.error(f"Failed to deserialize flush request {message}. Reason: {e}")
            self._send_request_validation_error_reply(f"Failed to deserialize flush request {message}. Reason: {e}")
            return

        if control_request.operation == InputControlOperations.FLUSH:
            logger.debug(f"Identified control message operation: {InputControlOperations.FLUSH.name}")
            observer = InputModuleFlushStateObserver(control_request, self.on_flush_callback)
            self.attach(observer)
            self.notify()

    def _send_control_reply(self, reply: FlushReply):
        try:
            self.control_sink.send(json.dumps(reply, cls=DataClassJSONEncoder))
        except Exception as e:
            logger.error(f"Failed to send input control reply {reply}. Reason: {e}")

    def to_json(self):
        d = super().to_json()
        d.on_update({"source": self.data_source.to_json()})
        return d


# Observer to check condition
class InputModuleFlushStateObserver(SubjectObserver):

    def __init__(
            self, flush_request: ControlRequest,
            callback: Callable[[InputModuleState, InputModuleFlushStateObserver], None]
    ):
        self.flush_request: ControlRequest = flush_request
        self._callback = callback

    def on_update(self, state: InputModuleState) -> None:
        logger.debug(f"Observed state change: {state}")
        if state:
            if state.order_num > self.flush_request.orderNum:
                self._callback(state, self)
            elif state.order_num == self.flush_request.orderNum and state.logs_counter >= self.flush_request.logsCount:
                self._callback(state, self)


def to_flush_reply(flush_reply_class: Type[TFlushReply], flush_request: ControlRequest, state: InputModuleState,
                   description: str) -> TFlushReply:
    return flush_reply_class(
        id=flush_request.id,
        orderNum=flush_request.orderNum,
        logsCount=flush_request.logsCount,
        currentLogsCount=state.logs_counter,
        description=description
    )
