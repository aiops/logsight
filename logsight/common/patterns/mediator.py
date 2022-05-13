import logging
import threading
import time
from typing import Any, Optional

from analytics_core.logs import LogBatch
from connectors import Connector
from connectors.sinks import Sink
from connectors.sources import Source, SourceQueue
from pipeline.modules.core.module import BaseModule

logger = logging.getLogger('logsight.')


class MediatorModule(BaseModule):
    def process(self, batch: LogBatch) -> LogBatch:
        raise NotImplementedError

    def __init__(self, data_source: Source, data_sink: Sink, control_source: Source, control_sink: Sink):
        super().__init__()
        self.data_sink = data_sink
        self.data_source = data_source
        self.control_source = control_source
        self.control_sink = control_sink
        self.has_internal_queue_src = isinstance(control_source, SourceQueue)
        self.has_data_queue_src = isinstance(data_source, SourceQueue)
        self.active_threads = []

    def process_data(self, input_data):
        raise NotImplementedError

    def stop(self):
        for thread in self.active_threads:
            thread.stop()

    def run(self):
        if self.control_source:
            logger.debug("Connecting to internal source.")
            logger.debug(f"Creating internal source thread for module {self.name}.")
            internal = threading.Thread(name=self.name + "IntSrc", target=self.start_internal_listener,
                                        daemon=True)
            internal.start()

    def start_internal_listener(self):
        if self.control_source is None:
            return
        while self.control_source.has_next():
            logger.debug("Waiting for message")
            msg = self.control_source.receive_message()
            self.process_internal_message(msg)
        logger.debug("Thread ended.")

    def process_internal_message(self, msg):
        raise NotImplementedError

    def connect(self):
        for conn in [self.control_sink, self.control_source, self.data_source, self.data_sink]:
            if conn and isinstance(conn, Connector):
                conn.connect()


class StatefulMediatorModule(MediatorModule):

    def process(self, batch: LogBatch) -> LogBatch:
        raise NotImplementedError

    def __init__(self, data_source: Source, data_sink: Sink, control_source: Source, control_sink: Sink, **_kwargs):
        super().__init__(data_source, data_sink, control_source, control_sink)
        self.data_sink = data_sink
        self.data_source = data_source
        self.internal_source = control_source
        self.internal_sink = control_sink
        self.state = None
        self.cnt = 0

    def run(self):
        super().run()
        logger.debug(f"Creating data source thread for module {self.name}.")
        stream = threading.Thread(name=self.name + "DatSrc", target=self.start_data_stream, daemon=True)
        stream.start()

    def start_data_stream(self):
        if hasattr(self.data_source, 'topic'):
            logger.debug(f"starting to listen on topic {self.data_source.topic}")
        while self.data_source.has_next():
            line = self.data_source.receive_message()
            if not line:
                continue
            result = self.process_input(line)
            self.cnt += 1
            t_send = 0
            if result:
                t_send = time.perf_counter()
                self.data_sink.send(result)
            if self.cnt % 10000 == 0:
                logger.debug(time.perf_counter() - t_send)

    def process_internal_message(self, msg):
        raise NotImplementedError

    def process_input(self, input_data):
        raise NotImplementedError

    def _process_data(self, data: Any) -> Optional[Any]:
        """NOT YET IMPLEMENTED"""
        pass

    def process_data(self, input_data):
        """NOT YET IMPLEMENTED"""
        pass

    def switch_state(self, state):
        self.state = state
