import threading
from abc import ABC
import logging
import time

from connectors.sources import Source, SourceQueue
from connectors.sinks import Sink
from .job_manager import JobManager

logger = logging.getLogger("logsight." + __name__)


class Module(ABC):
    def __init__(self, data_source: Source, data_sink: Sink, internal_source: Source, internal_sink: Sink):
        self.data_sink = data_sink
        self.data_source = data_source
        self.internal_source = internal_source
        self.internal_sink = internal_sink
        self.has_internal_queue_src = isinstance(internal_source, SourceQueue)
        self.has_data_queue_src = isinstance(data_source, SourceQueue)
        self.module_name = "module"
        self.active_threads = []

    def stop(self):
        for thread in self.active_threads:
            thread.stop()

    def run(self):
        if self.internal_source:
            logger.debug("Connecting to internal source.")
            self.internal_source.connect()
            logger.debug(f"Creating internal source thread for module {self.module_name}.")
            internal = threading.Thread(name=self.module_name + "IntSrc", target=self.start_internal_listener,
                                        daemon=True)
            internal.start()

    def start_internal_listener(self):
        if self.internal_source is None:
            return
        while self.internal_source.has_next():
            logger.debug("Waiting for message")
            msg = self.internal_source.receive_message()
            self.process_internal_message(msg)
        logger.debug("Thread ended.")

    def process_internal_message(self, msg):
        raise NotImplementedError

    def process_input(self, input_data):
        raise NotImplementedError

    def connect(self):
        if self.internal_source:
            self.internal_source.connect()
        if self.internal_sink:
            self.internal_sink.connect()
        if self.data_source:
            self.data_source.connect()
        if self.data_sink:
            self.data_sink.connect()

    def to_json(self):
        return {"name": self.module_name,
                "data_source": self.data_source.to_json()}


class StatefulModule(Module):
    def __init__(self, data_source: Source, data_sink: Sink, internal_source: Source, internal_sink: Sink, **kwargs):
        super().__init__(data_source, data_sink, internal_source, internal_sink)
        self.data_sink = data_sink
        self.data_source = data_source
        self.internal_source = internal_source
        self.internal_sink = internal_sink
        self.state = None
        self.cnt = 0

        self.total_recv = 0
        self.total_process = 0
        self.total_send = 0

    def run(self):
        super().run()
        logger.debug(f"Creating data source thread for module {self.module_name}.")
        stream = threading.Thread(name=self.module_name + "DatSrc", target=self.start_data_stream, daemon=True)
        stream.start()

    def start_data_stream(self):
        self.t = time.perf_counter()
        if hasattr(self.data_source, 'topic'):
            logger.debug(f"starting to listen on topic {self.data_source.topic}")
        while self.data_source.has_next():
            t_recv = time.perf_counter()
            line = self.data_source.receive_message()
            self.total_recv += (time.perf_counter() - t_recv)
            if not line:
                continue
            t_process = time.perf_counter()
            result = self.process_input(line)
            self.total_process += (time.perf_counter() - t_process)
            self.cnt += 1
            if result:
                t_send = time.perf_counter()
                self.data_sink.send(result)
                self.total_send += (time.perf_counter() - t_send)
            if self.cnt % 10000 == 0:
                logger.debug(f"{self.module_name} processed {self.cnt} messages in {time.perf_counter() - self.t} ")
                logger.debug(
                    f"Recv time:{round(self.total_recv, 2)}, process {round(self.total_process, 2)}, send:{round(self.total_send, 2)}")

    def process_internal_message(self, msg):
        raise NotImplementedError

    def process_input(self, input_data):
        raise NotImplementedError

    def switch_state(self, state):
        self.state = state


class JobDispatcherModule(Module):
    def __init__(self, data_source: Source, data_sink: Sink, internal_source: Source, internal_sink: Sink,
                 max_workers=None, **kwargs):
        super().__init__(data_source, data_sink, internal_source, internal_sink)
        self.data_sink = data_sink
        self.data_source = data_source
        self.internal_source = internal_source
        self.internal_sink = internal_sink
        self.job_manager = JobManager(max_workers=max_workers)

    def process_internal_message(self, msg):
        raise NotImplementedError

    def process_input(self, input_data):
        return input_data
