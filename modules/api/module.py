import threading
from abc import ABC
import logging

from connectors.source import Source, SourceQueue
from connectors.sink import Sink
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
            # raise Exception("Object does not have a listener.")
        while self.internal_source.has_next():
            logger.debug("Waiting for message")
            msg = self.internal_source.receive_message()
            self.process_internal_message(msg)
        logger.debug("Thread ended.")

    def process_internal_message(self, msg):
        pass

    def process_input(self, input_data):
        pass

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

    def run(self):
        super().run()
        logger.debug(f"Creating data source thread for module {self.module_name}.")
        stream = threading.Thread(name=self.module_name + "DatSrc", target=self.start_data_stream, daemon=True)
        stream.start()
        stream.join()

    def start_data_stream(self):

        if hasattr(self.data_source, 'topic'):
            logger.debug(f"starting to listen on topic {self.data_source.topic}")
        while self.data_source.has_next():
            line = self.data_source.receive_message()
            if not line:
                continue
            result = self.process_input(line)
            if result:
                self.data_sink.send(result)

    def process_internal_message(self, msg):
        pass

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
        pass

    def process_input(self, input_data):
        return input_data
