import threading
from abc import ABC
from threading import Thread

from connectors.source import Source, SourceQueue
from connectors.sink import Sink
from .job_manager import JobManager


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
        self.internal_source.connect()
        thrd = threading.Thread(target=self.start_internal_listener)
        thrd.start()

    def start_internal_listener(self):
        if self.internal_source is None:
            return
            # raise Exception("Object does not have a listener.")
        while self.internal_source.has_next():
            print(self.module_name, "waiting message on topic", self.internal_source.topic)
            msg = self.internal_source.receive_message()
            print(self.module_name, "recieved mesasge")
            self.process_internal_message(msg)
        print("Thread listener ended?")

    def process_internal_message(self, msg):
        pass

    def process_input(self, input_data):
        pass


class StatefulModule(Module):
    def __init__(self, data_source: Source, data_sink: Sink, internal_source: Source, internal_sink: Sink, **kwargs):
        super().__init__(data_source, data_sink, internal_source, internal_sink)
        self.data_sink = data_sink
        self.data_source = data_source
        self.internal_source = internal_source
        self.internal_sink = internal_sink
        self.state = None

    def start_data_stream(self):
        while self.data_source.has_next():
            line = self.data_source.receive_message()
            if not line:
                continue
            print(f"[{self.module_name}] Received message from source. Processing...")
            result = self.process_input(line)
            if result:
                print(f"[{self.module_name}]Sending to sink. ")
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
