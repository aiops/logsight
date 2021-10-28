import json
import logging
import threading
from connectors.sink import Sink
from connectors.source import Source
from modules.api import JobDispatcherModule, JobManager
from .jobs import TrainLogsyADModelJob, TrainCountADModelJob


class ModelTrainModule(JobDispatcherModule):
    def __init__(self, data_source: Source, data_sink: Sink, internal_source: Source, internal_sink: Sink,
                 config):
        super().__init__(data_source, data_sink, internal_source, internal_sink)
        self.configs = config  # must validate first
        self.job_manager = JobManager(max_workers=config.max_workers)
        self.module_name = "model_train"

    def run(self):
        self.internal_source.connect()
        internal = threading.Thread(target=self.start_internal_listener)
        internal.start()

    def process_internal_message(self, msg):
        job_constructor = TrainLogsyADModelJob if msg['model'] == 'logsy' else TrainCountADModelJob
        job = job_constructor(job_config=msg,
                              data_source=self.data_source,
                              data_sink=self.data_sink,
                              done_callback=self._done_callback,
                              error_callback=self._error_callback)

        self.job_manager.submit_job(job)

    def _done_callback(self, message):
        msg = {"private_ky": self.configs.private_key,
               "application_name": self.configs.application_name,
               "status": message}
        print("done callback sending message", msg)
        self.internal_sink.send(json.dumps(msg))

    def _error_callback(self, message):
        msg = {"private_ky": self.configs.private_key,
               "application_name": self.configs.application_name,
               "status": message}
        print(" err callback sending message", msg)

        self.internal_sink.send(msg)
