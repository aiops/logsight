import json
import logging
import threading
from connectors.sink import Sink
from connectors.source import Source
from modules.core import JobDispatcherModule, State, JobManager
from .jobs import LogQualityJob


class LogQualityModule(JobDispatcherModule):
    def __init__(self, data_source: Source, data_sink: Sink, internal_source: Source, internal_sink: Sink,
                 configs):
        super().__init__(data_source, data_sink, internal_source, internal_sink)
        self.configs = configs
        self.module_name = "log_quality"

    def process_internal_message(self, msg):
        job = LogQualityJob(job_config=msg,
                            data_source=self.data_source,
                            data_sink=self.data_sink,
                            done_callback=self._done_callback,
                            error_callback=self._error_callback)
        self.job_manager.submit_job(job)

    def _done_callback(self, message):
        msg = {"private_ky": self.configs.private_key,
               "application_name": self.configs.application_name,
               "status": message}
        self.internal_sink.send(json.dumps(msg))

    def _error_callback(self, message):
        msg = {"private_ky": self.configs.private_key,
               "application_name": self.configs.application_name,
               "status": message}
        self.internal_sink.send(msg)
