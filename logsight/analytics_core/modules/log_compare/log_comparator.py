from analytics_core.modules.log_quality.qulog_level_attention import QulogLevelAttention
from connectors.sinks import Sink
from connectors.sources import Source
from pipeline.modules.core import Job


class NoDataException(Exception):
    pass


class LogIncidentJob(Job):

    def __init__(self, job_config, data_source: Source, data_sink: Sink, **kwargs):
        super().__init__(job_config, **kwargs)
        self.data_source = data_source
        self.data_sink = data_sink
        self.job_config = job_config

        self.qlog_level = QulogLevelAttention()
        self.qlog_linguistic = None

    def _execute(self):
        logs = self._load_data()
        if len(logs) == 0:
            return ("The selected time frame contains no logs to be evaluated, "
                    "please check your request and try again!")
        logs = self._get_unique_logs(logs)

        preds_level, preds_ling = self._make_predictions(logs)
        results = self._append_results(logs, preds_level, preds_ling)
        self._store_results(results)
        return "OK"

    def _make_predictions(self, logs):
        preds_level = self.qlog_level.predict(logs.tolist())
        preds_ling = self.qlog_linguistic.predict(logs.tolist())
        return preds_level, preds_ling

    def _load_data(self):
        # TODO: DEFINE LOAD DATA FROM KIBANA
        data = self.data_source.receive_message()
        if not data:
            raise NoDataException("Failed to get data.")
        return data

    def _store_results(self, results):
        self.data_sink.store_results(results)

    def _get_unique_logs(self, logs):
        """needs to be implemented"""
        pass
