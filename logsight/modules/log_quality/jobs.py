from datetime import datetime

from modules.core import Job
from connectors.source import Source
from connectors.sink import Sink
from modules.log_quality.qulog_level_attention import QulogLevelAttention
from .qulog_linguistic_attention import QulogLinguisticAttention


class LogQualityJob(Job):
    label2id = {"INFO": 1, "DEBUG": 1, "TRACE": 1, "WARNING": 0, "WARN": 0, "ERROR": 0, "EXCEPTION": 0, "CRITICAL": 0}
    id2label = {1: "INFO, DEBUG, TRACE", 0: "WARNING, ERROR, EXCEPTION, CRITICAL"}

    def __init__(self, job_config, data_source: Source, data_sink: Sink, **kwargs):
        super().__init__(job_config, **kwargs)
        self.data_source = data_source
        self.data_sink = data_sink
        self.job_config = job_config

        self.qlog_level = QulogLevelAttention()
        self.qlog_linguistic = QulogLinguisticAttention()

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
            raise Exception("Failed to get data.")
        return data

    @staticmethod
    def _get_unique_logs(logs):
        return logs

    def _append_results(self, logs, preds_level, preds_ling):
        result = []
        for idx, log in enumerate(logs):
            log['_source'].update({'predicted_level', preds_level[idx]})

            try:
                actual_level = self.label2id[log['_source']['actual_level']]
                pred_level = log['_source']['predicted_level']
                log['_source']['log_level_score'] = int(actual_level == pred_level)
            except Exception as e:
                print(e)
                log['_source']['log_level_score'] = 1

            log['_source']['predicted_level'] = self.id2label[log['_source']['predicted_level']]
            log['_source'].update(preds_ling[idx])

            log['_source']['@timestamp'] = str(datetime.now().isoformat())
            result.append(log['_source'])

        return result

    def _store_results(self, results):
        self.data_sink.store_results(results)
