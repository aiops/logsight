from __future__ import annotations

import logging

from analytics_core.logs import LogBatch
from analytics_core.modules.anomaly_detection.log_anomaly_detection import LogAnomalyDetector
from pipeline.modules.core.module import TransformModule

logger = logging.getLogger("logsight." + __name__)


class AnomalyDetectionModule(TransformModule):
    """
    Transform module that uses the anomaly detection model to detect anomalies in the logs
    """

    def __init__(self, **kwargs):
        self.__dict__.update(**kwargs)
        self.ad = LogAnomalyDetector()
        super().__init__()

    def transform(self, data: LogBatch) -> LogBatch:
        return self.ad.predict(data)
