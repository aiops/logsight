from __future__ import annotations

import logging
from typing import Callable

from analytics_core.logs import LogsightLog
from analytics_core.modules.anomaly_detection.log_anomaly_detection import LogAnomalyDetector
from pipeline.modules.core.module import TransformModule

logger = logging.getLogger("logsight." + __name__)


class AnomalyDetectionModule(TransformModule):
    """
    Transform module that uses the anomaly detection model to detect anomalies in the logs
    """

    def _get_transform_function(self) -> Callable[[LogsightLog], LogsightLog]:
        return self.ad.process_log

    def __init__(self, **kwargs):
        self.__dict__.update(**kwargs)
        self.ad = LogAnomalyDetector()
        self.ad.load_model()
        super().__init__()
