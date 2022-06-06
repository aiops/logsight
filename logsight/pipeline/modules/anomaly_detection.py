from __future__ import annotations

import logging
from typing import Dict, Optional

from dacite import from_dict

from analytics_core.logs import LogBatch
from analytics_core.modules.anomaly_detection.core.config import AnomalyDetectionConfig
from analytics_core.modules.anomaly_detection.log_anomaly_detection import LogAnomalyDetector
from pipeline.modules.core.module import TransformModule

logger = logging.getLogger("logsight." + __name__)


class AnomalyDetectionModule(TransformModule):
    """
    Transform module that uses the anomaly detection model to detect anomalies in the logs
    """

    def __init__(self, ad_config: Optional[Dict] = None):
        if ad_config:
            ad_config = from_dict(data_class=AnomalyDetectionConfig, data=ad_config)
        self.ad = LogAnomalyDetector(ad_config)
        super().__init__()

    def transform(self, data: LogBatch) -> LogBatch:
        return self.ad.predict(data)
