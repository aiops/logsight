import logging
import os
import sys
import numpy as np

from .core.config import AnomalyDetectionConfig
from .core.base import BaseAnomalyDetector
from modules.anomaly_detection.models.onnx_model import RFModel

from ...logs import LogBatch
from .utils import get_padded_data

sys.path.append(os.path.join(os.path.dirname(__file__), "core"))
logger = logging.getLogger("logsight." + __name__)


class LogAnomalyDetector(BaseAnomalyDetector):
    def __init__(self):
        super().__init__()
        logger.debug("Initializing LogAnomalyDetector.")
        self.config = AnomalyDetectionConfig()
        self.model = RFModel()
        logger.debug("LogAnomalyDetector initialized successfully.")

    def predict(self, log_batch: LogBatch) -> LogBatch:
        prediction = self.model.predict(log_batch.logs)
        for i, log in enumerate(log_batch.logs):
            try:
                log_batch.logs[i].metadata['prediction'] = 1 if prediction == 0 else 0
            except Exception as e:
                logger.error(f"Exception in the LogBatch {str(e)}")
        return log_batch
