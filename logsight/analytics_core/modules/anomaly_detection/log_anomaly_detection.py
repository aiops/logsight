import logging
import os
import sys
from typing import Optional

import numpy as np

from .core.base import BaseAnomalyDetector
from .core.config import AnomalyDetectionConfig
from .core.tokenizer import LogTokenizer
from .models.onnx_model import OnnxModel
from .utils import get_padded_data
from ...logs import LogBatch

sys.path.append(os.path.join(os.path.dirname(__file__), "core"))
logger = logging.getLogger("logsight." + __name__)


class LogAnomalyDetector(BaseAnomalyDetector):
    def __init__(self, config: Optional[AnomalyDetectionConfig] = None):
        super().__init__()
        logger.debug("Initializing LogAnomalyDetector.")
        self.config = config or AnomalyDetectionConfig()
        self.model = OnnxModel(self.config.prediction_threshold).load_model()
        self.tokenizer = LogTokenizer.load_from_pickle()
        logger.debug("LogAnomalyDetector initialized successfully.")

    def predict(self, log_batch: LogBatch) -> LogBatch:
        log_messages = []
        tokenized = None

        for log in log_batch.logs:
            tokenized = np.array(self.tokenizer.tokenize_test(log.message))
            log_messages.append(tokenized[:self.config.max_len])

        log_messages[-1] = np.concatenate((tokenized, np.array([0] * self.config.pad_len)))[
                           :self.config.pad_len]

        padded = get_padded_data(log_messages, self.config.pad_len)
        # prediction = self.model.predict(padded)
        prediction = np.random.randint(0, 2, len(log_batch.logs))
        for i, log in enumerate(log_batch.logs):
            try:
                log_batch.logs[i].metadata['prediction'] = 1 if prediction[i] == 0 else 0
            except Exception as e:
                logger.error(f"Exception in the LogBatch {str(e)}")
        return log_batch
