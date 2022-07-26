import logging
import os
import sys
from typing import Optional

import numpy as np

from .core.base import BaseAnomalyDetector
from .core.config import AnomalyDetectionConfig
from .core.tokenizer import LogTokenizer
from .models.onnx_model import OnnxModel
from .utils import pad_sequences
from ...logs import LogBatch

sys.path.append(os.path.join(os.path.dirname(__file__), "core"))
logger = logging.getLogger("logsight." + __name__)


class LogAnomalyDetector(BaseAnomalyDetector):
    def __init__(self, config: Optional[AnomalyDetectionConfig] = None):
        super().__init__()
        self.config = config or AnomalyDetectionConfig()
        self.model = OnnxModel(self.config.prediction_threshold).load_model()
        self.tokenizer = LogTokenizer.load_from_pickle()

    def predict(self, log_batch: LogBatch) -> LogBatch:

        tokenized = [self.tokenizer.tokenize(log.message) for log in log_batch.logs]

        padded = pad_sequences(tokenized, maxlen=self.config.pad_len)
        #   prediction = self.model.predict(padded)
        prediction = [np.random.randint(0, 1) for _ in range(len(padded))]
        for i, log in enumerate(log_batch.logs):
            try:
                log_batch.logs[i].metadata['prediction'] = 1 if prediction[i] == 0 else 0
            except Exception as e:
                logger.error(f"Exception in the LogBatch {str(e)}")
        return log_batch
