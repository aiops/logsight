import logging
import os
import sys
from typing import Optional

import numpy as np

from logsight.analytics_core.modules.anomaly_detection.core.base import BaseAnomalyDetector
from logsight.analytics_core.modules.anomaly_detection.core.config import AnomalyDetectionConfig
from logsight.analytics_core.modules.anomaly_detection.core.tokenizer import LogTokenizer
from logsight.analytics_core.modules.anomaly_detection.models.onnx_model import OnnxModel
from logsight.analytics_core.modules.anomaly_detection.utils import pad_sequences
from logsight.analytics_core.logs import LogBatch

sys.path.append(os.path.join(os.path.dirname(__file__), "core"))
logger = logging.getLogger("logsight." + __name__)

class LogAnomalyDetector(BaseAnomalyDetector):
    """
    The LogAnomalyDetector class loads the pre-trained model and tokenizer, and then uses them to
    predict the anomaly score for each log message in the batch.
    """
    def __init__(self, config: Optional[AnomalyDetectionConfig] = None):
        super().__init__()
        self.config = config or AnomalyDetectionConfig()
        self.model = OnnxModel(self.config.prediction_threshold).load_model()
        self.tokenizer = LogTokenizer.load_from_pickle()

    def predict(self, log_batch: LogBatch) -> LogBatch:
        """
        We take a LogBatch, tokenize the messages, pad the sequences, and then predict the labels
        
        :param log_batch: LogBatch
        :type log_batch: LogBatch
        :return: The LogBatch with the prediction added to the metadata
        """

        tokenized = [self.tokenizer.tokenize(log.message) for log in log_batch.logs]

        padded = pad_sequences(tokenized, maxlen=self.config.pad_len)
        prediction = self.model.predict(padded)
        for i, log in enumerate(log_batch.logs):
            try:
                log_batch.logs[i].metadata['prediction'] = 1 if prediction[i] == 0 else 0
            except Exception as e:
                logger.error(f"Exception in the LogBatch {str(e)}")
        return log_batch
