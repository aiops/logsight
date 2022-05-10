import logging
import os
import pickle
import sys
import numpy as np

from modules.anomaly_detection.core.config import AnomalyDetectionConfig
from modules.anomaly_detection.core.base import BaseModel

sys.path.append(os.path.join(os.path.dirname(__file__), "../core"))
logger = logging.getLogger("logsight." + __name__)


class RFModel(BaseModel):
    def __init__(self):
        super().__init__()
        self.config = AnomalyDetectionConfig()
        self.model = None
        self.tokenizer = None
        self.load_model()

    def tokenize(self, logs):
        tokenized_logs = []
        for log in logs:
            words = log.split(" ")
            log = []
            for word in words:
                try:
                    log.append(self.tokenizer[word])
                except KeyError:
                    pass
            if len(log) == 0:
                log.append(self.tokenizer['info'])
            tokenized_logs.append(np.mean(log, axis=0))
        return tokenized_logs

    def predict(self, logs):
        if self.model is None:
            raise ValueError("The model is still not loaded")
        tokenized_logs = self.tokenize(logs)
        result = self.model.predict_proba(tokenized_logs)
        return np.where(result[:, 0] > self.config['prediction_threshold'], 0, 1)

    def load_model(self):
        cur_f = os.path.dirname(os.path.realpath(__file__))
        self.tokenizer = pickle.load(open(os.path.join(cur_f, "vectorizer_github_w.pickle"), 'rb'))
        self.model = pickle.load(open(os.path.join(cur_f, "rf_github.pickle"), 'rb'))
