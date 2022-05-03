import os
import logging

logger = logging.getLogger("logsight." + __name__)


class AnomalyDetectionConfig:
    def __init__(self):
        try:
            self.prediction_threshold = float(os.environ.get('PREDICTION_THRESHOLD', 0.85))
        except ValueError:
            self.prediction_threshold = 0.90
            logger.error(
                f"Prediction threshold is not a valid float. Using default prediction_threshold={self.prediction_threshold}")
        self.conf = {
            'pad_len': 50,
            'max_len': 20,
            'log_mapper': {0: 'anomaly', 1: 'normal'},
            'prediction_threshold': self.prediction_threshold
        }

    def get(self, name):
        return self.conf[name]

    def set(self, name, value):
        if name in self.conf.keys():
            self.conf[name] = value
        else:
            raise ValueError("The variable name does not exist! "
                             "The possible configs variables to be set are:",
                             self.conf.keys())
