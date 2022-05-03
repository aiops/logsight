import gc
import logging
import os
import pickle
import re
import sys

import numpy as np
import onnxruntime as ort

from .core.log_level_config import ConfigLogLevelEstimation
from ...logs import LogsightLog

sys.path.append(os.path.join(os.path.dirname(__file__), "core"))
logger = logging.getLogger("logsight." + __name__)

from .utils import get_padded_data, PREDICTION_THRESHOLD, softmax


class LogAnomalyDetector:
    def __init__(self):
        logger.debug("initializing the log anomaly detector")
        self.tokenizer = None
        self.model = None
        self.config = ConfigLogLevelEstimation()
        self.model_loaded = False
        self.ort_sess = None
        logger.debug("log anomaly detector initialized")

    def prediction2loglevel(self, prediction):
        return self.config.get('log_mapper')[prediction[0]]

    def predict(self, log):
        out_numpy = softmax(self.ort_sess.run(None, {'input': log,
                                                     'src_mask': None})[0])

        log_level_prediction = np.where(out_numpy[:, 0] > PREDICTION_THRESHOLD, 0, 1)
        del out_numpy
        gc.collect()
        return log_level_prediction, None

    def load_model(self):
        cur_f = os.path.dirname(os.path.realpath(__file__))
        self.tokenizer = pickle.load(open(os.path.join(cur_f, "models/github_tokenizer.pickle"), 'rb'))
        session_option = ort.SessionOptions()
        session_option.enable_mem_pattern = False
        session_option.enable_cpu_mem_arena = False
        self.ort_sess = ort.InferenceSession(os.path.join(cur_f, "models/model_github.onnx"),
                                             sess_options=session_option)
        self.model_loaded = True

    def tokenize(self, log):
        regex = re.compile('[^a-zA-Z ]')
        x = ' '.join(regex.sub('', log).strip().split())
        x = re.sub(r"([A-Z]?[^A-Z\s]+|[A-Z]+)", r" \1", x)
        return np.array(self.tokenizer.tokenize_test(x))

    def process_log(self, log: LogsightLog) -> LogsightLog:
        tmp = log.event.message
        tokenized = self.tokenize(tmp)[:self.config.get('max_len')]
        tokenized = np.concatenate((tokenized, np.array([0] * self.config.get('pad_len'))))[
                    :self.config.get('pad_len')]

        padded = get_padded_data([tokenized], self.config.get('pad_len'))
        # prediction, attention_scores = self.predict(padded)
        prediction = np.random.randint(0, 2)
        log.metadata['prediction'] = 1 if prediction == 0 else 0

        return log
