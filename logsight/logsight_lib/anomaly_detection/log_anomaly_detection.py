import gc
import logging
import re
import sys
import os
import pickle
import numpy as np
import onnxruntime as ort

from .core.log_level_config import ConfigLogLevelEstimation


sys.path.append(os.path.join(os.path.dirname(__file__), "core"))
logger = logging.getLogger("logsight." + __name__)

from .utils import get_padded_data, PREDICTION_THRESHOLD,  softmax


class NoneDetector:
    def process_log(self, log_batch):
        for i in enumerate(log_batch):
            log_batch[i]['prediction'] = 0
        return log_batch
    def load_model(self, version, user_app):
        pass



class LogAnomalyDetector:
    def __init__(self):
        logger.debug("initializing the log anomaly detector")
        self.tokenizer = None
        self.model = None
        self.config = ConfigLogLevelEstimation()
        self.model_loaded = False
        cur_f = os.path.dirname(os.path.realpath(__file__))
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

    def load_model(self, version, user_app):
        cur_f = os.path.dirname(os.path.realpath(__file__))
        self.tokenizer = pickle.load(open(os.path.join(cur_f, "models/github_tokenizer.pickle"), 'rb'))
        session_option = ort.SessionOptions()
        session_option.enable_mem_pattern = False
        session_option.enable_cpu_mem_arena = False
        self.ort_sess = ort.InferenceSession(os.path.join(cur_f, "models/model_github.onnx"), sess_options=session_option)
        self.model_loaded = True

    def tokenize(self, log):
        regex = re.compile('[^a-zA-Z ]')
        x = ' '.join(regex.sub('', log).strip().split())
        x = re.sub(r"([A-Z]?[^A-Z\s]+|[A-Z]+)", r" \1", x)
        return np.array(self.tokenizer.tokenize_test(x))

    def process_log(self, log_batch):
        result = []
        log_messages = []
        for log in log_batch:
            tmp = ''
            try:
                tmp = log['message']
            except Exception as e:
                logger.error(f"Exception: {e}")
            tokenized = self.tokenize(tmp)
            log_messages.append(tokenized[:self.config.get('max_len')])
        log_messages[-1] = np.concatenate((tokenized, np.array([0] * self.config.get('pad_len'))))[
                           :self.config.get('pad_len')]

        padded = get_padded_data(log_messages, self.config.get('pad_len'))
        prediction, attention_scores = self.predict(padded)
        for i, log_message in enumerate(log_batch):
            try:
                log_message["prediction"] = 1 if prediction[i] == 0 else 0
                result.append(log_message)
            except Exception as e:
                print("exception ad batch", e)

        del log_messages, padded, prediction, log_batch
        gc.collect()
        return result
