import logging
import re
import sys
import os
import pickle
import numpy as np
import torch as torch
from .core.log_level_config import ConfigLogLevelEstimation


sys.path.append(os.path.join(os.path.dirname(__file__), "core"))
logger = logging.getLogger("logsight." + __name__)

from .utils import get_padded_data, PREDICTION_THRESHOLD


class NoneDetector:
    def process_log(self, log_batch):
        return log_batch
    def load_model(self, version, user_app):
        pass



class LogAnomalyDetector:
    def __init__(self):
        self.tokenizer = None
        self.model = None
        self.config = ConfigLogLevelEstimation()
        self.model_loaded = False

    def prediction2loglevel(self, prediction):
        return self.config.get('log_mapper')[prediction[0]]

    def predict(self, log):
        with torch.no_grad():
            out = self.model.forward(log.long(), None)
            out_numpy = torch.functional.F.softmax(out, dim=1).detach().cpu().numpy()
            log_level_prediction = np.where(out_numpy[:, 0] > PREDICTION_THRESHOLD, 0, 1)
            del out, out_numpy
        return log_level_prediction, None

    def load_model(self, version, user_app):
        cur_f = os.path.dirname(os.path.realpath(__file__))
        self.tokenizer = pickle.load(open(os.path.join(cur_f, "models/github_tokenizer.pickle"), 'rb'))

        self.model = torch.load(os.path.join(cur_f, "models/model_github.pth"), map_location='cpu')
        self.model.cpu().eval()
        self.model_loaded = True

    def tokenize(self, log):
        regex = re.compile('[^a-zA-Z ]')
        x = ' '.join(regex.sub('', log).strip().split())
        x = re.sub(r"([A-Z]?[^A-Z\s]+|[A-Z]+)", r" \1", x)
        return torch.tensor(self.tokenizer.tokenize_test(x))

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
        log_messages[-1] = torch.cat((tokenized, torch.tensor([0] * self.config.get('pad_len'))))[
                           :self.config.get('pad_len')]
        padded = get_padded_data(log_messages)
        del log_messages
        prediction, attention_scores = self.predict(padded)
        del padded
        for i, log_message in enumerate(log_batch):
            try:
                log_message["prediction"] = 1 if prediction[i] == 0 else 0
                result.append(log_message)
            except Exception as e:
                print("exception ad batch", e)
        del prediction
        return result
