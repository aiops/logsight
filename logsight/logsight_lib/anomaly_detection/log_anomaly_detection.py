import re
import os
import pickle
import numpy as np
import torch as torch
from .core.log_level_config import ConfigLogLevelEstimation
from .utils import get_padded_data, PREDICTION_THRESHOLD


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

            attention_scores = torch.mean(self.model.encoder.layers[0].self_attn.attn[0, :, 0, :], dim=0)
            out_numpy = torch.functional.F.softmax(out, dim=1).detach().cpu().numpy()
            prediction = np.where(out_numpy[:, 0] > PREDICTION_THRESHOLD, 0, 1)
            log_level_prediction = prediction  # self.prediction2loglevel(prediction)
        return log_level_prediction, attention_scores

    def load_model(self, version, user_app):
        cur_f = os.path.dirname(os.path.realpath(__file__))
        self.tokenizer = pickle.load(open(os.path.join(cur_f, "models/github_tokenizer.pickle"), 'rb'))

        self.model = torch.load(os.path.join(cur_f, "models/model_github.pth"))
        self.model.cpu().eval()
        self.model_loaded = True

    def tokenize(self, log):
        regex = re.compile('[^a-zA-Z ]')
        x = ' '.join(regex.sub('', log).strip().split())
        x = re.sub(r"([A-Z]?[^A-Z\s]+|[A-Z]+)", r" \1", x)
        return torch.tensor(self.tokenizer.tokenize_test(x))

    def process_log(self, log_batch):

        log_messages = []
        for log in log_batch:
            tmp = ''
            try:
                tmp = log['message']
            except Exception as e:
                print("Exception:", e)
            tokenized = self.tokenize(tmp)
            log_messages.append(tokenized[:self.config.get('max_len')])

        log_messages[-1] = torch.cat((tokenized, torch.tensor([0] * self.config.get('max_len'))))[
                           :self.config.get('pad_len')]
        padded = get_padded_data(log_messages)

        prediction, attention_scores = self.predict(padded)
        for i in range(len(log_batch)):
            try:
                log_batch[i]["prediction"] = 1 if prediction[i] == 0 else 0
            except Exception:
                # print(log_batch)
                print("exception ad batch")

        return log_batch