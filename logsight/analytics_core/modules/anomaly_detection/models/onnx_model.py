import logging
import os
import sys

import numpy as np
import onnxruntime as ort

from logsight.analytics_core.modules.anomaly_detection.core.base import BaseModel
from logsight.analytics_core.modules.anomaly_detection.utils import softmax

sys.path.append(os.path.join(os.path.dirname(__file__), "../core"))
logger = logging.getLogger("logsight." + __name__)


class OnnxModel(BaseModel):
    def __init__(self, prediction_threshold: float = 0.85):
        super().__init__()
        self.th = prediction_threshold
        self.ort_sess = None

    def predict(self, logs):
        if self.ort_sess is None:
            raise ValueError("The model is still not loaded")
        out_numpy = softmax(self.ort_sess.run(None, {'input': logs, 'src_mask': None})[0])
        log_level_prediction = np.where(out_numpy[:, 0] > self.th, 0, 1)
        return log_level_prediction

    def load_model(self):
        cur_f = os.path.dirname(os.path.realpath(__file__))
        session_option = ort.SessionOptions()
        session_option.enable_mem_pattern = False
        session_option.enable_cpu_mem_arena = False

        self.ort_sess = ort.InferenceSession(os.path.join(cur_f, "model_github.onnx"),
                                             sess_options=session_option)
        return self
