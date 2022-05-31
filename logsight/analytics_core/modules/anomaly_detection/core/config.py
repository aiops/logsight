import os
from dataclasses import dataclass, field


@dataclass
class AnomalyDetectionConfig:
    pad_len: int = 50
    max_len: int = 20
    log_mapper: dict = field(default_factory=dict)
    prediction_threshold: float = float(os.environ.get('PREDICTION_THRESHOLD', 0.85))

    def __post_init__(self):
        if not len(self.log_mapper.keys()):
            self.log_mapper.update({0: 'anomaly', 1: 'normal'})
