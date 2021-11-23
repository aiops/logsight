import numpy as np
import pandas as pd

class LogAggregator:
    def __init__(self):
        pass

    def aggregate_logs(self, logs):
        if not isinstance(logs, list):
            logs = [logs]
        if len(logs) == 0:
            return
        df = pd.DataFrame(logs).set_index('@timestamp')

        result = dict(zip(*np.unique(np.array([log['actual_level'] for log in logs]), return_counts=True)))
        result.update(
            {
                "count": len(logs),
                "@timestamp": logs[-1]['@timestamp'],
                "prediction": np.sum([log['prediction'] for log in logs])
            })

        return result
