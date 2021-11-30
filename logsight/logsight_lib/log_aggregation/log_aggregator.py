from collections import defaultdict

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
        df.index = pd.to_datetime(df.index)
        grouped = df.groupby(pd.Grouper(freq='T')).agg(prediction=('prediction', 'sum'),
                                                       level=('actual_level',
                                                              lambda x: dict(zip(*np.unique(x, return_counts=True)))),
                                                       count=('app_name', 'count'))

        result = []
        for tpl in grouped.itertuples():
            result_dict = defaultdict()
            result_dict["log_levels"] = tpl.level
            result_dict["prediction"] = tpl.prediction
            result_dict["count"] = tpl.count
            result_dict["@timestamp"] = tpl.Index.strftime(format='%Y-%m-%dT%H:%M:%S.%f')
            result.append(result_dict)

        # for tpl in grouped.itertuples():
        #     tpl.level.update({"prediction": tpl.prediction})
        #     tpl.level.update({"count": tpl.count})
        #     tpl.level.update({"@timestamp": tpl.Index.strftime(format='%Y-%m-%dT%H:%M:%S.%f')})
        #     result.append(tpl.level)

        return result
