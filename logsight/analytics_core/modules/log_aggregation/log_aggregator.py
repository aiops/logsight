import logging
from typing import Dict, List

import numpy as np
import pandas as pd

logger = logging.getLogger("logsight." + __name__)


class LogAggregator:

    @staticmethod
    def aggregate_logs(logs: List[Dict]) -> List:
        df = pd.DataFrame(logs).set_index('timestamp')
        df.index = pd.to_datetime(df.index)
        grouped = df.groupby(pd.Grouper(freq='T')).agg(prediction=('prediction', 'sum'),
                                                       level=('level',
                                                              lambda x: dict(zip(*np.unique(x, return_counts=True)))),
                                                       count=('level', 'count'))
        result = []
        for tpl in grouped.itertuples():
            result_dict = dict()
            result_dict["log_levels"] = tpl.level
            result_dict["prediction"] = tpl.prediction
            result_dict["count"] = tpl.count
            result_dict["timestamp"] = tpl.Index.isoformat()
            result.append(result_dict)

        return result
