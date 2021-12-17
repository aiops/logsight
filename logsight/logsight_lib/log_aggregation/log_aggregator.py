import logging
from collections import defaultdict

import numpy as np
import pandas as pd
# from time import perf_counter

logger = logging.getLogger("logsight." + __name__)


class LogAggregator:

    @staticmethod
    def aggregate_logs(logs):
        if not isinstance(logs, list):
            logs = [logs]
        if len(logs) == 0:
            return
        # t0 = perf_counter()
        logger.info(f"Logs to be aggregated: {len(logs)}")
        df = pd.DataFrame(logs).set_index('@timestamp')
        # t1 = perf_counter()
        df.index = pd.to_datetime(df.index)
        # t2 = perf_counter()
        grouped = df.groupby(pd.Grouper(freq='T')).agg(prediction=('prediction', 'sum'),
                                                       level=('actual_level',
                                                              lambda x: dict(zip(*np.unique(x, return_counts=True)))),
                                                       count=('app_name', 'count'))
        # t3 = perf_counter()
        result = []
        for tpl in grouped.itertuples():
            result_dict = defaultdict()
            result_dict["log_levels"] = tpl.level
            result_dict["prediction"] = tpl.prediction
            result_dict["count"] = tpl.count
            result_dict["@timestamp"] = tpl.Index.strftime(format='%Y-%m-%dT%H:%M:%S.%f')
            result.append(result_dict)
        # t4 = perf_counter()
        # logger.debug(f"Create:{t1 - t0},datetime:{t2 - t0},group:{t3 - t0},dict:{t4 - t0},")
        return result
