import logging
from collections import defaultdict
from dataclasses import asdict

import numpy as np
import pandas as pd

from analytics_core.logs import LogBatch

logger = logging.getLogger("logsight." + __name__)


class LogAggregator:

    @staticmethod
    def aggregate_logs(batch: LogBatch) -> LogBatch:
        logger.info(f"Logs to be aggregated: {len(batch.logs)}")

        logs = [dict(**asdict(x.event), **x.metadata) for x in batch.logs]
        df = pd.DataFrame(logs).set_index('timestamp')
        df.index = pd.to_datetime(df.index)
        grouped = df.groupby(pd.Grouper(freq='T')).agg(prediction=('prediction', 'sum'),
                                                       level=('level',
                                                              lambda x: dict(zip(*np.unique(x, return_counts=True)))),
                                                       count=('level', 'count'))
        result = []
        for tpl in grouped.itertuples():
            result_dict = defaultdict()
            result_dict["log_levels"] = tpl.level
            result_dict["prediction"] = tpl.prediction
            result_dict["count"] = tpl.count
            result_dict["@timestamp"] = tpl.Index.strftime(format='%Y-%m-%dT%H:%M:%S.%f')
            result.append(result_dict)
        batch.metadata['aggregations'] = result

        return batch
