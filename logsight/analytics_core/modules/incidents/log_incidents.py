from datetime import timedelta
from typing import Dict, List

import pandas as pd


class IncidentDetector:

    @staticmethod
    def calculate_incidents(logs: List[Dict]):
        df = pd.DataFrame(logs).set_index('timestamp')
        df.index = pd.to_datetime(df.index)
        df.tags = df.tags.apply(sorted).astype(str)

        properties_list = []
        for time, grp in df.groupby(pd.Grouper(freq='T')):
            if not len(grp):
                continue
            start_time = time
            end_time = time + timedelta(minutes=1)
            dropped = grp.drop_duplicates(subset=['template'])
            semantic_ad = dropped.loc[dropped['prediction'] == 1]
            semantic_anomalies = [[element] for element in
                                  semantic_ad.dropna(axis='columns').reset_index().to_dict('records')]

            properties = {"timestamp": end_time, "total_score": len(semantic_anomalies),
                          "timestamp_start": start_time,
                          "timestamp_end": end_time,
                          "semantic_ad": semantic_anomalies}
            properties_list.append(properties)

        return properties_list
