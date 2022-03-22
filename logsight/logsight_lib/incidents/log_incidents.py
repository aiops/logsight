from datetime import datetime
from datetime import timedelta

import pandas as pd

from modules.core.wrappers import synchronized


class IncidentDetector:
    @synchronized
    def get_incident_properties(self, log_count_ad, log_ad):
        if len(log_ad) == 0:
            return None

        log_ad_df_full = pd.DataFrame(log_ad).sort_values(by='@timestamp')
        log_ad_df_full["@timestamp"] = pd.to_datetime(log_ad_df_full["@timestamp"])

        propertiy_list = []
        timestamp_start = log_ad_df_full.iloc[0]["@timestamp"]
        timestamp_end_final = log_ad_df_full.iloc[-1]["@timestamp"]
        while True:
            timestamp_end = timestamp_start + timedelta(minutes=1)
            mask = (log_ad_df_full['@timestamp'] >= timestamp_start) & (log_ad_df_full['@timestamp'] < timestamp_end)
            log_ad_df = log_ad_df_full.loc[mask]
            if len(log_ad_df) == 0:
                timestamp_start = timestamp_end
                continue
            total_score = 0
            new_templates = []
            tmp_count_anomalies = []
            semantic_anomalies = []
            semantic_count_anomalies = []
            application_id = log_ad_df.drop_duplicates(subset=['template']).reset_index()['application_id'].values[0]
            log_tmp = log_ad_df.loc[log_ad_df.prediction == 1].drop_duplicates(subset=['template']).reset_index()
            semantic_anomalies = [log_tmp.iloc[i:i + 1].dropna(axis='columns').to_dict('records') for i in
                                  range(len(log_tmp))]
            log_ad_score = len(semantic_anomalies)
            total_score += log_ad_score

            logs = [log_ad_df.iloc[i:i + 1].dropna(axis='columns').to_dict('records') for i in
                    range(len(log_ad_df))]
            properties = {"@timestamp": timestamp_end, "total_score": total_score, "timestamp_start": timestamp_start,
                          "timestamp_end": timestamp_end, "count_ads": tmp_count_anomalies,
                          "application_id": application_id,
                          "new_templates": new_templates, "semantic_ad": semantic_anomalies,
                          "semantic_count_ads": semantic_count_anomalies, "logs": logs}
            if (
                    not len(new_templates)
                    and not len(semantic_anomalies)
                    and not len(semantic_count_anomalies)
                    and not len(tmp_count_anomalies)
            ):
                properties = {"@timestamp": timestamp_end, "total_score": 0.0, "timestamp_start": timestamp_start,
                              "timestamp_end": timestamp_end, "count_ads": [], "application_id": application_id,
                              "new_templates": [], "semantic_ad": [],
                              "semantic_count_ads": []}

            propertiy_list.append(properties)
            timestamp_start = timestamp_end
            if timestamp_start > timestamp_end_final:
                break

        return propertiy_list
