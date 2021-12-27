from datetime import datetime

import pandas as pd


class IncidentDetector:
    def get_incident_properties(self, log_count_ad, log_ad):
        total_score = 0
        new_templates = []
        tmp_count_anomalies = []
        semantic_anomalies = []
        semantic_count_anomalies = []
        if len(log_ad) == 0:
            return None
        try:
            log_ad_df = pd.DataFrame(log_ad).sort_values(by='@timestamp')
        except KeyError:
            print(log_ad)

        log_tmp = log_ad_df.loc[log_ad_df.prediction == 1].drop_duplicates(subset=['template']).reset_index()
        semantic_anomalies = [log_tmp.iloc[i:i + 1].dropna(axis='columns').to_dict('records') for i in
                              range(len(log_tmp))]
        log_ad_score = len(semantic_anomalies)
        total_score += log_ad_score
        timestamp_start = log_ad_df.iloc[0]["@timestamp"]
        timestamp_end = log_ad_df.iloc[-1]["@timestamp"]
        if len(log_count_ad):
            log_count_df = pd.DataFrame(log_count_ad)
            tmp = []
            for j in log_count_df.new_templates:
                for k in j:
                    print("k", k)
                    try:
                        tmp.append(k[0])
                    except Exception as e:
                        print(e)
            tmp = pd.DataFrame(tmp)

            new_templates = tmp.drop_duplicates(subset=['template'])
            count_anomalies = []
            for i in log_count_df.message.values:
                count_anomalies += i
            tmp = pd.DataFrame(count_anomalies)
            tmp = tmp.drop_duplicates(subset=['template']).reset_index()
            tmp_count_anomalies = [tmp.iloc[i:i + 1].dropna(axis='columns').to_dict('records') for i in range(len(tmp))]
            print(tmp_count_anomalies)

            if len(new_templates) > 0:
                semantic_count_anomalies = [log_tmp.iloc[i:i + 1].dropna(axis='columns').to_dict('records') for i in
                                            range(len(log_tmp)) if
                                            (log_tmp.template.values[i] in new_templates.template.values or
                                             log_tmp.template.values[i] in count_anomalies)]
            else:
                semantic_count_anomalies = [log_tmp.iloc[i:i + 1].dropna(axis='columns').to_dict('records') for i in
                                            range(len(log_tmp)) if
                                            log_tmp.template.values[i] in count_anomalies]

            new_templates = [new_templates.iloc[i:i + 1].dropna(axis='columns').to_dict('records') for i in
                             range(len(new_templates))]
            new_templates_score = len(new_templates)
            count_score = len(tmp_count_anomalies)
            total_score += count_score
            total_score += new_templates_score
            total_score -= 1

            timestamp_start = log_count_df.iloc[0]["timestamp_start"]
            timestamp_end = log_count_df.iloc[-1]["timestamp_end"]

        logs = [log_ad_df.iloc[i:i + 1].dropna(axis='columns').to_dict('records') for i in
                              range(len(log_ad_df))]
        properties = {"@timestamp": timestamp_end, "total_score": total_score, "timestamp_start": timestamp_start,
                      "timestamp_end": timestamp_end, "count_ads": tmp_count_anomalies,
                      "new_templates": new_templates, "semantic_ad": semantic_anomalies,
                      "semantic_count_ads": semantic_count_anomalies, "logs": logs}
        if (
                not new_templates
                and not semantic_anomalies
                and not semantic_count_anomalies
                and not tmp_count_anomalies
        ):
            properties = {"@timestamp": timestamp_end, "total_score": 0.0, "timestamp_start": timestamp_start,
                          "timestamp_end": timestamp_end, "count_ads": [],
                          "new_templates": [], "semantic_ad": [],
                          "semantic_count_ads": []}

        return properties
