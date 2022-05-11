from typing import Dict, List

import pandas as pd


class IncidentDetector:

    def calculate_incidents(self, logs: List[Dict]):
        df = pd.DataFrame(logs).set_index('timestamp')
        df.index = pd.to_datetime(df.index)
        df.tags = df.tags.apply(sorted).astype(str)

        properties_list = []
        for tags, grp_df in df.groupby("tags"):
            for time, grp in grp_df.groupby(pd.Grouper(freq='T')):
                start_time = grp.index[0]
                end_time = grp.index[-1]
                dropped = grp.drop_duplicates(subset=['template'])
                semantic_ad = dropped.loc[dropped['prediction'] == 1]
                semantic_anomalies = [[element] for element in
                                      semantic_ad.dropna(axis='columns').reset_index().to_dict('records')]

                properties = {"@timestamp": end_time, "total_score": len(semantic_anomalies),
                              "timestamp_start": start_time,
                              "timestamp_end": end_time,
                              "semantic_ad": semantic_anomalies,
                              "tags": tags}
                properties_list.append(properties)

        return properties_list

    # def get_incident_properties(self, log_count_ad, log_ad):
    #     if len(log_ad) == 0:
    #         return None
    #
    #     log_ad_df_full = pd.DataFrame(log_ad).sort_values(by='@timestamp')
    #     log_ad_df_full["@timestamp"] = pd.to_datetime(log_ad_df_full["@timestamp"])
    #
    #     property_list = []
    #     timestamp_start = log_ad_df_full.iloc[0]["@timestamp"]
    #     timestamp_end_final = log_ad_df_full.iloc[-1]["@timestamp"]
    #     while True:
    #         timestamp_end = timestamp_start + timedelta(minutes=1)
    #         mask = (log_ad_df_full['@timestamp'] >= timestamp_start) & (log_ad_df_full['@timestamp'] < timestamp_end)
    #         log_ad_df = log_ad_df_full.loc[mask]
    #         if len(log_ad_df) == 0:
    #             timestamp_start = timestamp_end
    #             continue
    #         total_score = 0
    #         new_templates = []
    #         tmp_count_anomalies = []
    #         semantic_count_anomalies = []
    #         application_id = log_ad_df.drop_duplicates(subset=['template']).reset_index()['application_id'].values[0]
    #         log_tmp = log_ad_df.loc[log_ad_df.prediction == 1].drop_duplicates(subset=['template']).reset_index()
    #         semantic_anomalies = [[element] for element in log_tmp.dropna(axis='columns').to_dict('records')]
    #         log_ad_score = len(semantic_anomalies)
    #         total_score += log_ad_score
    #
    #         # This line can be used if all logs are needed to be stored in each incident. Currently, only the semantic
    #         # anomalies are shown. This line is remove to improve the performance
    #         # TODO for future: This should be handled by indexing somehow. Not by storing duplicates of logs
    #         # logs = [[element] for element in log_ad_df.dropna(axis='columns').to_dict('records')]
    #         logs = []
    #
    #         properties = {"@timestamp"        : timestamp_end, "total_score": total_score,
    #                       "timestamp_start"   : timestamp_start,
    #                       "timestamp_end"     : timestamp_end, "count_ads": tmp_count_anomalies,
    #                       "application_id"    : application_id,
    #                       "new_templates"     : new_templates, "semantic_ad": semantic_anomalies,
    #                       "semantic_count_ads": semantic_count_anomalies, "logs": logs}
    #         if (
    #                 not len(new_templates)
    #                 and not len(semantic_anomalies)
    #                 and not len(semantic_count_anomalies)
    #                 and not len(tmp_count_anomalies)
    #         ):
    #             properties = {"@timestamp"        : timestamp_end, "total_score": 0.0,
    #                           "timestamp_start"   : timestamp_start,
    #                           "timestamp_end"     : timestamp_end, "count_ads": [], "application_id": application_id,
    #                           "new_templates"     : [], "semantic_ad": [],
    #                           "semantic_count_ads": []}
    #
    #         property_list.append(properties)
    #         timestamp_start = timestamp_end
    #         if timestamp_start > timestamp_end_final:
    #             break
    #
    #     return property_list


if __name__ == "__main__":
    inc_dec = IncidentDetector()
    logs = [{"id": "", "tags": ["default"],
             "message": "Starting job execution: Process received messages via MessagingSubsystems for: OpenText",
             "timestamp": "2021-03-23T01:28:06.001", "level": "INFO",
             "template": "Starting job execution: Process received messages via MessagingSubsystems for: OpenText",
             "prediction": 1},
            {"id": "", "tags": ["default"],
             "message": "Starting job execution: Send waiting messages via MessagingSubsystems for: SieSales",
             "timestamp": "2021-03-23T01:28:06.001", "level": "INFO",
             "template": "Starting job execution: Send waiting messages via MessagingSubsystems for: SieSales",
             "prediction": 1},
            {"id": "", "tags": ["default", "default3", "default2"],
             "message": "Session Metrics {\n    2515515 nanoseconds spent acquiring 1 JDBC connections;\n    4031 nanoseconds spent releasing 1 JDBC connections;\n    2533650 nanoseconds spent preparing 1 JDBC statements;\n    1738179 nanoseconds spent executing 1 JDBC statements;\n    0 nanoseconds spent executing 0 JDBC batches;\n    0 nanoseconds spent performing 0 L2C puts;\n    0 nanoseconds spent performing 0 L2C hits;\n    0 nanoseconds spent performing 0 L2C misses;\n    0 nanoseconds spent executing 0 flushes (flushing a total of 0 entities and 0 collections);\n    239 nanoseconds spent executing 1 partial-flushes (flushing a total of 0 entities and 0 collections)\n}",
             "timestamp": "2021-03-23T01:28:05.009", "level": "INFO",
             "template": "Session Metrics { <*> nanoseconds spent acquiring <*> JDBC connections; <*> nanoseconds spent releasing <*> JDBC connections; <*> nanoseconds spent preparing <*> JDBC statements; <*> nanoseconds spent executing <*> JDBC statements; <*> nanoseconds spent executing <*> JDBC batches; <*> nanoseconds spent performing <*> <*> puts; <*> nanoseconds spent performing <*> <*> hits; <*> nanoseconds spent performing <*> <*> misses; <*> nanoseconds spent executing <*> flushes (flushing a total of <*> entities and <*> collections); <*> nanoseconds spent executing <*> partial-flushes (flushing a total of <*> entities and <*> collections) }",
             "prediction": 1},
            {"id": "", "tags": ["default", "default3", "default2"],
             "message": "Session Metrics {\n    2515515 nanoseconds spent acquiring 1 JDBC connections;\n    4031 nanoseconds spent releasing 1 JDBC connections;\n    2533650 nanoseconds spent preparing 1 JDBC statements;\n    1738179 nanoseconds spent executing 1 JDBC statements;\n    0 nanoseconds spent executing 0 JDBC batches;\n    0 nanoseconds spent performing 0 L2C puts;\n    0 nanoseconds spent performing 0 L2C hits;\n    0 nanoseconds spent performing 0 L2C misses;\n    0 nanoseconds spent executing 0 flushes (flushing a total of 0 entities and 0 collections);\n    239 nanoseconds spent executing 1 partial-flushes (flushing a total of 0 entities and 0 collections)\n}",
             "timestamp": "2021-03-23T01:28:05.009", "level": "INFO",
             "template": "Session Metrics { <*> nanoseconds spent acquiring <*> JDBC connections; <*> nanoseconds spent releasing <*> JDBC connections; <*> nanoseconds spent preparing <*> JDBC statements; <*> nanoseconds spent executing <*> JDBC statements; <*> nanoseconds spent executing <*> JDBC batches; <*> nanoseconds spent performing <*> <*> puts; <*> nanoseconds spent performing <*> <*> hits; <*> nanoseconds spent performing <*> <*> misses; <*> nanoseconds spent executing <*> flushes (flushing a total of <*> entities and <*> collections); <*> nanoseconds spent executing <*> partial-flushes (flushing a total of <*> entities and <*> collections) }",
             "prediction": 1},
            {"id": "", "tags": ["default", "default2"],
             "message": "Finished job execution: Send waiting messages via MessagingSubsystems for: Vertex; Duration: 0:00:00.008",
             "timestamp": "2021-03-23T01:28:05.01", "level": "INFO",
             "template": "Finished job execution: Send waiting messages via MessagingSubsystems for: Vertex; Duration: <*>",
             "param_0": "# 0:00:00.008", "prediction": 1}]
    print(inc_dec.calculate_incidents(logs))
