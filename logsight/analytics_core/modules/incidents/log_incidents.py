import json
import math
from datetime import timedelta
from typing import Dict, List

import pandas as pd

pd.options.mode.chained_assignment = None


class IncidentsStatus:
    RAISED = 1


def calculate_risk(data):
    if not (len(data)):
        return 0
    data = data.sort_values(by=['risk_score'], ascending=False)
    percentage = int(len(data) * 0.3)
    top_k = data.head(percentage)
    risk = top_k['risk_score'].max()
    if len(top_k['risk_score']) > 0:
        risk = risk + min(
            [int(top_k['risk_score'].sum() / len(top_k['risk_score'])), 100 - top_k['risk_score'].max()])
    else:
        risk = 0

    return risk


def level_as_binary(level):
    if str(level).upper() in ["ERROR", "ERR", "CRITICAL", "FAULT"]:
        return 1
    else:
        return 0


class IncidentDetector:

    @staticmethod
    def calculate_incidents(logs: List[Dict]):
        df = pd.DataFrame(logs).set_index('timestamp')
        df.index = pd.to_datetime(df.index)
        df['tag_string'] = df.tags.astype(str)
        properties_list = []
        for (interval, tags), grp in df.groupby([pd.Grouper(freq='1Min'), 'tag_string']):
            len_df = len(grp)
            if not len_df:
                continue
            start_time = interval
            end_time = interval + timedelta(minutes=1)
            risk = calculate_risk(grp)
            grp['risk_severity'] = ((grp['risk_score'] + 0.01)/34).apply(lambda x: math.ceil(x))
            data_json_list = [element for element in
                              grp.dropna(axis='columns').reset_index().to_dict('records')]
            tags = json.loads(tags.replace('\'', "\""))
            templates = grp.drop_duplicates(subset=['template'])
            if risk > 0:
                grp['level_binary'] = grp['level'].apply(lambda x: level_as_binary(x))
                properties = {"timestamp": end_time,
                              "risk": risk,
                              "count_messages": len_df,
                              "count_states": len(templates),
                              "status": IncidentsStatus.RAISED,
                              "added_states": grp['added_state'].sum(),
                              "level_faults": grp['level_binary'].sum(),
                              "semantic_anomalies": grp["prediction"].sum(),
                              # severity is mapped from range [0, 100] to [1,3]. 34 is chosen because if not,
                              # we need to use 33.33(3)
                              # the formula bellow takes care of for example:
                              # risk = 0, severity = math.ceil(0.01/34) = 1
                              # risk = 34, severity = math.ceil(34.01/34) = 2
                              # risk = 67, severity = math.ceil(67.01/34) = 3
                              # risk = 100, severity = math.ceil(100/34) = 3,
                              "severity": math.ceil((risk + 0.01) / 34),
                              "tags": tags,
                              "timestamp_start": start_time,
                              "timestamp_end": end_time,
                              "data": data_json_list}
                properties_list.append(properties)
        print(properties_list)
        return properties_list
