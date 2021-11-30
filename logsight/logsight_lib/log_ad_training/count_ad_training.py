import datetime
import json

import pandas as pd
from elasticsearch.helpers import scan
from sklearn.preprocessing import LabelEncoder
import numpy as np
import pickle
from .utils import GET_PARSING_QUERY_STRING, get_parsing_query_string_tag, WINDOW_SIZE_IN_MINUTES, THRESHOLD, \
    initialize_kafka_producer


def get_training_data_for_count_ad(es, es_index, training_control):
    print_on_every = 100
    if "baselineTagId" in training_control:
        query = get_parsing_query_string_tag(training_control["baselineTagId"])
    else:
        query = GET_PARSING_QUERY_STRING

    res = scan(es, index=es_index, doc_type='_doc', query=query)
    print("Started reading data!")
    templates = []
    timestamps = []
    template_dict = {}
    for i, item in enumerate(res):
        # print(i, item)
        if i % print_on_every == 0:
            print("Number of items read from Elasticsearch:", i)
        templates.append(item['_source']['template'])
        timestamps.append(item['_source']['@timestamp'])
        template_dict[item['_source']['template']] = item['_source']

    data = pd.DataFrame({'template': templates}, index=timestamps)
    data.index = pd.to_datetime(data.index)
    return data, template_dict


def train_count_ad_model(data):
    # print(data)
    all_data = {}
    offset = 0
    data_windows = []

    le = LabelEncoder()
    le.fit(np.append(data.values, np.array(["NEW_TEMPLATE"])))
    data['template_labels'] = le.transform(data.values)
    print("TEMPLATELABELS", list(data.template_labels.values))
    for window in range(0, len(data), 5):
        start_time = data.index[window].to_pydatetime() - datetime.timedelta(seconds=WINDOW_SIZE_IN_MINUTES)
        end_time = start_time + datetime.timedelta(minutes=WINDOW_SIZE_IN_MINUTES)
        # print("start and end time:", start_time, end_time, datetime.timedelta(seconds=WINDOW_SIZE_IN_MINUTES))
        mask = (data.index >= start_time) & (data.index <= end_time)
        data_windows.append(data.iloc[mask].template_labels.values)
        print("window templates ", list(data.iloc[mask].template_labels.values))
        # print("last time:", data.index[-1].to_pydatetime())
        if end_time > data.index[-1].to_pydatetime():
            break
    print("The number of windows in the data is", len(data_windows), data_windows)

    for i in data_windows:
        unique, counts = np.unique(i, return_counts=True)
        print("C: ", list(unique))
        print("C: ", list(counts))
        tmp = dict(zip(unique, counts))
        print("The window", i, "dict is:", tmp)
        for c in tmp.keys():
            try:
                all_data[c]
                all_data[c] += [tmp[c]]
            except Exception as e:
                all_data[c] = [0] * offset
                all_data[c].append(tmp[c])
        offset += 1
        tmp_idx = [i for i in all_data.keys() if i not in tmp.keys()]
        for t in tmp_idx:
            all_data[t].append(0)

    dataframe = pd.DataFrame(all_data)
    print("Dataframe:", dataframe)
    for i in dataframe.columns.sort_values():
        print("Columns Dataframe:", i)
    # print("Dataframe columns:", dataframe.columns.sort())

    df_corr = dataframe.corr()
    df_corr.fillna(1.0, inplace=True)
    print("DF corr:", df_corr)
    dict_mean = {}
    number_templates = len(df_corr.columns)
    print("The number of templates that are being correlated is", number_templates)
    for i in df_corr.columns:
        print("Template with ID:", i, "is processed.")
        # try:
        # print(df_corr[i])
        # print("TH: ", df_corr[i], THRESHOLD)
        idc = df_corr[i][np.abs(df_corr[i]) > THRESHOLD]
        # print("Indices:", idc, idc.index)
        sliced_mean = dataframe[idc.index].mean(axis=0)
        # print("Sliced mean", sliced_mean)
        sm_rate = sliced_mean / sliced_mean.sum()
        # print("Sm rate", sm_rate)
        dict_mean[i] = (np.array(idc.index), sm_rate.values)
        # except Exception as e:
        #     print("Tried to find the mean of the rate between templates, got this exception: ", e)
        #     pass
    # print("DM", dict_mean)
    return dict_mean, le


def save_model(model, le, templates, user_app, training_control, kafka_url):
    if "baselineTagId" in training_control:
        with open('/models/' + user_app + '_model_count_ad_' +
                  training_control["status"] + "_" + training_control["baselineTagId"] + '.pickle', "wb") as file:
            pickle.dump(model, file)

        with open('/models/' + user_app + '_template_count_ad_' +
                  training_control["status"] + "_" + training_control["baselineTagId"] + '.pickle', "wb") as file:
            pickle.dump(templates, file)

        with open('/models/' + user_app + '_le_count_ad_' +
                  training_control["status"] + "_" + training_control["baselineTagId"] + '.pickle', "wb") as file:
            pickle.dump(le, file)
            producer = initialize_kafka_producer(kafka_url)
            message = {"name": "compare", "compareTagId": training_control["compareTagId"], "baselineTagId": training_control["baselineTagId"]}
            producer.send(user_app + '_update-models', json.dumps(message).encode('UTF-8'))
            print("Saving the model for log compare")

    else:
        with open('/models/' + user_app + '_model_count_ad_' + training_control["status"] + "_" +
                  training_control["baselineTagId"] + '.pickle', "wb") as file:
            pickle.dump(model, file)
        with open('/models/' + user_app + '_le_count_ad_' + training_control["status"] + "_" +
                  training_control["baselineTagId"] + '.pickle', "wb") as file:
            pickle.dump(le, file)
            producer = initialize_kafka_producer(kafka_url)
            message = {"name": "count"}
            producer.send(user_app + '_update-models', json.dumps(message).encode('UTF-8'))