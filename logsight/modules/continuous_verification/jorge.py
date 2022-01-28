import datetime
import logging
import math
import os

import numpy as np
import pandas as pd

from config import global_vars
from services.configurator.config_manager import ConnectionConfig
from .es_query import ElasticsearchDataSource


class ContinuousVerification:
    def __init__(self, connections_conf_file):
        self.es = ElasticsearchDataSource(**ConnectionConfig(os.path.join(global_vars.CONFIG_PATH, connections_conf_file)).get_elasticsearch_params())

    def extract_data_for_tag(self, private_key, application_id, tag):
        # quality = self.es.get_log_ad_data(private_key=private_key, app=application_id, tag=tag)
        templates = self.es.get_log_ad_data(private_key=private_key, app=application_id, tag=tag)

        dft = pd.DataFrame().from_dict(templates)
        dft = dft.rename(columns={"@timestamp": "timestamp"})
        dft = dft.rename(columns={"actual_level": "level"})
        # if prediction == 1 --> predicted_level = "Fault", else --> "Report"
        dft['predicted_level'] = ["Fault" if p == 1 else "Report" for _, p in dft['prediction'].iteritems()]

        # dft['predicted_level'] = dft.level
        ## TODO: TRANSFORM TO DATAFRAME

        # This is to remove the LogQuality dependancy and not break the code
        # The predcted level now will always be equal to the actual level of the templateƒ
        # for t in quality:
        #     if t.prediction == 1:
        #         d[t.template] = t.predicted_level

        dft['timestamp'] = pd.to_datetime(dft['timestamp'])
        dft['timestamp'] = pd.to_datetime(pd.to_numeric(dft['timestamp']).interpolate())
        dft.index = dft['timestamp']

        # dft.set_index('timestamp', inplace=True)

        return dft[['level', 'template', 'tag', 'predicted_level']]

    def extract_data(self, private_key, application_id, baseline_tag_id, compare_tag_id):
        df_baseline = self.extract_data_for_tag(private_key, application_id, baseline_tag_id)
        df_candidate = self.extract_data_for_tag(private_key, application_id, compare_tag_id)
        return df_baseline, df_candidate

    def run_verification(self, private_key, application_id, baseline_tag_id, compare_tag_id):
        df_baseline, df_candidate = self.extract_data(private_key, application_id, baseline_tag_id, compare_tag_id)
        df_etl = transform_etl(df_baseline, df_candidate)
        df_html = transform_html(df_etl)
        return prepare_html(df_html)


def transform_etl(df_baseline, df_candidate):
    def get_start_date(df):
        return df.reset_index().drop(columns=['level']).groupby('template')['timestamp'].first().to_dict()

    def get_end_date(df):
        return df.reset_index().drop(columns=['level']).groupby('template')['timestamp'].last().to_dict()

    def get_template_ids(df):
        return df['template'].unique()

    def get_template_trend(df):
        df = df.groupby('template').resample('1Min', origin='start').count()
        df = df.rename(columns={'template': 't_count'}).reset_index().drop(columns=['timestamp', 'level'])
        df = df.groupby('template')['t_count'].apply(list)
        return df.to_dict()

    def get_level(df):
        return df.groupby('template')['level'].first().to_dict()

    def get_semantic_level(df):
        return df.groupby('template')['predicted_level'].first().to_dict()

    df = pd.concat([df_baseline, df_candidate], axis=0)
    level = get_level(df)
    semantic_level = get_semantic_level(df)

    trend_baseline = get_template_trend(df_baseline)
    trend_candidate = get_template_trend(df_candidate)

    start_date = get_start_date(df_baseline)
    end_date = get_end_date(df_candidate)

    df_csv = pd.DataFrame(columns=['start_date', 'end_date', 'template',
                                   'trend_baseline', 'trend_candidate',
                                   'count_baseline', 'count_candidate',
                                   'level', 'semantics'])

    for template in set(get_template_ids(df_baseline)).union(set(list(get_template_ids(df_candidate)))):
        df_csv = df_csv.append({'start_date': start_date.get(template, None),
                                'end_date': end_date.get(template, None),
                                'template': template,
                                'trend_baseline': ",".join(map(str, trend_baseline.get(template, []))),
                                'trend_candidate': ",".join(map(str, trend_candidate.get(template, []))),
                                'count_baseline': sum(trend_baseline.get(template, [0])),
                                'count_candidate': sum(trend_candidate.get(template, [0])),
                                'level': level.get(template, None),
                                'semantics': semantic_level.get(template, None)
                                }, ignore_index=True)
    return df_csv


def transform_html(df):
    def format_dates(start, end):
        def f(x):
            fmt = '%Y-%m-%d'
            return datetime.datetime.strptime(x, fmt).strftime('%b.&nbsp%d')

        s = f(str(start)[:10]) if start else '-'
        e = f(str(end)[:10]) if end else '-'

        return s + '-' + e

    def add_comma(i):
        return '{:}'.format(i)

    def get_risk(baseline_count, candidate_count, change_perc, level, semantics):

        def risk_as_binary(text):
            if any([True for i in ["Fault"] if i in text]):
                return 1
            return 0

        def level_as_binary(level):
            if str(level).upper() in ["ERROR", "ERR", "CRITICAL", "FAULT"]:
                return 1
            else:
                return 0

        # baseline, candidate, level, semantics, description, risk as a percentage, id
        #
        # risk_tbl = [(0, 1, 0, "Added state", 0, "fa fa-plus-circle font-medium-1"),
        #             (0, 1, 1, "Added state (Fault)", 100, "fa fa-exclamation-triangle font-medium-1"),
        #             (1, 0, 0, "Deleted state", 75, "fa fa-minus-circle font-medium-1"),
        #             (1, 0, 1, "Deleted state (F)", 0, "fa fa-minus-circle font-medium-1"),
        #             (1, 1, 0, "Recurring state", 0, "fa fa-check-circle font-medium-1"),
        #             (1, 1, 1, "Recurring state", 25, "fa fa-check-circle font-medium-1"),
        #             ]
        # baseline, candidate, semantics, description, risk as a percentage, id
        risk_tbl = [(0, 1, 0, 0, "Added state", 0, "fa fa-plus-circle font-medium-1"),
                    (0, 1, 1, 0, "Added state (E)", 80, "fa fa-exclamation-triangle font-medium-1"),
                    (0, 1, 0, 1, "Added state (F)", 80, "fa fa-exclamation-triangle font-medium-1"),
                    (0, 1, 1, 1, "Added state (E/F)", 100, "fa fa-exclamation-triangle font-medium-1"),
                    (1, 0, 0, 0, "Deleted state", 0, "fa fa-minus-circle font-medium-1"),
                    (1, 0, 1, 0, "Deleted state (E)", 0, "fa fa-minus-circle font-medium-1"),
                    (1, 0, 0, 1, "Deleted state (F)", 0, "fa fa-minus-circle font-medium-1"),
                    (1, 0, 1, 1, "Deleted state (E/F)", 0, "fa fa-minus-circle font-medium-1"),
                    (1, 1, 0, 0, "Recurring state", 0, "fa fa-check-circle font-medium-1"),
                    (1, 1, 1, 0, "Recurring state (E)", 25, "fa fa-exclamation-circle font-medium-1"),
                    (1, 1, 0, 1, "Recurring state (F)", 25, "fa fa-exclamation-circle font-medium-1"),
                    (1, 1, 1, 1, "Recurring state (E/F)", 50, "fa fa-exclamation-circle font-medium-1")
                    ]

        # REVISIT THIS

        r = ("Internal error", 100)
        for rule in risk_tbl:
            if rule[0] == min(baseline_count, 1) and rule[1] == min(candidate_count, 1) \
                    and rule[2] == level_as_binary(level) and rule[3] == risk_as_binary(semantics):
                r = (rule[4], rule[5], rule[6])

        if min(baseline_count, 1) == min(candidate_count, 1):
            if abs(change_perc) >= .2:
                r = ("Frequency change", 60, "fa fa-align-center font-medium-1")

        return r

    def get_risk_color(value, range_, reverse=False):

        min_ = 0
        max_ = abs(range_[1] - range_[0])
        value -= range_[0]

        if value <= int((min_ + max_) / 2):
            r = 255
            g = int(255 * value / int((min_ + max_) / 2))
            b = 0
        else:
            r = int(255 * (max_ - value) / int((min_ + max_) / 2))
            g = 255
            b = 0

        if reverse:
            r, g = g, r

        return f'rgba({r}, {g}, {b}, {1.})'

    def get_template_code(t):
        return 'https://github.com/apache/hadoop'

    def get_change_count(b, c, fmt=lambda s: s):
        lead = c - b
        if lead > 0:
            dataset = 'Baseline'
            return f'{fmt(dataset)}+{int(np.round(lead))}'
        else:
            dataset = 'Candidate'
            return f'{fmt(dataset)}{int(np.round(lead))}'

    def get_change_perc(b, c):
        if b == c == 0:
            ratio = 0
        elif b == 0:
            ratio = 1
        elif c == 0:
            ratio = -1
        else:
            ratio = (c - b) / b
        return ratio

    def get_change_color(change):
        alpha = .7
        if change.startswith('-'):
            return f'rgba(255, 0, 0, {alpha})'
        else:
            return f'rgba(0, 0, 0, {alpha})'

    def get_percentage(a, b):
        return int(100 * (a / (a + b)))

    def get_percentage_color(a, b, color='blue'):
        N = 10
        p = get_percentage(a, b)
        bins = np.linspace(0, 100, N)
        alphas = np.linspace(0.1, 0.6, N)
        g = np.digitize(p, bins)
        alpha = alphas[g - 1]

        if p == 0:
            color = 'red'

        if color == 'red':
            return f'rgba(255, 0, 0, {alpha})'
        if color == 'green':
            return f'rgba(0, 255, 0, {alpha})'
        if color == 'blue':
            return f'rgba(100,149,237, {alpha})'
        if color == 'silver':
            return f'rgba(192, 192, 192, {alpha})'

    def get_semantic_color(level, semantics):
        level = str(level).upper()
        alpha = .7
        if semantics == "Fault" and (level == 'ERROR' or level == 'WARNING' or level == 'CRITICAL' or level == 'WARN' or level == 'ERR'):
            return [f'rgba(255, 0, 0, {alpha})', f'rgba(255, 0, 0, {alpha})']
        elif semantics == 'Fault' and (level == 'INFO' or level == 'DEBUG' or level == 'FINE' or level == 'REPORT'):
            return [f'rgba(255, 0, 0, {alpha})', f'rgb(34,43,69)']
        elif semantics == 'Report' and (level == 'ERROR' or level == 'WARNING' or level == 'CRITICAL' or level == 'WARN' or level == 'ERR'):
            return [f'rgb(34,43,69)', f'rgba(255, 0, 0, {alpha})']
        elif semantics == 'Report' and (level == 'INFO' or level == 'DEBUG' or level == 'FINE' or level == 'REPORT'):
            return [f'rgb(34,43,69)', f'rgb(34,43,69)']

    formatted_df = df.assign(dates=lambda x: [format_dates(s, e) for s, e in
                                              x[['start_date', 'end_date']].itertuples(index=False)],
                             count_total=lambda x: [b + c for b, c in
                                                    x[['count_baseline', 'count_candidate']].itertuples(index=False)],
                             count_gtotal=lambda x: x['count_baseline'].sum() + x['count_candidate'].sum(),
                             perc_baseline=lambda x: [get_percentage(b, c)
                                                      for b, c in
                                                      x[['count_baseline', 'count_candidate']].itertuples(index=False)],
                             perc_candidate=lambda x: [get_percentage(c, b)
                                                       for b, c in x[['count_baseline', 'count_candidate']].itertuples(
                                     index=False)],
                             b_color=lambda x: [get_percentage_color(b, t, color='silver')
                                                for b, t in
                                                x[['count_baseline', 'count_total']].itertuples(index=False)],
                             c_color=lambda x: [get_percentage_color(c, t, color='silver')
                                                for c, t in
                                                x[['count_candidate', 'count_total']].itertuples(index=False)],
                             change_count=lambda x: [get_change_count(b, c, lambda s: f'{s[0:0]}')
                                                     for b, c in
                                                     x[['count_baseline', 'count_candidate']].itertuples(index=False)],
                             change_color=lambda x: x['change_count'].map(lambda y: get_change_color(y)),
                             change_perc=lambda x: [get_change_perc(a, b)
                                                    for a, b in
                                                    x[['count_baseline', 'count_candidate']].itertuples(index=False)],
                             coverage=lambda x: [np.round(100 * ((x + y) / z), 1) for x, y, z in
                                                 x[['count_baseline', 'count_candidate', 'count_gtotal']].itertuples(
                                                     index=False)],
                             risk_score=lambda x: [get_risk(b, c, p, l, s)[1] for b, c, p, l, s in
                                                   x[['count_baseline', 'count_candidate', 'change_perc', 'level',
                                                      'semantics']].itertuples(index=False)],
                             risk_description=lambda x: [get_risk(b, c, p, l, s)[0] for b, c, p, l, s in
                                                         x[['count_baseline', 'count_candidate', 'change_perc', 'level',
                                                            'semantics']].itertuples(index=False)],
                             risk_symbol=lambda x: [get_risk(b, c, p, l, s)[2] for b, c, p, l, s in
                                                    x[['count_baseline', 'count_candidate', 'change_perc', 'level',
                                                       'semantics']].itertuples(index=False)],
                             risk_color=lambda x: x['risk_score'].map(
                                 lambda y: get_risk_color(y, (0, 100), reverse=True)),
                             template_code=lambda x: x['template'].map(lambda y: get_template_code(y)),
                             count_base=lambda x: x['count_baseline'].map(add_comma),
                             count_cand=lambda x: x['count_candidate'].map(add_comma),
                             semantic_color=lambda x: [get_semantic_color(x, y) for x, y in
                                                       x[['level', 'semantics']].itertuples(index=False)]) \
        .fillna('')

    return formatted_df.sort_values(by=['risk_score', 'coverage'], ascending=False)


def prepare_html(df):
    def trend_symbol(v):
        return '+' if v >= 0 else '-'

    percentage = int(len(df)*0.3)
    top_k = df.head(percentage)
    if len(top_k['risk_score']) > 0:
        risk = int(top_k['risk_score'].sum() / len(top_k['risk_score']))
    else:
        risk = 0
    risk_color = 'blue' if risk < 50 else 'red'
    count_baseline = df['count_baseline'].sum()
    count_candidate = df['count_candidate'].sum()
    total_n_log_messages = count_baseline + count_candidate
    baseline_perc = int(round(100 * count_baseline / (count_baseline + count_candidate), 0))
    candidate_perc = int(round(100 * count_candidate / (count_baseline + count_candidate), 0))

    baseline_perc, candidate_perc = baseline_perc - candidate_perc, candidate_perc - baseline_perc
    baseline_perc = trend_symbol(baseline_perc) + str(baseline_perc)
    candidate_perc = trend_symbol(candidate_perc) + str(candidate_perc)

    added_states = len(df.loc[(df['count_baseline'] == 0) & (df['count_candidate'] > 0)])
    if added_states:
        added_states_info = math.floor(100 * len(
            df.loc[(df['count_baseline'] == 0) & (df['count_candidate'] > 0) & (
                    df['level'] == 'INFO')]) / added_states)
        added_states_fault = math.ceil(100 * len(
            df.loc[(df['count_baseline'] == 0) & (df['count_candidate'] > 0) & (
                    df['level'] != 'INFO')]) / added_states)
    else:
        added_states_info = 0
        added_states_fault = 0

    deleted_states = len(df.loc[(df['count_baseline'] > 0) & (df['count_candidate'] == 0)])
    if deleted_states:
        deleted_states_info = math.floor(100 * len(df.loc[(df['count_baseline'] > 0) & (df['count_candidate'] == 0) & (
               df['level'] == 'INFO')]) / deleted_states)
        deleted_states_fault = math.ceil(100 * len(df.loc[(df['count_baseline'] > 0) & (df['count_candidate'] == 0) & (
                df['level'] != 'INFO')]) / deleted_states)
    else:
        deleted_states_info = 0
        deleted_states_fault = 0

    recurring_states_df = df.loc[(df['count_baseline'] > 0) & (df['count_candidate'] > 0)].copy()
    recurring_states = len(recurring_states_df)

    if recurring_states:
        recurring_states_info = math.floor(100 * len(df.loc[(df['count_baseline'] > 0) & (df['count_candidate'] > 0) & (
                df['level'] == 'INFO')]) / recurring_states)
        recurring_states_fault = math.ceil(100 * len(df.loc[(df['count_baseline'] > 0) & (df['count_candidate'] > 0) & (
                df['level'] != 'INFO')]) / recurring_states)
    else:
        recurring_states_info = 0
        recurring_states_fault = 0
    frequency_change_threshold = .5
    frequency_change = len(
        recurring_states_df.loc[(recurring_states_df['change_perc'].abs() >= frequency_change_threshold)])
    if frequency_change:
        frequency_change_info = \
            (math.floor(100 * len(recurring_states_df.loc[
                               (recurring_states_df['change_perc'] < -frequency_change_threshold) & (
                                       df['level'] == 'INFO')]) / frequency_change),
             math.ceil(100 * len(recurring_states_df.loc[
                               (recurring_states_df['change_perc'] >= frequency_change_threshold) & (
                                       df['level'] == 'INFO')])) / frequency_change)
    else:
        frequency_change_info = 0

    frequency_change_fault = \
        (len(recurring_states_df.loc[
                 (recurring_states_df['change_perc'] < -frequency_change_threshold) & (df['level'] != 'INFO')]),
         len(recurring_states_df.loc[
                 (recurring_states_df['change_perc'] >= frequency_change_threshold) & (df['level'] != 'INFO')]))

    log_level_x_axis = [f'{i}.05' for i in range(1, 31, 3)]
    log_level_timeseries = [
        # ('Info', 'rgba(75, 192, 192, 1)', 'rgba(75, 192, 192, 0.2)', [1000, 1300, 1000, 1400, 4500, 4900, 1000, 1300, 1000, 1400]),
        # ('Debug', 'rgba(54, 162, 235, 1)', 'rgba(54, 162, 235, 0.2)', [2300, 2100, 2200, 2000, 6000, 4000, 2300, 2100, 2200, 6000]),
        ('Warning', 'rgba(255, 206, 86, 1)', 'rgba(255, 206, 86, 0.2)',
         [330, 300, 370, 380, 600, 400, 3300, 3000, 370, 380]),
        ('Error', 'rgba(255, 69, 0, 1)', 'rgba(255, 69, 0, 0.2)',
         [250, 270, 2300, 210, 600, 400, 270, 230, 210, 600]),
        ('Critical', 'rgba(255, 99, 132, 1)', 'rgba(255, 99, 132, 0.2)', [50, 5, 15, 20, 10, 5, 15, 20, 10, 5]),
    ]

    sdf = df.sort_values(by=['count_baseline', 'count_candidate'], ascending=False)
    frequency_topk = 20
    frequency_labels = [f'T{i}xxx' for i in range(1, len(sdf.template) + 1)][:frequency_topk]
    frequency_baseline = list(sdf.count_baseline)[:frequency_topk]
    frequency_candidate = list(sdf.count_candidate)[:frequency_topk]

    template_tbl_cols = ['Risk', 'Description', 'Baseline', 'Candidate', 'Template', 'Code',
                         'Count', 'Change', 'Coverage', 'Level', 'Semantics']
    template_tbl_rows = df.to_dict(orient='records')

    return dict(
        risk_color=risk_color,
        risk=risk,
        total_n_log_messages=int(total_n_log_messages),
        count_baseline=int(count_baseline),
        candidate_perc=candidate_perc,
        added_states=added_states,
        added_states_info=added_states_info,
        added_states_fault=added_states_fault,
        deleted_states=deleted_states,
        deleted_states_info=deleted_states_info,
        deleted_states_fault=deleted_states_fault,
        recurring_states=recurring_states,
        recurring_states_info=recurring_states_info,
        recurring_states_fault=recurring_states_fault,
        frequency_change_threshold=int(frequency_change_threshold * 100),
        frequency_change=frequency_change,
        frequency_change_info=frequency_change_info,
        frequency_change_fault=frequency_change_fault,
        log_level_x_axis=log_level_x_axis,
        log_level_timeseries=log_level_timeseries,
        frequency_labels=frequency_labels,
        frequency_baseline=frequency_baseline,
        frequency_candidate=frequency_candidate,
        cols=template_tbl_cols,
        rows=template_tbl_rows)
