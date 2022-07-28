from typing import Dict, List

import pandas as pd
from pandas import DataFrame

from .statistic import Statistic


class TemplateStatistics(Statistic):

    @classmethod
    def calculate(cls, logs_df: pd.DataFrame) -> DataFrame:
        template_counts = cls.get_template_counts(logs_df)
        template_level = cls.get_template_level(logs_df)
        template_prediction = cls.get_template_prediction(logs_df)
        template = dict(zip(template_prediction.keys(), template_prediction.keys()))

        return pd.DataFrame.from_records(
            [template,
             template_prediction,
             template_level,
             template_counts['count'],
             template_counts['coverage']
             ],
            index=['template',
                   'predicted_anomaly',
                   'level',
                   'count',
                   'coverage']).T

    @staticmethod
    def get_template_prediction(logs_df) -> Dict[str, List[int]]:
        return logs_df.groupby(['template'])['prediction'].first().to_dict()

    @staticmethod
    def get_template_counts(logs_df) -> Dict[str, List[int]]:
        df = logs_df.groupby(['template', pd.Grouper(freq='T')]) \
            .agg(count=('template', 'count')) \
            .reset_index() \
            .groupby('template')['count'] \
            .apply(sum).to_frame()
        df['coverage'] = df['count'] / len(logs_df)
        return df.to_dict()

    @staticmethod
    def get_template_level(logs_df) -> Dict[str, str]:
        return logs_df.groupby('template')['level'].first().to_dict()
