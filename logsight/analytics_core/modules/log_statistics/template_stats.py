from typing import Dict, List

import pandas as pd
from pandas import DataFrame

from .statistic import Statistic


class TemplateStatistics(Statistic):

    @classmethod
    def calculate(cls, logs_df: pd.DataFrame) -> DataFrame:
        """
        > The function takes a dataframe of logs and returns a dataframe of templates, their predicted
        anomaly, their level, their count, and their coverage
        
        :param cls: the class that is being called
        :param logs_df: the dataframe containing the logs
        :type logs_df: pd.DataFrame
        :return: A dataframe with the following columns:
            - template
            - predicted_anomaly
            - level
            - count
            - coverage
        """
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
        """
        > For each template, get the first prediction
        
        :param logs_df: The dataframe containing the logs
        :return: A dictionary with the template as the key and the first prediction as the value.
        """
        return logs_df.groupby(['template'])['prediction'].first().to_dict()

    @staticmethod
    def get_template_counts(logs_df) -> Dict[str, List[int]]:
        """
        > We group the logs by template and time, then aggregate the counts, then group by template and
        sum the counts, then calculate the coverage
        
        :param logs_df: the dataframe containing the logs
        :return: A dictionary with the template as the key and the count and coverage as the values.
        """
        df = logs_df.groupby(['template', pd.Grouper(freq='T')]) \
            .agg(count=('template', 'count')) \
            .reset_index() \
            .groupby('template')['count'] \
            .apply(sum).to_frame()
        df['coverage'] = df['count'] / len(logs_df)
        return df.to_dict()

    @staticmethod
    def get_template_level(logs_df) -> Dict[str, str]:
        """
        > For each template, get the first level
        
        :param logs_df: The dataframe containing the logs
        :return: A dictionary with the template as the key and the level as the value.
        """
        return logs_df.groupby('template')['level'].first().to_dict()
