from abc import ABC
from typing import Dict
import pandas as pd


class Statistic(ABC):

    @classmethod
    def calculate(cls, df: pd.DataFrame) -> Dict:
        raise NotImplementedError
