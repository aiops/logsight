from abc import ABC
from typing import Dict

import pandas as pd


class Statistic(ABC):
    """
    The Statistic class is an abstract class that defines the interface for all statistics.
    """
    @classmethod
    def calculate(cls, df: pd.DataFrame) -> Dict:
        raise NotImplementedError
