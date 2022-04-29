from abc import ABC, abstractmethod
from typing import Dict, Union


class Transformer(ABC):
    """
    Interface for transforming data from data source.
    """

    @abstractmethod
    def transform(self, data: Union[str, Dict]):
        raise NotImplementedError
