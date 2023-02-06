from abc import abstractmethod

from logsight.analytics_core.logs import LogBatch


class BaseAnomalyDetector:
    """
    The `BaseAnomalyDetector` class is an abstract class that defines the interface for all anomaly
    detectors
    """
    def __init__(self):
        self._module_name = self.__class__.__name__

    @property
    def name(self):
        return self._module_name

    @name.setter
    def name(self, name):
        self._module_name = name

    def __repr__(self):
        return f"{self.__class__.__name__}: {self._module_name}"

    @abstractmethod
    def predict(self, batch: LogBatch) -> LogBatch:
        raise NotImplementedError


class BaseModel:
    """
    The BaseModel class is an abstract class that defines the basic structure of a model
    """
    def __init__(self):
        self._module_name = self.__class__.__name__

    @property
    def name(self):
        return self._module_name

    @name.setter
    def name(self, name):
        self._module_name = name

    def __repr__(self):
        return f"{self.__class__.__name__}: {self._module_name}"

    @abstractmethod
    def predict(self, logs):
        raise NotImplementedError

    @abstractmethod
    def load_model(self):
        raise NotImplementedError
