from abc import ABC, abstractmethod
from tenacity import retry, stop_after_attempt, wait_fixed

from configs.properties import LogsightProperties

config_properties = LogsightProperties()


class Connectable(ABC):
    """Interface that allows connections to an endpoint """

    @retry(reraise=True,
           stop=stop_after_attempt(config_properties.retry_attempts),
           wait=wait_fixed(config_properties.retry_timeout))
    def connect(self):
        """Establish connection to endpoint."""
        self._connect()

    @abstractmethod
    def _connect(self):
        """Establish connection to endpoint."""
        raise NotImplementedError

    @abstractmethod
    def close(self):
        """Close connection to endpoint."""
        raise NotImplementedError
