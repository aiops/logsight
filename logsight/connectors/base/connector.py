from abc import ABC, abstractmethod

from tenacity import retry, stop_after_attempt, wait_fixed
from configs.global_vars import RETRY_ATTEMPTS, RETRY_TIMEOUT


class Connector(ABC):
    """Base class for connector"""

    @retry(reraise=True, stop=stop_after_attempt(RETRY_ATTEMPTS), wait=wait_fixed(RETRY_TIMEOUT))
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
