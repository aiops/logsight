import logging
import traceback
import uuid
from abc import ABC, abstractmethod
from functools import wraps

logger = logging.getLogger("logsight." + __name__)


def send_status(function):
    @wraps(function)
    def decorated(cls):
        try:
            msg = function(cls)
            cls._send_done(msg)
            logger.info(f"[x] Finished {cls.__class__.__name__}-{cls.name}.")

        except Exception as e:
            cls._send_error(f"<{e.__class__.__name__}> : {e}")

    return decorated


class Job(ABC):

    def __init__(self, notification_callback=None, done_callback=None, error_callback=None, name=None, **kwargs):
        """
            Parameters
            ----------
            notification_callback : function
                Callback function for notifications during execution of task

            done_callback : function
                Callback function for successful execution of task

            error_callback : function
                Callback function for unsuccessful execution of task
            """
        self._notification_callback = notification_callback
        self._done_callback = done_callback
        self._error_callback = error_callback
        self.__job_name__ = str(uuid.uuid4())[:8]
        if name:
            self.__job_name__ += name

    @property
    def name(self):
        return self.__job_name__

    @name.setter
    def name(self, name):
        self.__job_name__ = name

    def __repr__(self):
        return self.__job_name__

    def __str__(self):
        return str(self.__job_name__)

    @send_status
    def execute(self):
        """Run the task. """
        return self._execute()

    @abstractmethod
    def _execute(self):
        """This function is implemented by the inherited classes."""
        raise NotImplementedError

    def _send_notification(self, message):
        if self._notification_callback is not None:
            self._notification_callback(message)

    def _send_done(self, message):
        if self._done_callback is not None:
            self._done_callback(message)

    def _send_error(self, message):
        if self._error_callback is not None:
            self._error_callback(message)
