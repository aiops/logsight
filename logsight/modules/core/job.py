from abc import ABC, abstractmethod
import logging
from functools import wraps


def send_status(function):
    @wraps(function)
    def decorated(cls):
        try:
            msg = function(cls)
            cls._send_done(msg)
            logging.info(f"[*] Finished {cls.__class__.__name__} job.")

        except Exception as e:
            cls._send_error(f"<{e.__class__.__name__}> : {e}")

    return decorated


class Job(ABC):
    def __init__(self, job_config, notification_callback=None, done_callback=None, error_callback=None, **kwargs):
        """
            Parameters
            ----------
            job_config : dict
                Dictionary containing the request for the task

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
        self.job_config = job_config

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
