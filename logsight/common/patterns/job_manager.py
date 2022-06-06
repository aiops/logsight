import logging
from abc import ABC
from concurrent.futures import ProcessPoolExecutor

from .job import Job

logger = logging.getLogger("logsight." + __name__)


class JobManager(ABC):

    @staticmethod
    def done_callback(future):
        raise NotImplementedError("A callback function not provided")

    def __init__(self, max_workers=5):
        """ Orchestrates different jobs in a pool of threads
        Attributes
        ----------
        max_workers: The maximum number of threads that can be used to
                execute the given calls.
        """
        self.pool = ProcessPoolExecutor(max_workers)

    def submit_job(self, job: Job):
        """ Submit a job for execution to the pool of threads """
        logger.debug(f"Submitting job: {job}")
        return self.pool.submit(job.execute)
