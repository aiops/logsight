from concurrent.futures import ThreadPoolExecutor
from abc import ABC
from .job import Job


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
        self.pool = ThreadPoolExecutor(max_workers)

    def submit_job(self, job: Job):
        """ Submit a job for execution to the pool of threads """
        self.pool.submit(job.execute)
