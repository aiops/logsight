import functools
import logging
from collections import defaultdict

from .job import Job
from .job_manager import JobManager

logger = logging.getLogger("logsight." + __name__)


class QueueableJobManager(JobManager):

    def __init__(self, max_workers=5):
        """ Orchestrates different jobs in a pool of threads
        Attributes
        ----------
        max_workers: The maximum number of threads that can be used to
                execute the given calls.
        """
        super().__init__(max_workers)
        self.job_queue = defaultdict(list)
        self.running_jobs = set()

    def submit_job(self, job: Job):
        """
        The submit_job function accepts a Job object and adds it to the job queue.
        It then tries to start a job with the job name from the job queue.

        Args:
            job:Job: Pass the job object to the submit_job function
        """
        logger.debug(f"Submitting job {job} to job_queue.")
        self.job_queue[job.name].append(job)
        self._start_job(job.name)

    def _start_job(self, job_name):
        """
        The _start_job function is a helper function that is called when the job_queue has at least one job in it.
        It takes the first job from the queue and submits it to be executed by a worker process. The running_jobs set is updated
        to include this new running job.
        Args:
            self: Access the class variables
            job_name: Identify the job

        """
        # if there is already a running job with the same name do not submit a new job.
        if job_name in self.running_jobs:
            logger.debug(f"Job with {job_name} already running. Jobs in Queue {len(self.job_queue[job_name])}")
            return
        # if there is a job in the job_queue, submit that job
        if len(self.job_queue[job_name]) > 0:
            job = self.job_queue[job_name].pop(0)
            logger.debug(
                f"Submitting job {job.name + '-' + job.id} to executor. Jobs in Queue {len(self.job_queue[job_name])}")
            result = self.pool.submit(job.execute)
            self.running_jobs.add(job_name)
            result.add_done_callback(functools.partial(self._job_complete, job.name))

    def _job_complete(self, job_name, future):
        """
        The _job_complete function is called when a job is completed. It removes the job from the list of running jobs,
         and starts another one if there are any more jobs to be run.
        Args:
            job_name: Identify the job in the running_jobs list
            future: Retrieve the result of the job

        Returns:
            The future object of the job that is completed
        """
        self.running_jobs.remove(job_name)
        self._start_job(job_name)
        return future
