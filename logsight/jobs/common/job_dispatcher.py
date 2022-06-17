import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Type

from common.patterns.job import Job
from common.patterns.job_manager import JobManager
from pipeline.modules.core.timer import NamedTimer
from jobs.common.index_job import IndexJob
from jobs.persistence.dto import IndexInterval
from jobs.persistence.timestamp_storage import TimestampStorage
from services.service_provider import ServiceProvider

logger = logging.getLogger("logsight." + __name__)


class JobDispatcher(ABC):
    @abstractmethod
    def run(self):
        raise NotImplementedError

    @abstractmethod
    def submit_job(self):
        raise NotImplementedError


class PeriodicJobDispatcher(JobDispatcher):
    def __init__(self, job: Type[IndexJob], job_manager: JobManager, storage: TimestampStorage, timeout_period: int,
                 timer_name=""):
        """

        """
        self.manager = job_manager
        self.job = job
        self.storage = storage
        self.timer = NamedTimer(name=timer_name, callback=self.submit_job, timeout_period=timeout_period)

    def submit_job(self):
        """
        The callback function is called by the timer. It syncs the database with newly created indices and
        submits a job for calculating incidents to the job manager. When the job finishes,
        the update_status callback is called where the time range for calculating incidents is updated.

        """

        self.sync_index()
        index_intervals = self.storage.get_all()
        for it in index_intervals:
            job = self.job(index_interval=it, error_callback=logger.error,
                           name=it.index, table_name=self.storage.__table__)
            self.manager.submit_job(job)
        self.timer.reset_timer()

    def sync_index(self):
        """
        The sync_index function creates new indices in the database if they do not already exist.

        Returns:
            A set of indices that are in the application database but not in the index database

        """
        # find new indices
        available_idx = set(self.select_all_es_index())
        current_idx = set(self.storage.select_all_index())
        indices = available_idx.difference(current_idx)

        if len(indices):
            logger.debug(f"Creating new index intervals {indices}")
            for idx in indices:
                it = IndexInterval(idx, latest_ingest_time=datetime.min)
                self.storage.update_timestamps(it)

    @staticmethod
    def select_all_es_index():
        with ServiceProvider.provide_elasticsearch() as es:
            return set([x.split("_")[0] for x in es.get_all_indices("") if x[0] != "."])

    def run(self):
        """Start the timer"""
        self.timer.start()


class TimedJobDispatcher(JobDispatcher):
    def __init__(self, job: Job, timeout_period: int, timer_name=""):
        self.job = job
        self.timer = NamedTimer(name=timer_name, callback=self.submit_job, timeout_period=timeout_period)

    def submit_job(self):
        self.job.execute()
        self.timer.reset_timer()

    def run(self):
        """Start the timer"""
        self.timer.start()
