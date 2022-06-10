import logging
from datetime import datetime
from typing import Type

from common.patterns.job_manager import JobManager
from pipeline.modules.core.timer import NamedTimer
from results.common.index_job import IndexJob
from results.persistence.dto import IndexInterval
from results.persistence.timestamp_storage import TimestampStorage

logger = logging.getLogger("logsight." + __name__)


class PeriodicJobDispatcher:
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
            job = self.job(index_interval=it, error_callback=logger.error, done_callback=logger.info,
                           table_name=self.storage.__table__)
            self.manager.submit_job(job)
        self.timer.reset_timer()

    def sync_index(self):
        """
        The sync_index function creates new indices in the database if they do not already exist.

        Returns:
            A set of indices that are in the application database but not in the index database

        """
        # find new indices
        available_idx = set(self.storage.select_all_user_index())
        current_idx = set(self.storage.select_all_index())
        indices = available_idx.difference(current_idx)

        if len(indices):
            logger.debug(f"Creating new index intervals {indices}")
            for idx in indices:
                it = IndexInterval(idx, latest_ingest_time=datetime.min, latest_processed_time=datetime.min)
                self.storage.update_timestamps(it)

    def start(self):
        """Start the timer"""
        self.timer.start()
