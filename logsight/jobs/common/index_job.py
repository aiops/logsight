import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional
import dateutil.parser
from elasticsearch import NotFoundError, ConflictError

from common.patterns.job import Job
from configs.global_vars import PIPELINE_INDEX_EXT
from jobs.persistence.dto import IndexInterval
from jobs.persistence.timestamp_storage import TimestampStorageProvider
from services.service_provider import ServiceProvider

logger = logging.getLogger("logsight")


@dataclass
class IndexJobResult:
    index_interval: IndexInterval
    table: str


class IndexJob(Job, ABC):
    def __init__(self, index_interval: IndexInterval, index_ext: Optional[str] = None, notification_callback=None,
                 done_callback=None,
                 error_callback=None, name=None, table_name="", **kwargs):
        super().__init__(notification_callback, done_callback, error_callback, name, **kwargs)
        self.index_interval = index_interval
        self.index_ext = index_ext
        self.table_name = table_name

    def _execute(self):
        """
        Perform aggregation until done, return index interval for future aggregations
        Returns:
           IncidentTimestamps

        """
        logger.debug(f"[x] Executing {self.__class__.__name__}-{self.name} for index interval {self.index_interval}.")
        while self._perform_aggregation():
            continue
        with TimestampStorageProvider.provide_timestamp_storage(self.table_name) as db:
            db.update_timestamps(self.index_interval)
        return IndexJobResult(self.index_interval, self.table_name)

    def _perform_aggregation(self) -> bool:
        """
        The _perform_aggregation function is responsible for
        loading data from Elasticsearch, performing aggregation calculations on that data, and storing it in a
        specified elasticsearch index. The _perform_aggregation function takes no arguments and returns True when all the data is aggregated until end_date.


        Returns:
            True if the aggregation was successful
        """
        now = datetime.utcnow().replace(second=0, microsecond=0) - timedelta(minutes=1)
        logger.debug(
            f"Performing aggregation on {self.index_interval.index} for the interval [{str(self.index_interval.latest_ingest_time)}-{now}]")
        data = self._load_data(self.index_interval.index, self.index_interval.latest_ingest_time)
        if not len(data):
            return False
        # calculate
        min_ingest = min([x['ingest_timestamp'] for x in data])
        max_ingest = max([x['ingest_timestamp'] for x in data])
        logger.debug(
            f"Calculating incidents for {str(min_ingest)} - {str(max_ingest)}.")
        results = self._calculate(data)

        index = "_".join([self.index_interval.index, self.index_ext])

        # store
        self._store_results(results, index)
        logger.debug(f"Stored {len(results)} results")
        # ES Might not read all the messages in the specified period

        self._update_index_interval(dateutil.parser.isoparse(max_ingest))
        return True

    def _update_index_interval(self, last_ingest_datetime):
        self.index_interval.latest_ingest_time = last_ingest_datetime + timedelta(milliseconds=1)

    @staticmethod
    def _load_data(index, latest_ingest_time):
        """
        Load the data from elasticsearch
        Args:
            index: elasticsearch index
            latest_ingest_time: ingest time of the entries

        Returns:

        """
        index = "_".join([index, PIPELINE_INDEX_EXT])
        with ServiceProvider.provide_elasticsearch() as es:
            try:
                now = datetime.utcnow().replace(second=0, microsecond=0) - timedelta(minutes=1)

                return es.get_all_logs_after_ingest(index, str(latest_ingest_time.isoformat()), str(now.isoformat()))
            except NotFoundError:
                logger.warning(f"Data is not yet processed for index {index}")
                return []

    @staticmethod
    def _delete_historical_incidents(index: str, start_time, end_time):
        with ServiceProvider.provide_elasticsearch() as es:
            try:
                es.delete_logs_for_index(index, start_time.isoformat(),
                                         end_time.isoformat())
            except NotFoundError:
                logger.warning(f"Index {index} is empty and data cannot be deleted")
            except ConflictError:
                logger.warning(f"Data on index {index} is already deleted.")

    @staticmethod
    def _store_results(results: List, index: str):
        with ServiceProvider.provide_elasticsearch() as es:
            es.save(results, index)

    @abstractmethod
    def _calculate(self, logs) -> List:
        raise NotImplementedError
