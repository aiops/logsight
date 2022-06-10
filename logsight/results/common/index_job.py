import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional
import dateutil.parser
from elasticsearch import NotFoundError

from common.patterns.job import Job
from configs.global_vars import PIPELINE_INDEX_EXT
from results.persistence.dto import IndexInterval
from results.persistence.timestamp_storage import TimestampStorageProvider
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
        logger.info(f"[x] Executing {self.__class__.__name__}-{self.name} for index interval {self.index_interval}.")
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
        logger.debug(
            f"Performing aggregation on {self.index_interval.index} for the interval [{str(self.index_interval.latest_ingest_time)} - {str(self.index_interval.latest_ingest_time)}]")
        data = self._load_data(self.index_interval.index, self.index_interval.latest_ingest_time,
                               self.index_interval.latest_processed_time)
        if not len(data):
            return False
        # calculate
        results = self._calculate(data)
        # store
        self._store_results(results, "_".join([self.index_interval.index, self.index_ext]))
        logger.debug(f"Stored {len(results)} results")
        # ES Might not read all the messages in the specified period
        self._update_index_interval(dateutil.parser.isoparse(data[-1]['timestamp']),
                                    dateutil.parser.isoparse(data[-1]['ingest_timestamp']))
        return True

    def _update_index_interval(self, last_processed_datetime, last_ingest_datetime):
        self.index_interval.latest_processed_time = last_processed_datetime + timedelta(milliseconds=1)
        self.index_interval.latest_ingest_time = last_ingest_datetime + timedelta(milliseconds=1)

    @staticmethod
    def _load_data(index, latest_ingest_time, latest_processed_time):
        """
        Load the data from elasticsearch
        Args:
            index: elasticsearch index
            latest_ingest_time: ingest time of the entries
            latest_processed_time: last date of processed logs

        Returns:

        """
        index = "_".join([index, PIPELINE_INDEX_EXT])
        with ServiceProvider.provide_elasticsearch() as es:
            try:
                logs = es.get_all_logs_after_ingest(index, str(latest_ingest_time.isoformat()))
                if len(logs) == 0:
                    return logs
                if datetime.fromisoformat(logs[0]['timestamp']) > latest_processed_time:
                    return logs
                else:
                    return es.get_all_logs_for_index(index, logs[0]['timestamp'], logs[-1]['timestamp'])
            except NotFoundError:
                logger.warning(f"Data is not yet processed for index {'_'.join([index, PIPELINE_INDEX_EXT])}")
                return []

    @staticmethod
    def _store_results(results: List, index: str):
        with ServiceProvider.provide_elasticsearch() as es:
            es.delete_logs_for_index(index, results[0]['timestamp'], results[-1]['timestamp'])
            es.save(results, index)

    @abstractmethod
    def _calculate(self, logs) -> List:
        raise NotImplementedError
