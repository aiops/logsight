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
            f"Performing aggregation on {self.index_interval.index} for the interval [{str(self.index_interval.start_date)} - {str(self.index_interval.end_date)}]")
        data = self._load_data(self.index_interval.index, self.index_interval.start_date, self.index_interval.end_date)
        if not len(data):
            return False
        # calculate
        results = self._calculate(data)
        # store
        self._store_results(results, "_".join([self.index_interval.index, self.index_ext]))
        logger.debug(f"Stored {len(results)} results")
        # ES Might not read all the messages in the specified period
        self._update_index_interval(dateutil.parser.isoparse(data[-1]['timestamp']) + timedelta(milliseconds=1))
        return True

    def _update_index_interval(self, last_date):
        """
        Updates the index interval to start at the last date of
        the previous index.

        Args:
            last_date: Set the start_date of the index interval

        Returns:
            The start date of the index interval

        """
        self.index_interval.start_date = last_date
        self.index_interval.end_date = datetime.now()

    @staticmethod
    def _load_data(index, start_date, end_date):
        """
        Load the data from elasticsearch
        Args:
            index: elasticsearch index
            start_date: starting date for query
            end_date: end date for query

        Returns:

        """
        with ServiceProvider.provide_elasticsearch() as es:
            try:
                return es.get_all_logs_for_index("_".join([index, PIPELINE_INDEX_EXT]),
                                                 str(start_date.isoformat()),
                                                 str(end_date.isoformat()))
            except NotFoundError:
                logger.warning(f"Data is not yet processed for index {'_'.join([index, PIPELINE_INDEX_EXT])}")
                return []

    @staticmethod
    def _store_results(results: List, index: str):
        with ServiceProvider.provide_elasticsearch() as es:
            es.save(results, index)

    @abstractmethod
    def _calculate(self, logs) -> List:
        raise NotImplementedError
