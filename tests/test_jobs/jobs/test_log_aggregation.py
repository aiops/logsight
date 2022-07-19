import unittest
from unittest.mock import MagicMock
from datetime import datetime
from jobs.jobs.log_aggregation import CalculateLogAggregationJob
from jobs.persistence.dto import IndexInterval
from services.elasticsearch_service.elasticsearch_service import ElasticsearchService
from tests.inputs import agg_results, processed_logs


class CalculateLogAggregationTest(unittest.TestCase):
    def test__calculate(self):
        es = ElasticsearchService("scheme", "host", 9201, "user", "password")
        es.connect = MagicMock()
        es._connect = MagicMock()
        es.get_all_logs_for_index = MagicMock(return_value=processed_logs)

        job = CalculateLogAggregationJob(IndexInterval("log_agg", datetime.min))
        result = job._calculate(es.get_all_logs_for_index("test"))

        self.assertEqual(agg_results, result)
