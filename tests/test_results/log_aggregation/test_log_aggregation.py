import unittest
from unittest.mock import MagicMock
from datetime import datetime
from results.log_aggregation.log_aggregation import CalculateLogAggregationJob
from results.persistence.dto import IndexInterval
from services.elasticsearch.elasticsearch_service import ElasticsearchService
from tests.inputs import processed_logs


class CalculateLogAggregationTest(unittest.TestCase):
    def test__calculate(self):
        es = ElasticsearchService("scheme", "host", 9201, "user", "password")
        es.connect = MagicMock()
        es.get_all_logs_for_index = MagicMock(return_value=processed_logs)

        test_output = [{'log_levels': {'INFO': 20}, 'prediction': 20, 'count': 20,
                        'timestamp': '2021-12-16T05:15:00'}]
        job = CalculateLogAggregationJob(IndexInterval("log_agg", datetime.min, datetime.max))
        result = job._calculate(es.get_all_logs_for_index("test"))

        self.assertEqual(test_output, result)
