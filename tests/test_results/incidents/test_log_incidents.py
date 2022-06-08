import unittest
from unittest.mock import MagicMock
from datetime import datetime

from results.incidents.incidents import CalculateIncidentJob
from results.log_aggregation.log_aggregation import CalculateLogAggregationJob
from results.persistence.dto import IndexInterval
from services.elasticsearch.elasticsearch_service import ElasticsearchService
from tests.inputs import incident_results, processed_logs


class CalculateLogAggregationTest(unittest.TestCase):
    def test__calculate(self):
        es = ElasticsearchService("host", "port", "user", "password")
        es.connect = MagicMock()
        es.get_all_logs_for_index = MagicMock(return_value=processed_logs)

        job = CalculateIncidentJob(IndexInterval("incidents", datetime.min, datetime.max))
        result = job._calculate(es.get_all_logs_for_index("test"), [])

        self.assertEqual(incident_results, result)
