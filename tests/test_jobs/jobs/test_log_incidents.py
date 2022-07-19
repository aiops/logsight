import unittest
from unittest.mock import MagicMock
from datetime import datetime

from jobs.jobs.incidents import CalculateIncidentJob
from jobs.persistence.dto import IndexInterval
from services.elasticsearch_service.elasticsearch_service import ElasticsearchService
from tests.inputs import expected_incident_result, processed_logs


class CalculateIncidentsTest(unittest.TestCase):
    def test__calculate(self):
        es = ElasticsearchService("scheme", "host", 9201, "user", "password")
        es.connect = MagicMock()
        es._connect = MagicMock()
        es.get_all_logs_for_index = MagicMock(return_value=processed_logs, side_effect=[processed_logs])
        job = CalculateIncidentJob(IndexInterval("incidents", datetime.min))
        job.load_templates = MagicMock(return_value=[])
        result = job._calculate(es.get_all_logs_for_index("test"))

        self.assertEqual(expected_incident_result, result)
