import unittest
from unittest import mock
from unittest.mock import MagicMock

from jobs.jobs.delete_es_data_job import DeleteESDataJob
from services.elasticsearch_service.elasticsearch_service import ElasticsearchService
from services.service_provider import ServiceProvider


class CalculateIncidentsTest(unittest.TestCase):
    def test__execute(self):
        cleanup_age = "now-1y"
        es = ElasticsearchService("scheme", "host", 9201, "user", "password")
        es.connect = MagicMock()
        es._connect = MagicMock()
        es.delete_by_ingest_timestamp = MagicMock()
        ServiceProvider.provide_elasticsearch = MagicMock(return_value=es)

        job = DeleteESDataJob()
        job.execute()
        self.assertEqual(job.cleanup_age, cleanup_age)
        calls = [mock.call("*_pipeline", start_time="now-15y", end_time=cleanup_age),
                 mock.call("*_incidents", start_time="now-15y", end_time=cleanup_age)]
        es.delete_by_ingest_timestamp.assert_has_calls(calls, any_order=False)
        self.assertEqual(es.delete_by_ingest_timestamp.call_count, 2)
