from logsight.services.database.db import PostgresDBService
from logsight.services.elasticsearch_service.elasticsearch_service import ElasticsearchService
from logsight.services.service_provider import ServiceProvider


def test_create_postgres_connection():
    postgres = ServiceProvider.provide_postgres()
    assert isinstance(postgres, PostgresDBService)


def test_create_elasticsearch_connection():
    postgres = ServiceProvider.provide_elasticsearch()
    assert isinstance(postgres, ElasticsearchService)
