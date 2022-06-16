from services.database.postgres.db import PostgresDBConnection
from services.elasticsearch_service.elasticsearch_service import ElasticsearchService
from services.service_provider import ServiceProvider


def test_create_postgres_connection():
    postgres = ServiceProvider.provide_postgres()
    assert isinstance(postgres, PostgresDBConnection)


def test_create_elasticsearch_connection():
    postgres = ServiceProvider.provide_elasticsearch()
    assert isinstance(postgres, ElasticsearchService)
