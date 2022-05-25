from services.database.postgres.db import PostgresDBConnection
from services.service_provider import ServiceProvider


def test_create_postgres_connection():
    postgres = ServiceProvider.provide_postgres()
    assert isinstance(postgres, PostgresDBConnection)
