from services import ConnectionConfig
from services.database.postgres.db import PostgresDBConnection
from services.elasticsearch_service.elasticsearch_service import ElasticsearchService


class ServiceProvider:
    @staticmethod
    def provide_postgres():
        return PostgresDBConnection(**ConnectionConfig().get_postgres_params())

    @staticmethod
    def provide_elasticsearch() -> ElasticsearchService:
        return ElasticsearchService(**ConnectionConfig().get_elasticsearch_params())
