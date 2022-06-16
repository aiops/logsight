from services import ConnectionConfig
from services.database.postgres.db import PostgresDBService
from services.elasticsearch_service.elasticsearch_service import ElasticsearchService


class ServiceProvider:
    @staticmethod
    def provide_postgres():
        return PostgresDBService(**ConnectionConfig().get_postgres_params())

    @staticmethod
    def provide_elasticsearch() -> ElasticsearchService:
        return ElasticsearchService(**ConnectionConfig().get_elasticsearch_params())
