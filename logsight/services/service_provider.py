from connectors.builders.config_provider import ConnectorConfigProvider
from services import ConnectionConfig
from services.database.db import PostgresDBService
from services.elasticsearch_service.elasticsearch_service import ElasticsearchService


class ServiceProvider:
    @staticmethod
    def provide_postgres():
        return PostgresDBService(ConnectorConfigProvider().get_config("database")())

    @staticmethod
    def provide_elasticsearch() -> ElasticsearchService:
        return ElasticsearchService(ConnectorConfigProvider().get_config("elasticsearch")())
