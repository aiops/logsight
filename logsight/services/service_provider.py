from services import ConnectionConfig
from services.elasticsearch.elasticsearch_service import ElasticsearchService


class ServiceProvider:
    @staticmethod
    def provide_postgres():
        return

    @staticmethod
    def provide_elasticsearch() -> ElasticsearchService:
        return ElasticsearchService(**ConnectionConfig().get_elasticsearch_params())
