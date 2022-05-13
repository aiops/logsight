from services import ConnectionConfigParser
from services.elasticsearch.elasticsearch_service import ElasticsearchService


class ServiceProvider:
    @staticmethod
    def provide_postgres():
        return

    @staticmethod
    def provide_elasticsearch() -> ElasticsearchService:
        return ElasticsearchService(**ConnectionConfigParser().get_elasticsearch_params())
