from .configurator import ConnectionConfigParser, ManagerConfig, ModulePipelineConfig
from .database import PostgresDBConnection
from .elasticsearch.elasticsearch_service import ElasticsearchService
from .kafka_service import KafkaService

service_names = {"elasticsearch": ElasticsearchService, "kafka": KafkaService, "database": PostgresDBConnection}
