from .database import PostgresDBConnection
from .elasticsearch.elasticsearch_service import ElasticsearchService
from .kafka_service import KafkaService
from .configurator import ConnectionConfig, ModulePipelineConfig

service_names = {"elasticsearch": ElasticsearchService, "kafka": KafkaService, "database": PostgresDBConnection}
