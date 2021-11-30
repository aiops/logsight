from .admin_clients import ElasticSearchAdmin, KafkaAdmin
from .configurator import Config, ManagerConfig, ModuleConfig
from .database import PostgresDBConnection

service_names = {"elasticsearch_admin": ElasticSearchAdmin, "kafka_admin": KafkaAdmin, "database": PostgresDBConnection}
