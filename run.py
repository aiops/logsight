import json
import logging.config

logging.config.dictConfig(json.load(open("config/log.json", 'r')))

from manager import Manager
from services.configurator import ManagerConfig
from connectors.source import *
from connectors.sink import *
from services.admin_clients import KafkaAdmin, ElasticSearchAdmin
from services.database import PostgresDBConnection

logger = logging.getLogger('logsight')


def setup_connector(config, connector):
    conn_config = config.get_connector(connector)
    conn_params = config.get_connection(conn_config['connection'])
    conn_params.update(conn_config['params'])
    return eval(conn_config['classname'])(**conn_params)


def setup_services(config: ManagerConfig):
    service_names = config.list_services()
    services = {}
    for s_name in service_names:
        service_config = config.get_service(s_name)
        conn_params = config.get_connection(service_config['connection'])
        services[s_name] = eval(service_config['classname'])(**conn_params)

    return services


def create_manager():
    config = ManagerConfig()
    source = setup_connector(config, 'source')
    services = setup_services(config)
    producer = setup_connector(config, 'producer')
    topic_list = config.get_topic_list()

    return Manager(source=source, services=services, producer=producer, topic_list=topic_list)


def run():
    with open('config/banner.txt', 'r') as f:
        logger.info(f.read())
    manager = create_manager()
    manager.setup()
    manager.run()


if __name__ == '__main__':
    run()
