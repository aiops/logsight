import json
import logging.config
import os

from manager import Manager
from services.configurator import ManagerConfig
from services import service_names
from connectors import sources
from connectors import sinks
from config import global_vars

logging.config.dictConfig(json.load(open(os.path.join(global_vars.CONFIG_PATH, "config/log.json"), 'r')))
logger = logging.getLogger('logsight')


def setup_connector(config, connector):
    conn_config = config.get_connector(connector)
    conn_params = config.get_connection(conn_config['connection'])
    conn_params.update(conn_config['params'])
    try:
        c_name = getattr(sources, conn_config['classname'])
    except AttributeError:
        c_name = getattr(sinks, conn_config['classname'])
    return c_name(**conn_params)


def setup_services(config: ManagerConfig):
    services = {}
    for s_name in config.list_services():
        service_config = config.get_service(s_name)
        conn_params = config.get_connection(service_config['connection'])
        services[s_name] = service_names[s_name](**conn_params)

    return services


def create_manager():
    config = ManagerConfig()
    source = setup_connector(config, 'source')
    services = setup_services(config)
    producer = setup_connector(config, 'producer')
    topic_list = config.get_topic_list()

    return Manager(source=source, services=services, producer=producer, topic_list=topic_list)


def run():
    with open(os.path.join(global_vars.CONFIG_PATH, 'banner.txt'), 'r') as f:
        logger.info(f.read())
    manager = create_manager()
    manager.setup()
    logger.info("Running manager.")
    manager.run()


if __name__ == '__main__':
    run()
