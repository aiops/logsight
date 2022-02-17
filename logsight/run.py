import argparse
import json
import logging.config
import os
import platform
from typing import Dict

from builders.application_builder import ApplicationBuilder
from builders.connection_builder import ConnectionBuilder
from builders.module_builder import ModuleBuilder
from config.global_vars import CONFIG_PATH
from manager import Manager
from services.configurator import ManagerConfig
from services import service_names
from connectors import sources
from connectors import sinks
from config import global_vars
from utils.fs import verify_file_ext

from multiprocessing import set_start_method

# hello world
logging.config.dictConfig(json.load(open(os.path.join(global_vars.CONFIG_PATH, "log.json"), 'r')))
logger = logging.getLogger('logsight')

# needed for running on Windows or macOS
if platform.system() != 'Linux':
    logger.info(f"Start method fork for system {platform.system()}.")
    set_start_method("fork", force=True)


def setup_connector(config: ManagerConfig, connector: str):
    conn_config = config.get_connector(connector)
    conn_params = config.get_connection(conn_config['connection'])
    conn_params.on_update(conn_config['params'])
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
        if "kafka" in s_name:
            services[s_name].create_topics_for_manager(config.get_topic_list())

    return services


def create_manager(config: ManagerConfig):
    source = setup_connector(config, 'source')
    services = setup_services(config)
    # producer = setup_connector(config, 'producer')
    topic_list = config.get_topic_list()

    connection_builder = ConnectionBuilder(config=config)
    module_builder = ModuleBuilder(connection_builder=connection_builder)
    app_builder = ApplicationBuilder(services, module_builder=module_builder)

    return Manager(source=source, services=services, producer=None, app_builder=app_builder)


def parse_arguments() -> Dict:
    parser = argparse.ArgumentParser(description='Logsight monolith.')
    parser.add_argument('--cconf', help='Connection config to use (filename in logsight/config directory)',
                        type=str, default='connections', required=False)
    parser.add_argument('--mconf', help='Manager config to use (filename in logsight/config directory)',
                        type=str, default='manager', required=False)
    parser.add_argument('--pconf', help='Pipeline config to use (filename in logsight/config directory)',
                        type=str, default='pipeline', required=False)
    args = vars(parser.parse_args())
    return args


def get_config(args: Dict) -> ManagerConfig:
    connection_conf_file = verify_file_ext(args['cconf'], ".json")
    connection_conf_path = os.path.join(CONFIG_PATH, connection_conf_file)
    manager_conf_file = verify_file_ext(args['mconf'], ".json")
    manager_conf_path = os.path.join(CONFIG_PATH, manager_conf_file)
    return ManagerConfig(connection_conf_path, manager_conf_path)


def run():
    args = parse_arguments()
    config = get_config(args)

    with open(os.path.join(global_vars.CONFIG_PATH, 'banner.txt'), 'r') as f:
        logger.info(f.read())
    manager = create_manager(config)
    manager.setup()
    logger.info("Running manager.")
    manager.run()


if __name__ == '__main__':
    run()
