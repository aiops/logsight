from connectors.connection_builder import ConnectionBuilder
from common.logsight_classes.configs import AppConfig
from pipeline.modules.module_builder import ModuleBuilder
from run import get_config, parse_arguments, setup_services
from scrap_files.builders.application_builder import ApplicationBuilder
from services import ModulePipelineConfig

if __name__ == "__main__":
    app_config = AppConfig(application_id="app_id", application_name="app_name", private_key="private_key",
                           action="create")
    pipeline_cfg = ModulePipelineConfig()
    config = get_config(parse_arguments())
    services = setup_services(config)
    connection_builder = ConnectionBuilder(config=config)
    module_builder = ModuleBuilder(connection_builder=connection_builder)
    app_builder = ApplicationBuilder(services, module_builder=module_builder)

    application = app_builder.build(pipeline_cfg.pipeline_config)
    application.start()
