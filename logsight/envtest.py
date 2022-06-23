import environ


@environ.config(prefix="APP")
class AppConfig:
    @environ.config
    class DB:
        name = environ.var("default_db")
        host = environ.var("default.host")
        port = environ.var(5432, converter=int)  # Use attrs's converters and validators!
        user = environ.var("default_user")


cfg = environ.to_config(AppConfig)
print(cfg)
