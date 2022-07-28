from pydantic import BaseModel

from logsight.configs.properties import ConfigProperties


@ConfigProperties("connectors.database")
class DatabaseConfigProperties(BaseModel):
    host: str
    port: int
    username: str
    password: str
    db_name: str
    driver: str = "postgresql+psycopg2"
