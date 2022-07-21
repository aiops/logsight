from configs.properties import ConfigProperties
from pydantic import BaseModel


@ConfigProperties("connectors.database")
class DatabaseConfigProperties(BaseModel):
    host: str
    port: int
    username: str
    password: str
    db_name: str
    driver: str = "postgresql+psycopg2"
