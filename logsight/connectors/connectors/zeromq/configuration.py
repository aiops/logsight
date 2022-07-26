import zmq

from configs.properties import ConfigProperties
from pydantic import BaseModel

from connectors.connectors.zeromq.conn_types import ConnectionTypes


@ConfigProperties(prefix="connectors.zeromq")
class ZeroMQConfigProperties(BaseModel):
    endpoint: str = "localhost:9992"
    socket_type: int = zmq.SUB
    connection_type: ConnectionTypes = ConnectionTypes.CONNECT
    topic: str = ""

