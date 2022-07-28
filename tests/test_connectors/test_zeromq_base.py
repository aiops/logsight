import unittest

import zmq
from zmq import ZMQError

from logsight.connectors.connectors.zeromq import ZeroMQConfigProperties
from logsight.connectors.connectors.zeromq.connector import ConnectionTypes, ZeroMQConnector


class TestZeroMQBase(unittest.TestCase):
    endpoint = "tcp://0.0.0.0:4444"
    connections_connect = {zmq.SUB: ConnectionTypes.CONNECT}
    connections_bind = {zmq.PUB: ConnectionTypes.BIND, zmq.REP: ConnectionTypes.BIND}
    connections = {**connections_connect, **connections_bind}

    def test_socket_connect_success(self):

        for socket_type, connection_type in TestZeroMQBase.connections.items():
            config = ZeroMQConfigProperties(endpoint=TestZeroMQBase.endpoint, socket_type=socket_type,
                                            connection_type=connection_type)
            zeromq_base = ZeroMQConnector(config)
            try:
                zeromq_base.connect()
            except ConnectionError as e:
                self.fail(
                    f"ZeroMQBase.connect() raised ConnectionError unexpectedly for socket type {socket_type} and "
                    f"connection type {connection_type.name} while connecting: {e}"
                )
            try:
                zeromq_base.close()
            except Exception as e:
                self.fail(
                    f"ZeroMQBase.close() raised ConnectionError unexpectedly for socket type {socket_type} and "
                    f"connection type {connection_type.name} while closing the connection: {e}")

    def test_socket_connect_fail_in_use(self):
        for socket_type, connection_type in TestZeroMQBase.connections_bind.items():
            config = ZeroMQConfigProperties(endpoint=TestZeroMQBase.endpoint, socket_type=socket_type,
                                            connection_type=connection_type)
            zeromq_base1 = ZeroMQConnector(config)
            zeromq_base2 = ZeroMQConnector(config)
            try:
                zeromq_base1.connect()
            except ConnectionError as e:
                self.fail(
                    f"ZeroMQBase.connect() raised ConnectionError unexpectedly for socket type {socket_type} and "
                    f"connection type {connection_type.name} while connecting: {e}"
                )
            self.assertRaises(ZMQError, zeromq_base2.connect)
            try:
                zeromq_base1.close()
            except Exception as e:
                self.fail(
                    f"ZeroMQBase.close() raised ConnectionError unexpectedly for socket type {socket_type} and "
                    f"connection type {connection_type.name} while closing the connection: {e}")


if __name__ == '__main__':
    unittest.main()
