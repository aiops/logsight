import unittest

import zmq

from connectors.zeromq_base import ConnectionTypes, ZeroMQBase


class TestZeroMQBase(unittest.TestCase):
    endpoint = "tcp://0.0.0.0:4444"
    connections_good = {zmq.PUB: ConnectionTypes.BIND, zmq.SUB: ConnectionTypes.CONNECT, zmq.REP: ConnectionTypes.BIND}
    connections_bad = {zmq.PUB: ConnectionTypes.CONNECT, zmq.SUB: ConnectionTypes.BIND, zmq.REP: ConnectionTypes.CONNECT}

    def test_socket_connect_success(self):
        zeromq_base = ZeroMQBase(endpoint=TestZeroMQBase.endpoint, socket_type=zmq.PUB, connection_type=ConnectionTypes.BIND)
        try:
            zeromq_base.connect()
        except ConnectionError as e:
            self.fail(f"zeromq_base.connect() raised ConnectionError unexpectedly while connecting: {e}")

        try:
            zeromq_base.close()
        except Exception as e:
            self.fail(f"zeromq_base.connect() raised ConnectionError unexpectedly while closing the connection: {e}")


if __name__ == '__main__':
    unittest.main()
