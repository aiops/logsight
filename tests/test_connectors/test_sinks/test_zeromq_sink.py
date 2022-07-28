import json
import unittest

import zmq
from zmq import ZMQError

from logsight.connectors.connectors.zeromq import ZeroMQConfigProperties
from logsight.connectors.connectors.zeromq.connector import ConnectionTypes
from logsight.connectors.sinks import ZeroMQPubSink


class TestZeroMQPubSink(unittest.TestCase):
    config = ZeroMQConfigProperties(endpoint="tcp://0.0.0.0:4444", topic="test_topic_test", socket_type=zmq.PUB,
                                    connection_type=ConnectionTypes.CONNECT)
    test_msg = {"message": "hello"}
    test_msg_str = json.dumps(test_msg)

    def setUp(self) -> None:
        super().setUp()

    def tearDown(self) -> None:
        super().tearDown()

    def test_socket_connect_and_close_success(self):
        zeromq = ZeroMQPubSink(self.config)
        self.save_connect(zeromq)
        self.save_close(zeromq)

    def test_close_not_connected_success(self):
        zeromq = ZeroMQPubSink(self.config)
        self.save_close(zeromq)

    def test_socket_connect_fail_address_in_use(self):
        self.config.connection_type = ConnectionTypes.BIND
        zeromq1 = ZeroMQPubSink(self.config)
        zeromq2 = ZeroMQPubSink(self.config)
        self.save_connect(zeromq1)
        self.assertRaises(ZMQError, zeromq2.connect)
        self.save_close(zeromq1)

    def test_message_send_success(self):
        zeromq = ZeroMQPubSink(self.config)
        self.save_connect(zeromq)
        zeromq.send(TestZeroMQPubSink.test_msg_str)
        self.save_close(zeromq)

    def test_message_send_fail_not_connected(self):
        zeromq = ZeroMQPubSink(self.config)
        self.assertRaises(ConnectionError, zeromq.send, TestZeroMQPubSink.test_msg_str)

    def save_connect(self, socket: ZeroMQPubSink):
        try:
            socket.connect()
        except ConnectionError as e:
            self.fail(f"ZeroMQPubSink.connect() raised ConnectionError unexpectedly while connecting: {e}")

    def save_close(self, socket: ZeroMQPubSink):
        try:
            socket.close()
        except Exception as e:
            self.fail(f"ZeroMQPubSink.close() raised ConnectionError unexpectedly while closing the connection: {e}")


if __name__ == '__main__':
    unittest.main()
