# import json
# import time
# import unittest
#
# from connectors.base.zeromq import ConnectionTypes
# from connectors.sinks.zeromq import ZeroMQPubSink
# from connectors.sources import ZeroMQSubSource
#
#
# class TestZeroMQSubSource(unittest.TestCase):
#     endpoint = "tcp://0.0.0.0:4444"
#     topic = "test_topic_test"
#     test_pub = ZeroMQPubSink(endpoint, topic, connection_type=ConnectionTypes.BIND)
#     test_msg = {"message": "hello"}
#     test_msg_str = json.dumps(test_msg)
#
#     def setUp(self) -> None:
#         super().setUp()
#         TestZeroMQSubSource.test_pub.connect()
#
#     def tearDown(self) -> None:
#         super().tearDown()
#         TestZeroMQSubSource.test_pub.close()
#
#     def test_socket_connect_and_close_success(self):
#         zeromq = ZeroMQSubSource(endpoint=TestZeroMQSubSource.endpoint, topic="")
#         self.save_connect(zeromq)
#         self.save_close(zeromq)
#
#     def test_close_not_connected_success(self):
#         zeromq = ZeroMQSubSource(endpoint=TestZeroMQSubSource.endpoint, topic="")
#         self.save_close(zeromq)
#
#     def test_message_receive_success(self):
#         zeromq = ZeroMQSubSource(endpoint=TestZeroMQSubSource.endpoint, topic="")
#         self.save_connect(zeromq)
#         time.sleep(2)
#         TestZeroMQSubSource.test_pub.send(TestZeroMQSubSource.test_msg_str)
#         msg = zeromq.receive_message()
#         self.assertEqual(msg, TestZeroMQSubSource.test_msg)
#         self.save_close(zeromq)
#
#     def test_message_send_fail_not_connected(self):
#         zeromq = ZeroMQSubSource(endpoint=TestZeroMQSubSource.endpoint, topic="")
#         self.assertRaises(ConnectionError, zeromq.receive_message)
#
#     def save_connect(self, socket: ZeroMQSubSource):
#         try:
#             socket.connect()
#         except ConnectionError as e:
#             self.fail(f"ZeroMQPubSink.connect() raised ConnectionError unexpectedly while connecting: {e}")
#
#     def save_close(self, socket: ZeroMQSubSource):
#         try:
#             socket.close()
#         except Exception as e:
#             self.fail(f"ZeroMQPubSink.close() raised ConnectionError unexpectedly while closing the connection: {e}")
#
#
# if __name__ == '__main__':
#     unittest.main()
