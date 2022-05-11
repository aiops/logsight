import zmq
import time

port = "5556"
context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.bind("tcp://localhost:%s" % port)
msg = "Hi, I'm here!"
topic1 = "test_topic1"
topic2 = "test_topic2"
while True:
    print("Sending msg '%s' to topics [%s]" % (msg, ",".join([topic1, topic2])))
    socket.send(b"%s %s" % (topic1.encode(), msg.encode()))
    socket.send(b"%s %s" % (topic2.encode(), msg.encode()))
    time.sleep(1)
