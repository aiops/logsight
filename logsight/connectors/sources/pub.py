import zmq
import random
import sys
import time

port = "5556"
if len(sys.argv) > 1:
    port =  sys.argv[1]
    int(port)

context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.bind("tcp://127.0.0.1:%s" % port)
count = 0
topic = 0
while True:
    print("%d %d" % (topic, count))
    socket.send_string("%d %d" % (topic, count))