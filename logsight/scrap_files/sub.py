import sys
import time

import zmq

port = "5556"

# Socket to talk to server
context = zmq.Context()
socket = context.socket(zmq.SUB)

print("Collecting updates from weather server...")
socket.connect("tcp://localhost:%s" % port)

topic_filter = b"test"
socket.subscribe(topic_filter)
# Process 5 updates
while True:
    print("Receiving")
    string = socket.recv()
    print(string)
    time.sleep(3)
