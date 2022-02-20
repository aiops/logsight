import sys
import time

import zmq

port = "4444"

# Socket to talk to server
context = zmq.Context()
socket = context.socket(zmq.SUB)

print("Collecting updates from weather server...")
socket.bind("tcp://0.0.0.0:%s" % port)

topic_filter = "test_topic_test"
socket.subscribe(topic_filter)
# Process 5 updates
print("Receiving")
for i in range(1):
    string = socket.recv()
    print(string)
    time.sleep(2)
