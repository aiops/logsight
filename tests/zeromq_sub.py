import sys
import time

import zmq

port = "5556"

# Socket to talk to server
context = zmq.Context()
socket = context.socket(zmq.SUB)

print("Collecting updates from weather server...")
socket.connect("tcp://0.0.0.0:%s" % port)

topic_filter = "vpeytpjimudcchylcmok1rf1vhg_container1_ctrl"
socket.subscribe(topic_filter)
# Process 5 updates
print("Receiving")
string = socket.recv()
print(string)
time.sleep(3)
