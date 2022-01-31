import sys
import time

import zmq

port = "5556"
if len(sys.argv) > 1:
    port = sys.argv[1]
    int(port)

if len(sys.argv) > 2:
    port1 = sys.argv[2]
    int(port1)

# Socket to talk to server
context = zmq.Context()
socket = context.socket(zmq.SUB)

print("Collecting updates from weather server...")
socket.connect("tcp://127.0.0.1:%s" % port)

if len(sys.argv) > 2:
    socket.connect("tcp://127.0.0.1:%s" % port1)

# Subscribe to zipcode, default is NYC, 10001
# topicfilter = ""
# socket.setsockopt_string(zmq.SUBSCRIBE, topicfilter)

# Process 5 updates
total_value = 0
while True:
    print("Receiving")
    string = socket.recv()
    topic, messagedata = string.split()
    total_value += int(messagedata)
    print(topic, messagedata)
    time.sleep(0.1)
print("Average message data value for topic '%s' was %dF" % (topicfilter, total_value / update_nbr))