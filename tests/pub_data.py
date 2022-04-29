import json
import time

import zmq

port = "5559"
context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.bind("tcp://0.0.0.0:%s" % port)
time.sleep(1)

data = {
    "id"          : "test_app1",
    "tag"         : "default",
    "orderCounter": 1,
    "message"     : {
        "timestamp": "2022-04-26T11:58:46.321+02:00",
        "message"  : "Hello World!",
        "level"    : "INFO"
    }
}
topic = "private_key_app_name_input"
socket.send(b"%s %s" % (topic.encode('utf8'), json.dumps(data).encode('utf8')))
socket.send(b"%s %s" % (topic.encode('utf8'), json.dumps(data).encode('utf8')))

socket.send(b"%s %s" % (topic.encode('utf8'), json.dumps(data).encode('utf8')))

print("sent message on ", port, "on topic", topic)
time.sleep(10)
