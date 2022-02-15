import json

import zmq
import time

port = "5557"
context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.bind("tcp://0.0.0.0:%s" % port)
time.sleep(1)
msg_obj = {"id": "123", "orderCounter": 1, "logsCount": 10, "operation": "FLUSH"}
msg = json.dumps(msg_obj)
topic1 = "vpeytpjimudcchylcmok1rf1vhg_container1_ctrl"
i = 0
print("%d: Sending msg '%s' to topics [%s]" % (i, msg, ",".join([topic1])))
socket.send_string("%s %s" % (topic1, msg))
i += 1
time.sleep(1)
