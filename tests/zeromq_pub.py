import http
import json

import zmq
import time

port = "5558"
context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.connect("tcp://0.0.0.0:%s" % port)
time.sleep(1)
msg_obj = {"id": "123", "orderCounter": 1, "logsCount": 10, "operation": "FLUSH"}
msg = json.dumps(msg_obj)
topic1 = "result_init"
i = 0
print("%d: Sending msg '%s' to topics [%s]" % (i, msg, ",".join([topic1])))
socket.send(b"%s %s" % (topic1.encode('utf8'), msg.encode('utf8')))
i += 1
time.sleep(3)
msg_obj = {"id": "123e4567-e89b-12d3-a456-426614174000", "orderCounter": 1, "logsCount": 10, "currentLogsCount": 10,
           "description": "test", "status": http.HTTPStatus(http.HTTPStatus.OK).phrase}
msg = json.dumps(msg_obj)
socket.send(b"%s %s" % (topic1.encode('utf8'), msg.encode('utf8')))
time.sleep(1)

