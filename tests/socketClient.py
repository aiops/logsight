from time import sleep

import zmq

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://0.0.0.0:5555")
socket.send_string(
    '{"id": "60f299ae-ae46-458c-b1d2-1aff53a84a9d", "name": "myapp", "userKey": "myprivatekey", "action": "CREATE"}')
print(socket.recv())
