from time import sleep

import zmq

context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.bind("tcp://0.0.0.0:5556")
socket.send_string('{"id": "60f299ae-ae46-458c-b1d2-1aff53a84a9d", "application_name": "myapp", "private_key": "myprivatekey", "action": "CREATE"}')
sleep(15)