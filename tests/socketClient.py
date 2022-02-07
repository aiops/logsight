from time import sleep

import zmq

context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.bind("tcp://0.0.0.0:5559")
socket.send_string('doyhdffqxt8uv2pgb5vz9rsqbnq_myservice_input {"test":"test2"}')
sleep(15)