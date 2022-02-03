import zmq

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://0.0.0.0:5555")
socket.send_string('{"test":"test2"}')
