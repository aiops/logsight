import json
import socket
import sys

HOST, PORT = "localhost", 9990

mode = " ".join(sys.argv[1:])
if mode == "train":
    PORT = 9990
    data = {"type": "load"}

if mode == "init":
    PORT = 9999
    data = {'app_id': "app_id",
            'private_key': 'sample_key', 'user_name': 'sample_user', 'application_name': 'sample_app',
            'status': "create"}

# Create a socket (SOCK_STREAM means a TCP socket)
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    # Connect to server and send data
    sock.connect((HOST, PORT))

    sock.sendall(bytes(json.dumps(data) + "\n", "utf-8"))

    # Receive data from the server and shut down
    received = str(sock.recv(1024), "utf-8")

print("Sent:     {}".format(data))
print("Received: {}".format(received))
