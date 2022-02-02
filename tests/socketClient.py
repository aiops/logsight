import json
import socket

from connectors.sinks import SocketSink
from connectors.sources import SocketSource

send = SocketSink("localhost", 9996)
recv = SocketSource("127.0.0.1", 9997)
recv.connect()
## Send some data, this method can be called multiple times
data = {"message": "END"}
send.send(data)
## Receive up to 4096 bytes from a peer
while True:
    msg = recv.receive_message()
    print(msg)
