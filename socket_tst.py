from connectors.source.socket import SocketSource

source = SocketSource('localhost', 8898)
source.connect()
while source.has_next():
    # print("waiting")
    print(source.receive_message())