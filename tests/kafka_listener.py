from connectors.source import KafkaSource

source = KafkaSource(address='localhost:9093', topic='internal', offset='latest')
while True:
    print(source.receive_message())
