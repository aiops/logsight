import json
import sys

from kafka import KafkaProducer

from connectors.sink import KafkaSink

# sink = KafkaSink(address='localhost:9093', topic='manager_settings')
kafka_broker_ip = 'localhost:9093'

mode = " ".join(sys.argv[1:])
if mode == "train":
    data = {"type": "load"}
    topic = 'app_id_anomaly_detection_internal'
else:
    data = {'application_id': "app_id",
            'private_key': 'sample_key', 'user_name': 'sample_user', 'application_name': 'sample_app',
            'status': "create"}
    topic = 'manager_settings'


sink = KafkaProducer(bootstrap_servers=kafka_broker_ip)
print("Sending data", topic, data)

sink.send(topic=topic, value=json.dumps(data).encode('utf-8'))
sink.close()
# sink.send(data)
