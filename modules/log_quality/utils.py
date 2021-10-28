import getopt
import sys
import time
from json import loads

from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import scan
from kafka import KafkaConsumer, KafkaProducer


def get_settings(argv):
    try:
        opts, args = getopt.getopt(argv, "hi:o:",
                                   ["elasticsearch=", "kafka="])
    except getopt.GetoptError:
        print('manager.py -es <elasticsearch> -k <kafka> -d <deployment>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('manager.py -es <elasticsearch> -k <kafka> -d <deployment>')
            sys.exit()
        elif opt in ("-es", "--elasticsearch"):
            elasticsearch_url = arg
        elif opt in ("-k", "--kafka"):
            kafka_url = arg
    return elasticsearch_url, kafka_url


def get_logs(es, es_index, start_time='now-24h', end_time='now'):
    res = scan(
        es,
        index=es_index,
        doc_type='_doc',
        query={"query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "@timestamp": {
                                "format": "strict_date_optional_time",
                                "gte": start_time,  # should be set dynamically
                                "lte": end_time
                            }
                        }
                    }
                ],
                "filter": [
                    {
                        "match_all": {}
                    }
                ],
                "should": [],
                "must_not": []
            }
        }}
    )
    result = []
    for item in res:
        try:
            result.append(item)
        except Exception as e:
            print(e)
    return result


def initialize_elastic_client(elasticsearch_url):
    while True:
        try:
            elastic_client = Elasticsearch([{'host': elasticsearch_url, 'port': 9200}],
                                           http_auth=('elastic', 'elasticsearchpassword'))
            return elastic_client
        except Exception as e:
            print(e)
            time.sleep(5)
            continue


def initialize_kafka_consumer(topic_name, kafka_url):
    backoff_time = 5
    while True:
        try:
            consumer = KafkaConsumer(topic_name,
                                     bootstrap_servers=[kafka_url + ':9092'],
                                     auto_offset_reset='latest',
                                     group_id=None,
                                     api_version=(2, 0, 2),
                                     enable_auto_commit=True,
                                     value_deserializer=lambda x: loads(x.decode('utf-8'))
                                     )
            return consumer
        except Exception as e:
            print("Exception while trying to connect to kafka:", e)
            time.sleep(backoff_time)
            continue


def initialize_kafka_producer(kafka_url):
    while True:
        try:
            producer = KafkaProducer(bootstrap_servers=kafka_url + ':9092')
            return producer
        except Exception as e:
            print(e)
            time.sleep(5)
            continue


def write_to_elasticsearch(processed_log, elastic_client, es_index):
    elastic_client.delete_by_query(index=es_index, body={"query": {"match_all": {}}})
    helpers.bulk(elastic_client,
                 processed_log,
                 index=es_index,
                 doc_type='_doc',
                 request_timeout=200)
