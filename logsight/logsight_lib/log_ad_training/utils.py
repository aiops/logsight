import getopt
import sys
from datetime import time
from json import loads

from elasticsearch import Elasticsearch
from kafka import KafkaConsumer, KafkaProducer

THRESHOLD = 0.9
WINDOW_SIZE_IN_MINUTES = 1
GET_PARSING_QUERY_STRING = {"query": {
    "bool": {
        "must": [
            {
                "range": {
                    "@timestamp": {
                        "format": "strict_date_optional_time",
                        "gte": "now-7d",
                        "lte": "now"
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


def get_parsing_query_string_tag(tag):
    return {"query": {
        "bool": {
            "must": [
                {
                    "match_phrase": {
                        "tag.keyword": {
                            "query": str(tag)
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


def get_settings(argv):
    try:
        opts, args = getopt.getopt(argv, "hi:o:",
                                   ["time-logsy=", "time-count=", "elasticsearch=", "kafka=", "private-key=",
                                    "application-name="])
    except getopt.GetoptError:
        print('kafka_consumer.py -t1 <time-logsy> -t2 <time-count> -es <elasticsearch> -k <kafka> -pk <private-key> '
              '-an <application-name>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('kafka_consumer.py -t1 <time-logsy> -t2 <time-count> -es <elasticsearch> -k <kafka> -pk '
                  '<private-key> -an <application-name>')
            sys.exit()
        elif opt in ("-pk", "--private-key"):
            private_key = str(arg)
        elif opt in ("-an", "--application-name"):
            application_name = str(arg)
        elif opt in ("-t1", "--time-logsy"):
            time_logsy = arg
        elif opt in ("-t2", "--time-count"):
            time_count = arg
        elif opt in ("-es", "--elasticsearch"):
            elasticsearch_url = arg
        elif opt in ("-k", "--kafka"):
            kafka_url = arg
    return time_logsy, time_count, private_key, application_name, elasticsearch_url, kafka_url


def initialize_kafka_consumer(topic_name, kafka_url):
    backoff_time = 5
    while True:
        try:
            consumer = KafkaConsumer(topic_name,
                                     bootstrap_servers=[kafka_url + ':9092'],
                                     auto_offset_reset='earliest',
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


def initialize_kafka_producer(kafka_url):
    while True:
        try:
            producer = KafkaProducer(bootstrap_servers=kafka_url + ':9092')
            return producer
        except Exception as e:
            print(e)
            time.sleep(5)
            continue
