from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import scan
import datetime


class ElasticsearchDataSource:

    def __init__(self, host, port, username, password):
        print(f"Connecting to {host}:{port} with {username}:{password}")
        self.es = Elasticsearch(
            [{'host': host, 'port': port}],
            http_auth=(username, password),
            timeout=30,
            max_retries=5,
            retry_on_timeout=True
        )

    def get_log_ad_data(self, private_key: str, app: str, tag: str):
        index = f"{private_key}_{app}_log_ad"
        res = scan(
            self.es,
            index=index,
            doc_type='_doc',
            query={
                "query": {
                    "match": {"tag": tag}
                }
            }
        )

        return [r['_source'] for r in res]


def create_dummy_data(es, index, app, tag, start_time, amount=2500, recreate_index=False):
    mapping = {
        "mappings": {
            "properties": {
                "prediction": {
                    "type": "integer"  # formerly "string"
                },
            }
        }
    }
    d = {
        'message': 'Hello!',
    }

    if recreate_index:
        es.indices.delete(index=index, ignore=[400, 404])

    time = start_time
    print(f"start time: {time.strftime('%Y-%m-%dT%H:%M:%S.%f')}")
    processed_log = []
    for _ in range(amount):
        time = time + datetime.timedelta(0, 1)
        d = {
            "@timestamp": time.strftime('%Y-%m-%dT%H:%M:%S.%f'),
            "actual_level": "INFO",
            "app_name": app,
            "message": "org.apache.hadoop.hdfs.server.namenode.NNStorageRetentionManager: Going to retain 2 images with txid >= 0",
            "tag": tag,
            "template": "org.apache.hadoop.hdfs.server.namenode.NNStorageRetentionManager: Going to retain <*> images with txid > = <*>",
            "param_0": "2",
            "param_1": "0",
            "prediction": 0
        }
        processed_log.append(d)

    es.indices.create(index=index, body=mapping, ignore=400)
    es.index(index=index, body=d)
    success, failed = helpers.bulk(es, processed_log, index=index, doc_type='_doc', request_timeout=200)
    print(f"Success: {success}, failed: {failed}")
    return time + datetime.timedelta(0, 1)


if __name__ == '__main__':
    host = 'localhost'
    port = 9200
    username = 'elastic'
    password = 'elasticsearchpassword'

    private_key = 'test123'
    app = 'testapp'

    tag1 = "1.0"
    tag2 = "1.1"

    es = Elasticsearch(
        [{'host': host, 'port': port}],
        http_auth=(username, password),
        timeout=30,
        max_retries=5,
        retry_on_timeout=True
    )

    index = f"{private_key}_{app}_log_ad"
    #t = create_dummy_data(es, index, app, tag1, datetime.datetime.now(), 4000, True)
    #create_dummy_data(es, index, app, tag2, t, 7000)

    es_ds = ElasticsearchDataSource(host, port, username, password)
    res1 = es_ds.get_log_ad_data(private_key, app, tag1)
    res2 = es_ds.get_log_ad_data(private_key, app, tag2)

    print(len(res1))
    print(len(res2))
