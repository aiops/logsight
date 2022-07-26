import pandas as pd

from connectors.connectors.elasticsearch import ElasticsearchConfigProperties
from services import ElasticsearchService

if __name__ == '__main__':
    config = ElasticsearchConfigProperties(host="localhost")
    es = ElasticsearchService(config)
    es.connect()
    logs = []
    flag = True
    start = "now-2y"
    end = "now"
    fetch = es.get_tags_jorge("dwwd5tuvy6aqerrjkmr4pe635c_pipeline",
                              {'service': 'demo_hdfs_node', 'version': 'v1.1.0'})
    logs.extend(fetch)
    start = logs[-1]['ingest_timestamp']

    df = pd.DataFrame(logs).set_index('timestamp')
    df.index = pd.to_datetime(df.index)

    logs = []
    fetch = es.get_tags_jorge("dwwd5tuvy6aqerrjkmr4pe635c_pipeline",
                              {'service': 'demo_hdfs_node', 'version': 'v1.0.0'})
    logs.extend(fetch)
    start = logs[-1]['ingest_timestamp']
    if len(fetch) < 2:
        flag = False
    df2 = pd.DataFrame(logs).set_index('timestamp')
    df2.index = pd.to_datetime(df2.index)
    df.to_csv("v1.1.0.csv")
    df2.to_csv("v1.0.0.csv")
    print(df)
