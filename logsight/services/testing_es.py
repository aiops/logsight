from services import ElasticsearchService

es = ElasticsearchService(host='localhost', port='9200', username="elastic", password="elasticsearchpassword")

print(es.get_all_logs_for_index("*_log_ad", "2020-03-23T01:02:51", "2022-03-23T01:02:51"))
