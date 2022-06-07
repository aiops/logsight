import logging

from elasticsearch import helpers

from connectors.base.elasticsearch import ElasticsearchConnector
from services.elasticsearch.queries import GET_ALL_AD, GET_ALL_TEMPLATES

logger = logging.getLogger("logsight." + __name__)


class ElasticsearchService(ElasticsearchConnector):
    def __init__(self, host, port, username, password, **_kwargs):
        super(ElasticsearchService, self).__init__(host, port, username, password)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def get_all_logs_for_index(self, index, start_time, end_time):
        query = GET_ALL_AD
        query = eval(
            str(query).replace("$index", index).replace("$start_time", start_time).replace("$end_time", end_time))
        res = self.es.search(**query, size=10000)
        return [row['_source'] for row in res['hits']['hits']]

    def get_all_templates_for_index(self, index, end_time):
        query = GET_ALL_TEMPLATES
        query = eval(
            str(query).replace("$index", index).replace("$end_time", end_time))
        res = self.es.search(**query, size=10000)
        return [row['key'] for row in res['aggregations']['aggregations']['buckets']]

    def get_all_indices(self, extension):
        return list(self.es.indices.get(f"*{extension}").keys())

    def save(self, data, index: str):
        if not isinstance(data, list):
            data = [data]
        try:
            helpers.bulk(self.es,
                         data,
                         index=index,
                         request_timeout=200)
        except Exception as e:
            logger.warning(f"Failed to send data to elasticsearch. Reason: {e}. Retrying...")
            raise e