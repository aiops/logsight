import logging

from elasticsearch import helpers

from connectors.base.elasticsearch import ElasticsearchConnector
from services.elasticsearch.queries import DELETE_BY_QUERY, GET_ALL_AD, GET_ALL_LOGS_INGEST, GET_ALL_TEMPLATES

logger = logging.getLogger("logsight." + __name__)


class ElasticsearchService(ElasticsearchConnector):
    def __init__(self, scheme, host, port, username, password, **_kwargs):
        super(ElasticsearchService, self).__init__(scheme, host, port, username, password)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def get_all_logs_for_index(self, index, start_time, end_time):
        query = self._parse_query(GET_ALL_AD, index, start_time, end_time)
        res = self.es.search(**query, size=10000)
        return [row['_source'] for row in res['hits']['hits']]

    def get_all_logs_after_ingest(self, index, ingest_time):
        query = self._parse_query(GET_ALL_LOGS_INGEST, index, start_date=ingest_time)
        res = self.es.search(**query, size=10000)
        return [row['_source'] for row in res['hits']['hits']]

    def get_all_templates_for_index(self, index):
        query = self._parse_query(GET_ALL_TEMPLATES, index)
        res = self.es.search(**query, size=10000)
        return [row['key'] for row in res['aggregations']['aggregations']['buckets']]

    def delete_logs_for_index(self, index, start_time, end_time):
        query = self._parse_query(DELETE_BY_QUERY, index, start_time, end_time)
        self.es.delete_by_query(**query)

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

    @staticmethod
    def _parse_query(query, index, start_date=None, end_date=None):
        query = str(query).replace("$index", index)
        if start_date:
            query = query.replace("$start_time", start_date)
        if end_date:
            query = query.replace("$end_time", end_date)
        return eval(query)
