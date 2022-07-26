import logging

from connectors.connectors.elasticsearch import ElasticsearchConnector, ElasticsearchConfigProperties
from services.elasticsearch_service.queries import DELETE_BY_INGEST_TS_QUERY, DELETE_BY_QUERY, GET_ALL_AD, \
    GET_ALL_LOGS_INGEST, \
    GET_ALL_TEMPLATES

logger = logging.getLogger("logsight." + __name__)


class ElasticsearchService(ElasticsearchConnector):
    def __init__(self, config: ElasticsearchConfigProperties):
        config.ingest_pipeline = None
        super(ElasticsearchService, self).__init__(config)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def get_all_logs_for_index(self, index, start_time, end_time):
        query = self._parse_query(GET_ALL_AD, index, start_time, end_time)
        res = self.es.search(**query, size=10000)
        return [row['_source'] for row in res['hits']['hits']]

    def get_all_logs_after_ingest(self, index, start_time, end_time):
        query = self._parse_query(GET_ALL_LOGS_INGEST, index, start_time=start_time, end_time=end_time)

        res = self.es.search(**query, size=10000)
        return [row['_source'] for row in res['hits']['hits']]

    def get_all_templates_for_index(self, index):
        query = self._parse_query(GET_ALL_TEMPLATES, index)
        res = self.es.search(**query, size=10000)
        return [row['key'] for row in res['aggregations']['aggregations']['buckets']]

    def delete_logs_for_index(self, index, start_time, end_time):
        query = self._parse_query(DELETE_BY_QUERY, index, start_time, end_time)
        self.es.delete_by_query(**query, wait_for_completion=True)

    def delete_by_ingest_timestamp(self, index, start_time, end_time):
        query = self._parse_query(DELETE_BY_INGEST_TS_QUERY, index, start_time, end_time)
        self.es.delete_by_query(**query, wait_for_completion=True)

    def get_all_indices(self, extension):
        return list(self.es.indices.get(index=f"*{extension}").keys())

    def save(self, data, index: str, pipeline: bool = False):
        return self.bulk(data, index, pipeline)

    @staticmethod
    def _parse_query(query, index, start_time=None, end_time=None):
        query = str(query).replace("$index", index)
        if start_time:
            query = query.replace("$start_time", start_time)
        if end_time:
            query = query.replace("$end_time", end_time)
        return eval(query)

    def get_all_logs_for_tag(self, index, ingest_start_time, ingest_end_time, tags):
        query = self._parse_query(GET_ALL_LOGS_INGEST, index, start_time=ingest_start_time, end_time=ingest_end_time)

        filter_query = [{"match_phrase": {f"tags.{tag_key}.keyword": tags[tag_key]}} for tag_key in tags]
        query['body']['query']['bool']['filter'] = filter_query
        
        res = self.es.search(**query, size=10000)
        return [row['_source'] for row in res['hits']['hits']]

    def get_tags_jorge(self, index: str, tags):

        filter_query = []
        for tag_key in tags:
            filter_query.append({"match_phrase": {f"tags.{tag_key}.keyword": tags[tag_key]}})
        res = self.es.search(
            index=index,
            body={
                "size": 10000,
                "query": {
                    "bool": {
                        "must": [],
                        "filter": filter_query,
                        "should": [],
                        "must_not": []
                    }
                }
            }
        )

        return [r['_source'] for r in res['hits']['hits']]
