import logging

from tenacity import retry, stop_after_attempt, wait_fixed

from connectors.base.elasticsearch import ElasticsearchConnector
from services.elasticsearch.queries import GET_ALL_AD

logger = logging.getLogger("logsight." + __name__)


class ElasticsearchService(ElasticsearchConnector):
    def __init__(self, host, port, username, password, **_kwargs):
        super(ElasticsearchService, self).__init__(host, port, username, password)
        self.connect()

    def get_all_logs_for_index(self, index, start_time, end_time):
        query = GET_ALL_AD
        query = eval(
            str(query).replace("$index", index).replace("$start_time", start_time).replace("$end_time", end_time))
        res = self.es.search(**query)
        return res['hits']['hits']

    def get_all_indices(self):
        return list(self.es.indices.get("*log_ad").keys())

    @retry(stop=stop_after_attempt(10), wait=wait_fixed(18))
    def create_indices(self, private_key, app_name):
        app_id = "_".join([private_key, app_name])
        # create ES indices for the user/app
        mapping = {
            "mappings": {
                "properties": {
                    "prediction": {
                        "type": "integer"  # formerly "string"
                    },
                    "prediction.keyword": {
                        "type": "integer"
                    }
                }
            }
        }

        doc = {
            'message': 'Hello!',
        }

        self.es.indices.create(index="_".join([app_id, "log_quality"]), ignore=400)

        self.es.indices.create(index="_".join([app_id, "log_ad"]), body=mapping, ignore=400)
        self.es.index(index="_".join([app_id, "log_ad"]), body=doc)

        self.es.indices.create(index="_".join([app_id, "log_agg"]), body=mapping, ignore=400)
        self.es.index(index="_".join([app_id, "log_agg"]), body=doc)

        mapping = {
            "mappings": {
                "properties": {
                    "prediction": {
                        "type": "integer"  # formerly "string"
                    },
                    "prediction.keyword": {
                        "type": "integer"
                    },
                    "timestamp_start": {
                        "type": "date"
                    },
                    "timestamp_end": {
                        "type": "date"
                    }
                }
            }
        }
        self.es.indices.create(index="_".join([app_id, "count_ad"]), body=mapping, ignore=400)
        self.es.index(index="_".join([app_id, "count_ad"]), body=doc)
        mapping = {
            "mappings": {
                "properties": {
                    "total_score": {
                        "type": "double"
                    },
                    "total_score.keyword": {
                        "type": "double"
                    },
                    "timestamp_start": {
                        "type": "date"
                    },
                    "timestamp_end": {
                        "type": "date"
                    }
                }
            }
        }
        self.es.indices.create(index="_".join([app_id, "incidents"]), ignore=400, body=mapping)
        self.es.index(index="_".join([app_id, "incidents"]), body=doc)

    @retry(stop=stop_after_attempt(5), wait=wait_fixed(5))
    def delete_indices(self, private_key, app_name):
        app_id = str(private_key) + "_" + str(app_name)
        modules = ["log_quality", "log_ad", "count_ad", "incidents", "log_agg"]
        for module in modules:
            try:
                index_name = str(app_id) + "_" + str(module)
                self.es.indices.delete(index_name)
            except Exception as e:
                logger.error(e, f" Could not delete es index for module {module}")
