import logging

from elasticsearch import Elasticsearch
from tenacity import retry, stop_after_attempt, wait_fixed

logger = logging.getLogger("logsight." + __name__)


class ElasticSearchAdmin:
    def __init__(self, host, port, username, password, **_kwargs):
        self.client = Elasticsearch([{'host': host, 'port': port}],
                                    http_auth=(username, password))
        self.host = host
        self.port = port
        self.connect()

    @retry(reraise=True, stop=stop_after_attempt(5), wait=wait_fixed(5))
    def connect(self):
        logger.info(f"Verifying elasticsearch connection on {self.host}:{self.port}.")
        if not self.client.ping():
            msg = f"Elasticsearch endpoint {self.host}:{self.port} is unreachable."
            logger.error(msg)
            raise ConnectionError(msg)
        logger.info("Elasticsearch connected.")

    @retry(stop=stop_after_attempt(10), wait=wait_fixed(18))
    def create_indices(self, private_key, app_name):
        app_id = "_".join([private_key, app_name])
        # create ES indices for the user/app
        mapping = {
            "mappings": {
                "properties": {
                    "prediction"        : {
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

        self.client.indices.create(index="_".join([app_id, "log_quality"]), ignore=400)

        self.client.indices.create(index="_".join([app_id, "log_ad"]), body=mapping, ignore=400)
        self.client.index(index="_".join([app_id, "log_ad"]), body=doc)

        self.client.indices.create(index="_".join([app_id, "log_agg"]), body=mapping, ignore=400)
        self.client.index(index="_".join([app_id, "log_agg"]), body=doc)

        mapping = {
            "mappings": {
                "properties": {
                    "prediction"        : {
                        "type": "integer"  # formerly "string"
                    },
                    "prediction.keyword": {
                        "type": "integer"
                    },
                    "timestamp_start"   : {
                        "type": "date"
                    },
                    "timestamp_end"     : {
                        "type": "date"
                    }
                }
            }
        }
        self.client.indices.create(index="_".join([app_id, "count_ad"]), body=mapping, ignore=400)
        self.client.index(index="_".join([app_id, "count_ad"]), body=doc)
        mapping = {
            "mappings": {
                "properties": {
                    "total_score"        : {
                        "type": "double"
                    },
                    "total_score.keyword": {
                        "type": "double"
                    },
                    "timestamp_start"    : {
                        "type": "date"
                    },
                    "timestamp_end"      : {
                        "type": "date"
                    }
                }
            }
        }
        self.client.indices.create(index="_".join([app_id, "incidents"]), ignore=400, body=mapping)
        self.client.index(index="_".join([app_id, "incidents"]), body=doc)

    @retry(stop=stop_after_attempt(5), wait=wait_fixed(5))
    def delete_indices(self, private_key, app_name):
        app_id = str(private_key) + "_" + str(app_name)
        modules = ["log_quality", "log_ad", "count_ad", "incidents", "log_agg"]
        for module in modules:
            try:
                index_name = str(app_id) + "_" + str(module)
                self.client.indices.delete(index_name)
            except Exception as e:
                logger.error(e, f" Could not delete es index for module {module}")
