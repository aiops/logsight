import logging

from elasticsearch import Elasticsearch

logger = logging.getLogger(".logsight")


class ElasticSearchAdmin:
    def __init__(self, host, port, username, password, **_kwargs):
        self.client = Elasticsearch([{'host': host, 'port': port}],
                                    http_auth=(username, password))

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

        self.client.indices.create(index="_".join([app_id, "log_quality"]), ignore=400)

        self.client.indices.create(index="_".join([app_id, "log_ad"]), body=mapping, ignore=400)
        self.client.index(index="_".join([app_id, "log_ad"]), body=doc)

        self.client.indices.create(index="_".join([app_id, "log_agg"]), body=mapping, ignore=400)
        self.client.index(index="_".join([app_id, "log_agg"]), body=doc)

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
        self.client.indices.create(index="_".join([app_id, "count_ad"]), body=mapping, ignore=400)
        self.client.index(index="_".join([app_id, "count_ad"]), body=doc)
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
        self.client.indices.create(index="_".join([app_id, "incidents"]), ignore=400, body=mapping)
        self.client.index(index="_".join([app_id, "incidents"]), body=doc)

    def delete_indices(self, private_key, app_name):
        app_id = "_".join([private_key, app_name])
        modules = ["log_quality", "log_ad", "count_ad", "incidents", "log_agg"]
        for module in modules:

            try:
                self.client.indices.delete("_".join([app_id, module]))
            except Exception as e:
                logger.error(e, f" Could not delete es index for module {module}")
