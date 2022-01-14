from logsight.config import HOST_API_V1, PATH_RESULTS
from logsight.api_client import APIClient
from logsight.result.result_seq import ResultSeq
from logsight.result.template import Template
from logsight.result.incident import Incident
from logsight.result.quality import Quality


def _object_factory(anomaly_type, data):

    mapping = {
        "log_ad": Template,
        "incidents": Incident,
        "log_quality": Quality,
    }

    try:
        klass = mapping[anomaly_type.lower()]
    except KeyError as e:
        raise RuntimeError(f"No class found: {e}")
    except Exception as e:
        raise RuntimeError(f"Unknown error: {e}")

    return ResultSeq(data, klass)


class LogsightResult(APIClient):
    def __init__(self, private_key, email, app_name):
        """Class representing results returned from the server.

        Note:
            Timestamps are represented in ISO format with timezone information.
            e.g, 2021-10-07T13:18:09.178477+02:00.

        """
        super().__init__()
        self.private_key = private_key
        self.email = email
        self.app_name = app_name

    def get_results(self, start_time, end_time, anomaly_type):
        """Obtains the results from processing.

        Args:
            start_time (str): Timestamp of the start time of the interval requested.
            end_time (str): Timestamp of end time of the interval requested.
            anomaly_type (str): One of: incidents, log_quality, log_ad

        Returns:
            Union[Template, Incident, Quality]: Object that encapsulates the response.

        Raises:
            Unauthorized: If the private_key is invalid.
            BadRequest: If timestamp format or anomaly_type is incorrect.

        """
        data = {
            "private-key": self.private_key,
            "email": self.email,
            "app": self.app_name,
            "start-time": start_time,
            "end-time": end_time,
            "anomaly-type": anomaly_type,
        }
        return _object_factory(anomaly_type,
                               self._post(HOST_API_V1, PATH_RESULTS, data))
