
class Template:

    def __init__(self, data):
        """Class representing log templates.

        Note:
            Timestamps are represented in ISO format with timezone information.
            e.g, 2021-10-07T13:18:09.178477+02:00.

        """
        self._timestamp = data.get("@timestamp", None)
        self._actual_level = data.get("actual_level", None)
        self._app_name = data.get("app_name", None)
        self._message = data.get("message", None)
        self._name = data.get("name", None)
        self._params = data.get("params", None)
        self._template = data.get("template", None)

    def __repr__(self):
        return {"app_name": self._app_name, "template": self._template}

    @property
    def timestamp(self):
        """str: Timestamp when the log message was generated."""
        return self._timestamp

    @property
    def actual_level(self):
        """str: Log level of the message (e.g., WARNING)."""
        return self._actual_level

    @property
    def app_name(self):
        """str: Application name."""
        return self._app_name

    @property
    def message(self):
        """str: Log message."""
        return self._message

    @property
    def name(self):
        """str: Name."""
        return self._name

    @property
    def template(self):
        """str: Template generated from log message.

        Examples:
            nova.virt.libvirt.imagecache <*> ] <*> base <*> <*>

        """
        return self._template

    @property
    def params(self):
        """(:obj:`list` of :obj:`str`): Parameters extracted from log message.

        Examples:
            "param_0":"[req-addc1839-2ed5-4778-b57e-5854eb7b8b09"

            "param_1":"Unknown"

            "param_2":"file:"

            "param_3":"/var/lib/nova/instances/_base/a489c868..."

        """
        return self._params
