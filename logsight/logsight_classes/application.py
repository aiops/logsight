class Application:
    def __init__(self, handlers, application_id, private_key, application_name, input_module, services=None,
                 topic_list=None, **kwargs):
        self.application_id = str(application_id)
        self.application_name = application_name
        self.private_key = private_key
        self.services = services or []
        self.input_module = input_module
        self.handlers = handlers
        self.start_module = input_module
        self.topic_list = topic_list or []

    def start(self):
        self.start_module.start()

    def __repr__(self):
        return "-".join([self.application_id, self.application_name])

    def to_json(self):
        return {
            "application_id": self.application_id,
            "application_name": self.application_name,
            "input": self.input_module.to_json()
        }