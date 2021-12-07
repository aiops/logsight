class Pipeline:
    def __init__(self, handlers, start):
        self.handlers = handlers
        self.start_module = start

    def start(self):
        for handler in self.handlers.values():
            handler.start()

    def process(self, request):
        return self.handlers[self.start_module].handle(request)
