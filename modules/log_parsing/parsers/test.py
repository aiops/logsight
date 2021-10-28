class TestParser:
    @staticmethod
    def parse(message):
        if isinstance(message,list):
            message = " ".join(message)
        return "Parsed " + str(message)
