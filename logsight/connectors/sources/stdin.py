from logsight.connectors.base.source import Source


class StdinSource(Source):
    def receive_message(self) -> str:
        txt = input("[SOURCE] Enter message: ")
        return str(txt)
