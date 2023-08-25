import atexit
import json

from .._protocol import ExternalExecutionContextData, ExternalExecutionMessage
from .base import ExternalExecutionContextSource, ExternalExecutionMessageSink


class ExternalExecutionFileContextSource(ExternalExecutionContextSource):
    def __init__(self, path: str):
        self._path = path

    def load_context(self) -> ExternalExecutionContextData:
        with open(self._path, "r") as f:
            return json.load(f)


class ExternalExecutionFileMessageSink(ExternalExecutionMessageSink):
    def __init__(self, path: str):
        self._output_stream = open(path, "a")
        atexit.register(self._close_stream)

    def send_notification(self, notification: ExternalExecutionMessage) -> None:
        self._output_stream.write(json.dumps(notification) + "\n")
        self._output_stream.flush()

    def _close_stream(self) -> None:
        self._output_stream.close()
