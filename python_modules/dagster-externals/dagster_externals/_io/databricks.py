import json
import os

from dagster_externals import (
    ExternalExecutionContextData,
    ExternalExecutionContextSource,
    ExternalExecutionMessage,
    ExternalExecutionMessageSink,
)


class DbfsContextSource(ExternalExecutionContextSource):
    def __init__(self, path: str):
        self._path = os.path.join("/dbfs", path.lstrip("/"))

    def load_context(self) -> ExternalExecutionContextData:
        with open(self._path, "r") as f:
            return json.load(f)


class DbfsMessageSink(ExternalExecutionMessageSink):
    def __init__(self, path: str):
        self._path = os.path.join("/dbfs", path.lstrip("/"))
        self._counter = 1

    def send_message(self, notification: ExternalExecutionMessage) -> None:
        notification_path = os.path.join(self._path, f"{self._counter}.json")
        with open(notification_path, "w") as f:
            f.write(json.dumps(notification) + "\n")
        self._counter += 1
