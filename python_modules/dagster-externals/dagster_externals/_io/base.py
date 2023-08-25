from abc import ABC, abstractmethod

from typing_extensions import Self

from .._protocol import (
    ExternalExecutionContextData,
    ExternalExecutionMessage,
)
from .._util import (
    emit_orchestration_inactive_warning,
    get_mock,
    is_dagster_orchestration_active,
    param_from_env,
)


class ExternalExecutionContextSource(ABC):
    @classmethod
    def from_env(cls) -> Self:
        if not is_dagster_orchestration_active():
            emit_orchestration_inactive_warning()
            return get_mock()
        else:
            context_source_params = param_from_env("context_source")
            assert isinstance(context_source_params, dict)
            return cls(**context_source_params)

    @abstractmethod
    def load_context(self) -> ExternalExecutionContextData:
        raise NotImplementedError()


class ExternalExecutionMessageSink(ABC):
    @classmethod
    def from_env(cls) -> Self:
        if not is_dagster_orchestration_active():
            emit_orchestration_inactive_warning()
            return get_mock()
        else:
            message_sink_params = param_from_env("message_sink")
            assert isinstance(message_sink_params, dict)
            return cls(**message_sink_params)

    @abstractmethod
    def send_notification(self, notification: ExternalExecutionMessage) -> None:
        ...
