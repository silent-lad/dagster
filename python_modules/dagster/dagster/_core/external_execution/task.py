import json
from abc import abstractmethod
from typing import (
    Any,
    ContextManager,
    Generic,
    Mapping,
    Optional,
)

from dagster_externals import (
    DAGSTER_EXTERNALS_ENV_KEYS,
    ExternalExecutionContextData,
    ExternalExecutionExtras,
)
from typing_extensions import TypeVar

import dagster._check as check
from dagster import OpExecutionContext
from dagster._core.definitions.data_version import DataVersion
from dagster._core.definitions.events import AssetKey
from dagster._core.external_execution.context import build_external_execution_context

T_TaskParams = TypeVar("T_TaskParams")
T_TaskIOParams = TypeVar("T_TaskIOParams")


class ExternalExecutionTask(Generic[T_TaskParams, T_TaskIOParams]):
    def __init__(
        self,
        context: OpExecutionContext,
        extras: Optional[ExternalExecutionExtras],
    ):
        self._context = context
        self._extras = extras

    def run(self, params: T_TaskParams) -> None:
        with self._setup_io(params) as io_params:
            self._launch(self.get_base_env(), params, io_params)

    def get_base_env(self) -> Mapping[str, str]:
        return {DAGSTER_EXTERNALS_ENV_KEYS["is_orchestration_active"]: json.dumps(True)}

    def get_external_context(self) -> ExternalExecutionContextData:
        return build_external_execution_context(self._context, self._extras)

    # ########################
    # ##### PLUG POINTS
    # ########################

    @abstractmethod
    def _setup_io(self, params: T_TaskParams) -> ContextManager[T_TaskIOParams]:
        ...

    @abstractmethod
    def _launch(
        self,
        base_env: Mapping[str, str],
        params: T_TaskParams,
        io_params: T_TaskIOParams,
    ) -> None:
        ...

    # ########################
    # ##### HANDLE NOTIFICATIONS
    # ########################

    def handle_message(self, message: Mapping[str, Any]) -> None:
        if message["method"] == "report_asset_metadata":
            self._handle_report_asset_metadata(**message["params"])
        elif message["method"] == "report_asset_data_version":
            self._handle_report_asset_data_version(**message["params"])
        elif message["method"] == "log":
            self._handle_log(**message["params"])

    def _handle_report_asset_metadata(self, asset_key: str, label: str, value: Any) -> None:
        key = AssetKey.from_user_string(asset_key)
        output_name = self._context.output_for_asset_key(key)
        self._context.add_output_metadata({label: value}, output_name)

    def _handle_report_asset_data_version(self, asset_key: str, data_version: str) -> None:
        key = AssetKey.from_user_string(asset_key)
        self._context.set_data_version(key, DataVersion(data_version))

    def _handle_log(self, message: str, level: str = "info") -> None:
        check.str_param(message, "message")
        self._context.log.log(level, message)
