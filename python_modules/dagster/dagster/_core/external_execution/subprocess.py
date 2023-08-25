import os
import tempfile
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field
from subprocess import Popen
from typing import Iterator, Mapping, Optional, Sequence, Union

from dagster_externals import (
    ExternalExecutionExtras,
)
from pydantic import Field

from dagster._core.errors import DagsterExternalExecutionError
from dagster._core.execution.context.compute import OpExecutionContext
from dagster._core.external_execution.resource import ExternalExecutionResource
from dagster._core.external_execution.task import (
    ExternalExecutionTask,
)
from dagster._core.external_execution.utils import (
    file_context_source,
    file_message_sink,
)


@dataclass
class SubprocessTaskParams:
    command: Sequence[str]
    cwd: Optional[str] = None
    env: Mapping[str, str] = field(default_factory=dict)


@dataclass
class SubprocessTaskIOParams:
    env: Mapping[str, str] = field(default_factory=dict)


_CONTEXT_SOURCE_FILENAME = "context"
_MESSAGE_SINK_FILENAME = "messages"


class SubprocessExecutionTask(ExternalExecutionTask[SubprocessTaskParams, SubprocessTaskIOParams]):
    def _launch(
        self,
        base_env: Mapping[str, str],
        params: SubprocessTaskParams,
        io_params: SubprocessTaskIOParams,
    ) -> None:
        process = Popen(
            params.command,
            cwd=params.cwd,
            env={
                **base_env,
                **params.env,
                **io_params.env,
            },
        )
        process.wait()

        if process.returncode != 0:
            raise DagsterExternalExecutionError(
                f"External execution process failed with code {process.returncode}"
            )

    # ########################
    # ##### IO
    # ########################

    @contextmanager
    def _setup_io(self, params: SubprocessTaskParams) -> Iterator[SubprocessTaskIOParams]:
        with ExitStack() as stack:
            tempdir = stack.enter_context(tempfile.TemporaryDirectory())
            context_env = stack.enter_context(
                file_context_source(self, os.path.join(tempdir, _CONTEXT_SOURCE_FILENAME))
            )
            message_env = stack.enter_context(
                file_message_sink(self, os.path.join(tempdir, _MESSAGE_SINK_FILENAME))
            )
            yield SubprocessTaskIOParams(env={**context_env, **message_env})


class SubprocessExecutionResource(ExternalExecutionResource):
    cwd: Optional[str] = Field(
        default=None, description="Working directory in which to launch the subprocess command."
    )
    env: Optional[Mapping[str, str]] = Field(
        default=None,
        description="An optional dict of environment variables to pass to the subprocess.",
    )

    def run(
        self,
        command: Union[str, Sequence[str]],
        *,
        context: OpExecutionContext,
        extras: Optional[ExternalExecutionExtras] = None,
        env: Optional[Mapping[str, str]] = None,
        cwd: Optional[str] = None,
    ) -> None:
        params = SubprocessTaskParams(
            command=command,
            env={**(env or {}), **(self.env or {})},
            cwd=(cwd or self.cwd),
        )
        SubprocessExecutionTask(
            context=context,
            extras=extras,
        ).run(params)
