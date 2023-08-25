import os
import tempfile
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field
from typing import Iterator, Mapping, Optional, Sequence, Union

import docker
from dagster import OpExecutionContext
from dagster._core.external_execution.resource import (
    ExternalExecutionResource,
)
from dagster._core.external_execution.task import (
    ExternalExecutionTask,
)
from dagster._core.external_execution.utils import (
    file_context_source,
    file_message_sink,
)
from dagster_externals import (
    DagsterExternalsError,
    ExternalExecutionExtras,
)
from pydantic import Field
from typing_extensions import TypeAlias

VolumeMapping: TypeAlias = Mapping[str, Mapping[str, str]]


@dataclass
class DockerTaskParams:
    image: str
    command: Union[str, Sequence[str]]
    registry: Optional[Mapping[str, str]] = None
    volumes: Mapping[str, Mapping[str, str]] = field(default_factory=dict)
    env: Mapping[str, str] = field(default_factory=dict)


@dataclass
class DockerTaskIOParams:
    ports: Mapping[int, int] = field(default_factory=dict)
    volumes: Mapping[str, Mapping[str, str]] = field(default_factory=dict)
    env: Mapping[str, str] = field(default_factory=dict)


_CONTEXT_SOURCE_FILENAME = "context"
_MESSAGE_SINK_FILENAME = "messages"


class DockerExecutionTask(ExternalExecutionTask[DockerTaskParams, DockerTaskIOParams]):
    def _launch(
        self,
        base_env: Mapping[str, str],
        params: DockerTaskParams,
        io_params: DockerTaskIOParams,
    ) -> None:
        client = docker.client.from_env()
        if params.registry:
            client.login(
                registry=params.registry["url"],
                username=params.registry["username"],
                password=params.registry["password"],
            )

        # will need to deal with when its necessary to pull the image before starting the container
        # client.images.pull(image)

        container = client.containers.create(
            image=params.image,
            command=params.command,
            detach=True,
            environment={**base_env, **params.env, **io_params.env},
            volumes={
                **params.volumes,
                **io_params.volumes,
            },
            ports={
                **io_params.ports,
                **io_params.ports,
            },
        )

        result = container.start()
        try:
            for line in container.logs(stdout=True, stderr=True, stream=True, follow=True):
                print(line)  # noqa: T201

            result = container.wait()
            if result["StatusCode"] != 0:
                raise DagsterExternalsError(f"Container exited with non-zero status code: {result}")
        finally:
            container.stop()

    # ########################
    # ##### IO
    # ########################

    @contextmanager
    def _setup_io(self, params: DockerTaskParams) -> Iterator[DockerTaskIOParams]:
        with ExitStack() as stack:
            tempdir = stack.enter_context(tempfile.TemporaryDirectory())
            volumes = {tempdir: {"bind": tempdir, "mode": "rw"}}
            context_env = stack.enter_context(
                file_context_source(self, os.path.join(tempdir, _CONTEXT_SOURCE_FILENAME))
            )
            message_env = stack.enter_context(
                file_message_sink(self, os.path.join(tempdir, _MESSAGE_SINK_FILENAME))
            )
            yield DockerTaskIOParams(
                env={**context_env, **message_env},
                volumes=volumes,
            )


class DockerExecutionResource(ExternalExecutionResource):
    env: Optional[Mapping[str, str]] = Field(
        default=None,
        description="An optional dict of environment variables to pass to the subprocess.",
    )
    volumes: Optional[VolumeMapping] = Field(
        default=None,
        description="An optional dict of volumes to mount in the container.",
    )
    registry: Optional[Mapping[str, str]] = Field(
        default=None,
        description="An optional dict of registry credentials to use to pull the image.",
    )

    def run(
        self,
        image: str,
        command: Union[str, Sequence[str]],
        *,
        context: OpExecutionContext,
        extras: ExternalExecutionExtras,
        env: Optional[Mapping[str, str]] = None,
        volumes: Optional[Mapping[str, Mapping[str, str]]] = None,
        registry: Optional[Mapping[str, str]] = None,
    ) -> None:
        params = DockerTaskParams(
            image=image,
            command=command,
            registry=registry or self.registry,
            volumes={**(volumes or {}), **(self.volumes or {})},
            env={**(env or {}), **(self.env or {})},
        )

        DockerExecutionTask(
            context=context,
            extras=extras,
        ).run(params)
