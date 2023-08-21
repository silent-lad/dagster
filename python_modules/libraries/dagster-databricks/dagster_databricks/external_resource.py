import base64
import json
import os
import random
import string
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field
from threading import Event, Thread
from typing import Any, ContextManager, Iterator, Mapping, Optional, Union

from dagster._core.execution.context.compute import OpExecutionContext
from dagster._core.external_execution.context import build_external_execution_context
from dagster._core.external_execution.resource import ExternalExecutionResource
from dagster._core.external_execution.task import (
    ExternalExecutionExtras,
    ExternalExecutionTask,
)
from dagster._utils import tail_file
from dagster_externals import DAGSTER_EXTERNALS_ENV_KEYS, DagsterExternalsError
from databricks.sdk.service import files
from pydantic import Field

from dagster_databricks.databricks import DatabricksClient, DatabricksError, DatabricksJobRunner


@dataclass
class DatabricksTaskParams:
    host: Optional[str]
    token: Optional[str]
    script_path: str
    git_url: str
    git_branch: str
    env: Mapping[str, str] = field(default_factory=dict)


@dataclass
class DatabricksTaskIOParams:
    env: Mapping[str, str] = field(default_factory=dict)


# This has been manually uploaded to a test DBFS workspace.
# DAGSTER_EXTERNALS_WHL_PATH = "dbfs:/FileStore/jars/d1c12eb4_a505_46d5_8717_9c24f8c6c9d1/dagster_externals-1!0+dev-py3-none-any.whl"
# DAGSTER_EXTERNALS_WHL_PATH = "dbfs:/FileStore/jars/dagster_externals_current.whl"
DAGSTER_EXTERNALS_WHL_PATH = "dbfs:/FileStore/jars/dagster_externals-1!0+dev-py3-none-any.whl"

TASK_KEY = "DUMMY_TASK_KEY"

CLUSTER_DEFAULTS = {
    "size": {"num_workers": 0},
    "spark_version": "12.2.x-scala2.12",
    "nodes": {"node_types": {"node_type_id": "i3.xlarge"}},
}

_INPUT_FILENAME = "input"
_OUTPUT_DIRNAME = "output"


class DatabricksExecutionTask(ExternalExecutionTask[DatabricksTaskParams, DatabricksTaskIOParams]):
    def _launch(
        self,
        base_env: Mapping[str, str],
        params: DatabricksTaskParams,
        input_params: DatabricksTaskIOParams,
        output_params: DatabricksTaskIOParams,
    ) -> None:
        runner = DatabricksJobRunner(
            host=params.host,
            token=params.token,
        )

        config = {
            "install_default_libraries": False,
            "libraries": [
                {"whl": DAGSTER_EXTERNALS_WHL_PATH},
            ],
            # We need to set env vars so use a new cluster
            "cluster": {
                "new": {
                    **CLUSTER_DEFAULTS,
                    "spark_env_vars": {
                        **base_env,
                        **params.env,
                        **input_params.env,
                        **output_params.env,
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3",
                    },
                }
            },
        }
        task = {
            "task_key": TASK_KEY,
            "spark_python_task": {
                "python_file": params.script_path,
                "source": "GIT",
            },
            "git_source": {
                "git_url": params.git_url,
                "git_branch": params.git_branch,
            },
        }

        run_id = runner.submit_run(config, task)
        try:
            runner.client.wait_for_run_to_complete(
                logger=self._context.log,
                databricks_run_id=run_id,
                poll_interval_sec=1,
                max_wait_time_sec=60 * 60,
            )
        except DatabricksError as e:
            raise DagsterExternalsError(f"Error running Databricks job: {e}")

    # ########################
    # ##### IO
    # ########################

    def _setup_io(self, params: DatabricksTaskParams) -> ContextManager[DatabricksTaskIOParams]:
        with ExitStack() as stack:
            client = DatabricksClient(params.host, params.token)
            dbfs = files.DbfsAPI(client.workspace_client.api_client)
            tempdir = stack.enter_context(self._dbfs_tempdir(dbfs))
            stack.enter_context(self._dbfs_input(tempdir, dbfs))
            stack.enter_context(self._dbfs_output(tempdir, dbfs))

    @contextmanager
    def _dbfs_tempdir(self, dbfs: files.DbfsAPI) -> Iterator[str]:
        dirname = "".join(random.choices(string.ascii_letters, k=30))
        tempdir = f"/tmp/{dirname}"
        dbfs.mkdirs(tempdir)
        try:
            yield tempdir
        finally:
            dbfs.delete(tempdir, recursive=True)

    @contextmanager
    def _dbfs_input(self, tempdir: str, dbfs: files.DbfsAPI) -> Iterator[Mapping[str, str]]:
        path = os.path.join(tempdir, _INPUT_FILENAME)
        input_spec = {
            "class": "dagster_externals.io.databricks",
            "attribute": "DbfsInputReader",
            "params": {"path": path},
        }
        env = {DAGSTER_EXTERNALS_ENV_KEYS["input_reader"]: json.dumps(input_spec)}
        external_context = build_external_execution_context(self._context, self._extras)
        contents = base64.b64encode(json.dumps(external_context).encode("utf-8")).decode("utf-8")
        dbfs.put(path, contents=contents)
        yield env

    @contextmanager
    def _dbfs_output(self, tempdir: str, dbfs: DatabricksClient) -> Iterator[Mapping[str, str]]:
        path = os.path.join(tempdir, _OUTPUT_DIRNAME)
        output_spec = {
            "class": "dagster_externals.io.databricks",
            "attribute": "DbfsOutputWriter",
            "params": {"path": path},
        }
        env = {DAGSTER_EXTERNALS_ENV_KEYS["output_writer"]: json.dumps(output_spec)}
        is_task_complete = Event()
        thread = Thread(target=self._read_output, args=(path, is_task_complete), daemon=True)
        thread.start()
        yield env
        # try:
        #     yield DatabricksTaskIOParams(env=env)
        #     output = dbfs.read(remote_path).data
        #     for line in StringIO(output):
        #         notification = json.loads(line)
        #         self.handle_message(notification)

    def _read_output(self, path_or_fd: Union[str, int], is_task_complete: Event) -> Any:
        for line in tail_file(path_or_fd, lambda: is_task_complete.is_set()):
            notification = json.loads(line)
            self.handle_message(notification)


class DatabricksExecutionResource(ExternalExecutionResource):
    host: Optional[str] = Field(
        description="Databricks host, e.g. uksouth.azuredatabricks.com", default=None
    )
    token: Optional[str] = Field(description="Databricks access token", default=None)

    def run(
        self,
        context: OpExecutionContext,
        script_path: str,
        git_url: str,
        git_branch: str,
        *,
        extras: ExternalExecutionExtras,
    ) -> None:
        params = DatabricksTaskParams(
            host=self.host,
            token=self.token,
            script_path=script_path,
            git_url=git_url,
            git_branch=git_branch,
        )
        DatabricksExecutionTask(context=context, extras=extras).run(params)
