import atexit
import json
import os
import shutil
from pathlib import Path
from typing import List, Optional, Union

import pytest
from dagster import (
    AssetObservation,
    FloatMetadataValue,
    Output,
    TextMetadataValue,
    job,
    materialize,
    op,
)
from dagster._core.execution.context.compute import OpExecutionContext
from dagster_dbt import dbt_assets
from dagster_dbt.asset_utils import build_dbt_asset_selection
from dagster_dbt.core.resources_v2 import (
    PARTIAL_PARSE_FILE_NAME,
    DbtCliEventMessage,
    DbtCliResource,
)
from dagster_dbt.dbt_manifest import DbtManifestParam
from dagster_dbt.errors import DagsterDbtCliRuntimeError

from ..conftest import TEST_PROJECT_DIR

pytest.importorskip("dbt.version", minversion="1.4")


manifest_path = Path(TEST_PROJECT_DIR).joinpath("manifest.json")
manifest = json.loads(manifest_path.read_bytes())


@pytest.mark.parametrize("global_config_flags", [[], ["--quiet"]])
@pytest.mark.parametrize("command", ["run", "parse"])
def test_dbt_cli(global_config_flags: List[str], command: str) -> None:
    dbt = DbtCliResource(project_dir=TEST_PROJECT_DIR, global_config_flags=global_config_flags)
    dbt_cli_invocation = dbt.cli([command], manifest=manifest)

    assert dbt_cli_invocation.process.args == ["dbt", *global_config_flags, command]
    assert dbt_cli_invocation.is_successful()
    assert dbt_cli_invocation.process.returncode == 0
    assert dbt_cli_invocation.target_path.joinpath("dbt.log").exists()


@pytest.mark.parametrize("manifest", [manifest, manifest_path, os.fspath(manifest_path)])
def test_dbt_cli_manifest_argument(manifest: DbtManifestParam) -> None:
    dbt = DbtCliResource(project_dir=TEST_PROJECT_DIR)

    assert dbt.cli(["run"], manifest=manifest).is_successful()


def test_dbt_cli_project_dir_path() -> None:
    dbt = DbtCliResource(project_dir=Path(TEST_PROJECT_DIR))  # type: ignore

    assert dbt.cli(["run"], manifest=manifest).is_successful()


def test_dbt_cli_failure() -> None:
    dbt = DbtCliResource(project_dir=TEST_PROJECT_DIR)
    dbt_cli_invocation = dbt.cli(["run", "--selector", "nonexistent"], manifest=manifest)

    with pytest.raises(DagsterDbtCliRuntimeError):
        dbt_cli_invocation.wait()

    assert not dbt_cli_invocation.is_successful()
    assert dbt_cli_invocation.process.returncode == 2
    assert dbt_cli_invocation.target_path.joinpath("dbt.log").exists()


# as
def test_dbt_cli_subprocess_cleanup(caplog: pytest.LogCaptureFixture) -> None:
    dbt = DbtCliResource(project_dir=TEST_PROJECT_DIR)
    dbt_cli_invocation_1 = dbt.cli(["run"], manifest=manifest)

    assert dbt_cli_invocation_1.process.returncode is None

    atexit._run_exitfuncs()  # ruff: noqa: SLF001

    assert "Terminating the execution of dbt command." in caplog.text
    assert not dbt_cli_invocation_1.is_successful()
    assert dbt_cli_invocation_1.process.returncode < 0

    caplog.clear()

    dbt_cli_invocation_2 = dbt.cli(["run"], manifest=manifest).wait()

    atexit._run_exitfuncs()  # ruff: noqa: SLF001

    assert "Terminating the execution of dbt command." not in caplog.text
    assert dbt_cli_invocation_2.is_successful()
    assert dbt_cli_invocation_2.process.returncode == 0


def test_dbt_cli_get_artifact() -> None:
    dbt = DbtCliResource(project_dir=TEST_PROJECT_DIR)

    dbt_cli_invocation_1 = dbt.cli(["run"], manifest=manifest).wait()
    dbt_cli_invocation_2 = dbt.cli(["compile"], manifest=manifest).wait()

    # `dbt run` produces a manifest.json and run_results.json
    manifest_json_1 = dbt_cli_invocation_1.get_artifact("manifest.json")
    assert manifest_json_1
    assert dbt_cli_invocation_1.get_artifact("run_results.json")

    # `dbt compile` produces a manifest.json and run_results.json
    manifest_json_2 = dbt_cli_invocation_2.get_artifact("manifest.json")
    assert manifest_json_2
    assert dbt_cli_invocation_2.get_artifact("run_results.json")

    # `dbt compile` does not produce a sources.json
    with pytest.raises(Exception):
        dbt_cli_invocation_2.get_artifact("sources.json")

    # Artifacts are stored in separate paths by manipulating DBT_TARGET_PATH.
    # As a result, their contents should be different, and newer artifacts
    # should not overwrite older ones.
    assert manifest_json_1 != manifest_json_2


def test_dbt_profile_configuration() -> None:
    dbt = DbtCliResource(project_dir=TEST_PROJECT_DIR, profile="duckdb", target="dev")

    dbt_cli_invocation = dbt.cli(["parse"], manifest=manifest).wait()

    assert dbt_cli_invocation.process.args == [
        "dbt",
        "parse",
        "--profile",
        "duckdb",
        "--target",
        "dev",
    ]
    assert dbt_cli_invocation.is_successful()


@pytest.mark.parametrize("profiles_dir", [TEST_PROJECT_DIR, Path(TEST_PROJECT_DIR)])
def test_dbt_profile_dir_configuration(profiles_dir: Union[str, Path]) -> None:
    dbt = DbtCliResource(
        project_dir=TEST_PROJECT_DIR,
        profiles_dir=profiles_dir,  # type: ignore
    )

    assert dbt.cli(["parse"], manifest=manifest).is_successful()

    dbt = DbtCliResource(
        project_dir=TEST_PROJECT_DIR, profiles_dir=f"{TEST_PROJECT_DIR}/nonexistent"
    )

    with pytest.raises(DagsterDbtCliRuntimeError):
        dbt.cli(["parse"], manifest=manifest).wait()


def test_dbt_without_partial_parse() -> None:
    dbt = DbtCliResource(project_dir=TEST_PROJECT_DIR)

    dbt.cli(["clean"], manifest=manifest).wait()

    dbt_cli_compile_without_partial_parse_invocation = dbt.cli(["compile"], manifest=manifest)

    assert dbt_cli_compile_without_partial_parse_invocation.is_successful()
    assert any(
        "Unable to do partial parsing" in event.raw_event["info"]["msg"]
        for event in dbt_cli_compile_without_partial_parse_invocation.stream_raw_events()
    )


def test_dbt_with_partial_parse() -> None:
    dbt = DbtCliResource(project_dir=TEST_PROJECT_DIR)

    dbt.cli(["clean"], manifest=manifest).wait()

    # Run `dbt compile` to generate the partial parse file
    dbt_cli_compile_invocation = dbt.cli(["compile"], manifest=manifest).wait()

    # Copy the partial parse file to the target directory
    partial_parse_file_path = Path(
        TEST_PROJECT_DIR, dbt_cli_compile_invocation.target_path, PARTIAL_PARSE_FILE_NAME
    )
    original_target_path = Path(TEST_PROJECT_DIR, "target", PARTIAL_PARSE_FILE_NAME)

    original_target_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(partial_parse_file_path, Path(TEST_PROJECT_DIR, "target", PARTIAL_PARSE_FILE_NAME))

    # Assert that partial parsing was used.
    dbt_cli_compile_with_partial_parse_invocation = dbt.cli(["compile"], manifest=manifest).wait()

    assert dbt_cli_compile_with_partial_parse_invocation.is_successful()
    assert not any(
        "Unable to do partial parsing" in event.raw_event["info"]["msg"]
        for event in dbt_cli_compile_with_partial_parse_invocation.stream_raw_events()
    )


def test_dbt_cli_debug_execution() -> None:
    @dbt_assets(manifest=manifest)
    def my_dbt_assets(context: OpExecutionContext, dbt: DbtCliResource):
        yield from dbt.cli(["--debug", "run"], context=context).stream()

    result = materialize(
        [my_dbt_assets],
        resources={
            "dbt": DbtCliResource(project_dir=TEST_PROJECT_DIR),
        },
    )
    assert result.success


def test_dbt_cli_subsetted_execution() -> None:
    dbt_select = " ".join(
        [
            "fqn:dagster_dbt_test_project.subdir.least_caloric",
            "fqn:dagster_dbt_test_project.sort_by_calories",
        ]
    )

    @dbt_assets(manifest=manifest, select=dbt_select)
    def my_dbt_assets(context: OpExecutionContext, dbt: DbtCliResource):
        dbt_cli_invocation = dbt.cli(["run"], context=context).wait()

        assert dbt_cli_invocation.process.args == ["dbt", "run", "--select", dbt_select]

        yield from dbt_cli_invocation.stream()

    result = materialize(
        [my_dbt_assets],
        resources={
            "dbt": DbtCliResource(project_dir=TEST_PROJECT_DIR),
        },
    )
    assert result.success


def test_dbt_cli_asset_selection() -> None:
    dbt_select = [
        "fqn:dagster_dbt_test_project.subdir.least_caloric",
        "fqn:dagster_dbt_test_project.sort_by_calories",
    ]

    @dbt_assets(manifest=manifest)
    def my_dbt_assets(context: OpExecutionContext, dbt: DbtCliResource):
        dbt_cli_invocation = dbt.cli(["run"], context=context).wait()

        dbt_cli_args: List[str] = list(dbt_cli_invocation.process.args)  # type: ignore
        *dbt_args, dbt_select_args = dbt_cli_args

        assert dbt_args == ["dbt", "run", "--select"]
        assert set(dbt_select_args.split()) == set(dbt_select)

        yield from dbt_cli_invocation.stream()

    result = materialize(
        [my_dbt_assets],
        resources={
            "dbt": DbtCliResource(project_dir=TEST_PROJECT_DIR),
        },
        selection=build_dbt_asset_selection(
            [my_dbt_assets],
            dbt_select=(
                "fqn:dagster_dbt_test_project.subdir.least_caloric"
                " fqn:dagster_dbt_test_project.sort_by_calories"
            ),
        ),
    )
    assert result.success


@pytest.mark.parametrize("exclude", [None, "fqn:dagster_dbt_test_project.subdir.least_caloric"])
def test_dbt_cli_default_selection(exclude: Optional[str]) -> None:
    @dbt_assets(manifest=manifest, exclude=exclude)
    def my_dbt_assets(context: OpExecutionContext):
        dbt = DbtCliResource(project_dir=TEST_PROJECT_DIR)
        dbt_cli_invocation = dbt.cli(["run"], context=context)

        dbt_cli_invocation.wait()

        expected_args = ["dbt", "run", "--select", "fqn:*"]
        if exclude:
            expected_args += ["--exclude", exclude]

        assert dbt_cli_invocation.process.args == expected_args
        assert dbt_cli_invocation.process.returncode is not None

        yield from dbt_cli_invocation.stream()

    assert materialize([my_dbt_assets]).success


def test_dbt_cli_op_execution() -> None:
    @op
    def my_dbt_op(context: OpExecutionContext, dbt: DbtCliResource):
        dbt.cli(["run"], context=context, manifest=manifest).wait()

    @job
    def my_dbt_job():
        my_dbt_op()

    result = my_dbt_job.execute_in_process(
        resources={
            "dbt": DbtCliResource(project_dir=TEST_PROJECT_DIR),
        }
    )

    assert result.success


@pytest.mark.parametrize(
    "data",
    [
        {},
        {
            "node_info": {
                "unique_id": "a.b.c",
                "resource_type": "model",
                "node_status": "failure",
                "node_finished_at": "2024-01-01T00:00:00Z",
            }
        },
        {
            "node_info": {
                "unique_id": "a.b.c",
                "resource_type": "macro",
                "node_status": "success",
                "node_finished_at": "2024-01-01T00:00:00Z",
            }
        },
        {
            "node_info": {
                "unique_id": "a.b.c",
                "resource_type": "model",
                "node_status": "failure",
            }
        },
        {
            "node_info": {
                "unique_id": "a.b.c",
                "resource_type": "test",
                "node_status": "success",
            }
        },
    ],
    ids=[
        "node info missing",
        "node status failure",
        "not refable",
        "not successful execution",
        "not finished test execution",
    ],
)
def test_no_default_asset_events_emitted(data: dict) -> None:
    asset_events = DbtCliEventMessage(
        raw_event={
            "info": {"level": "info"},
            "data": data,
        }
    ).to_default_asset_events(manifest={})

    assert list(asset_events) == []


def test_to_default_asset_output_events() -> None:
    raw_event = {
        "info": {"level": "info"},
        "data": {
            "node_info": {
                "unique_id": "a.b.c",
                "resource_type": "model",
                "node_status": "success",
                "node_started_at": "2024-01-01T00:00:00Z",
                "node_finished_at": "2024-01-01T00:01:00Z",
            }
        },
    }
    asset_events = list(
        DbtCliEventMessage(raw_event=raw_event).to_default_asset_events(manifest={})
    )

    assert len(asset_events) == 1
    assert all(isinstance(e, Output) for e in asset_events)
    assert asset_events[0].metadata == {
        "unique_id": TextMetadataValue("a.b.c"),
        "Execution Duration": FloatMetadataValue(60.0),
    }


def test_to_default_asset_observation_events() -> None:
    manifest = {
        "nodes": {
            "a.b.c.d": {
                "resource_type": "model",
                "config": {},
                "name": "model",
            }
        },
        "sources": {
            "a.b.c.d.e": {
                "resource_type": "source",
                "source_name": "test",
                "name": "source",
            }
        },
        "parent_map": {
            "a.b.c": [
                "a.b.c.d",
                "a.b.c.d.e",
            ]
        },
    }
    raw_event = {
        "info": {"level": "info"},
        "data": {
            "node_info": {
                "unique_id": "a.b.c",
                "resource_type": "test",
                "node_status": "success",
                "node_finished_at": "2024-01-01T00:00:00Z",
            }
        },
    }
    asset_events = list(
        DbtCliEventMessage(raw_event=raw_event).to_default_asset_events(manifest=manifest)
    )

    assert len(asset_events) == 2
    assert all(isinstance(e, AssetObservation) for e in asset_events)
