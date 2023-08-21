import os

import pytest
from dagster import AssetExecutionContext, asset, materialize
from dagster_databricks import DatabricksExecutionResource

IS_BUILDKITE = os.getenv("BUILDKITE") is not None


@pytest.mark.skipif(IS_BUILDKITE, reason="Not configured to run on BK yet.")
def test_basic():
    @asset
    def number_x(
        context: AssetExecutionContext,
        databricks_resource: DatabricksExecutionResource,
    ):
        databricks_resource.run(
            context,
            extras={"multiplier": 2, "storage_root": "fake"},
            script_path="python_modules/dagster-test/dagster_test/toys/external_execution/numbers_example/number_x.py",
            git_url="https://github.com/dagster-io/dagster",
            git_branch="sean/numbers-databricks",
        )

    resource = DatabricksExecutionResource(
        host=os.environ["DATABRICKS_HOST"],
        token=os.environ["DATABRICKS_TOKEN"],
    )

    result = materialize(
        [number_x],
        resources={"databricks_resource": resource},
        raise_on_error=False,
    )
    assert result.success
    mats = result.asset_materializations_for_node(number_x.op.name)
    assert mats[0].metadata["path"]
