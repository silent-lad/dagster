from typing import TYPE_CHECKING, Optional

import dagster._check as check
from dagster import AssetKey
from dagster._core.instance import DagsterInstance
from dagster._core.storage.asset_check_execution_record import (
    AssetCheckExecutionRecord,
    AssetCheckExecutionStatus,
)
from dagster._core.storage.dagster_run import DagsterRunStatus

from ..schema.asset_checks import (
    GrapheneAssetCheck,
    GrapheneAssetChecks,
)

if TYPE_CHECKING:
    from ..schema.util import ResolveInfo


def fetch_asset_checks(
    graphene_info: "ResolveInfo",
    asset_key: AssetKey,
    check_name: Optional[str] = None,
) -> GrapheneAssetChecks:
    external_asset_checks = []
    for location in graphene_info.context.code_locations:
        for repository in location.get_repositories().values():
            for external_check in repository.external_repository_data.external_asset_checks or []:
                if external_check.asset_key == asset_key:
                    if not check_name or check_name == external_check.name:
                        external_asset_checks.append(external_check)

    return GrapheneAssetChecks(
        checks=[
            GrapheneAssetCheck(
                name=check.name,
                description=check.description,
                asset_key=asset_key,
            )
            for check in external_asset_checks
        ]
    )


def get_asset_check_execution_status(
    instance: DagsterInstance, execution: AssetCheckExecutionRecord
) -> AssetCheckExecutionStatus:
    """Asset checks stay in PLANNED status until the evaluation event arives. Check if the run is
    still active, and if not, return the actual status.
    """
    if execution.status != AssetCheckExecutionStatus.PLANNED:
        return execution.status

    run = check.not_none(instance.get_run_by_id(execution.run_id))

    if run.is_finished:
        if run.status == DagsterRunStatus.FAILURE:
            return AssetCheckExecutionStatus.RUN_FAILURE
        else:
            return AssetCheckExecutionStatus.SKIPPED
    else:
        return AssetCheckExecutionStatus.PLANNED
