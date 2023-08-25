from typing import Optional, cast

import dagster._check as check
import graphene
from dagster import EventLogEntry
from dagster._core.definitions.asset_check_evaluation import (
    AssetCheckEvaluation,
    AssetCheckEvaluationTargetMaterializationData,
)
from dagster._core.definitions.events import AssetKey
from dagster._core.storage.asset_check_execution_record import (
    AssetCheckExecutionRecord,
    AssetCheckExecutionStatus,
)

from dagster_graphql.implementation.events import iterate_metadata_entries
from dagster_graphql.schema.errors import GrapheneError
from dagster_graphql.schema.metadata import GrapheneMetadataEntry
from dagster_graphql.schema.util import non_null_list

from .asset_key import GrapheneAssetKey
from .util import ResolveInfo

GrapheneAssetCheckExecutionStatus = graphene.Enum.from_enum(AssetCheckExecutionStatus)


class GrapheneAssetCheckEvaluationTargetMaterializationData(graphene.ObjectType):
    storageId = graphene.NonNull(graphene.Int)
    runId = graphene.NonNull(graphene.String)
    timestamp = graphene.NonNull(graphene.Float)

    class Meta:
        name = "AssetCheckEvaluationTargetMaterializationData"

    def __init__(self, target_materialization_data: AssetCheckEvaluationTargetMaterializationData):
        self.storageId = target_materialization_data.storage_id
        self.runId = target_materialization_data.run_id
        self.timestamp = target_materialization_data.timestamp


class GrapheneAssetCheckEvaluation(graphene.ObjectType):
    timestamp = graphene.NonNull(graphene.Float)
    targetMaterialization = graphene.Field(GrapheneAssetCheckEvaluationTargetMaterializationData)
    metadataEntries = non_null_list(GrapheneMetadataEntry)

    class Meta:
        name = "AssetCheckEvaluation"

    def __init__(self, evaluation_event: EventLogEntry):
        self.timestamp = evaluation_event.timestamp

        evaluation_data = cast(
            AssetCheckEvaluation,
            check.not_none(evaluation_event.dagster_event).event_specific_data,
        )

        target_materialization_data = evaluation_data.target_materialization_data
        self.targetMaterialization = (
            GrapheneAssetCheckEvaluationTargetMaterializationData(target_materialization_data)
            if target_materialization_data
            else None
        )

        self.metadataEntries = list(iterate_metadata_entries(evaluation_data.metadata))


class GrapheneAssetCheckExecution(graphene.ObjectType):
    id = graphene.NonNull(graphene.Int)
    runId = graphene.NonNull(graphene.String)
    status = graphene.NonNull(GrapheneAssetCheckExecutionStatus)
    evaluation = graphene.Field(GrapheneAssetCheckEvaluation)

    class Meta:
        name = "AssetCheckExecution"

    def __init__(self, graphene_info: ResolveInfo, execution: AssetCheckExecutionRecord):
        from dagster_graphql.implementation.fetch_asset_checks import (
            get_asset_check_execution_status,
        )

        self.id = execution.id
        self.runId = execution.run_id
        self.status = get_asset_check_execution_status(graphene_info.context.instance, execution)
        self.evaluation = (
            GrapheneAssetCheckEvaluation(execution.evaluation_event)
            if execution.evaluation_event
            else None
        )


class GrapheneAssetCheck(graphene.ObjectType):
    name = graphene.NonNull(graphene.String)
    assetKey = graphene.NonNull(GrapheneAssetKey)
    description = graphene.String()
    executions = graphene.Field(
        non_null_list(GrapheneAssetCheckExecution),
        limit=graphene.NonNull(graphene.Int),
        cursor=graphene.String(),
    )

    class Meta:
        name = "AssetCheck"

    def __init__(self, name: str, asset_key: AssetKey, description: Optional[str] = None):
        self._name = name
        self._asset_key = asset_key
        self._description = description

    def resolve_name(self, _):
        return self._name

    def resolve_assetKey(self, _):
        return self._asset_key

    def resolve_description(self, _):
        return self._description

    def resolve_executions(self, graphene_info: ResolveInfo, **kwargs):
        executions = graphene_info.context.instance.event_log_storage.get_asset_check_executions(
            asset_key=self._asset_key,
            check_name=self._name,
            limit=kwargs["limit"],
            cursor=kwargs.get("cursor"),
        )

        return [GrapheneAssetCheckExecution(graphene_info, e) for e in executions]


class GrapheneAssetChecks(graphene.ObjectType):
    checks = non_null_list(GrapheneAssetCheck)

    class Meta:
        name = "AssetChecks"


class GrapheneAssetCheckNeedsMigrationError(graphene.ObjectType):
    message = graphene.NonNull(graphene.String)

    class Meta:
        interfaces = (GrapheneError,)
        name = "AssetCheckNeedsMigrationError"


class GrapheneAssetChecksOrError(graphene.Union):
    class Meta:
        types = (
            GrapheneAssetChecks,
            GrapheneAssetCheckNeedsMigrationError,
        )
        name = "AssetChecksOrError"
