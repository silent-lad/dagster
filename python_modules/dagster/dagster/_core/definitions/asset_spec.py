from typing import AbstractSet, Any, Iterable, Mapping, NamedTuple, Optional, Union

import dagster._check as check
from dagster._annotations import PublicAttr, experimental
from dagster._core.definitions.assets import AssetsDefinition
from dagster._core.definitions.source_asset import SourceAsset

from .auto_materialize_policy import AutoMaterializePolicy
from .events import (
    AssetKey,
    CoercibleToAssetKey,
)
from .freshness_policy import FreshnessPolicy
from .metadata import MetadataUserInput


@experimental
class AssetSpec(
    NamedTuple(
        "_AssetSpec",
        [
            ("asset_key", PublicAttr[AssetKey]),
            ("deps", PublicAttr[AbstractSet[AssetKey]]),
            ("description", PublicAttr[Optional[str]]),
            ("metadata", PublicAttr[Optional[Mapping[str, Any]]]),
            ("group_name", PublicAttr[Optional[str]]),
            ("skippable", PublicAttr[bool]),
            ("code_version", PublicAttr[Optional[str]]),
            ("freshness_policy", PublicAttr[Optional[FreshnessPolicy]]),
            ("auto_materialize_policy", PublicAttr[Optional[AutoMaterializePolicy]]),
        ],
    )
):
    """Specifies the core attributes of an asset. This object is attached to the decorated
    function that defines how it materialized.

    Attributes:
        asset_key (AssetKey): The unique identifier for this asset.
        deps (Optional[AbstractSet[AssetKey]]): The asset keys for the upstream assets that
            materializing this asset depends on.
        description (Optional[str]): Human-readable description of this asset.
        metadata (Optional[Dict[str, Any]]): A dict of static metadata for this asset.
            For example, users can provide information about the database table this
            asset corresponds to.
        skippable (bool): Whether this asset can be omitted during materialization, causing downstream
            dependencies to skip.
        group_name (Optional[str]): A string name used to organize multiple assets into groups. If
            not provided, the name "default" is used.
        code_version (Optional[str]): The version of the code for this specific asset,
            overriding the code version of the materialization function
        freshness_policy (Optional[FreshnessPolicy]): A policy which indicates how up to date this
            asset is intended to be.
        auto_materialize_policy (Optional[AutoMaterializePolicy]): AutoMaterializePolicy to apply to
            the specified asset.
        backfill_policy (Optional[BackfillPolicy]): BackfillPolicy to apply to the specified asset.
    """

    def __new__(
        cls,
        asset_key: CoercibleToAssetKey,
        deps: Optional[
            Iterable[
                Union[
                    CoercibleToAssetKey,
                    "AssetSpec",
                    AssetsDefinition,
                    SourceAsset,
                ]
            ]
        ] = None,
        description: Optional[str] = None,
        metadata: Optional[MetadataUserInput] = None,
        skippable: bool = False,
        group_name: Optional[str] = None,
        code_version: Optional[str] = None,
        freshness_policy: Optional[FreshnessPolicy] = None,
        auto_materialize_policy: Optional[AutoMaterializePolicy] = None,
    ):
        from .decorators.asset_decorator import (
            asset_key_from_coercible_or_definition,
        )

        dep_set = set()
        if deps:
            for dep in deps:
                if isinstance(dep, AssetSpec):
                    dep_set.add(dep.asset_key)
                else:
                    dep_set.add(asset_key_from_coercible_or_definition(dep))

        return super().__new__(
            cls,
            asset_key=AssetKey.from_coercible(asset_key),
            deps=dep_set,
            description=check.opt_str_param(description, "description"),
            metadata=check.opt_mapping_param(metadata, "metadata", key_type=str),
            skippable=check.bool_param(skippable, "skippable"),
            group_name=check.opt_str_param(group_name, "group_name"),
            code_version=check.opt_str_param(code_version, "code_version"),
            freshness_policy=check.opt_inst_param(
                freshness_policy,
                "freshness_policy",
                FreshnessPolicy,
            ),
            auto_materialize_policy=check.opt_inst_param(
                auto_materialize_policy,
                "auto_materialize_policy",
                AutoMaterializePolicy,
            ),
        )
