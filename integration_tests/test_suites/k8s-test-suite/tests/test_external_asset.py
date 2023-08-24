import os

import pytest
from dagster import AssetExecutionContext, asset, materialize
from dagster_k8s.external_resource import K8sPodExecutionResource
from dagster_test.test_project import (
    IS_BUILDKITE,
    find_local_test_image,
    get_buildkite_registry_config,
    get_test_project_docker_image,
)


@pytest.mark.default
def test_k8s_pod_exec_resource(namespace, cluster_provider):
    docker_image = get_test_project_docker_image()

    launcher_config = {}

    if IS_BUILDKITE:
        launcher_config["registry"] = get_buildkite_registry_config()
    else:
        find_local_test_image(docker_image)

    @asset
    def number_x(
        context: AssetExecutionContext,
        k8s_pod_resource: K8sPodExecutionResource,
    ):
        instance_storage = context.instance.storage_directory()
        host_storage = os.path.join(instance_storage, "number_example")
        os.makedirs(host_storage, exist_ok=True)

        k8s_pod_resource.run(
            namespace=namespace,
            image=docker_image,
            command=[
                "python",
                "-m",
                "numbers_example.number_x",
            ],
            context=context,
            extras={"multiplier": 2, "storage_root": "/tmp/"},
            env={"PYTHONPATH": "/dagster_test/toys/external_execution/"},
        )

    result = materialize(
        [number_x],
        resources={"k8s_pod_resource": K8sPodExecutionResource()},
        raise_on_error=False,
    )
    assert result.success
    mats = result.asset_materializations_for_node(number_x.op.name)
    # "path" is a default metadata entry added by fs io manager,
    # so this works but isn't the one from the external proc
    assert mats[0].metadata["path"]
