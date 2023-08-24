import json
import random
import string
from typing import Mapping, Optional, Sequence, Union

import kubernetes
from dagster import OpExecutionContext
from dagster._core.external_execution.resource import (
    ExternalExecutionResource,
)
from dagster._core.external_execution.task import (
    ExternalExecutionTask,
)
from dagster_externals import (
    DAGSTER_EXTERNALS_ENV_KEYS,
    ExternalExecutionExtras,
)

from .client import DagsterKubernetesClient, WaitForPodState


def get_pod_name(run_id: str, op_name: str):
    clean_op_name = op_name.replace("_", "-")
    suffix = "".join(random.choice(string.digits) for i in range(10))
    return f"dagster-{run_id[:18]}-{clean_op_name[:20]}-{suffix}"


class K8sPodExecutionTask(ExternalExecutionTask):
    def run(
        self,
        image: str,
        command: Union[str, Sequence[str]],
        namespace: Optional[str],
        env: Mapping[str, str],
    ) -> None:
        client = DagsterKubernetesClient.production_client()

        context = self.get_external_context()

        namespace = namespace or "default"
        pod_name = get_pod_name(self._context.run_id, self._context.op.name)
        context_config_map_name = pod_name

        io_env = {
            DAGSTER_EXTERNALS_ENV_KEYS["context_source"]: json.dumps(
                {"path": "/mnt/dagster/context.json"}
            ),
            DAGSTER_EXTERNALS_ENV_KEYS["message_sink"]: json.dumps(
                {"path": "/tmp/message_sink"}  # want to tell it to just sink to stdout
            ),
        }

        context_config_map_body = kubernetes.client.V1ConfigMap(
            metadata=kubernetes.client.V1ObjectMeta(
                name=context_config_map_name,
            ),
            data={
                "context.json": json.dumps(context),
            },
        )
        client.core_api.create_namespaced_config_map(namespace, context_config_map_body)

        pod_body = kubernetes.client.V1Pod(
            metadata=kubernetes.client.V1ObjectMeta(
                name=pod_name,
            ),
            spec=kubernetes.client.V1PodSpec(
                restart_policy="Never",
                volumes=[
                    {
                        "name": "dagster-externals-context",
                        "configMap": {
                            "name": context_config_map_name,
                        },
                    }
                ],
                containers=[
                    kubernetes.client.V1Container(
                        name="execution",
                        image=image,
                        command=command,
                        env=[
                            {"name": k, "value": v}
                            for k, v in {
                                **self.get_base_env(),
                                **io_env,
                                **env,
                            }.items()
                        ],
                        volume_mounts=[
                            {
                                "mountPath": "/mnt/dagster/",
                                "name": "dagster-externals-context",
                            }
                        ],
                    )
                ],
            ),
        )

        client.core_api.create_namespaced_pod(namespace, pod_body)
        # wait til done
        client.wait_for_pod(pod_name, namespace, wait_for_state=WaitForPodState.Terminated)


class K8sPodExecutionResource(ExternalExecutionResource):
    def run(
        self,
        context: OpExecutionContext,
        extras: ExternalExecutionExtras,
        image: str,
        command: Union[str, Sequence[str]],
        namespace: Optional[str] = None,
        env: Optional[Mapping[str, str]] = None,
    ) -> None:
        return K8sPodExecutionTask(
            context=context,
            extras=extras,
        ).run(
            image=image,
            command=command,
            env=env or {},
            namespace=namespace,
        )
