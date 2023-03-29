"""src/redun-utils/executors.py"""
import json
from typing import Dict, Optional, List, Tuple

from redun.config import Config
from redun.executors.docker import DockerExecutor
from redun.executors.gcp_batch import GCPBatchExecutor

try:
    from redun.executors.k8s import K8SExecutor

    KUBE_ANNOTATIONS = {
        "cluster-autoscaler.kubernetes.io/safe-to-evict": "true",
        "sidecar.istio.io/inject": "false",
        "traffic.sidecar.istio.io/excludeOutboundIPRanges": "169.254.169.254/32",
    }

    class CustomK8sExecutor(K8SExecutor):
        """
        Kubernetes Executor
        """

        def __init__(
            self,
            name: str,
            image: str,
            scratch: str = "scratch",
            namespace: str = "default",
            service_account_name: str = "default",
            annotations: Dict[str, str] = {},
        ):
            config = Config(
                {
                    f"executors.{name}": {
                        "image": image,
                        "scratch": scratch,
                        "namespace": namespace,
                        "service_account_name": service_account_name,
                        "annotations": json.dumps(annotations),
                    }
                }
            )
            super().__init__(name, config=config["executors"][name])

except:
    print("K8SExecutor not found")


class CustomDockerExecutor(DockerExecutor):
    """
    Docker Executor
    """

    def __init__(self, name: str, image: str, scratch: str = "scratch"):
        config = Config(
            {
                f"executors.{name}": {
                    "image": image,
                    "scratch": scratch,
                }
            }
        )
        super().__init__(name, config=config["executors"][name])


class CustomGCPBatchExecutor(GCPBatchExecutor):
    """
    GCP Batch Executor
    """

    def __init__(
        self,
        name: str,
        image: str,
        project: str,
        region: str,
        machine_type: Optional[str] = None,
        accelerators: List[Tuple[str, int]] = None,
        provisioning_model: str = "standard",
        scratch: str = "scratch",
        boot_disk_size_gb: Optional[int] = None,
    ):
        params = {
            "image": image,
            "gcs_scratch": scratch,
            "project": project,
            "region": region,
            "provisioning_model": provisioning_model,
        }
        if accelerators:
            params["accelerators"] = accelerators
        if machine_type:
            params["machine_type"] = machine_type
        if boot_disk_size_gb:
            # convert gb to gib
            boot_disk_size_gib = int(boot_disk_size_gb * (1e9 / 1024 ** 3))
            params["boot_disk_size_gib"] = str(boot_disk_size_gib)
        config = Config({f"executors.{name}": params})
        super().__init__(name, config=config["executors"][name])
