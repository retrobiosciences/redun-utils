"""src/redun-utils/executors.py"""
import json
from typing import Dict

from redun.config import Config
from redun.executors.docker import DockerExecutor

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
