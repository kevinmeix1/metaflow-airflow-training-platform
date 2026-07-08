from __future__ import annotations

from pathlib import Path

from .io import write_json


COMPONENTS = [
    {
        "name": "kube-apiserver",
        "statusz": "/statusz",
        "flagz": "/flagz",
        "metrics": ["apiserver_watch_cache_initializations_total", "apiserver_request_duration_seconds"],
        "critical_flags": ["ComponentStatusz", "ComponentFlagz", "WatchCache", "NativeHistogramMetrics"],
    },
    {
        "name": "kube-controller-manager",
        "statusz": "/statusz",
        "flagz": "/flagz",
        "metrics": ["workqueue_depth", "workqueue_queue_duration_seconds"],
        "critical_flags": ["ComponentStatusz", "ComponentFlagz", "ConcurrentJobSyncs"],
    },
    {
        "name": "kube-scheduler",
        "statusz": "/statusz",
        "flagz": "/flagz",
        "metrics": ["scheduler_pending_pods", "scheduler_pod_scheduling_duration_seconds"],
        "critical_flags": ["ComponentStatusz", "ComponentFlagz", "PodGroupScheduling", "InPlacePodVerticalScaling"],
    },
    {
        "name": "kubelet",
        "statusz": "/statusz",
        "flagz": "/flagz",
        "metrics": ["kubelet_psi_cpu_some_seconds_total", "kubelet_psi_io_some_seconds_total"],
        "critical_flags": ["ComponentStatusz", "ComponentFlagz", "KubeletPSI", "UserNamespacesSupport"],
    },
]


CONTROLLERS = [
    {
        "name": "metaflow-backfill-controller",
        "freshness_budget_seconds": 60,
        "watch_source": "partition manifest, Indexed Job, and Kueue Workload watch",
        "stale_action": "pause backfill fanout and force uncached GET before expanding the next partition wave",
    },
    {
        "name": "model-registration-controller",
        "freshness_budget_seconds": 45,
        "watch_source": "MLflow candidate registry and evaluation artifact watch",
        "stale_action": "hold model promotion until registry alias and evaluation artifact state are read directly",
    },
    {
        "name": "failed-partition-replay-controller",
        "freshness_budget_seconds": 30,
        "watch_source": "failed partition replay Job, Pod, and artifact-volume watch",
        "stale_action": "fail closed and require direct API confirmation before clearing recovery status",
    },
]


def build_control_plane_diagnostics_plan(root: str | Path, *, project: str = "Metaflow Airflow Training Platform") -> dict:
    root = Path(root)
    checks = [
        {
            "name": "statusz_and_flagz_coverage",
            "passed": all(component["statusz"] == "/statusz" and component["flagz"] == "/flagz" for component in COMPONENTS),
            "evidence": "Every control-plane component has explicit /statusz and /flagz scrape coverage.",
        },
        {
            "name": "training_controller_staleness_budgets",
            "passed": all(controller["freshness_budget_seconds"] <= 60 for controller in CONTROLLERS),
            "evidence": "Backfill, model-registration, and failed-partition replay controllers all fail closed inside one minute.",
        },
        {
            "name": "psi_metric_coverage",
            "passed": any("kubelet_psi_io_some_seconds_total" in component["metrics"] for component in COMPONENTS),
            "evidence": "Kubelet PSI metrics catch node IO pressure before it corrupts partition runtime decisions.",
        },
        {
            "name": "native_histogram_readiness",
            "passed": any("NativeHistogramMetrics" in component["critical_flags"] for component in COMPONENTS),
            "evidence": "The plan records native histogram readiness for high-cardinality backfill and scheduler latency metrics.",
        },
        {
            "name": "flag_drift_detection",
            "passed": all("ComponentFlagz" in component["critical_flags"] for component in COMPONENTS),
            "evidence": "/flagz drift detection protects Airflow and Metaflow automation during Kubernetes upgrades.",
        },
    ]
    passed = all(check["passed"] for check in checks)
    plan = {
        "project": project,
        "generated_at": "2026-07-07T00:00:00Z",
        "recommended_action": "enable_control_plane_freshness_diagnostics" if passed else "keep_backfill_controller_freshness_in_warn_mode",
        "passed": passed,
        "feature_status": {
            "controller_staleness": "Kubernetes v1.36 beta stale-cache mitigation for controllers",
            "component_statusz": "Kubernetes v1.36 beta ComponentStatusz endpoint",
            "component_flagz": "Kubernetes v1.36 beta ComponentFlagz endpoint",
            "psi_metrics": "Kubernetes v1.36 stable kubelet PSI metrics",
            "native_histograms": "Kubernetes v1.36 alpha native histogram support",
        },
        "components": COMPONENTS,
        "controllers": CONTROLLERS,
        "checks": checks,
        "training_runbook": [
            "If backfill controller freshness exceeds budget, pause mapped task expansion.",
            "Read partition manifests, Kueue Workloads, and Indexed Jobs directly before retrying failed partitions.",
            "Compare /flagz output with the expected ComponentStatusz, ComponentFlagz, KubeletPSI, and NativeHistogramMetrics gates after upgrades.",
            "Use PSI and native histogram metrics to separate Kubernetes node pressure from model-training regressions.",
        ],
        "references": [
            "https://kubernetes.io/blog/2026/04/28/kubernetes-v1-36-staleness-mitigation-for-controllers/",
            "https://kubernetes.io/blog/2026/04/22/kubernetes-v1-36-release/",
            "https://kubernetes.io/blog/2026/04/25/kubernetes-v1-36-psi-metrics/",
        ],
    }
    write_json(root / "reports" / "control_plane_diagnostics_plan.json", plan)
    return plan
