# Runtime Security

`make runtime-security` writes `.local/reports/runtime_security_plan.json`.

## What It Shows

- Kubernetes v1.36 user namespaces GA readiness with `pod.spec.hostUsers: false`.
- Runtime prerequisites for user namespaces: Linux 6.3+, idmap-capable filesystems, containerd 2.0+ or CRI-O 1.25+, and runc 1.2+ or crun 1.13+.
- Kubernetes v1.36 fine-grained kubelet authorization (`KubeletFineGrainedAuthz`) using `nodes/metrics`, `nodes/stats`, `nodes/log`, and `nodes/pods`.
- A policy example that blocks new training telemetry roles from granting broad `nodes/proxy`.
- Reduced blast radius for Metaflow partition workers, telemetry readers, and failed-partition replay.

## Production Notes

User namespaces let training images keep container-root compatibility for package, cache, or artifact setup while mapping the process to an unprivileged host UID. That reduces node impact if a partition worker is compromised during large backfills.

Fine-grained kubelet authorization removes the old pattern where telemetry agents needed `nodes/proxy` just to read kubelet metrics or logs. The manifest grants only the kubelet subresources required for training telemetry and leaves privileged kubelet access as an audited break-glass path.

## Senior Review Angle

This shows that training security is embedded in the orchestration story. It links Metaflow workers, Airflow recovery, telemetry, RBAC, admission policy, and node-pool readiness instead of treating runtime isolation as a cluster checkbox.

References:

- https://kubernetes.io/docs/concepts/workloads/pods/user-namespaces/
- https://kubernetes.io/docs/tasks/configure-pod-container/user-namespaces/
- https://kubernetes.io/blog/2026/04/24/kubernetes-v1-36-fine-grained-kubelet-authorization-ga/
- https://kubernetes.io/blog/2026/04/23/kubernetes-v1-36-userns-ga/
