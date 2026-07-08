# Topology-Aware Scheduling

`make topology-plan` writes `.local/reports/topology_placement_plan.json` and pairs it with `kubernetes/topology-aware-scheduling.yaml`.

## What It Shows

- Kueue `Topology` and topology-backed `ResourceFlavor` resources for distributed training.
- Required rack-level TAS annotations for all-reduce-heavy backfill jobs.
- Topology spread constraints for Airflow scheduler high availability.
- Admission check scaffolding for topology-aware capacity provisioning.
- Wave-splitting fallback when a single topology domain cannot fit the full backfill.

## Production Notes

Distributed training benefits from compact placement because network distance can dominate all-reduce time. Airflow control-plane replicas need the opposite behavior: spread across zones so one failure domain cannot stop scheduling or deferrable-task progress. The project models both placement intents and ties them back to backfill width, Kueue admission, and idempotent partition manifests.

References: Kueue Topology Aware Scheduling, Kubernetes topology spread constraints, Kueue AdmissionChecks, and Kubernetes Workload API TAS.
