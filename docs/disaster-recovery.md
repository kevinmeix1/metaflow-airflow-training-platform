# Disaster Recovery

This project includes a DR plan for Airflow metadata, partition manifests, lineage, MLflow artifacts, and backfill replay.

Run:

```bash
make dr-plan
```

The report is written to `.local/reports/disaster_recovery_plan.json`.

## Restore Order

1. Namespace and batch CRDs.
2. Airflow metadata database.
3. Partition manifests and lineage.
4. MLflow artifacts.
5. Backfill replay.

Velero and CSI snapshots cover cluster resources and volumes, while application consistency relies on metadata dumps and replaying one partition before unpausing broad backfills.
