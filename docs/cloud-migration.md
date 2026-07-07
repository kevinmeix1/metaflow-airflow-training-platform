# Cloud Migration Plan

Generate the machine-readable plan with:

```bash
make cloud-plan
```

## AWS Target

- Run Airflow through MWAA or the official Airflow Helm chart on EKS.
- Store Metaflow artifacts and partition manifests in versioned S3.
- Track models with MLflow on RDS PostgreSQL and S3 artifacts.
- Use EKS Auto Mode or Karpenter-style NodePools for partition training jobs.
- Use Kueue to admit large backfills and avoid starving daily training.
- Monitor with Amazon Managed Service for Prometheus and Grafana.

## Portability Notes

- Keep partition IDs, content hashes, and model versions provider-neutral.
- Keep Airflow DAGs free of direct cloud SDK calls where provider hooks exist.
- Keep cloud-specific IAM, S3, and cluster configuration in `infra/terraform/aws`.
