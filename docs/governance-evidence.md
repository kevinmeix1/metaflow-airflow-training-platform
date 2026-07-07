# Governance Evidence

The training platform generates release evidence around a successful training partition:

- `governance/model_card.json`
- `governance/data_card.json`
- `governance/risk_register.json`
- `governance/approval_record.json`
- `governance/reproducibility_manifest.json`
- `reports/governance_evidence_bundle.json`

The evidence connects Airflow backfill behavior, Metaflow-style training artifacts, MLflow-style run metadata, partition content hashes, quality gates, and lineage. This is the part many portfolio projects skip: it explains why a model artifact is acceptable to release and how to reproduce it.

Run it locally:

```bash
make demo
make governance-bundle
```

`kubernetes/governance-evidence.yaml` models the evidence generation job and a scheduled monthly review.
