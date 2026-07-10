# Operational Readiness Review

`make demo` writes `reports/operational_readiness_review.json` as the operator-facing packet for large training backfills.

The review aggregates Airflow backfill admission, SLO burn rate, Metaflow checkpoint evidence, Kueue capacity waves, asset-partition telemetry, training performance budgets, and supply-chain provenance. It is designed to answer whether the next backfill wave is safe to admit or should be held.

The packet is intentionally fail-closed. If backfill admission is incomplete, partition reliability is paging, capacity waves are missing, checkpoint recovery is not proven, provenance is absent, or telemetry lacks partition lineage, the recommendation becomes remediation instead of expansion.

Judge demo talking points:

- Failed-partition recovery is separated from bulk backfill expansion.
- Airflow assets, Metaflow artifacts, Kueue admission, checkpointing, and provenance are reviewed together.
- The packet makes a large DAG understandable to a production reviewer.
