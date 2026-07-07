# ADR 0001: Airflow Schedules, Metaflow Trains

## Status

Accepted

## Context

Training platforms need reliable scheduling and rich ML artifact tracking. Airflow is strong at schedule, catchup, retries, and dependency management. Metaflow is strong at ML flow structure, step outputs, reproducibility, and artifact lineage.

## Decision

Use Airflow as the outer scheduler and Metaflow as the inner training workflow. The local demo mirrors this split through a standard-library orchestrator so reviewers can run the project quickly.

## Consequences

Benefits:

- Backfills and catchup are explicit.
- Training steps stay portable.
- Artifacts are connected to run metadata.
- The project can scale from local demo to production orchestration.

Trade-offs:

- Two orchestration concepts must be explained clearly.
- Production deployments need shared secrets, artifact storage, and observability wiring.

