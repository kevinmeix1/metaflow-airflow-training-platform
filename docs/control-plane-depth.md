# Advanced Backfill Control Plane

This repo now includes a backfill capacity planner in `src/training_orchestration_platform/capacity_planner.py`.

## Operator Workflow

- Run `make demo` or `make backfill` to create historical run state.
- Run `make plan-backfill` to generate `reports/backfill_capacity_plan.json`.
- Inspect wave count, skipped partitions, CPU/memory budgets, and workload order.

## What The Planner Uses

- Requested date range.
- Already successful partitions.
- Model-family-specific CPU and memory estimates.
- Backfill priority.
- Airflow pool identity and Kueue queue identity.

## Production Signal

Backfills are planned as capacity-bounded waves rather than as a single fanout. That models the difference between replaying partitions and operating a shared training platform without overwhelming cluster capacity.
