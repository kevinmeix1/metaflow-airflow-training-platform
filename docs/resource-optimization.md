# Resource Optimization

This layer right-sizes the batch training mesh. It connects observed p95 CPU, p99 memory, forecasted partition count, Airflow pools, and Kueue quota so backfills scale intentionally instead of only adding more pods.

Run:

```bash
make optimize-resources
```

The report is written to `.local/reports/resource_optimization.json`.

## Decisions

- Use VPA in `Off` mode for indexed job templates before changing live resource requests.
- Prefer reducing backfill wave width before relaxing model gates or retry policies.
- Protect Airflow triggerer capacity because deferrable sensors only help when the triggerer has headroom.
- Align Airflow pool slots with Kueue nominal quota to keep the scheduler honest.

## References

Kubernetes requests and limits determine scheduling and enforcement. VPA produces lower, target, and upper resource recommendations. HPA behavior supports stabilization windows for less flapping. Airflow pools limit task parallelism when downstream systems or batch queues are scarce.
