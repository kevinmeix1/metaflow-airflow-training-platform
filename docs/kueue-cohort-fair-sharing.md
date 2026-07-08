# Kueue Cohort Fair Sharing

`make cohort-fair-sharing` writes `.local/reports/cohort_fair_sharing_plan.json` and pairs it with `kubernetes/kueue-cohort-fair-sharing.yaml`.

## What It Shows

- Kueue Fair Sharing with `preemptionStrategies` for borrowed training resources.
- Admission Fair Sharing so `LocalQueue` admission accounts for decayed historical usage and entry penalties.
- `borrowingLimit` and `lendingLimit` for forecasting-prod, data-quality, and feature-exploration tenants.
- `fairSharing.weight` that protects production backfills and failed-partition recovery.
- Preemption policy separation between `withinClusterQueue` and `reclaimWithinCohort`.

## Production Notes

Training systems need idle capacity for exploratory sweeps, but production backfills and failed-partition recovery must not be starved. Cohort borrowing raises utilization, while Fair Sharing, lending limits, and queue weights keep the blast radius of noisy experimentation visible and bounded.

Admission Fair Sharing adds time-based fairness inside each `ClusterQueue` so repeated HPO or notebook submissions build historical usage and stop jumping ahead of quieter queues.

## References

- Kueue ClusterQueue: <https://kueue.sigs.k8s.io/docs/concepts/cluster_queue/>
- Kueue Cohort: <https://kueue.sigs.k8s.io/docs/concepts/cohort/>
- Kueue Preemption and Fair Sharing: <https://kueue.sigs.k8s.io/docs/concepts/preemption/>
- Kueue Admission Fair Sharing: <https://kueue.sigs.k8s.io/docs/concepts/admission_fair_sharing/>
