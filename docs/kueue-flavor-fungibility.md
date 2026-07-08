# Kueue Flavor Fungibility

`make flavor-fungibility` writes `.local/reports/flavor_fungibility_plan.json` and pairs it with `kubernetes/kueue-flavor-fungibility.yaml`.

## What It Shows

- `ResourceFlavor` objects for on-demand CPU, spot CPU, reserved L4 GPU, and spot L4 GPU training pools.
- `ClusterQueue.spec.flavorFungibility.whenCanBorrow: TryNextFlavor`.
- `ClusterQueue.spec.flavorFungibility.whenCanPreempt: TryNextFlavor`.
- Explicit `flavorFungibility.preference: BorrowingOverPreemption`.
- Different flavor order for production backfills, data-quality replay, and HPO sweeps.

## Production Notes

Training platforms need both reliable recovery and cheap experimentation. Flavor fungibility lets Kueue try the next ResourceFlavor before it borrows quota from a peer or preempts already admitted work.

This repo uses stability-first flavor ordering for production backfills, spot-first ordering for data-quality replay, and spot-first GPU ordering for HPO sweeps. The policy makes the trade-off inspectable in code: production partitions protect recovery, replay jobs optimize cost, and experiments use fallback capacity without becoming the scheduling default.

## References

- Kueue ClusterQueue: <https://kueue.sigs.k8s.io/docs/concepts/cluster_queue/>
- Kueue ResourceFlavor: <https://kueue.sigs.k8s.io/docs/concepts/resource_flavor/>
- Kueue FlavorFungibility API: <https://kueue.sigs.k8s.io/docs/reference/kueue.v1beta1/#flavorfungibility>
