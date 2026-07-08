# DRA Advanced Device Sharing For Training

`make advanced-device-sharing` writes `.local/reports/advanced_device_sharing_plan.json` and pairs it with `kubernetes/dra-advanced-device-sharing.yaml`.

## What It Shows

- DRA prioritized device alternatives for feature-heavy backfills.
- Partitionable devices for exploratory HPO sweeps instead of whole-device reservations.
- Consumable capacity examples for bounded GPU memory during HPO.
- Device binding conditions that delay scheduler binding until memory-bound training devices are prepared.

## Production Notes

Backfills should be deterministic even when accelerator capacity changes. A prioritized list lets the platform try isolated MIG, then shared L4, then CPU baseline replay without hiding the selected path from MLflow or lineage evidence.

Partitionable devices and consumable capacity keep exploratory HPO from starving production backfills. Binding conditions turn fabric-attached accelerator preparation into an explicit scheduler gate, so preparation failures become targeted partition recovery decisions.

## References

- Kubernetes v1.36 DRA update: <https://kubernetes.io/blog/2026/05/07/kubernetes-v1-36-dra-136-updates/>
- Kubernetes Dynamic Resource Allocation: <https://kubernetes.io/docs/concepts/scheduling-eviction/dynamic-resource-allocation/>
- Kubernetes DRA consumable capacity: <https://kubernetes.io/blog/2025/09/18/kubernetes-v1-34-dra-consumable-capacity/>
