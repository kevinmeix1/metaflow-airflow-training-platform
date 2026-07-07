# Network Security

The training platform treats Airflow as the entrypoint and keeps batch workers on an allow list. Training pods can reach MLflow and object storage, but validators and arbitrary jobs cannot promote models or query orchestration databases directly.

Run:

```bash
make network-security
```

The report is written to `.local/reports/network_security.json`.

## Controls

- Default deny for ingress and egress.
- Explicit DNS egress allow.
- Training worker egress to MLflow tracking and object storage only.
- Airflow ingress to training workers for launch and monitoring.
- Strict mTLS and AuthorizationPolicy for worker-launch traffic.
- Direct validator-to-registry and worker-to-Airflow-DB paths denied by default.

## References

Kubernetes NetworkPolicy provides namespace traffic isolation once policies select pods. DNS must be separately allowed under default-deny egress. Istio strict mTLS and authorization policies add identity-aware service boundaries on top of layer-4 policies.
