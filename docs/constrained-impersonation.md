# Constrained Impersonation

`make constrained-impersonation` writes `.local/reports/constrained_impersonation_plan.json`.

## What It Shows

- Kubernetes v1.36 `ConstrainedImpersonation` beta behavior.
- Separate authorization for the impersonated service account identity and the actions performed while impersonating.
- Airflow support and backfill workflows that can inspect training partitions without broad trainer authority.
- Audit expectations for `authenticationMetadata.impersonationConstraint`.
- Alerts for legacy broad `impersonate` grants that bypass least-privilege intent.

## Production Notes

Training platforms often need support tools that inspect partitioned Jobs,
recover failed backfills, or patch status. Constrained impersonation prevents
those tools from inheriting the full trainer identity by requiring both
`impersonate:serviceaccount` and scoped `impersonate-on:serviceaccount:<verb>`
permissions.

This keeps Airflow debugging and partition-status recovery narrow even when the
target training identity has broader production permissions.

References:

- https://kubernetes.io/blog/2026/04/22/kubernetes-v1-36-release/
- https://kubernetes.io/docs/reference/access-authn-authz/user-impersonation/
