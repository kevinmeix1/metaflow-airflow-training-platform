# GitOps Promotion

This project models GitOps promotion for a batch training mesh. The release gates focus on partition dry-runs, lineage, backfill capacity, and artifact latency rather than live request traffic.

Run:

```bash
make gitops-plan
```

The report is written to `.local/reports/gitops_plan.json`.

## Design

- Apply namespace security and Kueue/Airflow capacity before training templates.
- Use pre-sync hooks for partition dry-runs and lineage checks.
- Use post-sync analysis for a small smoke backfill before broader catchup.
- Keep production sync manual and evidence-driven.
- Use Argo Rollouts for the training control-plane image, not for individual ephemeral jobs.

## References

Argo CD sync waves and hooks order resources and checks. Automated sync/self-heal is useful in lower environments, while production promotion can remain manual. Argo Rollouts AnalysisTemplates can evaluate backfill SLOs before full rollout.
