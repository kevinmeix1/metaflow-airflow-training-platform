# Executable Metaflow Runtime

## Decision

The repository runs model selection as a native Metaflow 2.19.29 `FlowSpec` in
CI. The earlier two-step wrapper hid extraction, validation, candidate
selection, and publication inside one ordinary Python function, so Metaflow
could not provide useful retry boundaries or artifact lineage.

The executable flow now owns these state transitions:

```text
start -> extract -> validate -> train_candidate foreach(4)
      -> select_model -> publish -> end
```

Airflow remains the production scheduling boundary. The repository does not run
an Airflow scheduler, Kubernetes cluster, MLflow server, or remote object store
inside this local contract.

## Runtime Command

Install the exact tested dependency set:

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install \
  --constraint requirements-metaflow.lock \
  --editable ".[metaflow-runtime]"
```

Then execute and verify the contract:

```bash
make clean
make metaflow-runtime-contract METAFLOW_PYTHON=.venv/bin/python
```

The Make target configures an explicit local metadata and artifact root at
`.local/metaflow/datastore`. Metaflow task metadata and cards therefore do not
leak into an untracked repository-level `.metaflow` directory.

## Evidence Contract

`tools/verify_metaflow_run.py` fails unless all of the following are true:

1. The latest `DemandTrainingFlow` run succeeded.
2. The graph has exactly seven expected steps and ten tasks.
3. `train_candidate` produced four tasks with unique configurations.
4. The selected candidate passed all gates and is the deterministic minimum by
   RMSE, MAPE, maximum SKU MAE, and name tie-break.
5. The end-step Metaflow artifact equals the published runtime contract.
6. Source and model SHA-256 digests reproduce from stored bytes and canonical
   JSON.
7. Every artifact path is relative to the output root and exists.
8. Four candidate cards and one selection card were rendered and are non-empty.
9. The model registration key reproduces from immutable inputs.
10. Exactly one pipeline event exists for the Metaflow run.

The verifier writes `.local/metaflow/verification.json`. GitHub Actions uploads
the verification, datastore metadata, cards, candidate comparison, model,
dashboard, and MLflow-style local run record as one evidence artifact.

## Retry and Recovery Semantics

| Boundary | Policy | Reason |
| --- | --- | --- |
| Extract | Two retries and 60 second timeout | Filesystem or object-store failures can be transient |
| Validate | No retry and 30 second timeout | A broken data contract requires changed input or code |
| Candidate train | Two retries per branch and 60 second timeout | One candidate can retry without repeating siblings |
| Select | No retry and 30 second timeout | No passing model is a deterministic gate result |
| Publish | Two retries and 30 second timeout | Atomic writes and stable keys make retry safe |

Metaflow persists step artifacts, so a failed run can resume from the last
successful boundary. A production scheduler should retain the origin run ID,
partition, input digest, candidate digest, and registration key in incident
metadata.

`RUNTIME_CONTRACT_VERSION` is part of the registration key. Any change to model
semantics or artifact shape must bump that version; CI should review the bump
alongside its compatibility and replay plan.

The `metaflow-checkpoint` package is not included. It is a separate extension,
and its API is documented as subject to change. Step-boundary resume is enough
for this small deterministic model. Large GPU jobs could adopt checkpointing
after explicitly versioning and testing that extension.

## Airflow Export

Metaflow supports exporting a flow to an Airflow DAG for teams with an existing
Airflow control plane:

```bash
python metaflow_flows/demand_training_flow.py \
  --with retry airflow create generated/demand_training_dag.py
```

Production prerequisites:

- shared S3, Azure Blob, or Google Cloud Storage artifact datastore
- shared Metaflow metadata service
- worker identity with least-privilege object and metadata access
- a DAG distribution mechanism such as a versioned bundle or Git sync
- remote task resources configured through `@resources` and Kubernetes
- an explicit release process for the generated DAG
- a dispatcher that maps the Airflow data interval or asset partition into the
  exported `ds` parameter

Important constraints from Metaflow's current Airflow integration:

- local datastore export is rejected because Airflow workers need shared state
- `foreach` is supported, but nested `foreach` is not
- conditional and recursive steps introduced in newer Metaflow flows are not
  supported by the Airflow exporter
- the Metaflow Deployer API does not deploy Airflow DAGs; the generated Python
  file must be copied through the Airflow deployment path
- parameter defaults are deployment-time values; the local fixed `ds` default
  must not be reused as the partition for every scheduled run

This project therefore verifies the native flow and the Airflow 3.3 SDK DAG as
separate executable contracts. It does not pretend that local file execution is
a distributed Airflow deployment.

## Production Follow-Through

Before describing this as a production training service, add evidence for:

- remote artifact and metadata stores under workload identity
- an actual Airflow scheduler and Kubernetes worker smoke test
- transactional model registration with a unique idempotency key
- secrets, network policy, retention, and data classification enforcement
- OpenTelemetry correlation across Airflow, Metaflow, object storage, and MLflow
- a controlled failed-task resume and duplicate-promotion drill
- load and cost measurements from the target cluster

## Official References

- [Metaflow repository and releases](https://github.com/Netflix/metaflow)
- [Scheduling Metaflow flows with Airflow](https://docs.metaflow.org/production/scheduling-metaflow-flows/scheduling-with-airflow)
- [Metaflow step decorators](https://docs.metaflow.org/api/step-decorators)
- [Metaflow cards](https://docs.metaflow.org/metaflow/visualizing-results/easy-custom-reports-with-card-components)
- [Metaflow checkpointing extension](https://docs.metaflow.org/scaling/checkpoint/introduction)
- [Metaflow Deployer](https://docs.metaflow.org/metaflow/managing-flows/deployer)
