from __future__ import annotations

import json
import os
from pathlib import Path

from metaflow import (
    FlowSpec,
    JSONType,
    Parameter,
    card,
    current,
    resources,
    retry,
    schedule,
    step,
    timeout,
)
from metaflow.cards import Markdown, Table

from training_orchestration_platform.data import generate_partition, validate_rows
from training_orchestration_platform.io import read_csv
from training_orchestration_platform.metaflow_runtime import (
    DEFAULT_CANDIDATE_GRID,
    RUNTIME_CONTRACT_VERSION,
    evaluate_candidate,
    normalize_candidate_grid,
    publish_runtime_artifacts,
    select_candidate,
    write_json_atomic,
)
from training_orchestration_platform.orchestrator import file_sha256, split_rows


STEP_CONTRACT = [
    "start",
    "extract",
    "validate",
    "train_candidate",
    "select_model",
    "publish",
    "end",
]


@schedule(daily=True)
class DemandTrainingFlow(FlowSpec):
    """Executable daily demand model-selection flow.

    Airflow owns the production schedule and partition policy. Metaflow owns
    artifact lineage, candidate fan-out, retry-safe training, selection, and
    publication of an immutable model result.
    """

    ds = Parameter("ds", default="2026-06-08", help="Training partition date")
    output = Parameter(
        "output",
        default=".local",
        help="Local artifact root used by the portfolio runtime",
    )
    candidates = Parameter(
        "candidates",
        type=JSONType,
        default=json.dumps(DEFAULT_CANDIDATE_GRID),
        help="Bounded JSON candidate grid used for foreach model selection",
    )

    @timeout(seconds=30)
    @step
    def start(self) -> None:
        self.partition = str(self.ds)
        self.output_root = str(Path(str(self.output)).expanduser().resolve())
        self.candidate_configs = normalize_candidate_grid(self.candidates)
        self.runtime_contract_version = RUNTIME_CONTRACT_VERSION
        failure_step = os.getenv("TRAINING_FAULT_STEP", "").strip()
        fault_enabled = os.getenv("TRAINING_ENABLE_FAULT_INJECTION", "").strip() == "1"
        if failure_step not in {"", "publish"}:
            raise ValueError(f"unsupported training fault step: {failure_step}")
        if failure_step and not fault_enabled:
            raise ValueError("training fault injection requires explicit opt-in")
        self.failure_injection_step = failure_step or None
        self.next(self.extract)

    @resources(cpu=1, memory=512)
    @retry(times=2, minutes_between_retries=0)
    @timeout(seconds=60)
    @step
    def extract(self) -> None:
        raw_path = (
            Path(self.output_root)
            / "data"
            / "raw"
            / f"ds={self.partition}"
            / "sales.csv"
        )
        generate_partition(raw_path, self.partition)
        self.rows = read_csv(raw_path)
        self.manifest = {
            "partition": self.partition,
            "path": str(raw_path),
            "content_sha256": file_sha256(raw_path),
            "idempotency_key": f"daily-demand:{self.partition}",
        }
        write_json_atomic(
            Path(self.output_root)
            / "data"
            / "manifests"
            / f"metaflow-ds={self.partition}.json",
            self.manifest,
        )
        self.next(self.validate)

    @resources(cpu=1, memory=512)
    @retry(times=0)
    @timeout(seconds=30)
    @step
    def validate(self) -> None:
        self.validation = validate_rows(self.rows)
        if not self.validation["passed"]:
            raise ValueError("partition failed the demand training data contract")
        self.train_rows, self.eval_rows = split_rows(self.rows)
        self.next(self.train_candidate, foreach="candidate_configs")

    @card(type="blank", id="candidate")
    @resources(cpu=1, memory=1024)
    @retry(times=2, minutes_between_retries=0)
    @timeout(seconds=60)
    @step
    def train_candidate(self) -> None:
        self.candidate_result = evaluate_candidate(
            ds=self.partition,
            config=self.input,
            train_rows=self.train_rows,
            eval_rows=self.eval_rows,
            validation=self.validation,
        )
        current.card["candidate"].append(
            Markdown(f"## Candidate: {self.candidate_result['name']}")
        )
        current.card["candidate"].append(
            Table(
                headers=["Metric", "Observed", "Gate"],
                data=[
                    [
                        "RMSE",
                        str(self.candidate_result["metrics"]["rmse"]),
                        "PASS" if self.candidate_result["gates"]["passed"] else "FAIL",
                    ],
                    [
                        "MAPE",
                        str(self.candidate_result["metrics"]["mape"]),
                        "PASS" if self.candidate_result["gates"]["passed"] else "FAIL",
                    ],
                    [
                        "Config digest",
                        self.candidate_result["config_digest"][:12],
                        "immutable",
                    ],
                ],
            )
        )
        self.next(self.select_model)

    @timeout(seconds=30)
    @step
    def select_model(self, inputs) -> None:
        first = inputs[0]
        self.partition = first.partition
        self.output_root = first.output_root
        self.manifest = first.manifest
        self.validation = first.validation
        self.runtime_contract_version = first.runtime_contract_version
        self.failure_injection_step = first.failure_injection_step
        self.candidate_results = [item.candidate_result for item in inputs]
        self.selected_candidate = select_candidate(self.candidate_results)
        self.next(self.publish)

    @card(type="blank", id="selection")
    @resources(cpu=1, memory=512)
    @retry(times=2, minutes_between_retries=0)
    @timeout(seconds=30)
    @step
    def publish(self) -> None:
        if (
            os.getenv("TRAINING_ENABLE_FAULT_INJECTION", "").strip() == "1"
            and os.getenv("TRAINING_FAULT_STEP", "").strip() == "publish"
        ):
            raise RuntimeError("injected publish failure for resume contract")
        self.runtime_contract = publish_runtime_artifacts(
            root=self.output_root,
            ds=self.partition,
            metaflow_run_id=str(current.run_id),
            metaflow_pathspec=str(current.pathspec),
            manifest=self.manifest,
            validation=self.validation,
            candidates=self.candidate_results,
            selected=self.selected_candidate,
            step_contract=STEP_CONTRACT,
            card_count=len(self.candidate_results) + 1,
            runtime_contract_version=self.runtime_contract_version,
            metaflow_origin_run_id=(
                str(current.origin_run_id) if current.origin_run_id else None
            ),
            publish_retry_count=int(current.retry_count),
            failure_injection_step=self.failure_injection_step,
        )
        current.card["selection"].append(Markdown("## Selected model"))
        current.card["selection"].append(
            Table(
                headers=["Field", "Value"],
                data=[
                    ["Candidate", self.runtime_contract["selected_candidate"]],
                    ["Model", self.runtime_contract["selected_model_version"]],
                    ["Candidates", str(self.runtime_contract["candidate_count"])],
                    [
                        "Registration key",
                        self.runtime_contract["registration_idempotency_key"][:16],
                    ],
                ],
            )
        )
        self.next(self.end)

    @step
    def end(self) -> None:
        self.result = self.runtime_contract
        print(
            json.dumps(
                {
                    "metaflow_run_id": self.result["metaflow_run_id"],
                    "partition": self.result["partition"],
                    "selected_candidate": self.result["selected_candidate"],
                    "selected_model_version": self.result["selected_model_version"],
                    "candidate_count": self.result["candidate_count"],
                    "registration_idempotency_key": self.result[
                        "registration_idempotency_key"
                    ],
                },
                sort_keys=True,
            )
        )


if __name__ == "__main__":
    DemandTrainingFlow()
