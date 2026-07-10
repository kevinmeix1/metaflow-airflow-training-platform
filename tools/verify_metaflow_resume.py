from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from metaflow import Runner

from training_orchestration_platform.io import read_json
from training_orchestration_platform.metaflow_runtime import write_json_atomic
from verify_metaflow_run import verify_run


REUSABLE_STEPS = {
    "start",
    "extract",
    "validate",
    "train_candidate",
    "select_model",
}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def save_process_log(path: Path, *, stdout: str, stderr: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f"--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}",
        encoding="utf-8",
    )


def task_attempts(task: Any) -> list[int]:
    attempts = {
        int(metadata.value)
        for metadata in task.metadata
        if metadata.type == "attempt"
    }
    return sorted(attempts)


def verify_resume(
    *,
    flow_file: Path,
    output_root: Path,
    datastore_root: Path,
    partition: str,
    expected_candidates: int,
) -> dict[str, Any]:
    repository = flow_file.resolve().parents[1]
    base_environment = dict(os.environ)
    fault_environment = {
        **base_environment,
        "TRAINING_ENABLE_FAULT_INJECTION": "1",
        "TRAINING_FAULT_STEP": "publish",
    }
    resume_environment = dict(base_environment)
    resume_environment.pop("TRAINING_FAULT_STEP", None)
    resume_environment.pop("TRAINING_ENABLE_FAULT_INJECTION", None)

    with Runner(
        str(flow_file),
        show_output=False,
        env=fault_environment,
        cwd=str(repository),
    ).run(
        ds=partition,
        output=str(output_root),
        max_workers=expected_candidates,
    ) as failed_execution:
        failed_status = failed_execution.status
        failed_run = failed_execution.run
        failed_stdout = failed_execution.stdout
        failed_stderr = failed_execution.stderr

    save_process_log(
        output_root / "metaflow" / "fault_run.log",
        stdout=failed_stdout,
        stderr=failed_stderr,
    )
    require(failed_status == "failed", "fault-injected run unexpectedly succeeded")
    require(not failed_run.successful, "Metaflow marked the fault-injected run successful")
    failed_publish_tasks = list(failed_run["publish"])
    require(len(failed_publish_tasks) == 1, "failed run must have one publish task")
    attempts = task_attempts(failed_publish_tasks[0])
    require(
        len(attempts) == 3,
        f"publish should exhaust one attempt and two retries, observed {attempts}",
    )

    with Runner(
        str(flow_file),
        show_output=False,
        env=resume_environment,
        cwd=str(repository),
    ).resume(
        origin_run_id=str(failed_run.id),
        max_workers=expected_candidates,
    ) as resumed_execution:
        resumed_status = resumed_execution.status
        resumed_run = resumed_execution.run
        resumed_stdout = resumed_execution.stdout
        resumed_stderr = resumed_execution.stderr

    save_process_log(
        output_root / "metaflow" / "resume_run.log",
        stdout=resumed_stdout,
        stderr=resumed_stderr,
    )
    require(resumed_status == "successful", "resumed Metaflow run failed")
    require(resumed_run.successful, "Metaflow did not mark the resumed run successful")
    require(str(resumed_run.id) != str(failed_run.id), "resume reused the failed run ID")

    cloned_pathspecs: list[str] = []
    for step_name in REUSABLE_STEPS:
        for task in resumed_run[step_name]:
            require(task.origin_pathspec is not None, f"{step_name} was recomputed")
            cloned_pathspecs.append(str(task.origin_pathspec))
    expected_origin = f"DemandTrainingFlow/{failed_run.id}/"
    require(
        all(pathspec.startswith(expected_origin) for pathspec in cloned_pathspecs),
        "a cloned task came from a run other than the injected failure",
    )
    for step_name in ("publish", "end"):
        require(
            all(task.origin_pathspec is None for task in resumed_run[step_name]),
            f"{step_name} should execute fresh during resume",
        )

    runtime_verification = verify_run(
        output_root=output_root,
        datastore_root=datastore_root,
        expected_candidates=expected_candidates,
    )
    contract_path = (
        output_root
        / "metaflow"
        / "runs"
        / str(resumed_run.id)
        / "runtime_contract.json"
    )
    contract = read_json(contract_path)
    require(contract["execution"]["resumed"], "contract does not record resume")
    require(
        str(contract["execution"]["origin_run_id"]) == str(failed_run.id),
        "contract origin run differs from the injected failure",
    )
    require(
        contract["execution"]["failure_injection_step"] == "publish",
        "contract lost the original failure-injection boundary",
    )

    report = {
        "passed": True,
        "verified_at": datetime.now(timezone.utc).isoformat(),
        "partition": partition,
        "failed_run_id": str(failed_run.id),
        "resumed_run_id": str(resumed_run.id),
        "failed_step": "publish",
        "failed_publish_attempts": attempts,
        "cloned_task_count": len(cloned_pathspecs),
        "fresh_steps": ["publish", "end"],
        "selected_candidate": contract["selected_candidate"],
        "registration_idempotency_key": contract[
            "registration_idempotency_key"
        ],
        "runtime_verification": {
            "step_count": runtime_verification["step_count"],
            "task_count": runtime_verification["task_count"],
            "card_count": runtime_verification["card_count"],
        },
        "checks": [
            "publish retry exhaustion",
            "explicit origin run selection",
            "upstream task cloning",
            "fresh publish and end execution",
            "resume lineage in runtime contract",
            "post-resume artifact and card verification",
        ],
    }
    write_json_atomic(output_root / "metaflow" / "resume_verification.json", report)
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inject a Metaflow failure, resume it, and verify cloned lineage"
    )
    parser.add_argument(
        "--flow",
        type=Path,
        default=Path("metaflow_flows/demand_training_flow.py"),
    )
    parser.add_argument("--output", type=Path, default=Path(".local"))
    parser.add_argument("--datastore-root", type=Path, required=True)
    parser.add_argument("--partition", default="2026-06-09")
    parser.add_argument("--expected-candidates", type=int, default=4)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = verify_resume(
        flow_file=args.flow.expanduser().resolve(),
        output_root=args.output.expanduser().resolve(),
        datastore_root=args.datastore_root.expanduser().resolve(),
        partition=args.partition,
        expected_candidates=args.expected_candidates,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
