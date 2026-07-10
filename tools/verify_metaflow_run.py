from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import metaflow
from metaflow import Flow, namespace

from training_orchestration_platform.io import read_json, read_jsonl
from training_orchestration_platform.metaflow_runtime import (
    canonical_hash,
    select_candidate,
    write_json_atomic,
)
from training_orchestration_platform.orchestrator import run_log_path


EXPECTED_STEPS = {
    "start",
    "extract",
    "validate",
    "train_candidate",
    "select_model",
    "publish",
    "end",
}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def task_list(step: Any) -> list[Any]:
    return list(step)


def verify_run(
    *,
    output_root: Path,
    datastore_root: Path,
    expected_candidates: int,
) -> dict[str, Any]:
    namespace(None)
    run = Flow("DemandTrainingFlow").latest_run
    require(run is not None, "DemandTrainingFlow has no local runs")
    require(run.successful, f"latest Metaflow run {run.id} did not succeed")

    steps = {step.id: task_list(step) for step in run}
    require(set(steps) == EXPECTED_STEPS, f"unexpected step graph: {sorted(steps)}")
    require(
        all(len(tasks) == 1 for name, tasks in steps.items() if name != "train_candidate"),
        "non-foreach steps must each have exactly one task",
    )
    train_tasks = steps["train_candidate"]
    require(
        len(train_tasks) == expected_candidates,
        f"expected {expected_candidates} candidate tasks, found {len(train_tasks)}",
    )

    candidate_results = [task.data.candidate_result for task in train_tasks]
    candidate_names = [str(item["name"]) for item in candidate_results]
    require(
        len(set(candidate_names)) == expected_candidates,
        f"candidate names are not unique: {candidate_names}",
    )
    expected_selection = select_candidate(candidate_results)

    end_result = steps["end"][0].data.result
    contract_path = output_root / "metaflow" / "latest.json"
    require(contract_path.exists(), f"missing runtime contract: {contract_path}")
    contract = read_json(contract_path)
    require(contract == end_result, "end-step artifact and published contract differ")
    require(str(contract["metaflow_run_id"]) == str(run.id), "contract run id is stale")
    require(contract["candidate_count"] == expected_candidates, "candidate count differs")
    require(
        contract["selected_candidate"] == expected_selection["name"],
        "published candidate is not the deterministic gate-aware winner",
    )
    require(
        contract["selected_config_digest"] == expected_selection["config_digest"],
        "selected configuration digest differs",
    )
    require(
        contract["registration_idempotency_key"]
        == canonical_hash(
            {
                "ds": contract["partition"],
                "input_manifest": contract["input_manifest"]["content_sha256"],
                "candidate_config": contract["selected_config_digest"],
                "runtime_contract": contract["contract_version"],
            }
        ),
        "registration idempotency key is not reproducible",
    )

    artifact_paths = {}
    for name, relative in contract["artifacts"].items():
        relative_path = Path(relative)
        require(not relative_path.is_absolute(), f"absolute {name} artifact path")
        require(".." not in relative_path.parts, f"unsafe {name} artifact path")
        path = (output_root / relative_path).resolve()
        require(path.is_file(), f"missing {name} artifact: {path}")
        require(path.is_relative_to(output_root), f"artifact escaped output root: {path}")
        artifact_paths[name] = path

    model = read_json(artifact_paths["model"])
    require(canonical_hash(model) == contract["model_digest"], "model digest differs")
    comparison = read_json(artifact_paths["candidate_comparison"])
    require(
        {item["name"] for item in comparison} == set(candidate_names),
        "candidate comparison is incomplete",
    )

    source_path = Path(contract["input_manifest"]["path"])
    require(source_path.is_file(), f"missing source partition: {source_path}")
    require(
        sha256_file(source_path) == contract["input_manifest"]["content_sha256"],
        "source partition digest differs",
    )

    card_root = (
        datastore_root
        / ".metaflow"
        / "DemandTrainingFlow"
        / "runs"
        / str(run.id)
        / "steps"
    )
    card_paths = sorted(card_root.glob("*/tasks/*/cards/*.html"))
    require(
        len(card_paths) == contract["card_count"],
        f"expected {contract['card_count']} cards, found {len(card_paths)}",
    )
    require(all(path.stat().st_size > 500 for path in card_paths), "a card is empty")
    require(
        sum("candidate" in path.name for path in card_paths) == expected_candidates,
        "candidate card count differs",
    )
    require(
        sum("selection" in path.name for path in card_paths) == 1,
        "selection card is missing",
    )

    run_events = [
        item
        for item in read_jsonl(run_log_path(output_root))
        if item.get("run_id") == f"metaflow-{run.id}"
    ]
    require(len(run_events) == 1, "runtime publication is not idempotent in run history")

    verification = {
        "passed": True,
        "verified_at": datetime.now(timezone.utc).isoformat(),
        "metaflow_version": metaflow.__version__,
        "flow": "DemandTrainingFlow",
        "run_id": str(run.id),
        "task_count": sum(len(tasks) for tasks in steps.values()),
        "step_count": len(steps),
        "candidate_count": len(candidate_results),
        "candidate_names": sorted(candidate_names),
        "passing_candidate_count": sum(
            1 for item in candidate_results if item["gates"]["passed"]
        ),
        "selected_candidate": contract["selected_candidate"],
        "selected_model_version": contract["selected_model_version"],
        "registration_idempotency_key": contract["registration_idempotency_key"],
        "card_count": len(card_paths),
        "model_digest": contract["model_digest"],
        "checks": [
            "successful run",
            "exact step graph",
            "bounded foreach fan-out",
            "gate-aware deterministic selection",
            "artifact and source digests",
            "registration idempotency",
            "rendered Metaflow cards",
        ],
    }
    write_json_atomic(output_root / "metaflow" / "verification.json", verification)
    return verification


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify the latest executable Metaflow training run."
    )
    parser.add_argument("--output", type=Path, default=Path(".local"))
    parser.add_argument("--datastore-root", type=Path, required=True)
    parser.add_argument("--expected-candidates", type=int, default=4)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    verification = verify_run(
        output_root=args.output.expanduser().resolve(),
        datastore_root=args.datastore_root.expanduser().resolve(),
        expected_candidates=args.expected_candidates,
    )
    print(json.dumps(verification, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
