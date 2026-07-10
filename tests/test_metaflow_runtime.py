from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from training_orchestration_platform.data import generate_partition, validate_rows
from training_orchestration_platform.io import read_csv, read_json, read_jsonl
from training_orchestration_platform.metaflow_runtime import (
    DEFAULT_CANDIDATE_GRID,
    CandidateSelectionError,
    RUNTIME_CONTRACT_VERSION,
    canonical_hash,
    evaluate_candidate,
    normalize_candidate_grid,
    publish_runtime_artifacts,
    select_candidate,
)
from training_orchestration_platform.model import train_forecaster
from training_orchestration_platform.orchestrator import run_log_path, split_rows


class MetaflowRuntimeTest(unittest.TestCase):
    def candidates(self, root: Path) -> tuple[list[dict], dict]:
        partition_path = root / "sales.csv"
        generate_partition(partition_path, "2026-06-08")
        rows = read_csv(partition_path)
        validation = validate_rows(rows)
        train_rows, eval_rows = split_rows(rows)
        results = [
            evaluate_candidate(
                ds="2026-06-08",
                config=config,
                train_rows=train_rows,
                eval_rows=eval_rows,
                validation=validation,
            )
            for config in DEFAULT_CANDIDATE_GRID
        ]
        return results, validation

    def test_candidate_grid_rejects_invalid_and_duplicate_entries(self) -> None:
        with self.assertRaisesRegex(ValueError, "between 2 and 12"):
            normalize_candidate_grid([DEFAULT_CANDIDATE_GRID[0]])
        with self.assertRaisesRegex(ValueError, "duplicate candidate"):
            normalize_candidate_grid(
                [DEFAULT_CANDIDATE_GRID[0], DEFAULT_CANDIDATE_GRID[0]]
            )
        with self.assertRaisesRegex(ValueError, "invalid candidate name"):
            normalize_candidate_grid(
                [DEFAULT_CANDIDATE_GRID[0], {"name": "Not Valid"}]
            )
        with self.assertRaisesRegex(ValueError, "must be a boolean"):
            normalize_candidate_grid(
                [
                    DEFAULT_CANDIDATE_GRID[0],
                    {"name": "bad-bool", "inventory_cap_enabled": "yes"},
                ]
            )

    def test_selection_is_deterministic_and_requires_a_passing_gate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            candidates, _ = self.candidates(Path(tmp))
        passing = [item for item in candidates if item["gates"]["passed"]]
        expected = min(
            passing,
            key=lambda item: (
                item["metrics"]["rmse"],
                item["metrics"]["mape"],
                item["metrics"]["max_sku_mae"],
                item["name"],
            ),
        )
        self.assertEqual(select_candidate(list(reversed(candidates))), expected)
        with self.assertRaises(CandidateSelectionError):
            select_candidate(
                [{**item, "gates": {"passed": False}} for item in candidates]
            )

    def test_runtime_publication_is_idempotent_and_content_addressed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            candidates, validation = self.candidates(root)
            selected = select_candidate(candidates)
            manifest = {
                "path": str(root / "sales.csv"),
                "content_sha256": "a" * 64,
                "idempotency_key": "daily-demand:2026-06-08",
            }
            kwargs = {
                "root": root,
                "ds": "2026-06-08",
                "metaflow_run_id": "42",
                "metaflow_pathspec": "DemandTrainingFlow/42/publish/9",
                "manifest": manifest,
                "validation": validation,
                "candidates": candidates,
                "selected": selected,
                "step_contract": ["start", "train_candidate", "end"],
                "card_count": 5,
                "runtime_contract_version": RUNTIME_CONTRACT_VERSION,
                "metaflow_origin_run_id": None,
                "publish_retry_count": 0,
                "failure_injection_step": None,
            }
            first = publish_runtime_artifacts(**kwargs)
            second = publish_runtime_artifacts(**kwargs)

            self.assertEqual(first, second)
            self.assertEqual(first["model_digest"], canonical_hash(selected["model"]))
            self.assertFalse(Path(first["artifacts"]["model"]).is_absolute())
            self.assertTrue((root / first["artifacts"]["model"]).exists())
            self.assertEqual(list(root.rglob("*.tmp")), [])
            matching_events = [
                row
                for row in read_jsonl(run_log_path(root))
                if row["run_id"] == "metaflow-42"
            ]
            self.assertEqual(len(matching_events), 1)
            self.assertEqual(
                read_json(root / "metaflow" / "latest.json")[
                    "registration_idempotency_key"
                ],
                first["registration_idempotency_key"],
            )
            self.assertEqual(
                first["execution"],
                {
                    "resumed": False,
                    "origin_run_id": None,
                    "publish_retry_count": 0,
                    "failure_injection_step": None,
                },
            )
            mlflow_run = read_json(
                root
                / "mlruns"
                / "daily-demand-forecasting"
                / "metaflow-42"
                / "run.json"
            )
            self.assertEqual(mlflow_run["execution"], first["execution"])

            with self.assertRaisesRegex(RuntimeError, "publication contract changed"):
                publish_runtime_artifacts(
                    **{
                        **kwargs,
                        "metaflow_pathspec": "DemandTrainingFlow/42/publish/changed",
                    }
                )

    def test_runtime_publication_records_resume_lineage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            candidates, validation = self.candidates(root)
            selected = select_candidate(candidates)
            contract = publish_runtime_artifacts(
                root=root,
                ds="2026-06-08",
                metaflow_run_id="43",
                metaflow_pathspec="DemandTrainingFlow/43/publish/19",
                manifest={
                    "path": str(root / "sales.csv"),
                    "content_sha256": "b" * 64,
                    "idempotency_key": "daily-demand:2026-06-08",
                },
                validation=validation,
                candidates=candidates,
                selected=selected,
                step_contract=["start", "train_candidate", "end"],
                card_count=5,
                runtime_contract_version=RUNTIME_CONTRACT_VERSION,
                metaflow_origin_run_id="41",
                publish_retry_count=1,
                failure_injection_step="publish",
            )

            self.assertEqual(
                contract["execution"],
                {
                    "resumed": True,
                    "origin_run_id": "41",
                    "publish_retry_count": 1,
                    "failure_injection_step": "publish",
                },
            )

    def test_model_candidate_parameters_are_bounded(self) -> None:
        rows = [
            {
                "sku": sku,
                "units_sold": 20 + index,
                "promo": index % 2,
            }
            for sku in ["coffee", "tea", "juice", "water", "snack"]
            for index in range(2)
        ]
        model = train_forecaster(
            rows,
            version="candidate-v1",
            price_coefficient=-1.7,
            promo_lift_scale=1.3,
            inventory_cap_enabled=False,
        )
        self.assertEqual(model["training_config"]["promo_lift_scale"], 1.3)
        self.assertFalse(model["inventory_cap_enabled"])
        with self.assertRaisesRegex(ValueError, "price_coefficient"):
            train_forecaster(rows, version="bad", price_coefficient=1.0)
        with self.assertRaisesRegex(ValueError, "promo_lift_scale"):
            train_forecaster(rows, version="bad", promo_lift_scale=4.0)
        with self.assertRaisesRegex(ValueError, "inventory_cap_enabled"):
            train_forecaster(rows, version="bad", inventory_cap_enabled="yes")
        with self.assertRaisesRegex(ValueError, "missing SKUs"):
            train_forecaster(rows[:-2], version="bad")
        with self.assertRaisesRegex(ValueError, "cannot be empty"):
            train_forecaster([], version="bad")
