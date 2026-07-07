from __future__ import annotations

"""
Metaflow sketch for the daily demand training flow.

Run in a full environment with:
    python metaflow_flows/demand_training_flow.py run --ds 2026-06-01
"""

try:
    from metaflow import FlowSpec, Parameter, step
except Exception:
    class FlowSpec:  # type: ignore
        pass

    class Parameter:  # type: ignore
        def __init__(self, *args, **kwargs):
            pass

    def step(func):  # type: ignore
        return func


class DemandTrainingFlow(FlowSpec):
    ds = Parameter("ds", default="2026-06-01")

    @step
    def start(self):
        from pathlib import Path
        from training_orchestration_platform.orchestrator import run_partition

        self.result = run_partition(Path(".local"), str(self.ds), force=True)
        self.next(self.end)

    @step
    def end(self):
        print(self.result)


if __name__ == "__main__":
    DemandTrainingFlow()
