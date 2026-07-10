from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


PACKAGE_NAME = "metaflow-airflow-training-platform"


def smoke_wheel(dist: Path) -> dict[str, str | bool]:
    wheels = sorted(dist.glob("*.whl"))
    if len(wheels) != 1:
        raise RuntimeError(f"expected exactly one wheel in {dist}, found {len(wheels)}")
    wheel = wheels[0].resolve()
    probe_code = (
        "import json, sys; "
        f"sys.path.insert(0, {str(wheel)!r}); "
        "from importlib.metadata import version; "
        "from training_orchestration_platform import __version__; "
        f"print(json.dumps({{'metadata': version({PACKAGE_NAME!r}), "
        "'package': __version__}, sort_keys=True))"
    )
    probe = subprocess.run(
        [sys.executable, "-I", "-S", "-c", probe_code],
        check=True,
        capture_output=True,
        text=True,
    )
    versions = json.loads(probe.stdout)
    if len(set(versions.values())) != 1:
        raise RuntimeError(f"wheel version contract is inconsistent: {versions}")
    return {
        "passed": True,
        "wheel": wheel.name,
        "version": versions["metadata"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Import the built wheel with site packages disabled"
    )
    parser.add_argument("--dist", type=Path, default=Path("dist"))
    args = parser.parse_args()
    print(json.dumps(smoke_wheel(args.dist), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
