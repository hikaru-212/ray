"""Portable launcher for one PR #67180 source variant.

This helper was derived after the recorded experiment and was not used to
produce observed/*. It verifies the source revision and refuses to overwrite
either output directory.
"""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time


EXPECTED_REVISIONS = {
    "head": "817d5f128c7e3905f87ca39a2d1e918d8c23ebb1",
    "baseline": "6a3ba37e9e09803703b06787cdab6b085581788c",
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--label", choices=EXPECTED_REVISIONS, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--ray-temp-dir", type=Path, required=True)
    parser.add_argument("--python", default=sys.executable)
    args = parser.parse_args()

    evidence = Path(__file__).resolve().parent
    source = args.source.resolve(strict=True)
    output = args.output_dir.resolve()
    ray_tmp = args.ray_temp_dir.resolve()
    output.mkdir(parents=True, exist_ok=False)
    if ray_tmp.exists():
        raise FileExistsError(f"Ray temp path already exists: {ray_tmp}")
    if len(str(ray_tmp)) >= 60:
        raise ValueError("Choose a shorter --ray-temp-dir to avoid AF_UNIX limits")

    revision = subprocess.run(
        ["git", "-C", str(source), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    expected = EXPECTED_REVISIONS[args.label]
    if revision != expected:
        raise RuntimeError(f"expected {expected}, found {revision}")

    env = dict(
        os.environ,
        PYTHONPATH=str(evidence),
        PYTHONDONTWRITEBYTECODE="1",
        PYTEST_DISABLE_PLUGIN_AUTOLOAD="1",
        PHASE2_SOURCE=str(source),
        PHASE2_LABEL=args.label,
        PHASE2_RUN_DIR=str(output),
        PHASE2_RAY_TMP=str(ray_tmp),
        RAY_USAGE_STATS_ENABLED="0",
        RAY_DATA_PARQUET_FOOTER_NUM_ACTORS="1",
        RAY_ENABLE_WINDOWS_OR_OSX_CLUSTER="1",
        TMPDIR=str(output),
    )
    command = [
        args.python,
        "-m",
        "pytest",
        "-p",
        "pytest_timeout",
        "-vv",
        "-s",
        "--timeout=120",
        f"--confcutdir={evidence}",
        f"--basetemp={output / 'pytest-tmp'}",
        "-p",
        "no:cacheprovider",
        f"--junitxml={output / 'junit.xml'}",
        str(evidence / "test_post_output_failure.py"),
    ]
    (output / "command.json").write_text(
        json.dumps(
            {
                "revision": revision,
                "command": command,
                "environment_overrides": {
                    key: env[key]
                    for key in (
                        "PYTHONPATH",
                        "PYTHONDONTWRITEBYTECODE",
                        "PYTEST_DISABLE_PLUGIN_AUTOLOAD",
                        "PHASE2_SOURCE",
                        "PHASE2_LABEL",
                        "PHASE2_RUN_DIR",
                        "PHASE2_RAY_TMP",
                        "RAY_USAGE_STATS_ENABLED",
                        "RAY_DATA_PARQUET_FOOTER_NUM_ACTORS",
                        "TMPDIR",
                    )
                },
            },
            indent=2,
        )
        + "\n"
    )
    started = time.monotonic()
    with (output / "pytest.log").open("x") as log:
        completed = subprocess.run(
            command,
            cwd=evidence,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
    (output / "pytest-exit.json").write_text(
        json.dumps(
            {
                "exit_code": completed.returncode,
                "elapsed_s": time.monotonic() - started,
            },
            indent=2,
        )
        + "\n"
    )
    if completed.returncode:
        print((output / "pytest.log").read_text()[-14000:])
        raise SystemExit(completed.returncode)
    print(f"PASS {args.label}: {output}")


if __name__ == "__main__":
    main()
