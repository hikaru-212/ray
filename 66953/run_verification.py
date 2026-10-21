"""Portable launcher for the two-arm PR #66953 verification harness.

This helper was derived after the recorded experiment and was not used to
produce observed/*.json. It refuses to overwrite an existing output directory.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    binary = args.binary.resolve(strict=True)
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=False)
    logs = output / "logs"
    run_tmp = output / "tmp"
    logs.mkdir()
    run_tmp.mkdir()

    base_env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("RAY_", "P1_", "GRPC_", "GTEST_"))
    }
    base_env.update(
        TMPDIR=str(run_tmp),
        TEST_TMPDIR=str(run_tmp),
        RAY_BACKEND_LOG_LEVEL="debug",
        RAY_USAGE_STATS_ENABLED="0",
    )
    command = [
        str(binary),
        "--gtest_filter=ObjectManagerTest.P1RealDeadlineRestoresQueuedPushCapacity",
        "--gtest_color=no",
    ]
    results = []
    for label, deadline in (("finite", "3000"), ("disabled", "-1")):
        env = dict(base_env, P1_PUSH_DEADLINE_MS=deadline)
        log_path = logs / f"{label}.log"
        with log_path.open("x") as log:
            started = time.monotonic()
            process = subprocess.Popen(
                command,
                cwd=output,
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            timed_out = False
            try:
                returncode = process.wait(timeout=45)
            except subprocess.TimeoutExpired:
                timed_out = True
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    returncode = process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    returncode = process.wait(timeout=5)
            elapsed = time.monotonic() - started
        text = log_path.read_text()
        result = {
            "arm": label,
            "deadline_ms": int(deadline),
            "returncode": returncode,
            "watchdog_fired": timed_out,
            "elapsed_s": elapsed,
            "pass_marker": "event=PASS " in text,
            "cleanup_marker": "event=cleanup_complete " in text,
            "log": str(log_path),
        }
        results.append(result)
        print(json.dumps(result), flush=True)

    summary = {
        "binary": str(binary),
        "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "command": command,
        "only_arm_environment_difference": "P1_PUSH_DEADLINE_MS",
        "results": results,
    }
    (output / "results.json").write_text(json.dumps(summary, indent=2) + "\n")
    assert all(
        item["returncode"] == 0
        and item["pass_marker"]
        and item["cleanup_marker"]
        and not item["watchdog_fired"]
        for item in results
    ), results


if __name__ == "__main__":
    main()
