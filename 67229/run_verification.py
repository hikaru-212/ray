"""Run the portable Ray PR #67229 Parent/HEAD public-API matrix."""

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time


PARENT_SHA = "938ab39460b257374624479c6739fa80315939ea"
HEAD_SHA = "72f39680c62f3afe3ed437a74ae360417452f1d9"
SOURCE_HASHES = {
    "parent": "6fb9f00195fd791b2863dacb49436ac7343866a509fd93b23ca185e588a47983",
    "head": "e150f10107de3995201a4e591fbb3f4eec35533c9150f02844fa0e4bb641182f",
}


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--python", type=Path, default=Path(sys.executable))
    parser.add_argument("--parent-ray", type=Path, required=True)
    parser.add_argument("--head-ray", type=Path, required=True)
    parser.add_argument("--parent-native-commit")
    parser.add_argument("--head-native-commit")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--timeout", type=int, default=120)
    return parser.parse_args()


ARGS = parse_args()
ARGS.output_dir.mkdir(parents=True, exist_ok=False)
SCRIPT = Path(__file__).resolve().with_name("verify_none.py")
MATRIX = [
    ("parent", "omitted", "enabled", "completed", [0, 0]),
    ("parent", "zero", "enabled", "completed", [0, 0]),
    ("parent", "none", "enabled", "completed", [0, 0]),
    ("head", "omitted", "enabled", "completed", [0, 0]),
    ("head", "zero", "enabled", "completed", [0, 0]),
    ("head", "half", "enabled", "completed", [0.5, 0.5]),
    ("head", "none", "enabled", "error", []),
    ("head", "none", "disabled", "completed", [0, 0]),
]


def run_process(command, environment, timeout):
    process = subprocess.Popen(
        command,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    try:
        stdout, _ = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGTERM)
        try:
            stdout, _ = process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            stdout, _ = process.communicate()
        raise RuntimeError(f"timed out: {' '.join(map(str, command))}\n{stdout}")
    return process.returncode, stdout


results = []
for variant, case, reservation, expected_status, expected_gpu in MATRIX:
    run_id = f"{variant}-{case}-{reservation}"
    ray_root = (ARGS.parent_ray if variant == "parent" else ARGS.head_ray).resolve()
    revision = PARENT_SHA if variant == "parent" else HEAD_SHA
    expected_native = (
        ARGS.parent_native_commit if variant == "parent" else ARGS.head_native_commit
    )
    case_output = ARGS.output_dir / run_id
    environment = os.environ.copy()
    environment.update(
        {
            "PYTHONPATH": str(ray_root.parent),
            "PYTHONDONTWRITEBYTECODE": "1",
            "RAY_DATA_DEFAULT_SHUFFLE_STRATEGY": "hash_shuffle",
            "RAY_DATA_USE_OP_RESOURCE_ALLOCATOR_VERSION": "V1",
            "RAY_USAGE_STATS_ENABLED": "0",
            "OMP_NUM_THREADS": "1",
            "OPENBLAS_NUM_THREADS": "1",
            "POLARS_MAX_THREADS": "2",
        }
    )
    for name in [
        "RAY_ADDRESS",
        "RAY_NAMESPACE",
        "RAY_RUNTIME_ENV_HOOK",
        "RAY_DATA_PUSH_BASED_SHUFFLE",
    ]:
        environment.pop(name, None)
    with tempfile.TemporaryDirectory(prefix="ray-67229-") as ray_temp:
        command = [
            str(ARGS.python),
            "-B",
            str(SCRIPT),
            "--variant",
            variant,
            "--source-revision",
            revision,
            "--ray-package-root",
            str(ray_root),
            "--expected-hash-shuffle-sha256",
            SOURCE_HASHES[variant],
            "--case",
            case,
            "--reservation",
            reservation,
            "--output-dir",
            str(case_output),
            "--ray-temp-dir",
            ray_temp,
        ]
        if expected_native:
            command.extend(["--expected-native-commit", expected_native])
        started = time.time()
        returncode, stdout = run_process(command, environment, ARGS.timeout)
    (ARGS.output_dir / f"{run_id}.stdout.txt").write_text(stdout)
    summary = json.loads((case_output / "summary.json").read_text())
    expected_returncode = 0 if expected_status == "completed" else 2
    if returncode != expected_returncode or summary["status"] != expected_status:
        raise AssertionError(
            f"{run_id}: return/status {returncode}/{summary['status']}, "
            f"expected {expected_returncode}/{expected_status}"
        )
    if expected_status == "completed":
        if not summary.get("output_correct"):
            raise AssertionError(f"{run_id}: incorrect output")
    else:
        if summary.get("error_type") != "TypeError" or "int" not in summary.get(
            "error", ""
        ):
            raise AssertionError(f"{run_id}: wrong failure {summary.get('error')!r}")
        exception_sources = [
            event["source"]
            for event in summary["production_events"]
            if event["event"] == "exception"
            and event["function"] == "min_max_resource_requirements"
        ]
        expected_source = "data/_internal/execution/operators/hash_shuffle.py"
        if not exception_sources or any(
            source != expected_source for source in exception_sources
        ):
            raise AssertionError(f"{run_id}: wrong exception sources {exception_sources}")
    observed_gpu = sorted(
        actor["required_resources"].get("GPU", 0)
        for actor in summary.get("aggregators", [])
    )
    if observed_gpu != expected_gpu:
        raise AssertionError(
            f"{run_id}: GPU requests {observed_gpu}, expected {expected_gpu}"
        )
    results.append(
        {
            "run_id": run_id,
            "variant": variant,
            "case": case,
            "reservation": reservation,
            "status": summary["status"],
            "returncode": returncode,
            "output_correct": summary.get("output_correct", False),
            "error_type": summary.get("error_type"),
            "error": summary.get("error"),
            "aggregator_gpu_requests": observed_gpu,
            "hash_shuffle_sha256": summary["hash_shuffle_sha256"],
            "native_commit": summary["native_commit"],
            "elapsed_s": summary["elapsed_s"],
            "wall_s": time.time() - started,
        }
    )

matrix_summary = {
    "classification": "fresh portable package validation",
    "parent": PARENT_SHA,
    "head": HEAD_SHA,
    "python": str(ARGS.python.resolve()),
    "parent_ray": str(ARGS.parent_ray.resolve()),
    "head_ray": str(ARGS.head_ray.resolve()),
    "results": results,
}
(ARGS.output_dir / "matrix-summary.json").write_text(
    json.dumps(matrix_summary, indent=2, sort_keys=True) + "\n"
)
print(json.dumps(matrix_summary, indent=2, sort_keys=True))
