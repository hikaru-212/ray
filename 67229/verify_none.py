"""Portable public-API reproducer derived from the executed PR #67229 audit.

The caller must put the selected Ray package's parent directory first on
PYTHONPATH. This script verifies that import root and the exact hash-shuffle
source before starting a private logical-GPU Ray cluster.
"""

import argparse
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import signal
import sys
import threading
import time
import traceback


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--variant", choices=["parent", "head"], required=True)
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--ray-package-root", type=Path, required=True)
    parser.add_argument("--expected-hash-shuffle-sha256", required=True)
    parser.add_argument("--expected-native-commit")
    parser.add_argument(
        "--case", choices=["omitted", "zero", "none", "half"], required=True
    )
    parser.add_argument("--reservation", choices=["enabled", "disabled"], required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--ray-temp-dir", type=Path, required=True)
    parser.add_argument("--timeout", type=int, default=90)
    return parser.parse_args()


ARGS = parse_args()
ARGS.output_dir.mkdir(parents=True, exist_ok=False)

import ray  # noqa: E402
from ray.data import DataContext  # noqa: E402
from ray.data.context import ShuffleStrategy  # noqa: E402
from ray.data._internal.execution.operators import hash_shuffle  # noqa: E402
from ray.data._internal.execution.operators.hash_shuffle import (  # noqa: E402
    HashShufflingOperatorBase,
)
from ray.data._internal.execution.operators.join import JoinOperator  # noqa: E402


EXPECTED_OUTPUT = [
    {"id": 2, "left": 20, "right": 200},
    {"id": 3, "left": 30, "right": 300},
]


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


package_root = ARGS.ray_package_root.resolve()
imported_root = Path(ray.__file__).resolve().parent
if imported_root != package_root:
    raise RuntimeError(f"ray imported from {imported_root}, expected {package_root}")

hash_shuffle_path = Path(hash_shuffle.__file__).resolve()
try:
    hash_shuffle_relative = str(hash_shuffle_path.relative_to(package_root))
except ValueError as exc:
    raise RuntimeError(
        f"hash_shuffle imported from {hash_shuffle_path}, outside {package_root}"
    ) from exc

actual_source_sha = sha256(hash_shuffle_path)
if actual_source_sha != ARGS.expected_hash_shuffle_sha256:
    raise RuntimeError(
        "unexpected hash_shuffle.py: "
        f"{actual_source_sha}, expected {ARGS.expected_hash_shuffle_sha256}"
    )

owns_pr_method = "min_max_resource_requirements" in HashShufflingOperatorBase.__dict__
if owns_pr_method != (ARGS.variant == "head"):
    raise RuntimeError(
        f"variant={ARGS.variant} does not match HashShufflingOperatorBase at "
        f"{hash_shuffle_relative}"
    )
if ARGS.expected_native_commit and ray.__commit__ != ARGS.expected_native_commit:
    raise RuntimeError(
        f"native commit {ray.__commit__} != expected {ARGS.expected_native_commit}"
    )


events = []
seen = set()
stage = "setup"
started = time.monotonic()


def relative_source(filename):
    path = Path(filename).resolve()
    try:
        return str(path.relative_to(package_root))
    except ValueError:
        return path.name


def append_event(event, frame, **fields):
    events.append(
        {
            "event": event,
            "elapsed_s": time.monotonic() - started,
            "stage": stage,
            "function": frame.f_code.co_name,
            "source": relative_source(frame.f_code.co_filename),
            "line": frame.f_lineno,
            **fields,
        }
    )


def local_trace(frame, event, arg):
    function = frame.f_code.co_name
    if event == "exception":
        _, exception, _ = arg
        append_event("exception", frame, error=repr(exception))
    elif event == "return":
        if function == "plan_join_op" and arg is not None:
            append_event(
                "planned",
                frame,
                operator=type(arg).__name__,
                override=getattr(arg, "_aggregator_ray_remote_args_override", "absent"),
                aggregators=getattr(arg, "_num_aggregators", None),
            )
        elif function == "_build_aggregator_ray_remote_args":
            append_event("aggregator_options", frame, options=arg)
        elif function == "min_max_resource_requirements" and arg is not None:
            append_event("minmax_return", frame, max_gpu=arg[1].gpu)
    return local_trace


TRACKED = {
    "plan_join_op",
    "min_max_resource_requirements",
    "_update_reservation",
    "_build_aggregator_ray_remote_args",
    "_start_shuffle",
}


def global_trace(frame, event, arg):
    if event != "call" or frame.f_code.co_name not in TRACKED:
        return None
    source = Path(frame.f_code.co_filename).resolve()
    try:
        relative = source.relative_to(package_root)
    except ValueError:
        return None
    if "data" not in relative.parts:
        return None
    operator = frame.f_locals.get("self")
    if (
        frame.f_code.co_name == "min_max_resource_requirements"
        and not isinstance(operator, JoinOperator)
    ):
        return None
    frame.f_trace_lines = False
    key = (frame.f_code.co_name, id(operator))
    if key not in seen:
        seen.add(key)
        append_event(
            "call",
            frame,
            operator=type(operator).__name__,
            override=getattr(operator, "_aggregator_ray_remote_args_override", None),
            aggregators=getattr(operator, "_num_aggregators", None),
            pool_started=getattr(operator, "_shuffle_started", None),
        )
    return local_trace


def actor_rows():
    from ray.core.generated import gcs_pb2

    accessor = ray._private.state.state._connect_and_get_accessor()
    rows = []
    for raw in accessor.get_actor_table(None, None):
        entry = gcs_pb2.ActorTableData.FromString(raw)
        if entry.class_name == "HashShuffleAggregator":
            rows.append(
                {
                    "state": gcs_pb2.ActorTableData.ActorState.Name(entry.state),
                    "required_resources": dict(entry.required_resources),
                }
            )
    return rows


def expire(_signal, _frame):
    raise TimeoutError(f"verification exceeded {ARGS.timeout} seconds")


signal.signal(signal.SIGALRM, expire)
signal.alarm(ARGS.timeout)
summary = {
    "classification": "fresh portable reproducer output",
    "variant": ARGS.variant,
    "source_revision": ARGS.source_revision,
    "case": ARGS.case,
    "reservation": ARGS.reservation,
    "ray_version": ray.__version__,
    "native_commit": ray.__commit__,
    "ray_package_root": str(package_root),
    "hash_shuffle_relative": hash_shuffle_relative,
    "hash_shuffle_sha256": actual_source_sha,
    "python": sys.version,
    "platform": platform.platform(),
    "packages": {
        name: importlib.metadata.version(name)
        for name in ["numpy", "pandas", "pyarrow", "polars"]
    },
    "cluster": {"num_cpus": 4, "logical_num_gpus": 2},
}

try:
    ray.init(
        num_cpus=4,
        num_gpus=2,
        include_dashboard=False,
        object_store_memory=256 * 1024**2,
        _memory=2 * 1024**3,
        _temp_dir=str(ARGS.ray_temp_dir),
    )
    context = DataContext.get_current()
    context.enable_progress_bars = False
    context.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE
    context.op_resource_reservation_enabled = ARGS.reservation == "enabled"
    context.op_resource_reservation_ratio = 0.5

    sys.settrace(global_trace)
    threading.settrace(global_trace)
    stage = "construct"
    left = ray.data.from_items([{"id": i, "left": i * 10} for i in range(4)])
    right = ray.data.from_items([{"id": i, "right": i * 100} for i in range(2, 6)])
    options = {}
    if ARGS.case != "omitted":
        options["aggregator_ray_remote_args"] = {
            "num_gpus": {"zero": 0, "none": None, "half": 0.5}[ARGS.case]
        }
    dataset = left.join(
        right,
        join_type="inner",
        num_partitions=2,
        on=("id",),
        **options,
    )
    summary["api_construction"] = "PASS"
    stage = "execute"
    output = sorted(dataset.take_all(), key=lambda row: row["id"])
    if output != EXPECTED_OUTPUT:
        raise AssertionError(f"unexpected output: {output!r}")
    summary.update(status="completed", output=output, output_correct=True)
except BaseException as exception:
    summary.update(
        status="error",
        error_type=type(exception).__name__,
        error=str(exception),
        failure_stage=stage,
        traceback=traceback.format_exc(),
    )
finally:
    sys.settrace(None)
    threading.settrace(None)
    signal.alarm(0)
    if ray.is_initialized():
        try:
            summary["aggregators"] = actor_rows()
        except Exception as actor_error:
            summary["actor_observation_error"] = repr(actor_error)
        ray.shutdown()
    summary["production_events"] = events
    summary["elapsed_s"] = time.monotonic() - started
    (ARGS.output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n"
    )

print(json.dumps(summary, indent=2, sort_keys=True))
sys.exit(0 if summary["status"] == "completed" else 2)
