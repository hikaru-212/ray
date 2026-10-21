"""Fresh-process microbenchmark for Ray PR #67192.

The public ``run`` mode launches this file once per case and version.  The
private ``worker`` mode imports Ray, executes one measured ``batch_blocks``
call, checks correctness, and emits one JSON object.
"""

import argparse
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys


CASES = ("single", "multi", "mixed")


def _worker(args):
    import gc
    import resource
    import time

    import numpy as np
    import pyarrow as pa
    import ray
    from ray.data._internal import batcher as batcher_module
    from ray.data._internal.arrow_block import ArrowBlockAccessor
    from ray.data._internal.arrow_ops import transform_pyarrow as transform
    from ray.data._internal.block_batching import block_batching
    from ray.data._internal.block_batching import util
    from ray.data._internal.block_batching.block_batching import batch_blocks

    package_root = Path(os.environ["RAY_REVIEW_PACKAGE_ROOT"]).absolute()
    for module in (ray, batcher_module, block_batching, transform, util):
        try:
            Path(module.__file__).absolute().relative_to(package_root)
        except ValueError as error:
            raise RuntimeError(
                f"{module.__name__} imported from outside {package_root}: {module.__file__}"
            ) from error
    has_pr_api = hasattr(transform, "converts_to_non_object_numpy")
    if has_pr_api != (args.variant == "pr"):
        raise RuntimeError("variant label does not match the imported implementation")

    batch_size = None if args.batch_size == "none" else args.rows
    list(
        batch_blocks(
            iter([pa.table({"warmup": [1, 2]})]),
            batch_size=None,
            batch_format="numpy",
            ensure_copy=True,
        )
    )

    columns = {}
    for index in range(args.columns):
        if args.case == "multi":
            if args.rows % args.chunks:
                raise ValueError("rows must be divisible by chunks")
            chunk_size = args.rows // args.chunks
            columns[f"c{index}"] = pa.chunked_array(
                [
                    pa.array(
                        np.arange(
                            chunk * chunk_size,
                            (chunk + 1) * chunk_size,
                            dtype=np.int64,
                        )
                    )
                    for chunk in range(args.chunks)
                ]
            )
        else:
            columns[f"c{index}"] = pa.array(np.arange(args.rows, dtype=np.int64))
    if args.case == "mixed":
        columns["label"] = pa.repeat("x", args.rows)
    table = pa.table(columns)
    del columns
    gc.collect()

    counts = {
        "arrow_slice_copy": 0,
        "arrow_combine": 0,
        "numpy_copy_calls": 0,
        "numpy_arrays_copied": 0,
        "numpy_copy_bytes": 0,
    }
    original_slice = ArrowBlockAccessor.slice
    original_combine = transform._try_combine_chunks_safe
    original_copy = util._copy_batch

    def traced_slice(self, start, end, copy=False):
        counts["arrow_slice_copy"] += int(copy)
        return original_slice(self, start, end, copy=copy)

    def traced_combine(array):
        counts["arrow_combine"] += 1
        return original_combine(array)

    def traced_copy(batch):
        counts["numpy_copy_calls"] += 1
        arrays = (
            [value for value in batch.values() if isinstance(value, np.ndarray)]
            if isinstance(batch, dict)
            else [batch] if isinstance(batch, np.ndarray) else []
        )
        counts["numpy_arrays_copied"] += len(arrays)
        counts["numpy_copy_bytes"] += sum(array.nbytes for array in arrays)
        return original_copy(batch)

    ArrowBlockAccessor.slice = traced_slice
    transform._try_combine_chunks_safe = traced_combine
    util._copy_batch = traced_copy

    source_arrays = [chunk.to_numpy() for chunk in table["c0"].chunks]
    before_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    started = time.perf_counter()
    batches = list(
        batch_blocks(
            iter([table]),
            batch_size=batch_size,
            batch_format="numpy",
            ensure_copy=True,
        )
    )
    elapsed = time.perf_counter() - started
    peak_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

    assert sum(len(batch["c0"]) for batch in batches) == args.rows
    assert batches[0]["c0"][0] == 0
    assert batches[-1]["c0"][-1] == args.rows - 1
    assert all(batch["c0"].flags.writeable for batch in batches)
    assert all(
        not np.shares_memory(batch["c0"], source)
        for batch in batches
        for source in source_arrays
    )

    print(
        json.dumps(
            {
                "variant": args.variant,
                "case": args.case,
                "batch_size": batch_size,
                "rows": args.rows,
                "numeric_columns": args.columns,
                "chunks_per_numeric_column": args.chunks if args.case == "multi" else 1,
                "input_bytes": table.nbytes,
                "elapsed_ms": elapsed * 1000,
                "additional_peak_rss_mib": (peak_rss - before_rss) / 1024,
                "counts": counts,
                "ray_commit": ray.__commit__,
                "ray_package_root": str(package_root),
                "python": sys.version.split()[0],
                "pyarrow": pa.__version__,
                "numpy": np.__version__,
            }
        )
    )


def _run(args):
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    script = Path(__file__).absolute()
    roots = {
        "parent": Path(args.parent_package_root).absolute(),
        "pr": Path(args.pr_package_root).absolute(),
    }
    for root in roots.values():
        if root.name != "ray" or not (root / "__init__.py").exists():
            raise ValueError(f"expected a ray package directory, got {root}")

    results = []
    raw_path = output_dir / "benchmark-raw.jsonl"
    with raw_path.open("w") as stream:
        for repeat in range(args.repetitions):
            order = ("parent", "pr") if repeat % 2 == 0 else ("pr", "parent")
            for case in CASES:
                for batch_size in ("none", "all"):
                    for variant in order:
                        env = dict(os.environ)
                        env.update(
                            {
                                "PYTHONPATH": str(roots[variant].parent),
                                "RAY_REVIEW_PACKAGE_ROOT": str(roots[variant]),
                                "RAY_REVIEW_VARIANT": variant,
                                "PYTHONDONTWRITEBYTECODE": "1",
                                "OPENBLAS_NUM_THREADS": "1",
                                "RAY_USAGE_STATS_ENABLED": "0",
                            }
                        )
                        command = [
                            args.python,
                            "-B",
                            str(script),
                            "worker",
                            "--variant",
                            variant,
                            "--case",
                            case,
                            "--batch-size",
                            batch_size,
                            "--rows",
                            str(args.rows),
                            "--columns",
                            str(args.columns),
                            "--chunks",
                            str(args.chunks),
                        ]
                        process = subprocess.run(
                            command,
                            env=env,
                            capture_output=True,
                            text=True,
                            timeout=args.timeout,
                            check=True,
                        )
                        result = json.loads(process.stdout)
                        result["repeat"] = repeat
                        results.append(result)
                        stream.write(json.dumps(result) + "\n")
                        stream.flush()
            print(
                f"completed paired repetition {repeat + 1}/{args.repetitions}",
                file=sys.stderr,
                flush=True,
            )

    summaries = []
    for case in CASES:
        for batch_size in (None, args.rows):
            for variant in ("parent", "pr"):
                samples = [
                    result
                    for result in results
                    if (result["case"], result["batch_size"], result["variant"])
                    == (case, batch_size, variant)
                ]
                first_counts = samples[0]["counts"]
                if any(sample["counts"] != first_counts for sample in samples):
                    raise AssertionError("copy-operation counts changed between repetitions")
                summaries.append(
                    {
                        "case": case,
                        "batch_size": batch_size,
                        "variant": variant,
                        "median_ms": statistics.median(
                            sample["elapsed_ms"] for sample in samples
                        ),
                        "median_extra_peak_rss_mib": statistics.median(
                            sample["additional_peak_rss_mib"] for sample in samples
                        ),
                        "min_ms": min(sample["elapsed_ms"] for sample in samples),
                        "max_ms": max(sample["elapsed_ms"] for sample in samples),
                        "counts": first_counts,
                    }
                )
    summary = {
        "method": "fresh process per case; alternating parent/PR order",
        "repetitions": args.repetitions,
        "rows": args.rows,
        "numeric_columns": args.columns,
        "multi_chunks_per_column": args.chunks,
        "batch_format": "numpy",
        "ensure_copy": True,
        "results": summaries,
    }
    summary_path = output_dir / "benchmark-summary.json"
    summary_path.write_text(json.dumps(summary, indent=2) + "\n")
    print(summary_path)


def _parser():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    run = subparsers.add_parser("run")
    run.add_argument("--parent-package-root", required=True)
    run.add_argument("--pr-package-root", required=True)
    run.add_argument("--python", default=sys.executable)
    run.add_argument("--output-dir", required=True)
    run.add_argument("--rows", type=int, default=1_000_000)
    run.add_argument("--columns", type=int, default=8)
    run.add_argument("--chunks", type=int, default=4)
    run.add_argument("--repetitions", type=int, default=5)
    run.add_argument("--timeout", type=int, default=120)

    worker = subparsers.add_parser("worker")
    worker.add_argument("--variant", choices=("parent", "pr"), required=True)
    worker.add_argument("--case", choices=CASES, required=True)
    worker.add_argument("--batch-size", choices=("none", "all"), required=True)
    worker.add_argument("--rows", type=int, required=True)
    worker.add_argument("--columns", type=int, required=True)
    worker.add_argument("--chunks", type=int, required=True)
    return parser


if __name__ == "__main__":
    arguments = _parser().parse_args()
    if arguments.command == "worker":
        _worker(arguments)
    else:
        _run(arguments)
