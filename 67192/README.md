# Ray PR #67192: avoid a redundant Arrow copy before NumPy copying

- Issue: <https://github.com/ray-project/ray/issues/67185>
- PR: <https://github.com/ray-project/ray/pull/67192>
- Baseline: `05d07da40d2c62d803f08533db75a7f3e76f079f`
- Reviewed PR: `ba3c7a61bdb2c8b8dda1dd69e46d1a0f90b16fbe`

## Invariant and change

With `zero_copy_batch=False`, a writable NumPy batch may be mutated without
changing its source block.  Before this PR, `batch_blocks(..., ensure_copy=True)`
copied the Arrow block and `_copy_batch` copied the converted NumPy arrays.  The
PR adds `numpy_copy_follows`: `Batcher` may omit the Arrow copy only for an Arrow
schema that its classifier considers safe, while the later formatting stage
still performs the NumPy copy.  Object-like/ragged schemas and non-NumPy formats
retain the Arrow-copy fallback.

This is a performance optimization.  The tested mutation-isolation behavior
passes on both revisions; it is not a Parent RED / PR GREEN correctness fix.

## Preserved independent evidence

[`test_copy_semantics.py`](test_copy_semantics.py) runs the real production
`batch_blocks` pipeline and instruments the real Arrow slice and `_copy_batch`
operations while delegating to their original implementations.  Its 17 cases
verify:

- writable, source-isolated primitive and fixed-shape tensor NumPy batches for
  both `batch_size=None` and an integer batch size;
- fallback for a variable-shaped tensor read from immutable Arrow IPC buffers;
- fallback for a mixed numeric/ragged schema;
- nullable boolean's actual object dtype, immutable `bool`/`None` referents,
  writable outer array, and source isolation;
- multi-chunk numeric values, ordering, partial batches, and source isolation;
- retained Arrow copies for `pyarrow`, `pandas`, low-level `default`, and `None`
  batch formats, plus the unchanged Pandas-source path.

These cases add IPC, nullable-boolean, multi-chunk, partial-batch, and format
boundaries beyond the PR's own classifier, `Batcher`, and `map_batches` tests.
The package intentionally does not duplicate those author tests.

The test requires `RAY_REVIEW_PACKAGE_ROOT` and `RAY_REVIEW_VARIANT`, checks the
critical imported module paths, and checks whether the PR-only classifier API
matches the variant label.  This prevents silently testing an unrelated
installed Ray Python package.

## Requirements and correctness reproduction

Use two separate Ray checkouts at the exact SHAs above and a virtual environment
with Python, NumPy, Pandas, PyArrow, pytest, and a loadable Ray native extension
compatible with each checkout and Python ABI.  The evidence repository does not
vendor a wheel, native library, generated code, or environment.  Put each
checkout's `python/` directory first on `PYTHONPATH`; do not mix arbitrary Ray
Python and native versions.

```bash
export EVIDENCE_REPO=/path/to/ray-review-evidence
export PY=/path/to/compatible/venv/bin/python
export PARENT_RAY=/path/to/baseline-checkout/python/ray
export PR_RAY=/path/to/pr-checkout/python/ray

PYTHONDONTWRITEBYTECODE=1 OPENBLAS_NUM_THREADS=1 \
PYTHONPATH="$(dirname "$PARENT_RAY")" \
RAY_REVIEW_PACKAGE_ROOT="$PARENT_RAY" RAY_REVIEW_VARIANT=parent \
  "$PY" -B -m pytest -q -s -p no:cacheprovider \
  --confcutdir="$EVIDENCE_REPO/67192" \
  "$EVIDENCE_REPO/67192/test_copy_semantics.py"

PYTHONDONTWRITEBYTECODE=1 OPENBLAS_NUM_THREADS=1 \
PYTHONPATH="$(dirname "$PR_RAY")" \
RAY_REVIEW_PACKAGE_ROOT="$PR_RAY" RAY_REVIEW_VARIANT=pr \
  "$PY" -B -m pytest -q -s -p no:cacheprovider \
  --confcutdir="$EVIDENCE_REPO/67192" \
  "$EVIDENCE_REPO/67192/test_copy_semantics.py"
```

Set `RAY_REVIEW_NATIVE_COMMIT=<sha>` to require a particular native Ray build.
Phase 2 used the official aarch64 wheel built at the exact baseline SHA for both
variants because this PR changes only Python code, then overlaid verbatim PR
Python files.  A normal checkout reproduction should use a revision-compatible
build and must not assume native compatibility automatically.

Packaged-test observations on the Phase 2 environment (Python 3.12.3, aarch64,
NumPy 2.2.6, Pandas 2.3.3, PyArrow 23.0.1; the same 17 cases also passed with
PyArrow 26.0.0):

| Revision | Correctness result | Safe-schema Arrow copy | Ragged/mixed/non-NumPy Arrow copy |
| --- | --- | --- | --- |
| Parent | 17 passed | retained | retained |
| PR | 17 passed | omitted; final NumPy copy retained | retained |

The exact Phase 3 commands and smoke-run result are recorded in
[`observed/phase3-verification.md`](observed/phase3-verification.md).

## Focused benchmark

[`benchmark_batch_copy.py`](benchmark_batch_copy.py) uses a fresh process for
each case, alternates parent/PR order, and runs single-chunk numeric,
four-chunk numeric, and numeric-plus-string fallback inputs.  Defaults are one
million rows, eight `int64` columns, `batch_size=None` and `batch_size=rows`,
`batch_format="numpy"`, `ensure_copy=True`, and five repetitions.  It checks
values, writability, source isolation, and import roots; it has no timing or RSS
pass/fail threshold.

On Linux:

```bash
"$PY" -B "$EVIDENCE_REPO/67192/benchmark_batch_copy.py" run \
  --python "$PY" \
  --parent-package-root "$PARENT_RAY" \
  --pr-package-root "$PR_RAY" \
  --output-dir /tmp/ray-67192-benchmark
```

Phase 2's five-repetition observations are retained verbatim in
[`observed/phase2-benchmark-summary.json`](observed/phase2-benchmark-summary.json).
For `batch_size=None`, the medians were:

| Input | Parent time / extra peak RSS | PR time / extra peak RSS |
| --- | ---: | ---: |
| Single-chunk numeric | 14.00 ms / 122.20 MiB | 4.57 ms / 61.00 MiB |
| Multi-chunk numeric | 15.19 ms / 122.15 MiB | 15.53 ms / 122.28 MiB |
| Mixed fallback | 21.06 ms / 142.40 MiB | 20.81 ms / 142.53 MiB |

The full-size integer-batch observations show the same pattern.  These are local
microbenchmark results, not a guarantee for distributed workloads or other
machines.  In particular, multi-chunk conversion still combines Arrow chunks,
so omitting the explicit Arrow slice copy did not reduce observed peak RSS.

## Historical observations and limits

[`observed/phase2-environment.json`](observed/phase2-environment.json) records
the original environment.  [`observed/phase2-test-results.json`](observed/phase2-test-results.json)
is the compact historical result index; its referenced XML/log files were not
copied into this minimal package.  Phase 2 also passed the author's 21 focused
unit cases and four real `map_batches` cases, the broader parent/PR independent
matrix, and a PyArrow 26 comparison.

Nullable boolean is classified safe although Ray converts it to an object-dtype
NumPy array.  The observed referents are immutable `bool`/`None`, and the final
outer-array copy preserves source isolation.  This contradicts the helper's
name/docstring, but no correctness regression was demonstrated.

The preserved pytest file exercises production batching and conversion without
starting a Ray cluster.  Distributed planner/worker behavior is covered only by
the historical Phase 2 and author tests.  The review did not cover GPU/cuDF,
arbitrary custom Arrow extension types, greater-than-2-GiB buffers, or a full
Ray suite.  Peak RSS uses Linux `ru_maxrss` units and includes allocator effects.
