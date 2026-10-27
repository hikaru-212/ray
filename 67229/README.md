# Ray PR #67229: explicit `num_gpus=None` aborts V1 hash-shuffle joins

- PR: <https://github.com/ray-project/ray/pull/67229>
- Parent: `938ab39460b257374624479c6739fa80315939ea`
- Verified HEAD: `72f39680c62f3afe3ed437a74ae360417452f1d9`

PR #67229 replaces the inherited unbounded maximum resource declaration for
`HashShufflingOperatorBase` with a GPU maximum derived from the configured
aggregator count and `aggregator_ray_remote_args["num_gpus"]`. This keeps a
default CPU-only hash shuffle from receiving shared GPU budget and accounts for
positive GPU overrides.

The new calculation does not handle the legal explicit value `num_gpus=None`.
With HASH_SHUFFLE V1 and reservation enabled, public `Dataset.join` reaches
`aggregator_count * None` and fails before the aggregator pool starts. The
Parent completes the same join, but its inherited `max.gpu=inf` is unbounded
legacy behavior, not a correct pre-existing GPU upper bound.

## Invariant

Ray accepts `num_gpus=None`; Core omits the GPU resource from the request. A
hash-shuffle maximum-resource calculation must preserve that zero-GPU meaning
and must not interrupt an otherwise valid join. The audit verified option
validation, a real Core actor, and GCS resource records for this invariant.

## Production path

The source path and runtime trace agree:

```text
Dataset.join(aggregator_ray_remote_args={"num_gpus": None})
  -> logical Join stores the override unchanged
  -> non-SHUFFLE_V2 planner creates JoinOperator
  -> HashShufflingOperatorBase stores the override unchanged
  -> StreamingExecutor scheduling loop
  -> ResourceManager.update_usages()
  -> ReservationOpResourceAllocator.update_budgets()
  -> _update_reservation()
  -> JoinOperator.min_max_resource_requirements()
  -> HashShufflingOperatorBase.min_max_resource_requirements()
  -> self._num_aggregators * None
```

There is no earlier normalization that makes the failure unreachable. The
public join constructs successfully and fails only when execution updates the
reservation budget.

## Runtime-verified observations

All successful joins returned exactly rows `id=2,3`. Each public run used two
aggregators, 4 CPUs, 2 logical GPUs, no CUDA, HASH_SHUFFLE V1, and a fresh local
Ray cluster.

| Revision / case | Reservation | Result | Aggregator GPU requests |
| --- | --- | --- | --- |
| Parent omitted | enabled | PASS | 0, 0 |
| Parent `0` | enabled | PASS | 0, 0 |
| Parent `None` | enabled | PASS | 0, 0 |
| HEAD omitted | enabled | PASS | 0, 0 |
| HEAD `0` | enabled | PASS | 0, 0 |
| HEAD `0.5` | enabled | PASS | 0.5, 0.5 |
| HEAD `None` | enabled | **FAIL: TypeError** | none; failure preceded actor start |
| HEAD `None` | disabled | PASS | 0, 0 |

The direct production `JoinOperator` constructor also covered 1 and 4
aggregators. HEAD reported `max.gpu=0` for omitted/zero, `0.5`/`2.0` for a 0.5
override, and TypeError for explicit None. Parent returned inherited infinity in
all cases. See [`observed/results.json`](observed/results.json) and the
path-sanitized [`failure-excerpt.txt`](observed/failure-excerpt.txt).

## Source-confirmed findings

- `Dataset.join` accepts and forwards `aggregator_ray_remote_args`.
- The V1 planner forwards the dictionary to the physical `JoinOperator` without
  converting None to zero.
- Ray resource option validation allows None, and Core's resource conversion
  omits GPU when the value is None.
- The PR-added maximum calculation at `hash_shuffle.py:1357` multiplies the
  aggregator count by the dictionary value without handling explicit None.
- Parent-to-HEAD production scope is the new hash-shuffle method; the other two
  changed files are tests. Exact source hashes are in
  [`observed/provenance.json`](observed/provenance.json).

## Portable reproduction

[`verify_none.py`](verify_none.py) is a portable derivative of the original
executed audit script. It preserves the public `Dataset.join`, HASH_SHUFFLE V1,
reservation setting, logical-GPU cluster, production-call tracing, output
oracle, and GCS actor-resource observation. It adds strict import-root and exact
`hash_shuffle.py` checks and refuses to overwrite an output directory.

Use exact Parent and HEAD source trees with a Python/Ray runtime compatible with
each source revision. `PARENT_RAY` and `HEAD_RAY` must name the imported `ray/`
package roots, not checkout roots. The environment is not vendored.

```bash
export EVIDENCE_PACKAGE=/path/to/review-evidence/67229
export PY=/path/to/compatible/venv/bin/python
export PARENT_RAY=/path/to/parent-runtime/ray
export HEAD_RAY=/path/to/head-runtime/ray

"$PY" -B "$EVIDENCE_PACKAGE/run_verification.py" \
  --python "$PY" \
  --parent-ray "$PARENT_RAY" \
  --head-ray "$HEAD_RAY" \
  --output-dir /tmp/ray-67229-verification
```

The output directory must not exist. The runner creates a fresh process and
cluster per case, checks the expected Parent/HEAD source hashes, verifies output
and GCS GPU requests, and treats the expected HEAD/None TypeError as a verified
RED result. Optional `--parent-native-commit` and `--head-native-commit` values
can enforce native build identity. The exact historical environment used one
Parent native wheel for both variants because all Parent-to-HEAD changes were
under `python/ray/data`; that compatibility judgment must not be generalized to
unrelated revisions.

The package-preparation run of this portable derivative is recorded separately
in [`observed/fresh-validation.json`](observed/fresh-validation.json). It reused
the already audited exact source overlays and shared Parent native runtime; it
is not an independent native build. Historical observations remain separately
labeled in `results.json`.

## Provenance and limits

Historical execution used Python 3.12.3 on Linux/aarch64 with Ray
3.0.0.dev0, NumPy 2.2.6, Pandas 2.3.3, PyArrow 23.0.1, and Polars 1.40.0.
Both variants used byte-identical Parent native binaries and exact-revision Ray
Data sources. The package contains no wheel, native binary, virtual environment,
full Ray session, or private path. Full hashes and derivation identifiers are in
[`observed/provenance.json`](observed/provenance.json); package-file hashes are
in [`SHA256SUMS`](SHA256SUMS).

Not verified:

- the default SHUFFLE_V2 join path;
- public API behavior of other `HashShufflingOperatorBase` subclasses;
- physical CUDA devices or separate full native builds for both revisions;
- the full Ray test suite, distributed multi-node behavior, or all resource
  policies;
- runtime behavior of PR #67382.

No #67382 patch or source snapshot is included. Its `base_resource_usage`
change concerns current usage and is separate from this PR's failing maximum
calculation.
