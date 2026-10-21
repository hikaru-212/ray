# Ray PR #67180: Ray Data task completion

PR: <https://github.com/ray-project/ray/pull/67180>

Issue: <https://github.com/ray-project/ray/issues/67179>

- Pre-fix Python-source baseline: `6a3ba37e9e09803703b06787cdab6b085581788c`
- PR: `817d5f128c7e3905f87ca39a2d1e918d8c23ebb1`
- Shared Core wheel: `a70516160723a5dca49b3d70fb0ee9c6823382e2`

## Invariant

After a streaming MapOperator producer emits one valid block/metadata pair and
then raises `ValueError`, a zero output-read budget must preserve the emitted
output and leave the unread error pending. Restoring positive budget must
propagate the error once, invoke completion/accounting once, release logical CPU
accounting, and keep the valid output available for exactly one consumer read.

[`test_post_output_failure.py`](test_post_output_failure.py) uses a real
MapOperator, DataOpTask, `_map_task`, `process_completed_tasks()`, operator
output queue, executor metrics, and both `InlineMetadataFetcher` and
`ThreadedMetadataFetcher`. An existing SignalActor barrier orders the valid
pair before the producer failure. Callback wrappers record observable calls and
delegate to the original callbacks. Budget transitions are explicitly
`1 → 0 → 1`; bounded condition polling supplies synchronization.

The producer body error is represented by an unread ObjectRefGenerator stream
item, separate from the successful `gen.completed()` terminal object. The test
therefore does not require immediate failure propagation while budget is zero.

## Recorded environment and result

The original runs used Python 3.12.3 on Linux aarch64 with pytest 7.4.4,
pytest-timeout 2.1.0, PyArrow 23.0.1, pandas 2.3.3, and NumPy 2.2.6. Exact
Python source exports for both revisions were loaded above Ray 3.0.0.dev0 Core
wheel revision `a70516160723a5dca49b3d70fb0ee9c6823382e2`.

The wheel SHA-256 was
`81c59d91cac85afb1047dae6761bde8670f692c371b422e144e5b03336c9c472`;
`_raylet.so` was
`122c10cacd8e7c17c1362af428c7a383221285acdac410da8c3e67409e416b26`.
The tested DataOpTask source hashes were:

- PR: `0964b67c2e78c3f64e6e78b4ccbfbfb9616fcb298b89a081ece18a4e095dd501`
- baseline: `4a3eb16831dafa54f5df5d0645611c2eec44672cc81a84323b77b9fa3e45e6f3`

All four combinations passed: PR inline/threaded in 10.199 seconds total and
baseline inline/threaded in 9.134 seconds total. Under zero budget, five
executor ticks left the task ACTIVE with one queued output, no callback, no
executor error, and one logical CPU accounted. `ray.get(gen.completed())`
returned `None`, while the unread error kept the stream unexhausted.

After budget restoration, each run observed one `RayTaskError(ValueError)`, one
completion callback, one executor error, and logical CPU changing from one to
zero. Metrics ended with one finished and one failed task. Queue/live-output
accounting remained 140 bytes until the consumer read `{"id": [42]}` once.
Three later ticks and direct finished-task/fetcher calls found no duplicate
output, callback, error, or resource release.

The compact byte-identical records are under [`observed/head`](observed/head/)
and [`observed/baseline`](observed/baseline/): one result JSON per metadata mode.
Packaging did not rerun the tests.

Because the baseline also passed, this is positive failure-path non-regression
evidence. It does not distinguish the PR's new completion policy and does not
report a newly found correctness defect.

## Reproduce

Prepare detached checkouts at the two exact revisions and a virtual environment
with the compatible shared Core wheel and versions listed above. Set paths:

```bash
export RAY_CHECKOUT=/path/to/ray
export EVIDENCE_REPO=/path/to/ray-review-evidence
export BASELINE=6a3ba37e9e09803703b06787cdab6b085581788c
export PR=817d5f128c7e3905f87ca39a2d1e918d8c23ebb1
export VERIFY_PYTHON=/path/to/compatible/venv/bin/python
```

Create isolated source checkouts:

```bash
tmp_dir=$(mktemp -d)
git -C "$RAY_CHECKOUT" worktree add --detach "$tmp_dir/baseline" "$BASELINE"
git -C "$RAY_CHECKOUT" worktree add --detach "$tmp_dir/head" "$PR"
```

Run each source selection with a short, unused Ray runtime path:

```bash
"$VERIFY_PYTHON" "$EVIDENCE_REPO/67180/run_verification.py" \
  --python "$VERIFY_PYTHON" --label baseline \
  --source "$tmp_dir/baseline" \
  --output-dir "$tmp_dir/results-baseline" \
  --ray-temp-dir /tmp/r67180-base

"$VERIFY_PYTHON" "$EVIDENCE_REPO/67180/run_verification.py" \
  --python "$VERIFY_PYTHON" --label head \
  --source "$tmp_dir/head" \
  --output-dir "$tmp_dir/results-head" \
  --ray-temp-dir /tmp/r67180-head
```

[`sitecustomize.py`](sitecustomize.py) loads the selected checkout's Python
modules above the shared Core wheel. The test verifies the loaded DataOpTask
path/hash and `ray.__commit__` before exercising the scenario.

The packaged test and runner are path-portable derivatives of the preserved
original harness. Their scenario and assertions are unchanged; only the
original temporary-path/source-manifest dependency was replaced by explicit
arguments and fixed expected hashes. These derivatives were not used to
produce the recorded results.

## Information beyond the PR tests

The existing exception helper failed before any valid output. The successful
queued-output test did not then fail its producer. This case joins those
conditions and establishes output preservation plus exactly-once observable
completion/accounting for both real metadata fetchers.

## Limitations

- The baseline comparison uses baseline Python sources with a shared newer Core
  wheel. It is not a complete source-built reconstruction of either revision.
- The test does not cover deferred metadata, ObjectLostError, lineage
  reconstruction, retries, cancellation, or whole-pipeline abort behavior.
- It manually consumes the real operator queue after failure instead of
  scheduling a downstream operator task.
- The budget policy is explicit. This is not an allocator, admission latency,
  polling overhead, or performance benchmark.
