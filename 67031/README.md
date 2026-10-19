# Ray PR #67031: callback registration ordering

PR: <https://github.com/ray-project/ray/pull/67031>

- Parent: `12d35b8ddb0ed86f668610a81c6758b6b7d64374`
- PR: `4216f80182940b163ad47cf9bade96d6992ea28d`

## Invariant

For an accepted unary request, registering the readiness callback must satisfy
both sides of the ordering race:

- If the callback fires before registration returns, no spent cancellation
  handle is stored.
- If the callback is still pending when registration returns, its cancellation
  handle remains available.

[`test_additional_interleavings.py`](test_additional_interleavings.py) runs the
real `ActorReplicaResult` and `_consume_generator_ref_when_ready` Python
implementation with controlled test doubles. It preserves four scenarios:

1. callback after handle storage;
2. callback with an error;
3. cancellation after registration;
4. cancellation overlapping registration while a modeled stream-terminal
   error callback fires.

The event-order and cancellation assertions are intentional. This is mocked
Python-level verification; it does not execute native `ObjectRef._on_ready`, a
real `ObjectRefGenerator`, or native stream-terminal cancellation.

## Minimum environment

- A Ray checkout containing both revisions.
- A virtual environment supported by those revisions.
- `pytest`, `pytest-asyncio`, Ray Serve dependencies, generated protobuf
  modules, and a loadable `_raylet` built for the checkout and Python ABI.
- The checkout's `python/` directory must win import resolution. Verify
  `ray.__file__` and `ray.serve._private.replica_result.__file__` before testing.

Do not combine newer Python sources with an older `_raylet`; that cannot verify
the native path and may fail before the test executes.

## Reproduce the parent RED / PR GREEN

Set paths without embedding them in the evidence:

```bash
export RAY_CHECKOUT=/path/to/ray
export EVIDENCE_REPO=/path/to/ray-review-evidence
export TEST_FILE=python/ray/serve/tests/test_actor_replica_wrapper.py
export PARENT=12d35b8ddb0ed86f668610a81c6758b6b7d64374
export PR=4216f80182940b163ad47cf9bade96d6992ea28d
```

Create isolated checkouts. This does not modify the main Ray checkout:

```bash
tmp_dir=$(mktemp -d)
git -C "$RAY_CHECKOUT" worktree add --detach "$tmp_dir/parent" "$PARENT"
git -C "$RAY_CHECKOUT" worktree add --detach "$tmp_dir/pr" "$PR"
```

Apply only the PR's test change to the parent checkout:

```bash
git -C "$RAY_CHECKOUT" diff "$PARENT" "$PR" -- "$TEST_FILE" \
  | git -C "$tmp_dir/parent" apply
```

In a compatible Ray development environment, run the same nodes in each
checkout:

```bash
cd "$tmp_dir/parent"
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD/python" \
  python -m pytest -p no:cacheprovider -vv \
  "$TEST_FILE::test_consume_wait_fired_before_registration_leaves_no_cancel_handle" \
  "$TEST_FILE::test_pending_consume_wait_keeps_cancel_handle"

cd "$tmp_dir/pr"
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD/python" \
  python -m pytest -p no:cacheprovider -vv \
  "$TEST_FILE::test_consume_wait_fired_before_registration_leaves_no_cancel_handle" \
  "$TEST_FILE::test_pending_consume_wait_keeps_cancel_handle"
```

Expected observations:

- Parent: callback-before-return fails because `_cancel_consume_wait` contains
  the spent handle; pending-handle test passes (`1 failed, 1 passed`).
- PR: both pass (`2 passed`).

## Run the four additional interleavings

Use the PR checkout and confirm imports resolve there:

```bash
cd "$tmp_dir/pr"
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD/python" python -c \
  'import ray; import ray.serve._private.replica_result as r; print(ray.__file__); print(r.__file__)'

PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD/python" \
  python -m pytest -p no:cacheprovider -vv -s \
  "$EVIDENCE_REPO/67031/test_additional_interleavings.py"
```

Expected: `4 passed`, with the asserted event order printed for each scenario.

## Limitations

- The packaged test is not fully production-integrated.
- It does not prove native callback scheduling or real stream-terminal
  cancellation semantics.
- The original review could not run the current native path because the
  available `_raylet` predated `ObjectRef._on_ready`; a matching Ray build or
  upstream CI is still required for that evidence.
