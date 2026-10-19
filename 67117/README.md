# Ray PR #67117: decorated multiplexed method binding

PR: <https://github.com/ray-project/ray/pull/67117>

- Baseline: `804929ec21d8ccd842594db8e10e01c1c40d936e`
- PR: `2c305f07a6306d64d19d3a0abdbbc721c1d6ce92`

## Invariant

When a method decorated with `@serve.multiplexed` is wrapped by one or more
outer decorators that preserve `__wrapped__`, Ray must recover the correct
receiver instance. Each instance must own a distinct multiplex wrapper and
model cache.

[`multiplex-independent-test.patch`](multiplex-independent-test.patch) is a
test-only patch for `python/ray/serve/tests/test_multiplex.py`. It covers one
and two outer decorators and asserts:

- calls bind to the correct receiver (`A` versus `B`);
- each receiver loads model `X` once and then uses its cache;
- the wrapper is stored on each instance, not on the class;
- the wrappers' `self_arg` values identify their owning instances;
- wrapper and model-cache objects are distinct;
- cached values remain receiver-specific;
- every created wrapper and `MetricsPusher` is shut down in `finally`.

The patch also calls `ray.init(include_dashboard=False)` in the existing Serve
fixture so the focused test can run independently. It changes no production
file.

## Minimum environment

- A Ray checkout containing both revisions.
- A virtual environment supported by those revisions with `pytest`,
  `pytest-asyncio`, and Ray Serve dependencies.
- Native Ray extension, daemons, and generated protobuf modules compatible
  with the selected checkout and Python ABI.
- A host able to start a local Ray/Serve instance; the configured local ports,
  including Serve's HTTP port, must be available.

## Apply and execute

Set paths:

```bash
export RAY_CHECKOUT=/path/to/ray
export EVIDENCE_REPO=/path/to/ray-review-evidence
export BASELINE=804929ec21d8ccd842594db8e10e01c1c40d936e
export PR=2c305f07a6306d64d19d3a0abdbbc721c1d6ce92
export PATCH_FILE="$EVIDENCE_REPO/67117/multiplex-independent-test.patch"
export TEST_NODE=python/ray/serve/tests/test_multiplex.py::test_multiplexed_method_wrapped_by_other_decorators
```

Create isolated checkouts:

```bash
tmp_dir=$(mktemp -d)
git -C "$RAY_CHECKOUT" worktree add --detach "$tmp_dir/baseline" "$BASELINE"
git -C "$RAY_CHECKOUT" worktree add --detach "$tmp_dir/pr" "$PR"
```

Check and apply the same test-only patch to both:

```bash
git -C "$tmp_dir/baseline" apply --check "$PATCH_FILE"
git -C "$tmp_dir/pr" apply --check "$PATCH_FILE"
git -C "$tmp_dir/baseline" apply "$PATCH_FILE"
git -C "$tmp_dir/pr" apply "$PATCH_FILE"
```

Run the focused test in a compatible Ray development environment:

```bash
cd "$tmp_dir/baseline"
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD/python" \
  python -m pytest -p no:cacheprovider -vv -s "$TEST_NODE"

cd "$tmp_dir/pr"
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$PWD/python" \
  python -m pytest -p no:cacheprovider -vv -s "$TEST_NODE"
```

Expected observations:

- Baseline: both parameter cases fail while calling the multiplexed method,
  with a `TypeError` reporting more than one argument (`2 failed`).
- PR: both parameter cases pass (`2 passed`).

The prior review also established batching-focused baseline/PR results and a
green full batching module. Those tests are not duplicated here; this package
preserves only the independent multiplex receiver/cache-ownership regression.

## Limitations

- The test starts real local Ray and Serve processes, but it is not a full
  deployment-handle or multi-node end-to-end test.
- Reproduction still depends on a revision-compatible Ray development
  environment; this repository intentionally does not include binaries,
  generated protobufs, or a virtual environment.
