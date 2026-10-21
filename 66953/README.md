# Ray PR #66953: ObjectManager Push RPC deadline

PR: <https://github.com/ray-project/ray/pull/66953>

Issue: <https://github.com/ray-project/ray/issues/66952>

- Base / merge-base: `3c9eb4865cce54acf8b4c58b6d8510ee73935fef`
- PR: `86bce6815d3253452552577c1123962ab6a24dbe`

## Invariant

A real gRPC deadline on Push A must reach the production completion callback,
release its `PushManager` in-flight slot, and admit independent Push B that was
already queued. With the same binary and deadline disabled, B must remain
queued through the same finite observation window until A's reply is released.

[`p1_deadline_integration_test.cc`](p1_deadline_integration_test.cc) exercises:

`ObjectManager::Push` → `PushFromPlasma` → `PushObjectInternal` →
`PushManager::StartPush` → `ScheduleRemainingPushes` → `SendObjectChunk` →
`ObjectManagerClient::Push` → real gRPC deadline/completion queue → production
completion callback → `OnChunkComplete` → `ScheduleRemainingPushes`.

The observation wrapper forwards the real callback. It does not synthesize a
timeout, cancel the RPC, release capacity, or invoke `OnChunkComplete`. Fake
GCS, directory, and Plasma components supply addresses and two local one-byte
objects; the sender ObjectManager, PushManager, RPC client, transport, deadline,
and completion callbacks are production implementations.

## Recorded environment and result

The original verification ran on Ubuntu 24.04.4, Linux aarch64, GCC 13.3.0,
Bazel 7.5.0, C++17 optimized mode, and Ray's pinned gRPC 1.58.0 plus patches. A
`git archive` of the exact PR revision was built separately from the review
worktree. The excluded test binary had SHA-256
`d99744ac758d4c0932edd0c85649f0c1d0bbbdd38a6da4dc8c63b0057430b08d`.

Both fresh-process arms passed:

- `P1_PUSH_DEADLINE_MS=3000`: A's client callback reported `Deadline Exceeded`
  at about 3004 ms; B reached the server at about 3005 ms, before A's explicit
  release. The 4000 ms checkpoint was `flight=1 remaining=1 queued=0`.
- `P1_PUSH_DEADLINE_MS=-1`: no A callback or B dispatch occurred before the
  4000 ms checkpoint, which remained `flight=1 remaining=2 queued=1`.
  Releasing A then admitted B.
- Both arms drained to `flight=0 remaining=0 queued=0`, with one dispatch and
  one observed callback per object.

The byte-identical compact records are
[`results-v3.json`](observed/results-v3.json) and
[`trace-audit-v3.json`](observed/trace-audit-v3.json). The latter preserves the
ordered event summaries and SHA-256 of each full archived log. This packaging
step did not rerun the experiment.

This is positive integration verification. It does not report a newly found
correctness bug.

## Build and reproduce

Use an isolated checkout or source export at the exact PR revision, then set:

```bash
export RAY_CHECKOUT=/path/to/ray-at-86bce6815d3253452552577c1123962ab6a24dbe
export EVIDENCE_REPO=/path/to/ray-review-evidence
export OUTPUT_DIR=/short/path/ray-66953-output
```

Copy the harness and append the target block only in that isolated source tree:

```bash
cp "$EVIDENCE_REPO/66953/p1_deadline_integration_test.cc" \
  "$RAY_CHECKOUT/src/ray/object_manager/tests/"
cat "$EVIDENCE_REPO/66953/BUILD.bazel.append" >> \
  "$RAY_CHECKOUT/src/ray/object_manager/tests/BUILD.bazel"
```

Build with the checkout's supported Bazel toolchain:

```bash
cd "$RAY_CHECKOUT"
bazel --batch build \
  //src/ray/object_manager/tests:p1_deadline_integration_test \
  --check_visibility=false --jobs=2 --local_ram_resources=3072 \
  --color=no --curses=no
```

Run the finite and disabled controls through the portability helper:

```bash
python "$EVIDENCE_REPO/66953/run_verification.py" \
  --binary "$RAY_CHECKOUT/bazel-bin/src/ray/object_manager/tests/p1_deadline_integration_test" \
  --output-dir "$OUTPUT_DIR"
```

[`run_verification.py`](run_verification.py) is a path-portable derivative of
the original runner. It was not used to produce the recorded results. It keeps
the original 45-second watchdog and fresh-process controls and refuses to
overwrite an existing output directory.

## Information beyond the PR tests

The PR's client test checked a real gRPC timeout. Its PushManager test injected
a failure through a fake client and queued independent work only after that
failure. This harness joins the real deadline and production callback to
PushManager admission while B is already queued, with a same-binary disabled
deadline counterfactual.

## Limitations

- The harness covers one slot, one chunk, two one-byte objects, and one remote
  address; it is not a throughput or latency benchmark.
- The controlled server is real gRPC, but is not a complete receiving
  ObjectManager/Plasma stack.
- It does not cover multi-chunk objects, retries, deduplication, node loss,
  shutdown races, retransmission correctness, or slow healthy links.
- The `-1` arm establishes blocking only through the shared 4000 ms window.
- No parent-revision regression run was performed.
