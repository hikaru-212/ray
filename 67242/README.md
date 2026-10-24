# Ray PR #67242: unordered actor watermark reachability

- Issue: <https://github.com/ray-project/ray/issues/64692>
- PR: <https://github.com/ray-project/ray/pull/67242>
- Baseline (B): `0a509f3448c1f3c71f80b80f46a6fb6a4df971ff`
- Reviewed HEAD (H): `7430fd6fecf4d2069421cd764ce3fa3128bc8af1`

## Result and claim boundary

The observed production-component path for unordered actor submissions sent
`skip_queue=true`; `CoreWorkerClient` then wrote
`client_processed_up_to=-1` on the serialized loopback gRPC request. Six
unordered cases covered dependency reordering, a test-driven retry shape,
separate concurrency groups, and a newly constructed client. Every wire
watermark was `-1`. An ordered-branch positive control observed `-1` and then
`0`, showing that the observation seam could detect a positive watermark.

The queue-level production-valid `-1` schedules passed on both B and H:

| Revision | Component tests | Result |
| --- | ---: | --- |
| B | 3 | PASS |
| H | 3 | PASS |

These are PASS/PASS non-regression results. They show that the PR does not
change the tested production-valid `-1` schedules. They do not demonstrate a
parent failure repaired by H.

The issue's regression test directly injects a nonnegative watermark into the
unordered execution queue. No supported production producer of such a value
was established for the matching unordered path. The repository search cannot
prove that every custom, legacy, or mixed-version client is unreachable, so
universal unreachability remains unknown. Within the inspected ordinary Ray
path, the evidence does **not** establish an original production bug repair.

## Issue-first semantic model

The protocol comment describes `client_processed_up_to` as an obsolete request
prefix: requests at or below the value may be canceled because the client has
processed their responses or no longer needs them. A later request completing
does not, by itself, revoke an earlier valid unordered request. Unordered
execution changes execution order; it does not turn a maximum-completed
sequence into a safe obsolete prefix.

At B and H, `OutofOrderActorSubmitQueue::PopNextTaskToSend()` returns
`skip_queue=true`. The real client branch for `skip_queue=true` overwrites the
watermark with `-1`. H changes only the unordered execution-queue consumer and
its queue test; it does not add a nonnegative unordered producer or a
producer-side contiguous/obsolete-prefix guarantee.

Consequently, the author test remains a valid conditional queue-policy test:
if a caller supplies a sound nonnegative obsolete prefix, H rejects a matching
dependency-waiting request at its final queue admission check. That fixture is
not evidence that the input is produced by the inspected production path.

## Evidence

[`wire_test.cc`](wire_test.cc) uses the real unordered submit queue, a small
request-construction bridge, the real `CoreWorkerClient`, and a real loopback
gRPC service. The bridge deliberately does not assign
`client_processed_up_to`. The recorded output is
[`observed/wire.stdout.txt`](observed/wire.stdout.txt).

[`queue_test.cc`](queue_test.cc) compiles the exact B or H unordered queue
source with Ray's real `ActorTaskExecutionArgWaiter` and concurrency-group
executors. It textually includes the exact B queue-test fixture preserved as
[`baseline-fixture.cc`](baseline-fixture.cc). The tests cover:

- one group with execution order B, C, A and watermark `-1`;
- separate groups with the same production-valid watermark and outcome;
- same-task retry-attempt serialization, including an explicit-cancel control.

The exact outputs are
[`queue-baseline.stdout.txt`](observed/queue-baseline.stdout.txt) and
[`queue-head.stdout.txt`](observed/queue-head.stdout.txt). A compact result and
scope record is in [`results.json`](observed/results.json).

The three test-only sources are byte-identical to the final Phase 3 sources.
Their SHA-256 values, and those of the observed records, are listed in
[`SHA256SUMS`](SHA256SUMS). Exact B/H production blob identities are listed in
[`SOURCE_MANIFEST.tsv`](SOURCE_MANIFEST.tsv).

## Reproduction

Use isolated Ray checkouts at the exact SHAs above with Ray's supported C++
build environment. This package does not vendor Ray, generated protobuf code,
third-party dependencies, Bazel caches, or binaries.

For each checkout, copy the test-only files into the existing test package and
append the supplied targets:

```bash
export EVIDENCE_REPO=/path/to/ray-review-evidence
export RAY_CHECKOUT=/path/to/isolated/ray-checkout

cp "$EVIDENCE_REPO/67242/baseline-fixture.cc" \
  "$RAY_CHECKOUT/src/ray/core_worker/task_execution/tests/"
cp "$EVIDENCE_REPO/67242/queue_test.cc" \
  "$RAY_CHECKOUT/src/ray/core_worker/task_execution/tests/"
cp "$EVIDENCE_REPO/67242/wire_test.cc" \
  "$RAY_CHECKOUT/src/ray/core_worker/task_execution/tests/"
cat "$EVIDENCE_REPO/67242/BUILD.bazel.append" >> \
  "$RAY_CHECKOUT/src/ray/core_worker/task_execution/tests/BUILD.bazel"
```

Run the queue evidence on both B and H:

```bash
cd "$RAY_CHECKOUT"
bazel test \
  //src/ray/core_worker/task_execution/tests:pr_67242_queue_evidence_test \
  --check_visibility=false --test_filter='Phase3Queue.*' --test_output=errors
```

Run the wire observation on H:

```bash
cd "$RAY_CHECKOUT"
bazel test \
  //src/ray/core_worker/task_execution/tests:pr_67242_wire_evidence_test \
  --check_visibility=false --test_filter='Phase3Wire.*' --test_output=errors
```

Use a fresh isolated checkout for each revision or remove the appended block and
copied test-only files afterward. The original Phase 3 run compiled the selected
B/H production translation units against one frozen existing support
environment; it did not execute these full Bazel targets. These portable Bazel
instructions were prepared during evidence packaging and were not used to
produce the recorded outputs.

## Component-level limitations

- The wire seam does not execute `ActorHandle`, `ActorTaskSubmitter`, automatic
  retry policy, or automatic reconnect machinery. The real submit queue
  supplies `skip_queue`; a bridge builds the request passed to the real client.
- The queue seam does not execute `CoreWorker`, `TaskReceiver`, a real object
  store, actor user code, asyncio fibers, or end-to-end RPC completion.
- The original Phase 3 binaries used exact selected B/H components with cached
  support libraries that were not rebuilt from the same revisions. This is not
  a complete matching-revision Ray build.
- The issue-first audit and component tests did not establish a supported
  nonnegative watermark producer for an unordered receiver.
- Full integration, mixed-version interoperability, and custom clients remain
  outside the evidence boundary.

The complete raw investigation, including preliminary harnesses, failed setup
attempts, binaries, build records, exact compiler inputs, and local provenance,
is retained privately and is intentionally excluded from this minimal public
package.
