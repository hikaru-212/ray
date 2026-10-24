# Worker cleanup after a canceled actor lease

Independent component evidence for [Ray PR #67315](https://github.com/ray-project/ray/pull/67315).

| Source | Exact revision |
| --- | --- |
| Parent | `4b067744d800b4961877501a2b07714d386556e3` |
| HEAD | `7ac19753f293035c8120a4a9497c771d95a980f3` |

The isolated cancellation control is RED on parent and GREEN on HEAD. A second,
valid same-node restart accepts W1 for the current creation attempt in both
versions; HEAD additionally cleans up unused W2. The original claim that W1
must always be killed was too strong and is not a confirmed bug.

## Evidence and boundaries

| Experiment | Parent | HEAD | Interpretation |
| --- | --- | --- | --- |
| Cancel L1, mark actor RESTARTING, deliver W1 with no L2 | FAIL: no cleanup; one modeled orphan | PASS: cleanup targets W1, with nil intended actor | Valid original cancellation cleanup RED/GREEN. |
| Real manager restart, same node, late compatible W1, then W2 | FAIL only on missing W2 cleanup | PASS | Both use W1 once, with current attempt 1 and its valid CPU mapping; HEAD cleans up W2 without killing adopted W1. |
| Stop after W1 becomes ALIVE, before granting L2 | Pending L2 observed | Pending L2 observed | Inherited follow-up concern, not a regression introduced by this PR. |
| Separate raylet resource accounting | Fresh packaging run PASS | Original and fresh run PASS | With 8 CPU, W1 holds 4 and pending L2 reserves 4; cancel L2 returns 4, release W1 returns 8. |

The GCS harness executes real actor manager, scheduler, node manager and storage
code with fake raylet/worker transport. Cleanup means an observed kill request
and modeled worker cleanup, not an OS process death. W1 has not executed the
old actor constructor and matches the actor, node, owner, job, resources and
placement constraints. Therefore receiving W1 through the old request callback
does not itself make W1 unusable for the new attempt. The validated mapping is
CPU aggregate instance index 0, quantity 4, under the default unit-instance
configuration (`GPU`, not CPU).

Pending L2 remains ungranted at the observed drained event-loop boundary; no
Cancel(L2) was sent and GCS leasing membership was absent. Neither this snapshot
nor the separate raylet test measures its lifetime. Later grant/cleanup, worker
startup timeout and recovery are outside this experiment. The resource test
uses real cluster/local lease managers and resource accounting, with a fake
worker pool. It is not an integrated GCS-to-raylet cluster reproduction and
does not establish a permanent resource leak.

## Correction history

1. Phase 2 required W1 to be killed, W2 alone to create the actor, and L2
   membership to survive. Both versions failed that oracle, including repeated
   runs. Issue-first review established that compatible W1 reuse is valid; the
   failed oracle is retained privately and excluded from the executable package.
2. The first Issue-first fixture used synthetic CPU indices 2/5 with quantity
   4. A second used indices 2/5 with quantity 1 based on an incorrect assumption
   that CPU was a unit-instance resource. Those are not valid CPU mapping proof.
3. The coordinator corrected the fixture to index 0, quantity 4. Its historical
   run selected only `CompatibleGrantUsesCurrentAttemptAndOwnResources`:
   parent failed only W2 cleanup; HEAD passed. The pending-L2 historical trace
   came from the earlier fixture and did not depend on mapping assertions.
4. This package combines the final valid fixture with the original isolated
   cancellation control. Its fresh run also repeats the pending observation
   with the valid mapping. Historical results were not produced by this newly
   combined file.

## Files and reproduction

- `worker_cleanup_test.cc`: explicitly labeled combined derivative, three cases.
- `raylet_resource_test.cc`: byte-identical original independent resource harness,
  including its upstream fixture and license.
- `*.BUILD.bazel.append`: target fragments for an existing Ray development checkout.
- `observed/historical-results.json`: compact audited historical observations.
- `observed/package-validation.json`: separately labeled fresh execution results.
- `provenance.json`: revisions and SHA256 identifiers for private original inputs.

Use Linux and the Ray C++ development dependencies for the selected revision.
The observed environment was Ubuntu 24.04.4/aarch64, GCC 13.3.0 and Bazel 7.5.0.
Follow Ray's [build guide](https://docs.ray.io/en/latest/ray-contribute/development.html)
and use a virtual environment for any Python build tooling. Neither binaries
nor build caches are included.

In a disposable **existing** checkout at either exact revision, set absolute
`RAY_SOURCE` and `EVIDENCE_PACKAGE` paths, then install these test-only files once:

```bash
cp "$EVIDENCE_PACKAGE/worker_cleanup_test.cc" "$RAY_SOURCE/src/ray/gcs/actor/tests/"
cat "$EVIDENCE_PACKAGE/gcs.BUILD.bazel.append" >> "$RAY_SOURCE/src/ray/gcs/actor/tests/BUILD.bazel"
cp "$EVIDENCE_PACKAGE/raylet_resource_test.cc" "$RAY_SOURCE/src/ray/raylet/scheduling/tests/"
cat "$EVIDENCE_PACKAGE/raylet.BUILD.bazel.append" >> "$RAY_SOURCE/src/ray/raylet/scheduling/tests/BUILD.bazel"
cd "$RAY_SOURCE"
bazel test --config=ci --test_output=all //src/ray/gcs/actor/tests:independent_worker_cleanup_test
bazel test --config=ci --test_output=all //src/ray/raylet/scheduling/tests:independent_raylet_resource_test
```

The parent GCS target must report two failures (isolated cancellation and W2
cleanup) and one pass (pending observation); HEAD must pass all three. The
raylet target must pass its one case. Do not reinterpret the expected parent
nonzero exit as a packaging failure or the passing pending observation as proof
that no concern exists.

Fresh packaging validation compiled these exact test files using the archived
GCC compile/link commands and checked loaded library hashes. It used the same
HEAD-built component set with only the audited scheduler library exchanged for
parent/HEAD, matching the original comparison method. The raylet component is
unchanged between these revisions. The Bazel commands above are portable
reproduction instructions; they were not newly executed during packaging.
No claim is made about independent clean builds, all schedules, real worker
processes, restart lifetime, or whole-cluster correctness.
