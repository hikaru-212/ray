# Ray PR #67360: independent verification of lifetime status summaries

Subject: [ray-project/ray#67360](https://github.com/ray-project/ray/pull/67360).
Experiment date: 2026-10-27.

The objective is to independently check retry accounting, QUEUED and REQUESTED ordering, and decisions after failed storage commits when status history is truncated, following the migration of lifetime summaries from protobuf fields into InstanceManager-owned memory.

This directory contains a minimal public record of completed experiments. **Packaging and English documentation finalization did not rerun tests or execute the portable environment setup.** This README and REPRODUCE.md are editorial documentation, not original experiment output. The earlier evidence in the parent directory remains unchanged. Its 16-case suite and this round's 31-case suite are separate experiments and should not be combined or substituted for one another.

## Revisions and scope of the conclusion

| Label | Exact revision |
| --- | --- |
| Original Parent (parent) | `3608367390ff80151fe0ab02a54d8e8223905146` |
| Previously Verified HEAD (old) | `ad540e643022b8c449299653e476f20da35afd2d` |
| Current HEAD (new; the revision specified for this experiment) | `ed1357405af868d81389047835e3562b29ca2751` |

**GREEN within the tested Python production component contract.** Old and New both passed all 31 common behavioral invariant cases. The investigation did not identify a correctness defect introduced by New with an established production path. This is not a guarantee of complete Autoscaler correctness, native ABI compatibility, or live cluster behavior, and it does not imply maintainer acceptance.

| Evidence category | What the evidence supports |
| --- | --- |
| Runtime verified | Behavior of the real InstanceManager update API, InstanceStorage, and Reconciler in the controlled cases below: retry exhaustion, bounded history, storage failure and retry, and ordering decisions. |
| Source confirmed | Summary ownership, removal of the protobuf summary fields, commit ordering, deletion cleanup, the inspected Autoscaler construction path, and the details propagation gap relevant to #67381. |
| Component characterization | Selection under ties and clock rollback, and information lost when reconstructing a manager while retaining truncated storage. Passing these cases does not establish a general ordering policy or safe recovery. |
| Not verified | Complete Ray Autoscaler correctness; live GCS or cloud provider E2E; a native rebuild; process-crash recovery or production restart safety; arbitrary manager/storage replacement or external status writers; all deployment configurations; or combined #67360 + #67381 execution. |

## The 31 common behavioral invariant cases

The following table includes only the common behavioral invariants. Four sorting characterization cases and two manager reconstruction boundary cases are reported separately.

| Category | Cases | Parent passed / failed | Old passed / failed | New passed / failed |
| --- | ---: | ---: | ---: | ---: |
| Exact retry exhaustion boundary | 4 | 2 / 2 | 4 / 0 | 4 / 0 |
| Bounded history and serialization roundtrip | 2 | 2 / 0 | 2 / 0 | 2 / 0 |
| Failed batch and exactly-once retry accounting | 6 | 6 / 0 | 6 / 0 | 6 / 0 |
| Lazy initialization and failed commit | 1 | 0 / 1 | 1 / 0 | 1 / 0 |
| Failed deletion and deletion/recreation | 3 | 3 / 0 | 3 / 0 | 3 / 0 |
| QUEUED/REQUESTED ordering after truncation | 4 | 0 / 4 | 4 / 0 | 4 / 0 |
| Timestamp zero remains usable after truncation | 2 | 0 / 2 | 2 / 0 | 2 / 0 |
| Clock rollback and timeout boundary | 1 | 1 / 0 | 1 / 0 | 1 / 0 |
| Write/delete failure after long-history truncation | 8 | 0 / 8 | 8 / 0 | 8 / 0 |
| **Common behavioral invariant total** | **31** | **14 / 17** | **31 / 0** | **31 / 0** |

**The 17 failures are test cases, not 17 distinct correctness defects.** A common cause can appear across history caps, injected failures, and consumers.

The original pytest executions used a main suite and a follow-up suite, rather than a single 37-case invocation:

| Original execution | Parent passed / failed | Old passed / failed | New passed / failed |
| --- | ---: | ---: | ---: |
| Main suite: 23 common cases + 4 sorting observations + 2 reconstruction boundaries | 20 / 9 | 29 / 0 | 29 / 0 |
| Follow-up: 8 common cases | 0 / 8 | 8 / 0 | 8 / 0 |
| **Total pytest results** | **20 / 17** | **37 / 0** | **37 / 0** |

These counts were cross-checked against the six original result JSON files, JUnit XML files, and pytest logs during evidence preparation. See [summary.json](observed/summary.json), [per-case results](observed/cases.json), and the [log summary excerpts](observed/pytest-summary.txt).

## Oracles and RED/GREEN behavior

The [main tests](tests/test_behavior.py) create legal transitions through the real `update_instance_manager_state()` API. A subscriber records successful update events as an independent ledger of committed REQUESTED transitions. The tests also inspect stored state and Reconciler outcomes. Some cases include white-box cache assertions, but the final retry oracle is not derived solely from the new summary getter. The signature adapter in `Rig.decision()` supplies the manager argument where the internal API requires it.

- History caps are 10 and 200; retry limits are 3 and 100. The existing exhaustion condition is `REQUESTED count > limit`: limit 3 reaches ALLOCATION_FAILED at the fourth REQUESTED transition, and limit 100 at the 101st. At limit 100, Parent remains QUEUED for both caps where Old and New reach ALLOCATION_FAILED.
- After 120 committed REQUESTED transitions, history contains only 10 or 200 entries, and the protobuf roundtrip retains the bounded history. This checks serialization of retained history; **it does not require New to retain the removed protobuf summary fields**.
- For QUEUED launch and REQUESTED allocation, the tests first establish that the original timestamp has been trimmed, then ask Reconciler to select the older instance. Parent fails the oracle; Old and New select `older`. The two cases with an original timestamp of zero have the same RED/GREEN result.

## Storage failure, lazy cache initialization, and deletion/recreation

Runtime cases cover a request-version mismatch, an actual storage write compare-and-swap (CAS) conflict, a failed write without a version change, warm and cold caches, and a multi-instance batch. The CAS conflict is produced by **a real commit to an unrelated table in the same InMemoryStorage**. Generic storage failures are injected as `StoreStatus(False, version)`; these are not tests of actual disk or distributed-storage failure.

The failed batch includes the existing `subject`, a second instance, and a new ID. Assertions check that existing serialized bytes remain unchanged, the new ID is absent, and no subscriber notification is emitted. After a successful retry, committed request 3 returns to QUEUED at limit 3; request 4 reaches ALLOCATION_FAILED. These decisions check that a failed candidate is not counted and a successful transition is not counted twice in this scenario.

New's lazy initialization **can create a cache entry before a storage commit**. That entry is seeded from already committed history. The corresponding test retains all 201 history entries, reconstructs the manager, reduces the cap, and injects a failure. The cache contains the 100 existing REQUESTED transitions, excluding the failed candidate. The next successful transition is number 101 and exhausts limit 100.

The [long-history follow-up](tests_followup/test_long_history_failures.py) first commits 120 REQUESTED transitions and truncates history, then separately injects a write CAS conflict, write failure, delete-version mismatch, or delete failure. With limit 121, Old and New remain QUEUED after committed request 121 and reach ALLOCATION_FAILED after request 122. All eight Parent parameterizations fail the full-lifetime exhaustion oracle.

Failed deletion retains stored data and the observed retry behavior. After successful deletion and recreation with the same ID, New does not reuse the old summary. The recreated instance is ordered behind the existing `older-survivor`; its new lifetime reaches ALLOCATION_FAILED only on its second REQUESTED transition at limit 1.

## State ownership and control flow: source findings

In Old, the `Instance` protobuf owns `status_transition_counts` and `first_status_transition_times_ns`, so summaries are stored with the instance. New removes those two protobuf fields. Each InstanceManager instead owns `_status_summaries[instance_id]`, containing counts and the minimum timestamp for each status.

The relevant order in [New's InstanceManager](https://github.com/ray-project/ray/blob/ed1357405af868d81389047835e3562b29ca2751/python/ray/autoscaler/v2/instance_manager/instance_manager.py#L23) is:

1. `update_instance_manager_state()`, lines 93-108, reads storage and checks the request version.
2. Lines 113-125 seed an existing instance's summary from stored history before a candidate update can truncate it. A new instance starts with an empty summary that has not yet been inserted into the cache.
3. Lines 128-147 attempt the storage CAS and return on failure.
4. Lines 152-158 record the new transition and update the cache only after a successful commit, then notify subscribers.
5. `delete_instances()`, lines 231-238, deletes from storage first and removes cached summaries only on success.
6. `_get_status_summary()`, lines 244-250, can reconstruct only information still present in history.

`record_transition()`, lines 29-35, reads only status and timestamp. `first_times_ns` stores the **minimum timestamp**, which need not be the timestamp of the first call when the clock moves backward. Reconciler's launch, allocation, and stuck-requested paths obtain summaries from the manager. This avoids extending the protobuf for lifetime accounting, but makes that accounting depend on the manager's lifetime.

## Sorting characterization

The four characterization cases use a fixed creation and transition sequence for instances `b` and `a`:

| Scenario | Parent selects | Old selects | New selects |
| --- | --- | --- | --- |
| QUEUED, equal first timestamps | a | b | b |
| REQUESTED, equal first timestamps | a | b | b |
| QUEUED, clock rollback | a | a | a |
| REQUESTED, clock rollback | b | a | a |

The differences from Parent already exist in Old. These tests require one valid candidate to be selected; they do not impose an independently established total-order or fairness contract for ties. Their passing status therefore does not prove the correctness of an ordering policy.

A separate timeout invariant checks that clock rollback and elapsed time exactly equal to the timeout leave the instance REQUESTED; handling occurs after the timeout is exceeded. Other recorded Old/New observations agree after excluding reconstruction and the white-box cache-ID difference. This is not proof of equivalence across all possible executions.

## Manager reconstruction: an observed component boundary

The boundary test deliberately retains the same InstanceStorage and reconstructs InstanceManager after 101 REQUESTED transitions, with retry limit 100:

| History cap | Retained REQUESTED entries | Old after reconstruction | New after reconstruction |
| --- | ---: | --- | --- |
| 10 | 5 | Restores protobuf count 101; immediately reaches ALLOCATION_FAILED | Count becomes 5; initially QUEUED, then fails after 96 additional successful REQUESTED transitions |
| 200 | 100 | Restores protobuf count 101; immediately reaches ALLOCATION_FAILED | Count becomes 100; initially QUEUED, then fails after 1 additional successful REQUESTED transition |

New's first timestamps are likewise reconstructed only from the retained tail. Parent does not exhaust retries within the test's finite observation window of 102 additional requests; that observation alone does not prove infinite execution. A reconstruction test passing means the recorded component behavior was observed, **not that lifetime counts survived reconstruction**.

The inspected [Autoscaler._init_instance_manager()](https://github.com/ray-project/ray/blob/ed1357405af868d81389047835e3562b29ca2751/python/ray/autoscaler/v2/autoscaler.py#L128) creates a new InMemoryStorage at lines 139-142 together with a new manager at lines 178-181. The ordinary reconciliation exception handler at lines 232-234 retains the existing objects. The source investigation found no supported production path that replaces only the manager while retaining the same storage, and no production status writer bypassing the manager.

This is a demonstrated **component boundary and state-ownership tradeoff**, without an established production path for the retained-storage reconstruction scenario. It is not a confirmed production regression. It also does not rule out recovery risk or establish that arbitrary manager/storage replacement is safe. A single reconstruction in New added a finite number of retries in these tests; no production infinite-retry scenario was reproduced.

## Environment, provenance, and limitations

The original environment was Linux aarch64, Python 3.12.3, pytest 7.4.4, protobuf 6.33.6, and grpcio/grpcio-tools 1.75.0. Each revision used its exact Python sources and separately regenerated bindings for 52 protobuf files. Reporter bindings were retained from the wheel.

The native import scaffold reused `_raylet.so` from Ray 2.58.0, with SHA-256:

`af46848e8b413a027aa17b958d1dd1bf93f41c4a55b9009002ce7e1112ef6ec0`

**No native rebuild was performed at any of the three tested revisions.** No Ray cluster, live GCS, or cloud provider was started. Native/protobuf compatibility evidence is limited to the components actually loaded and exercised.

The original provenance audit checked 5,454 exported source files and 229 loaded source modules per revision. Parent and Old were checked against read-only Git objects. New was checked using Old plus blob metadata from the pinned GitHub commit and comparison. This public package includes selected source hashes, verification results, and dependency versions, rather than full source manifests, source snapshots, or binaries.

During initial packaging, all 17,679 files covered by the private archive's SHA256SUMS.json were rehashed successfully. The archive manifest has SHA-256:

`6e25d471b7f5fdc2643e37933d70e5198e614249a3beb2e0128a52239a1ade1e`

This identifies the private manifest; it does not let public readers independently inspect unpublished originals. [provenance.json](provenance.json) records original input hashes and the transformations used for public files.

The [historical preservation record](observed/preservation-result.json) belongs to the original runtime experiment: 40 existing worktrees, refs, and working diffs matched before and after the experiment, and one pre-existing missing/prunable inventory entry remained unchanged. **Packaging and documentation finalization did not operate on source/review worktrees or rerun that snapshot procedure.**

The original setup attempt stopped because the New archive was not yet available. Parent and Old setup had completed; a later new-only setup completed New before testing. Those setup logs remain private. The setup failure is not an additional failed behavioral case.

## Files and reproduction

| Files | Purpose and provenance |
| --- | --- |
| `tests/test_behavior.py`, `tests/conftest.py` | Unchanged copies of the executed main suite, event ledger, and failure/environment recording hook. |
| `tests_followup/test_long_history_failures.py`, `tests_followup/conftest.py` | Unchanged copies of the eight executed failure cases after history truncation. |
| `scripts/run_matrix.py`, `scripts/run_followup.py` | Unchanged executed runners, preserving pytest arguments, environment settings, and separate revision processes. |
| `scripts/prepare_runtime.py` | Portable derivative of the original setup: a provenance docstring was added and the fixed native package path was replaced with an environment variable. **This public derivative has not been executed.** |
| `scripts/check_sources.py` | Helper added during packaging to check selected source hashes. **It has not been executed in a reproduction environment** and does not replace the original full provenance audit. |
| `REPRODUCE.md` | Editorial instructions for a fresh execution directory, with command attribution, expected results, and limitations. |
| `observed/summary.json` | Unchanged original result summary, separating common invariants from characterization and boundary cases. |
| `observed/cases.json` | Per-case setup/call/teardown results, categories, and observations extracted from six original result files; full tracebacks and absolute trace paths are omitted. |
| `observed/commands.json` | Actual pytest/protoc arguments, working directories, environments, and return codes, with documented placeholders replacing local path prefixes. |
| `observed/pytest-summary.txt` | Summary lines from six original pytest logs and the corresponding original log hashes. Only the two introductory documentation lines were translated; the excerpts and hashes are unchanged. |
| `observed/provenance-verification.json` | Unchanged source, loaded-module, and #67381 static comparison results. |
| `observed/preservation-result.json` | Unchanged historical worktree preservation result, without the worktree inventory. |
| `provenance.json`, `SHA256SUMS` | Input attribution, public transformation records, selected source/environment metadata, and public file checksums. |

Follow [REPRODUCE.md](REPRODUCE.md) and copy the scripts into a fresh private execution directory. The original runners overwrite their output paths; do not run them in this public package or an existing experiment directory. The unexecuted environment examples are not additional verification results.

The public `SHA256SUMS` covers every other file in this directory, including provenance.json, and excludes itself. Provenance records original input hashes and documentation transformations; it does not embed the public manifest's hash. There is no self-referential or circular checksum dependency.

## Relationship to independent PR #67381

[#67381](https://github.com/ray-project/ray/pull/67381), at `c17420b096e539dd28094b4087539ec13bba8bd2`, addresses transition-details propagation described in [#67380](https://github.com/ray-project/ray/issues/67380). It is independent of #67360's lifetime status summary accounting.

The source comparison found that New's `_update_instance()` still does not pass event details to `InstanceUtil.set_status()`; `details=update.details` remains a valid independent correction. Summaries read status and timestamp, not details. The two PRs were not combined, and no combined tests were run. This investigation identified no reason to require a change to #67381.

The evidence remains limited to three-revision Python component verification using common behavioral oracles, source control-flow findings, and the stated reconstruction boundary. Complete original experiment artifacts remain in the private archive.
