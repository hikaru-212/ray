# Bounded instance history and lifetime retry accounting

Independent in-process evidence for [Ray PR #67360](https://github.com/ray-project/ray/pull/67360).

| Source | Exact revision |
| --- | --- |
| Parent | `3608367390ff80151fe0ab02a54d8e8223905146` |
| HEAD | `ad540e643022b8c449299653e476f20da35afd2d` |

HEAD repairs retry exhaustion after diagnostic history truncation by retaining
lifetime counters. This does not recover facts already lost from legacy
records, and the new ordering is not equivalent for all ties or clock rollback.
The separately observed loss of history `details` occurs in both versions,
before truncation; it was not introduced by this PR.

## Audited observations

| Check | Parent | HEAD |
| --- | --- | --- |
| Retry cap 10, retry limit 5 | Still nonterminal after 11 requests | Fails allocation at request 6 |
| Retry cap 200, retry limit 100 | Still nonterminal after 106 requests | Fails allocation at request 101 |
| Retry limits 0 and 3 | Terminates at requests 1 and 4 | Same |
| Legacy truncated record: 12 actual requests, 5 retained, retry limit 7 | Still REQUESTED after 10 additional requests | Terminates after 3 additional requests, actual lifetime total 15 |
| Queue/allocation equal-first-time ties, input A then B | Selects B | Selects A |
| Allocation with clock rollback | Selects A | Selects B |
| Queue rollback and distinct-first-time controls | Select A | Select A |
| Stale-at-entry / commit-race CAS rejection | Stored instance bytes unchanged; no notifications | Same |

The timeout checks also verify that the exact boundary is inert and one
nanosecond past it triggers handling. The final Phase 2 suite reports parent
10 PASS / 3 FAIL and HEAD 13 PASS. The three parent failures are two retry cases
and the finite legacy retry budget. Sorting tests characterize actual behavior,
not an independently established fairness contract. CAS retry emits one event;
an invalid replay emits none and changes no stored instance bytes. The
commit-race case includes a real competing storage write.

The legacy fixture is an actual 154-byte parent protobuf, encoded here as text
hex. SHA256: `c7f0060a45849b3aeda44726bbb1ad807b0763d0f116e5de922570b54f279b80`.
It was generated through 12 real manager/reconciler requests with the same retry
limit 7 used after migration. Only 5 requests remain visible in its 10-event
tail. HEAD can initialize summaries from retained information and count future
transitions; it cannot reconstruct the 7 lost requests or discarded timestamps.
The first experimental round used different retry limits (100 before, 7 after)
and was corrected. That failed/confounded history remains in the private archive.

## Additional Issue-first runtime verification

The Stage A contract, original failed attempts, corrected harness and raw logs
are preserved privately. Corrected recent-consumer tests perform 101 legal
request/timeout cycles with a history cap of 10, compare the retained tail's
statuses/timestamps against an independent commit ledger, serialize/reparse into
a fresh in-memory backend and manager, and check recent failed/allocated views.
Both revisions pass the two corrected tests. The latest REQUESTED time is 302s,
whereas the first is 2s. HEAD's two-status summaries occupy 34 serialized bytes
at requests 6, 51 and 101; three statuses occupy 52 bytes at the final outcome.
These are protobuf wire-size observations, not RSS measurements or OOM proof.

A separate GC test creates 40 terminal records, retaining at most 6 after GC
under the selected cadence/retention. It checks the exact expiry boundary and
CAS conflict deferral, then a successful retry. Both versions pass. Historical
initial suites contained this one passing GC test and two failing recent tests;
the corrected historical reruns selected only the two recent tests. The fresh
packaging run executes all three together and is reported separately.

The initial recent tests expected event `details` to survive in status history.
They failed in **both revisions at the first REQUESTED transition**, before any
truncation. The corrected tests preserve and explicitly assert the observed
omission: event details exist, but later history entries and consumer reason
strings are empty. This documents an inherited diagnostic limitation, not
successful preservation of details or a regression attributable to this PR.

## Files and reproduction

`test_history.py` and `test_recent_consumers.py` are portable derivatives of the
final original tests. Only output placement changed; assertions and experiments
are unchanged. `run_verification.py` is a new launcher, not the producer of the
historical logs. It checks critical source hashes, the protobuf summary fields,
and loaded Ray module paths, and writes fresh results outside the package.

`observed/phase2-results.json` is the unchanged original compact summary.
`observed/issue-first-results.json` combines original compact observations with
explicit run attribution. `observed/package-validation.json` records this
package's new executions. `provenance.json` identifies original inputs by hash.
Complete logs, module inventories and failed experimental rounds stay private.

Use an existing checkout/export at each exact revision and a virtual environment
with its dependencies. Build/install Ray for that revision following the
[Ray development guide](https://docs.ray.io/en/latest/ray-contribute/development.html).
The launcher expects the source tree's `python/ray` to contain the corresponding
generated protobuf modules and a usable native `_raylet` extension. An unrelated
installed Ray package or merely checking out Python files is insufficient.
Set `RAY_SOURCE`, `EVIDENCE_PACKAGE` and a new `EVIDENCE_OUTPUT` directory:

```bash
python -B "$EVIDENCE_PACKAGE/run_verification.py" \
  --source "$RAY_SOURCE" --variant parent --suite all \
  --output-dir "$EVIDENCE_OUTPUT"
```

Repeat in a fresh Python process for `--variant head`, with the HEAD source and
a different output directory. Expected results: parent **13 PASS / 3 FAIL**,
exit 1; HEAD **16 PASS**, exit 0. No parent failures are skipped or marked xfail.
Use `--suite phase2`, `--suite recent` or `--suite gc` to isolate the components.
Use `--suite legacy --variant parent` with another fresh output directory to
regenerate the parent payload and check its exact hash. This launcher pins the
synthetic launch ID to the archived fixture; an unpinned UUID changes the bytes. The output contains
`results.xml`, compact observations and `run-provenance.json`; preserve console
output separately if full raw logs are needed.

Observed environment: Linux/aarch64, Python 3.12.3, pytest 7.4.4, protobuf 6.33.6,
grpcio/grpcio-tools 1.75.0. All original source files were rehashed against their
per-revision manifests (10,465 files each, no mismatch). The experiments used
exact Python sources and 52 regenerated protobuf modules per revision. Their
native import scaffold came from Ray 2.58.0, commit
`cec0a091b12675bbf101cf8a350c8706107baee1`, with `_raylet` SHA256
`af46848e8b413a027aa17b958d1dd1bf93f41c4a55b9009002ce7e1112ef6ec0`.
The wheel's fallback reporter protobuf was not loaded. Fresh package runs reuse
that audited environment; they do not certify a native build at either PR SHA.
Six protobuf deprecation warnings per full run concern production code assigning
a boolean to the enum `node_kind`; the pinned runtime still accepts it.

The storage is in-memory, the clock/provider observations are controlled, and
no Ray cluster or cloud provider is started. Serialization into a fresh backend
is not durable process-crash recovery. These tests do not prove RPC wire
interoperability, production memory bounds, fairness policy, or preservation of
history that was already truncated.
