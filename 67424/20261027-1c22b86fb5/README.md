# Ray PR #67424: delayed GetObjectStatus resource retention

**A production-reachable delayed-status ordering causes PR HEAD to retain RPC and callback resources that the parent completes, while the public API impact and eventual resource lifetime remain unquantified.**

This package preserves the existing A1 experiment and source analysis for [PR #67424](https://github.com/ray-project/ray/pull/67424). No experiment was rerun during preparation. The comparison uses production C++ methods with controlled transport and task-completion seams; it is not a full-cluster workload reproduction.

| Revision | Exact source SHA | Existing successful evidence |
| --- | --- | --- |
| Parent / comparison base | ed1d83ac1f353aec7c0473bc46a57e04d8767ee2 | [five-case run](logs/parent-experiments-1.output), [A1-only confirmation](logs/parent-a1-only.output) |
| PR HEAD | 1c22b86fb5769f09a436bfdd4666aedef4a73847 | [five-case run containing A1](logs/head-experiments-3.output) |

Both five-case runs compiled and exited **0**; the parent A1-only confirmation also exited **0**. The preserved command, output, exit, start/end and XML files are authoritative. These are characterization tests with revision-specific expectations, not a parent-RED / HEAD-GREEN regression test.

The exact successful test source on both revisions has SHA-256 `0783cc096ed4d0421c11ff472b54cfdb334ba00eb85382f14ed51ba5249b9b21`. See [reproduction instructions](REPRODUCTION.md), [provenance](PROVENANCE.json), [source audit](checks/source-verification.json), and [original final audit](logs/verify-snapshots-final-comparison.output).

## Original issue and new behavior

**Source-verified.** The patch addresses owner-local memory-store entries left behind when the final borrower releases an object through an asynchronous reference-counter path without a caller-provided deletion vector. It installs a deletion callback in [CoreWorker](https://github.com/ray-project/ray/blob/1c22b86fb5769f09a436bfdd4666aedef4a73847/src/ray/core_worker/core_worker.cc#L491-L500) and invokes it from [DeleteReferenceInternal](https://github.com/ray-project/ray/blob/1c22b86fb5769f09a436bfdd4666aedef4a73847/src/ray/core_worker/reference_counter.cc#L776-L798) when the object becomes out of scope.

The intended benefit is independently visible in the preserved A2 controls: both versions keep the entry while one borrower remains; only HEAD removes it after the final borrower releases it. Full raw logs retain these controls, including the nested-return Put case.

An owner tracks distributed references and reconstruction metadata. Borrowers hold or forward references owned elsewhere. An owner-local memory-store entry is a separate value or status marker. A1's entry is an `OBJECT_IN_PLASMA` marker, not the plasma payload. Deleting it, unpinning plasma residency, and erasing ownership metadata are different operations.

For a reconstructable object, lineage alone can retain metadata while `OutOfScope=true`; `ShouldDelete` remains false until lineage reaches zero. This is explicit in [Reference::OutOfScope / ShouldDelete](https://github.com/ray-project/ray/blob/1c22b86fb5769f09a436bfdd4666aedef4a73847/src/ray/core_worker/reference_counter.h#L368-L394). The new callback can therefore remove the marker while `GetOwner` still succeeds.

## A1 ordering and observations

The identical [fixture](reproducer/phase2_borrower_rpc_test.cc), lines 593–690, establishes this sequence:

1. O submits retryable F, which will return X. O obtains ownership information **before F completes**, when no inline value or plasma-status marker exists.
2. Task Q receives a nested ObjectRef to X. B registers ownership and the real FutureResolver sends a real gRPC status request. A proxy holds dispatch before the owner's production handler. Q finishes while B retains X; B's real ReferenceCounter produces the borrowed-reference table used by O.
3. F completes with a plasma result. O completes retryable G(X), retaining its output Y. G's retained lineage leaves X with lineage count 1.
4. O drops its local X reference. B drops its final X reference. The fixture delivers B's actual reference-removal publication to O's actual subscription callback.
5. The delayed request enters O's production handler. The fixture polls both event loops, then releases Y, removing the final lineage reference to X.

At the decisive cleanup, O has no local or submitted-task reference to X and the borrower count falls from 1 to 0. Neither fixture edits reference tables directly or inserts an arbitrary GetAsync waiter.

**Runtime-verified.** This table extracts only the A1 block. `1/0` means present/absent or true/false; handler, pending, active and reply are counts. `N/A` means metadata was erased and the field was **not emitted**, not a measured zero. Owner metadata measures `GetOwner`. Reply counts cover the delayed request through the proxy.

<!-- BEGIN A1 OBSERVATION TABLE -->
| Stage | Revision | Store | Owner metadata | Lineage | OutOfScope | ShouldDelete | Handler | Pending GetAsync | Active client RPC | Reply |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| before_owner_release | Parent | 1 | 1 | 1 | 0 | 0 | 0 | 0 | 1 | 0 |
| before_owner_release | HEAD | 1 | 1 | 1 | 0 | 0 | 0 | 0 | 1 | 0 |
| before_borrower_release | Parent | 1 | 1 | 1 | 0 | 0 | 0 | 0 | 1 | 0 |
| before_borrower_release | HEAD | 1 | 1 | 1 | 0 | 0 | 0 | 0 | 1 | 0 |
| after_borrower_release | Parent | 1 | 1 | 1 | 1 | 0 | 0 | 0 | 1 | 0 |
| after_borrower_release | HEAD | 0 | 1 | 1 | 1 | 0 | 0 | 0 | 1 | 0 |
| after_delayed_handler | Parent | 1 | 1 | 1 | 1 | 0 | 1 | 0 | 0 | 1 |
| after_delayed_handler | HEAD | 0 | 1 | 1 | 1 | 0 | 1 | 1 | 1 | 0 |
| after_idle_poll | Parent | 1 | 1 | 1 | 1 | 0 | 1 | 0 | 0 | 1 |
| after_idle_poll | HEAD | 0 | 1 | 1 | 1 | 0 | 1 | 1 | 1 | 0 |
| after_final_lineage_release | Parent | 1 | 0 | N/A | N/A | N/A | 1 | 0 | 0 | 1 |
| after_final_lineage_release | HEAD | 0 | 0 | N/A | N/A | N/A | 1 | 1 | 1 | 0 |
<!-- END A1 OBSERVATION TABLE -->

The **first observable divergence** is `store` after borrower cleanup, before either production status handler runs: parent 1, HEAD 0. All other emitted fields still match, including owner metadata, lineage 1, `OutOfScope=1`, `ShouldDelete=0`, zero waiters and one active client request. Parent's later handler posts a reply from the retained marker; HEAD registers a waiter for a missing entry.

After a 100 ms idle poll and after final lineage release, HEAD still has one active client request and one waiter. A fresh direct status request after metadata erasure returns OUT_OF_SCOPE in both versions; it does not complete the older request. The fixture then performs explicit test-only teardown, described below. [Machine-readable observations](results/A1-observations.json) also retain local, submitted-task and borrower counts.

## Production reachability and the caller precondition

**Source-verified protocol; controlled runtime ordering.** The [deserializer](https://github.com/ray-project/ray/blob/1c22b86fb5769f09a436bfdd4666aedef4a73847/python/ray/_private/serialization.py#L68-L98) creates an ObjectRef, calls ownership registration, and returns without waiting for status resolution. [GetOwnershipInfo / RegisterOwnershipInfoAndResolveFuture](https://github.com/ray-project/ray/blob/1c22b86fb5769f09a436bfdd4666aedef4a73847/src/ray/core_worker/core_worker.cc#L1229-L1260) can avoid the RPC by inlining an existing value/status. A1 avoids that optimization by serializing before F completes. Being a plasma object alone does **not** establish that an RPC is required.

The status request follows B → O through [FutureResolver::ResolveFutureAsync](https://github.com/ray-project/ray/blob/1c22b86fb5769f09a436bfdd4666aedef4a73847/src/ray/core_worker/future_resolver.cc#L23-L40) and CoreWorkerClient. Independently, O subscribes to B's reference-removal channel. B [publishes its reference-removal message](https://github.com/ray-project/ray/blob/1c22b86fb5769f09a436bfdd4666aedef4a73847/src/ray/core_worker/reference_counter.cc#L1403-L1425); O's [long-polling connection to B](https://github.com/ray-project/ray/blob/1c22b86fb5769f09a436bfdd4666aedef4a73847/src/ray/pubsub/subscriber.cc#L298-L314) receives it and [posts the subscription callback](https://github.com/ray-project/ray/blob/1c22b86fb5769f09a436bfdd4666aedef4a73847/src/ray/pubsub/subscriber.cc#L140-L148). This enters [CleanupBorrowersOnRefRemoved](https://github.com/ray-project/ray/blob/1c22b86fb5769f09a436bfdd4666aedef4a73847/src/ray/core_worker/reference_counter.cc#L1252-L1276).

These are independent RPC/completion paths. Pubsub's [sequence checks](https://github.com/ray-project/ray/blob/1c22b86fb5769f09a436bfdd4666aedef4a73847/src/ray/pubsub/subscriber.cc#L348-L378) order publisher messages, not incoming status requests. No application-level acknowledgement ties borrower release to status completion. Thus reference removal can be processed first. This conclusion follows the paths, not a blanket gRPC ordering assumption.

[ObjectRef destruction](https://github.com/ray-project/ray/blob/1c22b86fb5769f09a436bfdd4666aedef4a73847/python/ray/includes/object_ref.pxi#L73-L77) removes a local reference. [CoreWorker::RemoveLocalReference](https://github.com/ray-project/ray/blob/1c22b86fb5769f09a436bfdd4666aedef4a73847/src/ray/core_worker/core_worker.h#L451-L463) updates references and local entries but does not cancel the status RPC. FutureResolver captures an ObjectID and owner address, not an ObjectRef or additional reference-counter hold. The ObjectRef can disappear while its request remains active.

Lineage-only metadata follows real submission/completion and release. [TaskManager retains retryable lineage](https://github.com/ray-project/ray/blob/1c22b86fb5769f09a436bfdd4666aedef4a73847/src/ray/core_worker/task_manager.cc#L1397-L1448); this need not retain X's local marker. [HandleGetObjectStatus](https://github.com/ray-project/ray/blob/1c22b86fb5769f09a436bfdd4666aedef4a73847/src/ray/core_worker/core_worker.cc#L4095-L4117) checks metadata and explicit plasma freeing, then assumes a value will eventually become available; it does not test `OutOfScope`.

**Important contract qualification.** The [protobuf comment](https://github.com/ray-project/ray/blob/1c22b86fb5769f09a436bfdd4666aedef4a73847/src/ray/protobuf/core_worker.proto#L471-L475) requires callers to ensure the reference is not removed during GetObjectStatus. The inspected FutureResolver production path does not establish that guarantee through a dedicated reference hold or release-triggered cancellation. A1 exercises that existing caller/contract gap. This PR-specific resource-retention difference does **not** establish that the PR independently violates the RPC precondition or a public Ray API contract.

## RPC resource lifetime

| Obligation | Ownership and normal completion | Evidence |
| --- | --- | --- |
| Client RPC | Heap ClientCallTag holds shared ClientCallImpl, which owns reply, ClientContext, response reader, statistics and callback. Its callback retains RetryableGrpcRequest state. Completion processing deletes the tag; a retryable error can retain/retry the logical request. | Source-verified ownership; Runtime-verified active count 1 |
| Owner waiter | The memory-store map holds a closure capturing raw CoreWorker/reply pointers and the send callback. GetObjectStatus discards the callback ID, with no registered cancellation bridge. | Source-verified ownership; Runtime-verified pending count 1 |
| Server call | Heap ServerCallImpl owns request/context/response writer and an arena-backed reply. The send callback refers to the call via raw this, a protocol-lifetime dependency rather than shared ownership. Finish/completion normally permits deletion. | Source-verified; allocation counts/bytes not separately measured |
| Final lineage release | Erases metadata, without replying to the old status RPC or removing its waiter. | Runtime-verified and Source-verified |
| Later legitimate Put/recovery | Put drains waiters and posts callbacks holding a shared RayObject, even if the store will not retain that entry. | Source-verified; preserved A3 recovery control corroborates completion |

Sources: [client fields/tag](https://github.com/ray-project/ray/blob/1c22b86fb5769f09a436bfdd4666aedef4a73847/src/ray/rpc/client_call.h#L115-L173), [CreateCall](https://github.com/ray-project/ray/blob/1c22b86fb5769f09a436bfdd4666aedef4a73847/src/ray/rpc/client_call.h#L272-L289), [client completion](https://github.com/ray-project/ray/blob/1c22b86fb5769f09a436bfdd4666aedef4a73847/src/ray/rpc/client_call.h#L324-L351), [retry ownership](https://github.com/ray-project/ray/blob/1c22b86fb5769f09a436bfdd4666aedef4a73847/src/ray/rpc/retryable_grpc_client.h#L274-L306), [GetAsync / CancelGetAsync](https://github.com/ray-project/ray/blob/1c22b86fb5769f09a436bfdd4666aedef4a73847/src/ray/core_worker/store_provider/memory_store/memory_store.cc#L144-L185), [server allocation](https://github.com/ray-project/ray/blob/1c22b86fb5769f09a436bfdd4666aedef4a73847/src/ray/rpc/server_call.h#L553-L574), [server send callback](https://github.com/ray-project/ray/blob/1c22b86fb5769f09a436bfdd4666aedef4a73847/src/ray/rpc/server_call.h#L312-L320), [server reclamation](https://github.com/ray-project/ray/blob/1c22b86fb5769f09a436bfdd4666aedef4a73847/src/ray/rpc/grpc_server.cc#L222-L257), [lineage release](https://github.com/ray-project/ray/blob/1c22b86fb5769f09a436bfdd4666aedef4a73847/src/ray/core_worker/reference_counter.cc#L572-L609), and [Put dispatch](https://github.com/ray-project/ray/blob/1c22b86fb5769f09a436bfdd4666aedef4a73847/src/ray/core_worker/store_provider/memory_store/memory_store.cc#L231-L275).

The status wrapper uses timeout -1; the production [manager construction](https://github.com/ray-project/ray/blob/1c22b86fb5769f09a436bfdd4666aedef4a73847/src/ray/core_worker/core_worker_process.cc#L797-L798) leaves its default -1. [ClientCallImpl](https://github.com/ray-project/ray/blob/1c22b86fb5769f09a436bfdd4666aedef4a73847/src/ray/rpc/client_call.h#L73-L85) sets a deadline only for another value. Missing a deadline alone does not prove permanent retention: transport errors, process exit, or later legitimate value/error Put can change the outcome. Retaining or releasing lineage does not itself demand reconstruction.

The preserved A3 cancellation control adds and cancels a separate raw gRPC call; its server waiter remains. This does not show production ObjectRef release invoking cancellation, or cancellation of the original FutureResolver request. The same saved suite shows legitimate reconstruction completing the pending status callback. These controls constrain A1's claim.

## Fixture equivalence and falsification

Both snapshots have 10,494 tracked files verified against their exact Git blobs. Only an appended temporary BUILD rule differs; existing BUILD bytes and all production source remain unchanged. The successful test source and BUILD append are identical. `PHASE2_EXPECT_DELETE` changes A1 assertions and its bounded observation wait predicate, not the ownership graph or operation sequence.

Two earlier failures occurred on **HEAD**, in the harness: a redundant direct dependency violated Bazel visibility, and an lvalue PubMessage failed the callback's rvalue parameter. Both snapshots received identical minimal fixes: remove the redundant dependency (CoreWorkerClient remains available transitively) and pass a copied PubMessage as an rvalue. Message content/order is unchanged. [The adjustment audit](checks/harness-adjustments.json) verifies the exact transformations. No production visibility or signature changed. Earlier identity logs retain their older hashes; the final successful source hash is given above.

The real components are CoreWorker, TaskManager, ReferenceCounter, memory store, FutureResolver, CoreWorkerClient and status gRPC transport. Fakes replace raylet/GCS/plasma infrastructure, metrics and pubsub transport. The fixture injects task-completion replies through TaskManager and models frontend reference calls, rather than executing Python serialization or distributed tasks. Pubsub forwards real generated contents at a chosen time. The proxy delays the real handler and counts replies. `-fno-access-control` applies only to the temporary test target; private counters are observed, not overwritten.

The source diff puts the new deletion callback on the exact cleanup path. The first logged divergence precedes handler delivery, with all other emitted state equal. It therefore precedes handler assertions and test-only teardown. Historical builds reused a Bazel output base sequentially after shutdown; the parent build rebuilt actions using the parent source path, and its A1-only confirmation agreed. This is not hermetic binary attestation. The reproduction guide uses separate fresh output bases.

**Test-only cleanup is not production cleanup.** After recording the final observations, A1 enumerates waiter IDs, calls CancelGetAsync and injects an Invalid reply through the proxy (`AbortForHarness`, lines 649–662 and 111–116), then waits for the active count to reach zero. This bounds teardown. It does not show Ray doing so on ObjectRef/lineage release. The injected error explains the raw HEAD teardown warning.

## Limits and classification

- **Runtime-verified:** parent/HEAD states, one active client request and one waiter in HEAD, and their persistence at the checkpoint after final lineage release.
- **Source-verified:** production paths, independent ordering, no inspected release-to-status-cancellation bridge, protobuf precondition, independently allocated RPC/callback state.
- **Reasoned hypothesis:** without another completion event, obligations can outlive the useful borrower reference. Accumulation depends on occurrence frequency and later recovery/transport/shutdown events.
- **Not verified:** permanent application hang, permanently unrecoverable RPC, actual memory growth rate, full-cluster reproduction, occurrence frequency, guaranteed normal-workload failure, or exact release of every allocation during graceful shutdown.

“Not completed during A1” does not mean “can never complete.” B no longer holds X; the unfinished RPC does not prove a user is blocked on X. Callback/client/server resources are independent of X's local marker. A legitimate later Put or reconstruction can complete the callback. Forced test teardown and eventual process-memory reclamation do not measure normal graceful-shutdown behavior. Unrelated lineage-memory cleanup behavior is not attributed to this PR.

## Integrity and publication

[PUBLICATION-ALLOWLIST.txt](PUBLICATION-ALLOWLIST.txt) lists every intended file. [MANIFEST.sha256](MANIFEST.sha256) hashes all files except itself, including the allowlist. [PUBLICATION-AUDIT.md](PUBLICATION-AUDIT.md) documents privacy, exclusions and Git isolation. Raw copies have no redactions. `checks/verify_package.py` checks hashes, XML/exits, the derived table and first divergence without building or running Ray.

The README and JSON are newly authored summaries, not purported historical reports. Original commands, output, exits and XML summaries are preserved. AI assistance was used to prepare and audit the package. No staging, commit, push, Git ref change or GitHub publication was performed.
