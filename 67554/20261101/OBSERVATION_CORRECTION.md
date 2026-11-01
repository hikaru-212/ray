# D3B-2 observation correction

## Status of the original result

The byte-identical runtime output is preserved as
`summaries/d3b2-final-summary.original.json`. It reports:

- classification: `CANCELLATION_NOT_ESTABLISHED`;
- `exact_production_cleanup_coroutine_for_target_seen=false`;
- `original_selection_future_alive_when_cleanup_first_seen=false`;
- `original_selection_future_terminal_success=false`;
- `final_native_accounting_clean=false`.

This document is a retrospective read-only audit. It does not edit or replace
the original result.

## Observer predicate defect

The observer imported a module-level reference:

```python
_CLEANUP_CODE = KVTokenTracker._free_abandoned_reservation.__code__
```

It then required:

```python
_code(candidate) is _CLEANUP_CODE
```

before treating a coroutine as exact and retaining its `reserve` Future. Python
code-object identity is not a stable provenance test across Serve/cloudpickle
serialization. `RequestRouterConfig` itself serializes configured router classes
by value, and deployment classes also cross serialization boundaries.

In this run:

| Identity | Object ID |
|---|---:|
| observer's module-level expected cleanup code | `259382941767824` |
| active cleanup coroutine code | `259383360385264` |

Because the IDs differed, the original `exact_cleanup_for_target` flag stayed
false even though all semantic and object-level invariants matched.

## Complete raw correlation

The first matching task appears at `snapshots.jsonl` line 42,
`time_unix_ns=1793517306048095748` (2026-11-01 07:15:06.048095748 UTC).
The preceding line 41 at 07:15:06.021218552 contains no production pending task.

The task chain was:

| Element | Identity / state |
|---|---|
| task | object `259382507479744`; member of production tracker `_pending_tasks`; not done; not cancelled |
| wrapper | `KVTokenTracker._schedule.<locals>._run`, object `259382517836736`, source first line 287 |
| scheduled coroutine | `KVTokenTracker._free_abandoned_reservation`, object `259382507152352`, source first line 494 |
| `self` frame local | object `259382932763328`; exactly the active production tracker |
| `request_id` frame local | `ray-67554-d3b2-target-0001` |
| `reserve` frame local | `_asyncio.Future` object `259382507685488`; pending; not cancelled |
| terminal await node | `_asyncio.FutureIter` object `259382507492096` |

Both `_run`'s frame-local `awaitable` and its `cr_await` point to the same cleanup
coroutine. The coroutine's own frame exposes the exact tracker, target ID and
original reserve Future. Its await chain ends at that Future.

Across all 2,768 post-appearance samples through final line 2809:

- task ID was always `259382507479744`;
- cleanup coroutine ID was always `259382507152352`;
- reserve Future ID was always `259382507685488`;
- semantic method was always `_free_abandoned_reservation`, first line 494;
- `self is tracker` was always true;
- request ID was always target X;
- reserve stayed `done=false`, `cancelled=false`;
- wrapper task stayed pending and not cancelled.

This is not a sampling-gap hypothesis: the cleanup was continuously present
from the first post-timeout sample until the last pre-shutdown snapshot.

## Revised interpretation

Production source creates this cleanup only in `KVTokenTracker.select_worker()`:

```text
reserve = ensure_future(SelectionService.select_and_reserve(request))
await shield(reserve)
except CancelledError:
    _schedule(_free_abandoned_reservation(request_id, reserve))
    raise
```

The exact semantic cleanup(X) task therefore establishes that caller
`CancelledError` reached the production tracker handler. The internal HTTP 408
alone would only prove that direct ingress called `request_task.cancel()`; the
task/frame/Future chain supplies the missing causal evidence.

The deployment explicitly set both
`RAY_SERVE_RUN_USER_CODE_IN_SEPARATE_THREAD=0` and
`RAY_SERVE_RUN_ROUTER_IN_SEPARATE_LOOP=0`. The cancellation traveled on the
same asyncio loop through:

```text
Replica._direct_ingress_asgi
  → UserCallableWrapper._call_http_entrypoint
  → LLMRouter.route / _pick_replica
  → DeploymentHandle.choose_replica(_reserve=False)
  → AsyncioRouter.choose_replica
  → KVAwareRouter.choose_replicas
  → KVTokenTracker.select_worker
```

Retrospective classification for the cancellation boundary:

**CLEANUP_PRESENT_OBSERVER_MISSED**

Equivalent experiment-level wording:

**CANCELLATION_REACHED_CLEANUP_INCOMPLETE**

## What remains unestablished

The correction must not be upgraded to cleanup success:

- The original native selection Future had no terminal result during observation.
- X never entered Python tracker booking state.
- No native `Selected worker` record for X exists.
- `_free_abandoned_reservation` remained blocked at `await reserve`.
- No direct or reliable inferred `free_reservation(X)` exists.
- Final Dynamo state was `active_requests=0`, `pending_count=1`.
- Shutdown removed workers only after the final snapshot; no post-shutdown native
  accounting observation exists.

Thus the independent native-outcome classification remains:

**NATIVE_RESERVATION_OUTCOME_UNRESOLVED**

The correction proves production cancellation-handler reachability, not
completion of cleanup and not the natural Same-ID stale-free interleaving.

