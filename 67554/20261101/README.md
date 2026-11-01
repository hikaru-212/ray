# Ray PR #67554 — independent review evidence (2026-11-01)

This compact package archives the deployment-level follow-up to the controlled
review evidence in `../20261030/`. It is an evidence index, not a portable test
environment and not proof of a naturally occurring production incident.

## Findings at a glance

| Phase | Observed result |
|---|---|
| D2 | A real local Ray Serve LLM application ran with production `LLMRouter`, production `KVAwareRouter`, the exact PR-HEAD `KVTokenTracker`, Dynamo 1.4.0, and two healthy CPU mock `LLMServer` replicas. |
| D3A | One real HTTP request preserved its client request ID through native Dynamo booking to the selected/serving replica; the production router did not use its fallback. |
| D3B | Normal routing completed in **9.219821 ms**, before the configured 1-second timeout; cancellation was not exercised. |
| D3B-2 | Supported Dynamo queue backpressure made target X genuinely pending. The internal `LLMRouter` returned 408 after about 1 second; HAProxy then used its production fallback and returned external 200. |
| D3B-2 audit | Full snapshots identify the production `_free_abandoned_reservation(X, reserve)` coroutine and its original pending reserve Future. The original runtime observer missed it because it required Python code-object identity across serialization. |

The cancellation handler was reached, but the native selection Future never
completed during the observation window. No `free_reservation(X)` was directly
observed or safely inferred. This is therefore **not** a complete cleanup
success and **not** a full deployed reproduction of the same-ID stale-free bug.

The earlier controlled Parent/HEAD counterexample remains valid for its
explicitly gated ordering. Natural production reachability of that same-ID
interleaving remains unverified.

## Package map

- `REPORT.zh-TW.md` — detailed findings, methodology, and limits.
- `TIMELINE.md` — factual event chronology in UTC.
- `SOURCE_LEDGER.md` — revisions, hashes, source provenance, and raw evidence references.
- `OBSERVATION_CORRECTION.md` — retrospective correction without changing the original runtime summary.
- `PUBLIC_SAFETY.md` — public-content safety review and retained low-risk provenance.
- `summaries/` — the byte-identical final runtime summary plus clearly labeled audit-derived summaries.
- `excerpts/` — minimal line-addressed excerpts; large raw logs and snapshots remain private.
- `MANIFEST.sha256` — SHA-256 for every packaged file except itself.

Verify from this directory:

```bash
sha256sum -c MANIFEST.sha256
```

