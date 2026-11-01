# Factual timeline

All wall-clock timestamps are UTC. Nanosecond timestamps come from saved JSON
fields; millisecond-only timestamps come from Serve/Dynamo logs.

## 2026-10-30 controlled comparison

The complete chronology remains in `../20261030/head/timeline.json` and
`../20261030/parent/timeline.json`. The controlled ordering was:

1. A reserves request ID X on W1.
2. The harness holds A's real PyO3 completion delivery before enqueue.
3. W1 is retired through the real worker-removal path.
4. B reuses X and reserves on W2.
5. The gate releases A completion.
6. HEAD abandoned cleanup calls native `free(X)` and removes B accounting;
   Parent has no equivalent cleanup. Distinct-ID control remains booked.

This sequence was deliberately controlled; natural deployment reachability was
not established by those runs.

## 2026-11-01 deployment follow-up

| UTC | Phase | Event | Evidence |
|---|---|---|---|
| 05:34:04.364 | D2 | Dynamo `SelectionService` created in production LLMRouter. | `excerpts/d2-registration.txt` |
| 05:34:08.196 | D2 | LongPoll adds first LLMServer worker. | same |
| 05:34:08.300 | D2 | LongPoll adds second worker; tracker now has two. | same |
| after deploy | D2 | Serve reports LLMRouter RUNNING and two LLMServer replicas RUNNING; both Dynamo workers schedulable; no router fallback. | `summaries/d2-deployment.audit.json` |
| 05:54:46.742398 | D3A | Dynamo selects worker `11068153126354715887` for client ID `ray-67554-d3a-http-booking-0001`. | `excerpts/d3a-http-booking.txt` |
| 05:54:46.743 | D3A | Internal LLMRouter returns 200 after 12.2 ms. | same |
| 05:54:46.744 | D3A | Reservation report accepted. | same |
| 05:54:46.745 | D3A | Replica `qfsmp1b0`, mapped to selected worker, serves the same request ID with HTTP 200. | same |
| 06:07:37.898627688 | D3B | Normal cancellation-probe HTTP request starts. | `excerpts/d3b-normal-routing.txt` |
| 06:07:37.907847509 | D3B | Response ends HTTP 200 after exactly 9.219821 ms; 1-second timeout not reached. | same |
| 07:15:05.012003 | D3B-2 final | P1 selects worker `13316335369824107713`. | `excerpts/d3b2-timeout-fallback.txt` |
| 07:15:05.023315 | D3B-2 final | P2 selects distinct worker `4363950666218866675`. | same |
| 07:15:05.032619203 | D3B-2 final | External target X HTTP starts with `x-request-timeout-seconds: 1`. | same |
| 07:15:05.035055550 | D3B-2 final | First snapshot with `pending_count=1`, both workers active, X absent from tracker booking state. | `excerpts/d3b2-snapshot-chain.jsonl`, source line 4 |
| 07:15:06.021218552 | D3B-2 final | Last sampled state before cleanup task appears: native pending remains 1, production `_pending_tasks` count 0. | same, source line 41 |
| 07:15:06.033 | D3B-2 final | LLMRouter direct ingress records internal 408 after 1000.7 ms. | `excerpts/d3b2-timeout-fallback.txt` |
| 07:15:06.035 | D3B-2 final | HAProxy fallback LLMServer returns 200 in 0.9 ms. | same |
| 07:15:06.035325200 | D3B-2 final | External X HTTP ends 200 after 1.002706164 s. | same |
| 07:15:06.048095748 | D3B-2 final | First snapshot containing exact semantic cleanup(X) task and original pending reserve Future. | `excerpts/d3b2-cleanup-task.json`, source line 42 |
| 07:16:04.230493 | D3B-2 final | Native expiry sweep releases P1. | `excerpts/d3b2-timeout-fallback.txt` |
| 07:16:04.230516 | D3B-2 final | Native expiry sweep releases P2. | same |
| 07:16:04.242771774 | D3B-2 final | Both workers now have zero active load; X remains pending and cleanup awaits its reserve Future. | `excerpts/d3b2-snapshot-chain.jsonl`, source line 2215 |
| 07:16:20.055921368 | D3B-2 final | Final snapshot: active load 0, pending count 1, same cleanup task/Future pending. | source line 2809 |
| 07:16:20.095 | D3B-2 final | `serve.shutdown()` starts after the final snapshot. | `excerpts/d3b2-timeout-fallback.txt` |
| 07:16:20.273 | D3B-2 final | LongPoll removes both workers during shutdown. No later native snapshot exists. | same |
| 07:16:24.728553 | D3B-2 final | Runner shutdown sequence completes; Ray is no longer initialized. This is not evidence that abandoned cleanup(X) completed. | original final summary/events |

The original `target_http_outcome` event was serialized after the observation
loop, so its event-write time is later than the embedded request start/end
nanoseconds. This timeline uses the embedded request timestamps.

