# Ray correctness review evidence

Minimal, executable evidence for selected Ray correctness reviews. This
repository contains focused test code, compact observed results, and
reproduction instructions, not full review logs, Ray binaries, generated
files, or Python environments.

| Review | Invariant | Evidence |
| --- | --- | --- |
| [PR #67031](67031/) | A readiness callback cannot leave a spent cancellation handle when it fires during registration. | Four controlled Python-level interleavings plus commands for the PR's parent-red/PR-green tests. |
| [PR #67117](67117/) | Decorated multiplexed methods recover the correct receiver and keep model caches instance-local. | A test-only patch covering one and two outer decorators. |
| [PR #66953](66953/) | A real gRPC Push deadline releases PushManager capacity and admits an already-queued independent Push. | A production-path C++ harness with same-binary finite and disabled deadline controls. |
| [PR #67180](67180/) | A valid Ray Data output remains intact when its producer later fails while the output-read budget is zero. | Inline and threaded metadata modes on the verified baseline and PR HEAD; this is non-regression evidence. |
| [PR #67192](67192/) | Writable NumPy batches remain source-isolated when a safe Arrow pre-copy is omitted. | Independent batching boundaries plus a fresh-process single/multi/mixed microbenchmark. |
| [PR #67229](67229/) | A legal explicit `num_gpus=None` must remain a zero-GPU request and must not abort a V1 hash-shuffle join during max-resource accounting. | Public `Dataset.join` Parent/HEAD controls, reservation on/off isolation, Core/GCS resource observations, and a portable reproducer. |
| [PR #67242](67242/) | The inspected unordered actor submission path serializes `client_processed_up_to=-1`; a supported nonnegative producer remains unestablished. | Real-client loopback wire observations plus B/H PASS/PASS component non-regression tests; no original production bug repair is established. |
| [PR #67315](67315/) | Canceled actor lease replies clean up unused workers while compatible W1 reuse remains valid. | Cancellation and W2 cleanup RED/GREEN, corrected CPU mapping, and separately scoped inherited pending-L2 observations. |
| [PR #67360](67360/) | Lifetime retry accounting survives diagnostic history truncation within the information available. | Retry and legacy migration RED/GREEN, ordering/CAS characterization, and inherited details-loss limits. |

Each case must be run with a Ray development environment compatible with the
specified source revision. The environment is deliberately not vendored here.
Each package states its tested boundary and limitations; none claims exhaustive
correctness or, by itself, a newly discovered bug.
