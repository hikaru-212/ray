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

Each case must be run with a Ray development environment compatible with the
specified source revision. The environment is deliberately not vendored here.
Each package states its tested boundary and limitations; none claims exhaustive
correctness or, by itself, a newly discovered bug.
