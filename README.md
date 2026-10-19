# Ray correctness review evidence

Minimal, executable evidence for two Ray Serve correctness reviews. This
repository contains test code and reproduction instructions, not review logs,
Ray binaries, generated files, or Python environments.

| Review | Invariant | Evidence |
| --- | --- | --- |
| [PR #67031](67031/) | A readiness callback cannot leave a spent cancellation handle when it fires during registration. | Four controlled Python-level interleavings plus commands for the PR's parent-red/PR-green tests. |
| [PR #67117](67117/) | Decorated multiplexed methods recover the correct receiver and keep model caches instance-local. | A test-only patch covering one and two outer decorators. |

Each case must be run with a Ray development environment compatible with the
specified source revision. The environment is deliberately not vendored here.

