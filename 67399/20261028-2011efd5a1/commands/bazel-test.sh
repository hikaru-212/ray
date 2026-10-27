#!/usr/bin/env bash
set -euo pipefail

: "${VIRTUAL_ENV:?Activate a Ray-compatible Python virtual environment first}"

export PYTHON3_BIN_PATH="${PYTHON3_BIN_PATH:-$VIRTUAL_ENV/bin/python3}"
PUBLIC_EVIDENCE_DIR="${PUBLIC_EVIDENCE_DIR:-$PWD/ray-syncer-evidence}"

test "$(git rev-parse HEAD)" = "2011efd5a1e48a0438c8246dfb9342a49291bb3b"
test -z "$(git status --porcelain=v1 --untracked-files=all)"

mkdir -p "$PUBLIC_EVIDENCE_DIR/results" "$PUBLIC_EVIDENCE_DIR/testlogs"

timeout --signal=TERM --kill-after=30s 45m \
  bazel --host_jvm_args=-Xmx1024m test \
  //src/ray/ray_syncer/tests:ray_syncer_test \
  --announce_rc \
  --nocache_test_results \
  --test_timeout=180 \
  --jobs=2 \
  --local_test_jobs=1 \
  --runs_per_test=1 \
  --flaky_test_attempts=1 \
  --test_output=all \
  --test_summary=detailed \
  --symlink_prefix=/ \
  --color=no \
  --curses=no \
  --show_progress_rate_limit=15 \
  --build_event_json_file="$PUBLIC_EVIDENCE_DIR/results/build-events.jsonl" \
  --execution_log_json_file="$PUBLIC_EVIDENCE_DIR/results/execution-log.json" \
  --profile="$PUBLIC_EVIDENCE_DIR/results/bazel-profile.json.gz"

bazel_testlogs="$(bazel --host_jvm_args=-Xmx1024m info bazel-testlogs)"
cp "$bazel_testlogs/src/ray/ray_syncer/tests/ray_syncer_test/test.log" \
  "$PUBLIC_EVIDENCE_DIR/testlogs/test.log"
cp "$bazel_testlogs/src/ray/ray_syncer/tests/ray_syncer_test/test.xml" \
  "$PUBLIC_EVIDENCE_DIR/testlogs/test.xml"
