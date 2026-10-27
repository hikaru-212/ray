#!/usr/bin/env bash
# Parameterized publication copy of the command that was executed. Not rerun during packaging.
set -uo pipefail
: "${VALIDATION_ROOT:?set VALIDATION_ROOT to an isolated scratch directory}"
: "${HEAD_SOURCE:?set HEAD_SOURCE to the patched HEAD snapshot}"
: "${VENV_ACTIVATE:?set VENV_ACTIVATE to the validation venv activate script}"
: "${BAZEL_BIN:?set BAZEL_BIN to Bazel 7.5.0}"
mkdir -p "$VALIDATION_ROOT/logs" "$VALIDATION_ROOT/results"
cd "$HEAD_SOURCE"
source "$VENV_ACTIVATE"
export PYTHON3_BIN_PATH="$VIRTUAL_ENV/bin/python"
export PYTHONDONTWRITEBYTECODE=1
"$BAZEL_BIN" --output_user_root="$VALIDATION_ROOT/bazel-root" --output_base="$VALIDATION_ROOT/bazel-head" test //src/ray/gcs/tests:gcs_node_manager_test //src/ray/gcs_rpc_client/tests:gcs_client_test --repository_cache="$VALIDATION_ROOT/repository-cache" --jobs=2 --local_resources=memory=4096 --test_filter='GcsNodeManagerTest.TestRestoredHeadRegistrationReplayHasNoDeathSideEffects:GcsNodeManagerTest.TestInitializeDoesNotNotifyNodeAddedListeners:*TestReregisterSameHeadNode*:*TestRegisterHeadNode*:*TestGcsTableReload*' --test_timeout=180 --nocache_test_results --test_output=all --color=no --curses=no 2>&1 | tee "$VALIDATION_ROOT/logs/head-bazel-attempt1.log"
result=${PIPESTATUS[0]}
printf '%s\n' "$result" > "$VALIDATION_ROOT/results/head-bazel-attempt1.exit"
exit "$result"
