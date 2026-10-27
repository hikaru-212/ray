# Reproducing the PR #67417 validation

本文件區分兩件事：`commands/*.sh` 是原驗證命令的可攜式參數化副本；下列 snapshot 建立與保存步驟是可攜式等價流程。封存沒有保留一段可直接重播的完整 provisioning transcript，因此不宣稱任意環境只靠這三個腳本即可重現。

## Recorded environment and dependencies

原驗證環境為 Linux 6.8.0-147-generic aarch64、Ubuntu GCC/G++ 13.3.0、Python 3.12.3、Bazel 7.5.0。需要可讀取兩個 exact commits 的 Ray Git repository、可寫的獨立 scratch directory、Ray C++ build dependencies，以及 Bazel dependency repository access 或已填充的 repository cache。測試會啟動 fixture 所需的本機 Redis；不要把它當成 production Redis crash recovery。

設定環境變數；路徑只是範例，不能直接使用原驗證已消失的 `/tmp` 位置：

```bash
export RAY_REPO=/path/to/ray
export EVIDENCE_DIR=/path/to/this/67417/20261027-1885cbd86e
export VALIDATION_ROOT=/path/to/a/new/scratch-directory
export BAZEL_BIN=/path/to/bazel-7.5.0
export HEAD_SOURCE="$VALIDATION_ROOT/head"
export PARENT_SOURCE="$VALIDATION_ROOT/parent"
export VENV_ACTIVATE="$VALIDATION_ROOT/venv/bin/activate"
mkdir -p "$HEAD_SOURCE" "$PARENT_SOURCE" \
  "$VALIDATION_ROOT/logs" "$VALIDATION_ROOT/results" \
  "$VALIDATION_ROOT/bazel-root" "$VALIDATION_ROOT/repository-cache"
python3 -m venv "$VALIDATION_ROOT/venv"
```

## Exact source snapshots and test injection

建立不含 `.git` 的 exact commit exports：

```bash
git -C "$RAY_REPO" archive 1885cbd86ef61cd93890de804c64feaeb3f4ed9f \
  | tar -x -C "$HEAD_SOURCE"
git -C "$RAY_REPO" archive 0a509f3448c1f3c71f80b80f46a6fb6a4df971ff \
  | tar -x -C "$PARENT_SOURCE"
```

原驗證在 HEAD 與 Parent 注入相同的 recovery test；Parent 另外注入 PR 原有的 `TestReregisterSameHeadNode`，使兩邊測試語意相同：

```bash
patch -d "$HEAD_SOURCE" -p1 < \
  "$EVIDENCE_DIR/patches/head-gcs_node_manager_test.cc.patch"
patch -d "$PARENT_SOURCE" -p1 < \
  "$EVIDENCE_DIR/patches/parent-gcs_node_manager_test.cc.patch"
patch -d "$PARENT_SOURCE" -p1 < \
  "$EVIDENCE_DIR/patches/parent-gcs_client_test.cc.patch"
```

`source-audit.json` 記錄兩邊各核對 10,471 個 tracked blobs；除了列出的 test source 外，production source 未被改動。`source-manifest.json` 保存三個關鍵 source file 的 SHA256。`pr-production-delta.patch` 只供閱讀，HEAD snapshot 已包含該 production change，請勿再次套用。

## Original Bazel command semantics

依序執行：

```bash
"$EVIDENCE_DIR/commands/run_head.sh"
head_status=$?

"$EVIDENCE_DIR/commands/run_parent.sh"
parent_status=$?

"$EVIDENCE_DIR/commands/run_additional_head.sh"
neighbor_status=$?

printf 'head=%s parent=%s head-neighbors=%s\n' \
  "$head_status" "$parent_status" "$neighbor_status"
```

原始執行結果是 `head=0 parent=3 head-neighbors=0`。Parent 的 Bazel 3 是三個預期 assertions failure，不是 infrastructure failure。腳本保留原本 targets、filters、`--jobs=2`、`--local_resources=memory=4096`、`--test_timeout=180`、`--nocache_test_results` 與 shared output base。原驗證的絕對 Bazel binary、scratch path、venv path 與 log path已改成環境變數；參數化副本在 publication preparation 中沒有重新執行。

原驗證順序為 HEAD primary、保存其 Bazel testlogs、Parent、保存 Parent testlogs、HEAD neighbors、保存 neighbors testlogs。因三批共用 output base，若要保存 XML/test.log，必須在下一批覆寫前複製：

```bash
cp "$HEAD_SOURCE/bazel-testlogs/src/ray/gcs/tests/gcs_node_manager_test/test.log" /safe/place/
cp "$HEAD_SOURCE/bazel-testlogs/src/ray/gcs/tests/gcs_node_manager_test/test.xml" /safe/place/
cp "$HEAD_SOURCE/bazel-testlogs/src/ray/gcs_rpc_client/tests/gcs_client_test/test.log" /safe/place/
cp "$HEAD_SOURCE/bazel-testlogs/src/ray/gcs_rpc_client/tests/gcs_client_test/test.xml" /safe/place/
```

Parent 與 neighbors 對應地從當時執行的 source snapshot 複製。公開套件已保存原執行的五組 test.log/XML，不需要用 aggregate log 推估個別案例。

## Expected results

- HEAD primary：8 PASS。
- HEAD neighbors：8 PASS。
- HEAD 合計：16 PASS、0 FAIL。
- Parent：13 PASS、3 expected FAIL。
- HEAD recovery final：alive/dead `1/0`、stored ALIVE、removed/added `0/0`、DEAD node-info/liveness `0/0`。
- Parent recovery final：alive/dead `1/1`、stored ALIVE、removed/added `1/1`、DEAD node-info/liveness `1/1`。

這個流程依賴 Ray 在指定平台的完整 C++ build 環境及 Bazel external dependencies。它不建立真實 GCS crash、transport reply loss、網路 PubSub subscriber 或 raylet fatal；要驗證這些項目需要另行設計 integration/e2e harness。
