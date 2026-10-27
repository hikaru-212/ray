# Reproduce the scoped RaySyncer verification

本文件只重現 PR HEAD 的既有測試。Parent `8d53e59e3da91a39beb6b1b614265cf05d3d26a4` 沒有在原始驗證中執行，因此不要把任何 parent 結果描述成此套件的觀測。

## Source revision

在乾淨的 Ray checkout 取得並核對精確 revision：

```bash
git fetch origin 2011efd5a1e48a0438c8246dfb9342a49291bb3b
git checkout --detach 2011efd5a1e48a0438c8246dfb9342a49291bb3b
test "$(git rev-parse HEAD)" = 2011efd5a1e48a0438c8246dfb9342a49291bb3b
test -z "$(git status --porcelain=v1 --untracked-files=all)"
```

原始 run 使用 repository 的 `.bazelversion` 與 `.bazelrc`：Bazel 7.5.0、opt、C++17。主機是 Ubuntu 24.04.4 LTS、aarch64；GCC/G++ 13.3.0、GNU gold 1.16（Binutils 2.42）、glibc 2.39、OpenJDK 21.0.5+11-LTS、Python 3.12.3。repository pins gRPC 1.58.0 與 GoogleTest 1.14.0。

依 Ray 開發文件準備 Python virtual environment，先啟用它；不要把套件安裝到 system Python。原始 run 使用既有環境，沒有安裝新套件。

```bash
source /path/to/venv/bin/activate
export PYTHON3_BIN_PATH="$VIRTUAL_ENV/bin/python3"
```

## Run

從 Ray checkout 執行公開的 command script。`EVIDENCE_PACKAGE` 指向本套件 checkout；輸出放在目前 Ray checkout 下，沒有固定 `/tmp` 路徑。

```bash
export EVIDENCE_PACKAGE=/path/to/ray-review-evidence/67399/20261028-2011efd5a1
export PUBLIC_EVIDENCE_DIR="$PWD/ray-syncer-evidence"
"$EVIDENCE_PACKAGE/commands/bazel-test.sh"
```

原始 run 的 target 與主要 flags 完整保留在 script：

- `//src/ray/ray_syncer/tests:ray_syncer_test`
- `--nocache_test_results`
- `--test_timeout=180`
- `--jobs=2`、`--local_test_jobs=1`
- `--runs_per_test=1`、`--flaky_test_attempts=1`
- `--test_output=all`、`--test_summary=detailed`
- `--build_event_json_file`、`--execution_log_json_file`、`--profile`
- 外層 `timeout --signal=TERM --kill-after=30s 45m`

`commands/executed-command.txt` 記錄原始 argv；只把原機器的 checkout、venv 與 evidence 絕對路徑替換成描述性 placeholder，所有 Bazel flags、順序與值均未改變。

## Validate a reproduced run

成功結果應包含：

```text
[==========] 24 tests from 5 test suites ran.
[  PASSED  ] 24 tests.
Executed 1 out of 1 test: 1 test passes.
```

確認 XML：

```bash
python3 - "$PUBLIC_EVIDENCE_DIR/testlogs/test.xml" <<'PY'
import sys
import xml.etree.ElementTree as ET

root = ET.parse(sys.argv[1]).getroot()
assert root.attrib["tests"] == "24"
assert root.attrib["failures"] == "0"
assert root.attrib["errors"] == "0"
assert root.attrib["disabled"] == "0"
assert all(suite.attrib.get("skipped", "0") == "0" for suite in root)
cases = list(root.iter("testcase"))
assert len(cases) == 24
assert all(case.attrib.get("status") == "run" for case in cases)
assert all(case.attrib.get("result") == "completed" for case in cases)
print("24 passed, 0 failed, 0 errors, 0 skipped, 0 disabled")
PY
```

`--nocache_test_results` 要求 Bazel 真正執行測試。若另外保存 compact execution evidence，TestRunner 應顯示 `cacheHit=false`、`exitCode=0`。原始公開證據的對應欄位在 `observed/bazel-actions.json`。

## Interpretation boundary

重跑這個套件可驗證既有功能是否在該環境通過，不能直接觀察 reactor destructor timing、最後一個 `shared_ptr` 的釋放、原始 ASAN UAF，或完整 gRPC stream/service lifetime。Source ownership 結論需獨立依 `source-audit/ownership.md` 所列位置及 factory 前提審查。
