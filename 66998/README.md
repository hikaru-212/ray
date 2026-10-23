# Ray #66998：可重現的快取正確性證據

[Issue #66982](https://github.com/ray-project/ray/issues/66982) ·
[PR #66998](https://github.com/ray-project/ray/pull/66998)

原版在加入時加上初始目錄大小，驅逐時卻扣除當下大小。目錄成長可能使總額
變負，阻止後續驅逐。作者以每 URI 的 recorded size 計帳，重加時更新差額；
後續提交將 membership、recorded size 與 total 的移除放在可失敗的 callback 前。
上限約束紀錄貢獻，並非即時磁碟配額。

| 名稱 | 完整 revision SHA |
| --- | --- |
| `baseline`：第一個 PR commit 的直接 parent | `de28d2005add958b576b0bf7d9da8d905c042bb7` |
| `initial`：初版 recorded-size 修補 | `337a326cabc5ae2d48a379fb4f59fd8fcc9a12b1` |
| `followup`：刪除例外修補 | `451b8fa2354452236c96f0c0820c174c37df141c` |
| `head`：合併 master 後的 PR HEAD | `772d9c718608dccb82473d524c876be21bda9bfd` |

HEAD 的第一 parent 已含修補，不能用它代替完整未修 baseline。四版分開執行，
即使 follow-up 與 HEAD 的快取程式相同也不合併其結果。

## 保留的五個案例

沿用 Phase 2 已執行的測試與觀察程式，收斂到以下案例。全部使用正式環境預設
`debug=False`，由獨立斷言檢查 total、used/unused 互斥與 map/membership 一致；
baseline 尚無 map，仍檢查 membership 及已知貢獻總和。

| 測試 | 證明內容；超出作者現有回歸的部分 |
| --- | --- |
| `test_multiple_evictions_ignore_actual_return` | 同次驅逐三筆各 3 的 unused URI；callback 各回傳 100 仍只扣記錄值，保留 used=10。作者未測這個單次多重驅逐組合。 |
| `test_add_eviction_failure_mid_sequence_and_resume` | 第二次驅逐拋錯前 total 已由 19→16→13；新加入的 URI 已提交，剩餘 URI 正確。後續驅逐、重插及再次驅逐均有效。沿用原測試，末尾補上重插項目的實際再次驅逐。 |
| `test_three_growing_directories` | 真實 caller→plugin create→解壓→目錄成長→delete；三個 URI 各由 2 成長至 20，cap=1。最後才斷言，保留完整 baseline 負數與殘留時序。 |
| `test_real_delete_failure_and_redownload[before]` | 真實 chmod 權限錯誤，兩檔都留下，邏輯計帳仍應一致；解除權限故障後重新下載並完整恢復。 |
| `test_real_delete_failure_and_redownload[partial]` | 明確的 syscall 故障注入：真實 rmtree 刪掉第一檔，第二個 package-file unlink 拋 PermissionError。證明部分刪除後的計帳、重建與再次清理。 |

後兩項同時驗證：已失敗 URI 的 `mark_unused` 不會自動重試刪檔；真正恢復走
`WorkingDirPlugin.create(overwrite=True)`，並檢查再次下載及兩個檔案的完整內容。
**實體檔案殘留是已明言的清理限制，不是此 PR 新引入的回歸。**

未保留全部 54 個參數案例、作者已有的單純重加/縮小/零回傳重複測試、debug
驗證器刻意破壞測試、交錯建立的較大測試，以及完整原始日誌。這些已列為 Phase 2
歷史證據；本包聚焦計帳、連續驅逐故障與實體清理邊界。沒有收錄 production source、
wheel、venv、native binary 或 cache。

## 環境與來源驗證

實際驗證環境：Linux aarch64、非 root、Python 3.12.3、pytest 7.4.4、Ray 2.58.0
（wheel commit `cec0a091b12675bbf101cf8a350c8706107baee1`）、filelock 3.29.0。
本次在新的 venv 唯讀重用已安裝依賴，未重新下載 wheel。另建環境可使用：

```bash
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install 'pytest==7.4.4' 'ray[default]==2.58.0' 'filelock==3.29.0'
```

純 cache 檔只需要 Python、pytest、Git；元件檔另需可 import 的 Ray/native 依賴。
`source_helpers.py` 從使用者指定的本機 Ray Git checkout 以 `git show <SHA>:<path>`
讀取真實程式，不 checkout、不修改 production。所有四個 commit 必須已存在。
元件的十個 Python 模組在臨時目錄載入，檢查 import 路徑、程式函式路徑及與
Git blob 的逐位元組一致性，輸出 SHA-256。wheel 的目錄大小 helper 必須與選定
revision 的 AST 相同，否則直接失敗，不把環境錯誤算成修補回歸。

`MemoryGCS` 只提供 ZIP bytes；正式 caller、下載解壓流程、檔案鎖、plugin、cache
與 rmtree 都實際執行。這是 **Python 元件測試，不是 cluster E2E**，也不證明該
wheel 的原生 runtime 與 PR HEAD 完整相容。

真實權限案例在 root、非 POSIX 或不執行 chmod 權限的檔案系統會明確 skip。
部分刪除注入要求 fd-based `shutil.rmtree`，否則 skip；skip 不能報成通過。
Windows、完整 agent reference-table/RPC 故障恢復及持續 I/O 故障均不在已驗證範圍。

## 重現四個版本

先啟用相容 venv，在此 evidence repository 根目錄執行；Ray checkout 的位置可更換。
每個 revision 使用獨立 Python process。停用非必要 pytest 外掛，避免外部環境影響。

```bash
export RAY_SOURCE_REPO=/path/to/ray-checkout
export PYTHONDONTWRITEBYTECODE=1
export PYTEST_DISABLE_PLUGIN_AUTOLOAD=1
evidence_dir="$PWD/66998"

for revision in baseline initial followup head; do
  result=0
  RAY_EVIDENCE_REVISION="$revision" python -B -m pytest -q -s \
    -c "$evidence_dir/pytest.ini" \
    "$evidence_dir/test_uri_cache.py" "$evidence_dir/test_working_dir.py" \
    --tb=short || result=$?
  printf '%s: pytest exit=%s\n' "$revision" "$result"
done
```

只跑不需 Ray wheel 的兩個 cache 案例：

```bash
RAY_EVIDENCE_REVISION=head python -B -m pytest -q -s \
  -c "$evidence_dir/pytest.ini" "$evidence_dir/test_uri_cache.py"
```

單獨執行三個目錄重現時，可將上述測試路徑改成
`"$evidence_dir/test_working_dir.py::test_three_growing_directories"`。

不需要 Phase 2 暫存目錄或任何未收錄 helper。工具生成的來源快照位於系統暫存
目錄，process 結束即清除；pytest 資料也寫到暫存目錄。

## Phase 3：封裝版本的實際執行結果

2026-10-23（Asia/Taipei）實際執行上述五個案例；以下亦為相同環境下的預期結果。
每版 errors=0、skipped=0，沒有 xfail 或針對 revision 放寬斷言。
詳細 testcase 結果、來源 hash 與環境見 [observed.json](observed.json)。
另將同一份可執行檔案搬至獨立位置驗證，HEAD 仍為 5 通過；確認純 cache loader
不會 import Ray。這項搬移驗證不依賴原 Phase 2 目錄。

| revision | 通過 | 失敗 | pytest exit | 三個目錄 total | 殘留目錄 |
| --- | ---: | ---: | ---: | --- | ---: |
| baseline | 0 | 5 | 1 | `[-18, -16, -14]` | 2/3 |
| initial | 2 | 3 | 1 | `[0, 0, 0]` | 0/3 |
| followup | 5 | 0 | 0 | `[0, 0, 0]` | 0/3 |
| head | 5 | 0 | 0 | `[0, 0, 0]` | 0/3 |

initial 通過多重正常驅逐及成長目錄重現，失敗於連續驅逐例外與兩個 filesystem
例外後的 accounting assertion。baseline/initial 在第二次驅逐失敗後 total=16，
follow-up/HEAD 為正確的 13。兩個 filesystem 例外後，前兩版 total=6，後兩版=1；
所有版本都已失去 target membership 且仍有 2/1 個實體檔案。follow-up/HEAD 才繼續
通過重新下載、內容恢復及再次刪除的 assertions。失敗的 baseline/initial pytest
不會繼續驗證該 testcase 後半段，因此不聲稱它們的恢復部分已通過。

## Phase 2 歷史摘要（不是本包的新增執行次數）

先前已觀察 baseline 留下 2/3 個目錄、total 變負；最終修補留下 0/3。initial 在
callback 例外後留下 orphan recorded entry，follow-up 修復一致性。真實權限錯誤
與部分刪除會留下檔案，故障解除後可經重建恢復。Phase 2 的 HEAD 有 54 個獨立
案例及 16 個既有案例通過；本包沒有重新執行或保留整組測試。

此包足以支持範圍限定在 recorded accounting、失敗一致性及元件恢復的正向技術
review；不代表磁碟清理保證、完整 E2E 證明或新發現的未修 bug。未發布 upstream review。
