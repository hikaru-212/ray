# Ray PR #67399：RaySyncer reactor ownership 的獨立驗證

本套件是 [Ray PR #67399](https://github.com/ray-project/ray/pull/67399) 的 **Independent Verification** 公開證據。驗證結論是正面但有明確範圍：指定 PR HEAD 的既有 RaySyncer 測試確實重新編譯並執行，24 個測試全部通過；另以原始碼稽核確認 factory 建立的 reactor 在八個 dispatch handler 中持有 owning `shared_ptr`，batch timer 則先以 `weak_ptr::lock()` 取得存活物件。

| 角色 | revision | 動態測試 |
| --- | --- | --- |
| PR HEAD | `2011efd5a1e48a0438c8246dfb9342a49291bb3b` | 是 |
| Parent | `8d53e59e3da91a39beb6b1b614265cf05d3d26a4` | 否，僅作來源關係記錄 |

## Runtime 結果

執行目標為 `//src/ray/ray_syncer/tests:ray_syncer_test`。Bazel 與 TestRunner exit code 都是 `0`；GoogleTest XML 記錄 24 executed、24 passed、0 failed、0 errors、0 skipped、0 disabled。GoogleTest 內部時間是 4.861 秒，Bazel TestRunner action 是 5.238 秒。

`--nocache_test_results` 明確停用測試結果快取；execution evidence 另記錄 TestRunner 的 `cacheHit=false`。同一次執行亦重新編譯 `node_state.cc`、`ray_syncer.cc`、`ray_syncer_client.cc`、`ray_syncer_server.cc` 與 `ray_syncer_test.cc`，相關 compile、link、TestRunner actions 都是 `processwrapper-sandbox`、`cacheHit=false`、`exitCode=0`。

既有套件中有 12 個測試走 production factory：5 個 `SyncerTest` 案例涵蓋實際 client/server factory、訊息同步、斷線清理、顯式重新連線與多節點傳播；7 個 authentication 案例涵蓋 client/server 或 server factory 的成功與拒絕路徑。精確案例與斷言邊界列於 [factory-test-scope.md](source-audit/factory-test-scope.md)。其餘 12 個是 NodeState／fake reactor 單元測試或直接建立 reactor 的測試，不能當作 production factory lifetime 證據。

原始測試輸出保存在 [test.log](observed/test.log) 與 [test.xml](observed/test.xml)。[summary.json](observed/summary.json) 是由 XML、GoogleTest marker、Bazel events 與 execution log 交叉整理的結果；[bazel-actions.json](observed/bazel-actions.json) 保存足以稽核重新編譯及 `cacheHit=false` 的選定 action 欄位。

## Source ownership 結論

原始碼稽核在同一個 HEAD 上確認八個 dispatch handler 都以 `self = self_ref_` 捕捉 owning `shared_ptr`：base class 三個、client 兩個、server 三個。client/server `OnDone` 在 owning handler 內執行 cleanup 並 reset `self_ref_`；handler 的 local `self` 仍使 reactor 存活到該 handler 返回。batch timer 保存 `weak_ptr`，callback 在接觸 reactor state 前先 `lock()`，失敗即返回。

這個結論依賴正常 factory 與 gRPC callback 契約：factory 以 `make_shared` 建立 reactor，client 在 `StartCall()` 前設定 self reference，server 在把 raw reactor pointer 交還 gRPC 前設定 self reference。完整位置與前提列於 [ownership.md](source-audit/ownership.md)，原始搜尋輸出則在 [ownership-sites.txt](source-audit/ownership-sites.txt)。

## 證據邊界

Runtime verified：指定 HEAD 的既有套件在記錄的 Linux aarch64／Bazel 7.5.0／GCC 13.3.0 環境通過；production factory 的既有連線、訊息、斷線 cleanup、顯式 reconnect 與 authentication 行為沒有觀察到回歸。

Source verified：八個 dispatched handler 的 owning capture、兩個 factory 的 self-reference 設定，以及 timer 的 weak-lock guard。

未驗證：原始 issue #62008 的 ASAN UAF 重現；reactor destructor 或最後一個 owning reference 的精確時間；任何 parent A/B 結果；所有 use-after-free 的消除；inherited double-`Finish` 正確性；完整 gRPC stream、service、`RaySyncer` 或 `io_context` lifetime safety；失敗後 2 秒 timer 驅動的自動重連。Parent 沒有在本階段建置或執行。

因此，本證據支持限定範圍的正面 Independent Verification comment，不能描述成完整記憶體安全認證或 ASAN 修復證明。

## 公開資料與完整性

公開套件刻意不包含 81 MB execution log、19 MB action dump、binary、Bazel cache、virtual environment 或完整 source snapshot。`test.log` 與 `test.xml` 是未修改的原始檔；其餘衍生檔的來源與路徑清理記錄在 [PUBLICATION-ALLOWLIST.json](PUBLICATION-ALLOWLIST.json) 及 [publication-audit.json](source-audit/publication-audit.json)。

使用下列命令驗證公開檔案：

```bash
sha256sum --check SHA256SUMS
```

重現方式與執行命令見 [REPRODUCE.md](REPRODUCE.md)。
