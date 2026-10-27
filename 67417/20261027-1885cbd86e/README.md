# Ray PR #67417：same-ID head registration replay recovery 驗證

本目錄保存 [Ray PR #67417](https://github.com/ray-project/ray/pull/67417) 對 [Issue #67414](https://github.com/ray-project/ray/issues/67414) 的獨立 A/B 驗證證據。分類為 **INDEPENDENT VERIFICATION**：Parent 在相同測試語意下為 **13 PASS / 3 expected FAIL**，PR HEAD 為 **16 PASS**；沒有發現可歸因於此 PR 的新 correctness finding。

| 角色 | 完整 revision |
| --- | --- |
| Parent | `0a509f3448c1f3c71f80b80f46a6fb6a4df971ff` |
| PR HEAD | `1885cbd86ef61cd93890de804c64feaeb3f4ed9f` |

## Failure mechanism 與修改

Issue 的實際症狀是 head raylet 在註冊流程後收到自身的 node-death side effect。PR 作者提出的觸發時間線是：GCS 已將 head H 的 ALIVE registration 寫入 persistent store，但 registration reply 遺失；GCS 重新啟動後從 store 恢復 H，raylet 再重送完全相同的 `RegisterNode` request。這個套件驗證該 handler-level 時間線造成的狀態變化，但沒有重現真實 TCP RST、遺失 reply、GCS process crash 或 raylet fatal。因此，遺失 reply 與重啟的因果串接仍是作者的觸發假說，不應描述成此套件的端到端觀測。

Production change 位於 `src/ray/gcs/gcs_node_manager.cc` 的 `GcsNodeManager::HandleRegisterNode`：

```diff
-    if (head_nodes.size() == 1) {
+    if (head_nodes.size() == 1 && head_nodes[0] != node_id) {
```

Parent 只要 alive cache 中已有一個 head，就先對該 head 呼叫 `OnNodeFailure`。當既有 head 和重播 request 都是 H 時，H 被從 alive cache 移除並加入 dead cache，觸發 removal listener 與兩個 DEAD publication；callback 隨後再 `NodeTable.Put(ALIVE)`、`AddNodeToCache` 並發布 ALIVE。最終 store 是 ALIVE，但同一 NodeID 同時出現在 alive/dead cache，observer 已看見虛假的死亡事件。

HEAD 對同一 NodeID 跳過 `OnNodeFailure`，直接走 registration 的 ALIVE write/cache/publication 路徑。不同 NodeID 的 head replacement 仍符合條件並保留原分支。本驗證接受 PR 明示的 identity model：同一 NodeID 表示同一 node lifetime；刻意讓不同 incarnation 重用 NodeID 不在此修正的支援範圍。

## Recovery fixture

獨立測試 `TestRestoredHeadRegistrationReplayHasNoDeathSideEffects` 使用既有 `GcsNodeManagerTest` fixture 與 production handlers：

1. 第一個 manager 透過真實 `HandleRegisterNode` 將 ALIVE head H 寫入 fixture 共用的 `InMemoryStoreClient`。
2. 第一個 manager 在 callbacks 完成後銷毀；第二個 manager 以空的 alive/dead cache 建立。
3. 既有 `LoadInitData()` 呼叫 `GcsInitData::AsyncLoad`；production `GcsNodeManager::Initialize` 從該資料恢復 H。測試確認 restore 後 alive/dead 為 `1/0`，且 restore 本身沒有 listener 或 publication side effect。
4. 測試以 serialization byte-for-byte 相同的 `RegisterNodeRequest` 呼叫第二個 manager 的真實 `HandleRegisterNode`。
5. 測試重新用 `LoadInitData()` 讀 storage，並觀測 caches、node-added/node-removed listeners，以及 production `GcsPublisher` 透過既有 `FakePublisher` 記錄的兩個 node channels。

測試沒有直接修改 manager cache，也沒有製造 production 不可能的狀態。Storage wrapper 與 `InMemoryStoreClient` 跨 manager lifetime 保留，模擬外部 store 對新 manager 可見；這不是 RocksDB/Redis crash recovery。

## A/B observed transitions

| 觀測點 | Parent | PR HEAD |
| --- | --- | --- |
| restore 後 alive/dead | `1/0` | `1/0` |
| handler return 當下 alive/dead | `0/1` | `1/0` |
| callbacks 完成後 alive/dead | `1/1` | `1/0` |
| final NodeTable state | `ALIVE` | `ALIVE` |
| removal listener | `1` | `0` |
| added listener | `1` | `0` |
| DEAD node-info / liveness publications | `1 / 1` | `0 / 0` |
| ALIVE node-info / liveness publications | `1 / 1` | `1 / 1` |

Parent 的三個 expected failures 是 recovery regression 的六個 side-effect assertions，以及 `TestReregisterSameHeadNode/{0,1}` 各自觀測 `nodes.size() == 2` 而非 `1`。HEAD 的 recovery test、`TestReregisterSameHeadNode/{0,1}`、`TestRegisterHeadNode/{0,1}`、`TestGcsTableReload/{0,1}`、fixture initialize test 與八個鄰近 registration/node API tests 全部通過。

完整結果在 [summary.json](observed/summary.json)，原始 Bazel `test.log` 與 XML 分別保存在 [head](observed/head/) 與 [parent](observed/parent/)；`patches/` 保存 production delta、作者測試及兩個 snapshot 所使用的完全相同 recovery test。

## 已驗證與證據邊界

**Runtime verified：** production `HandleRegisterNode` 的 same-ID replay；production `GcsInitData`/`Initialize` restore 路徑；最終 storage state；alive/dead caches；node listener counts；送入 production `GcsPublisher` 的 DEAD/ALIVE channel messages；Parent RED / HEAD GREEN；既有 same-ID 及鄰近測試的結果。

**未經端到端驗證：** 真實 GCS process crash；RocksDB/Redis crash recovery 與 durability；transport-level reply loss、TCP RST 或自動 gRPC retry；真實 PubSub 網路傳遞、subscriber ordering；raylet `NodeRemoved` fatal；不同 NodeID head replacement 的專用 recovery A/B；不同 node incarnation 重用 NodeID；獨立 death/write completion race；exactly-once ALIVE notifications。`FakePublisher` 證明 production publisher 收到的 message，不等同完整網路 PubSub。`TestGcsTableReload/1` 在 memory 參數下會提前返回，不能視為 recovery coverage。

原始封存 `ray-67417-phase2-evidence.tar.gz` 的 SHA256 是 `36c567ed4f837b75bb501e4698f6055c42f0d86a534b2764759cd53b7b107c7f`；封存本身、build cache、source snapshots 與 binaries 均未納入公開套件。為使新增檔案通過 `git diff --check`，公開副本只對原始 logs/patches 的行尾水平空白與多餘 EOF 空行做機械清理；allowlist 逐檔標記受影響副本，測試文字、順序、結果與 patch 邏輯不變。每個公開檔案列於 [PUBLICATION-ALLOWLIST.json](PUBLICATION-ALLOWLIST.json)。可用下列命令驗證位元完整性：

```bash
sha256sum --check SHA256SUMS
```
