# Production-factory test scope

以下 12 個案例在本次 24/24 PASS 中走 production `RaySyncer::Connect` 及／或 `RaySyncerService::StartSync` factory。這是由指定 HEAD 的測試原始碼與實際 XML case list 對照所得，不是 compiler coverage instrumentation。

| 測試 | Factory | Runtime 斷言範圍 |
| --- | --- | --- |
| `SyncerTest.Test1To1` | client + server | 連線登錄、雙向訊息、版本更新與 messages-received callback |
| `SyncerTest.Reconnect` | client + server | 顯式 `Disconnect` 後立即再次 `Connect`，重連後收到版本更新 |
| `SyncerTest.Broadcast` | client + server | 三節點傳播；斷線後兩端 cluster view 移除節點 |
| `SyncerTest.Test1ToN` | client + server | 20 節點星狀連線、訊息傳播及 state 收斂 |
| `SyncerTest.TestMToN` | client + server | 20 節點樹狀連線、訊息傳播及 state 收斂 |
| `SyncerAuthenticationTest.MatchingTokens` | client + server | token 相符後 client connected-node set 非空；沒有額外訊息 round-trip 斷言 |
| `SyncerAuthenticationTest.MismatchedTokens` | client + server | token 不符後 client connected-node set 為空 |
| `SyncerAuthenticationTest.ServerHasTokenClientDoesNot` | server | 缺少 authorization metadata 回傳 `UNAUTHENTICATED` |
| `SyncerAuthenticationTest.ServerWithAuthDisabledRejectsToken` | server | auth disabled 卻送 token 時回傳預期 `UNAUTHENTICATED` message |
| `SyncerAuthenticationTest.MissingNodeIdAndTokenIsRejected` | server | auth mode 下缺 token/node ID 的拒絕順序 |
| `SyncerAuthenticationTest.MissingNodeIdWithValidTokenIsRejected` | server | valid token 但缺 node ID 回傳 `INVALID_ARGUMENT` |
| `SyncerAuthenticationTest.MissingOrMalformedNodeIdWithoutAuthIsRejected` | server | 缺失、非 hex、無效 hex node ID 均回傳 `INVALID_ARGUMENT` |

`SyncerTest.Reconnect` 測的是顯式斷線後重新呼叫 `Connect`，沒有等待或斷言 failure cleanup 排程的 2 秒自動 reconnect timer。

其餘 12 個案例的分類：10 個是 NodeState、message-type 或 fake reactor 單元測試；2 個 `SyncerReactorTest` 直接建立 client/server reactor，檢查訊息與 cleanup callback 回傳，但沒有透過 `RaySyncer` production factory 設定 owning `self_ref_`，也沒有析構斷言。既有測試中某些 fake/direct cases 會直接呼叫 reaction；本驗證沒有新增 reaction 呼叫、修改 callback 或引入取消競態。
