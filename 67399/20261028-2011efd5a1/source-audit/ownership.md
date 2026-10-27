# Reactor ownership source audit

本稽核只描述 revision `2011efd5a1e48a0438c8246dfb9342a49291bb3b` 的 source-level 保證。它不是 destructor timing 的 runtime 測試。

## Factory 前提

`RaySyncer::Connect(node_id, channel)` 以 `std::make_shared<RayClientBidiReactor>` 建立 client reactor，呼叫 `SetSelfRef(reactor)`，加入 `sync_reactors_`，最後才呼叫 `StartCall()`。`RaySyncerService::StartSync` 以 `std::make_shared<RayServerBidiReactor>` 建立 server reactor，在註冊或回傳 raw pointer 前呼叫 `SetSelfRef(reactor)`。

因此在正常 factory 路徑中，`self_ref_` 是實際 owning `shared_ptr`。直接 stack-allocate fake reactor 或直接 `new` reactor 的測試不滿足這個 factory 前提，不能拿來否定或證明同一個 ownership closure。

## 八個 dispatched handler

| # | 位置 | handler | owning capture 的用途 |
| --- | --- | --- | --- |
| 1 | `ray_syncer_bidi_reactor_base.h:243` | `OnWriteDone` dispatch | `SendNext()` 或 `Disconnect()` 完成前保持 reactor 存活 |
| 2 | `ray_syncer_bidi_reactor_base.h:261` | `OnReadDone(false)` dispatch | read failure cleanup 完成前保持存活 |
| 3 | `ray_syncer_bidi_reactor_base.h:318` | buffered message processing dispatch | `ProcessBufferedSyncMessages()` 完成前保持存活 |
| 4 | `ray_syncer_client.cc:62` | client `OnDone` dispatch | cleanup callback 與 `self_ref_.reset()` 完成前保持存活 |
| 5 | `ray_syncer_client.cc:71` | client `DoDisconnect` dispatch | `StartWritesDone()`／`RemoveHold()` 完成前保持存活 |
| 6 | `ray_syncer_server.cc:112` | server `DoDisconnect` dispatch | `Finish(OK)` 呼叫完成前保持存活 |
| 7 | `ray_syncer_server.cc:116` | server `OnCancel` dispatch | `Disconnect()` 呼叫完成前保持存活 |
| 8 | `ray_syncer_server.cc:123` | server `OnDone` dispatch | cleanup callback 與 `self_ref_.reset()` 完成前保持存活 |

每個 lambda 都以 `self = self_ref_` 進行 copy capture。即使 `OnDone` handler 內 reset member `self_ref_`，local captured `self` 仍持有 ownership，直到該 handler 返回；其他已排入 io_context 的 handler 也各自保有副本。

## Batch timer

`ray_syncer_bidi_reactor_base.h:114` 從 `self_ref_` 建立 `weak_ptr`。timer callback 第一個 reactor-lifetime 動作是 `auto self = weak_self.lock()`；lock 失敗立即 return。只有成功取得 owning `self` 後才修改 `batch_timer_active_`、讀取 disconnected state 或呼叫 `StartSend()`。timer 不會單獨延長已完成 reactor 的生命週期。

## Source 與 runtime 的分界

上述 ownership closure 是 source verified，依賴 factory 初始化順序及正常 callback contract。既有 runtime 套件會實際經過多個 factory connection／cleanup 路徑，但沒有 destructor observer、`weak_ptr::expired()` 斷言、last-owner 計數或 handler/`OnDone` ordering barrier，因此 24/24 PASS 不能被解讀成直接證明析構時間。

本套件沒有執行 ASAN、TSAN 或 UBSAN，也沒有重現 issue #62008 的原始 UAF。它不主張 complete UAF elimination、double-`Finish` correctness 或完整 gRPC stream/service lifetime safety。
