# Ray PR #67554：部署層獨立正確性調查

## 結論分層

本封存補充 `../20261030/` 的受控 Parent/HEAD 反例，將 investigation
推進到真實 Ray Serve/HAProxy/Dynamo 部署。它建立了 production cancellation
到達 abandoned-cleanup coroutine 的可達性，但沒有建立 cleanup 完成，也沒有
在未修改的 production scheduling 中重現 Same-ID stale-free。

| 問題 | 結論 |
|---|---|
| 最小真實 Serve LLM deployment 是否可行？ | **是，D2 PASS。** |
| HTTP request ID 是否進入 production tracker 與 real Dynamo booking？ | **是，D3A PASS。** |
| 正常 1 秒 timeout 是否會取消 selection？ | **沒有；D3B 於 9.219821 ms 完成。** |
| 支援的 native queue backpressure 是否能讓 X 卡在 real selection？ | **是，D3B-2 PASS。** |
| timeout 是否到達 HEAD `select_worker()` cancellation handler？ | **是；事後完整快照稽核確認 cleanup(X) coroutine。** |
| cleanup 是否等到 native result 並執行 `free_reservation(X)`？ | **未建立；reserve Future 到最後仍 pending。** |
| 自然 production Same-ID stale-free 是否重現？ | **否，仍未驗證。** |

## D2：真實部署與 membership

D2 啟動真實本機 Ray runtime、Serve controller 與 HAProxy，部署一個 production
`LLMRouter` 及一個具有兩個 CPU mock replicas 的 `LLMServer` deployment。request
router 是 public production `ray.serve.llm.request_router.KVAwareRouter`；其 active
router 綁定 LLMRouter 所持有的 exact-HEAD production `KVTokenTracker`，沒有採用
load-balanced fallback。

LongPoll 依序將兩個 replica membership 傳給 tracker；Dynamo
`SelectionService` 最終具有兩個 lifecycle=`schedulable` 的不同 workers。兩個
replicas 的 Serve replica ID、Dynamo worker ID、routing metadata 與 backend HTTP
endpoint 均可區分。D2 的八項 criteria 全部為 true，且 Serve/Ray 均正常關閉。

這裡的 mock 只取代 LLM engine/weights；controller、deployment actor、HTTP
transport、request router、tracker、LongPoll 與 Dynamo 都是真實 production
components。

## D3A：HTTP → booking → serving replica

D3A 發送一個真正的 HTTP POST 至 `/v1/chat/completions`，使用 client header：

`x-request-id: ray-67554-d3a-http-booking-0001`

觀察到的鏈為：

```text
HTTP/HAProxy
  → production LLMRouter.route()
  → DeploymentHandle.choose_replica(_reserve=False)
  → production KVAwareRouter.choose_replicas()
  → exact-HEAD KVTokenTracker.select_worker()
  → Dynamo SelectionService.select_and_reserve()
  → selected LLMServer replica serves HTTP 200
```

request ID 在 response header、production tracker state 與 Dynamo selection log 中
保持一致。tokenization 產生七個 prompt tokens；Dynamo selected worker 的
`active_requests` 從 0 變 1、`potential_prefill_tokens` 從 0 變 7，而 HTTP response
由映射到同一 worker 的 replica `qfsmp1b0` 提供。active router 的 production
inheritance、tracker object identity 與 no-fallback 條件均成立。

限制：CPU `RegistrationMockVLLMEngine` 不會發出 production vLLM completion/KV
events。因此 D3A 證明 booking 與 HTTP serving，不證明完整 request lifecycle
completion；runner 沒有製造 completion event。

## D3B：正常路由不進入取消窗

D3B 保留 D3A topology，發送一個正常 HTTP request。端到端時間為
`0.009219821 s`（9.219821 ms），HTTP 200；遠早於設定的 1 秒 timeout。native
booking 成功，但沒有 cancellation handler、abandoned cleanup 或 native free
證據。原始分類
`D3B_NOT_ESTABLISHED_ROUTING_COMPLETED_BEFORE_TIMEOUT` 是正確且維持不變。

## D3B-2：支援的 native queue backpressure

D3B-2 使用 Dynamo 支援的設定，不修改 native methods 或 Future：

- `DYN_ROUTER_QUEUE_THRESHOLD=0`
- `DYN_ROUTER_ACTIVE_REQUEST_EXPIRY_SECS=5`
- target Serve timeout：1 秒
- HAProxy ingress-router timeout：5 秒（嚴格大於 target timeout）
- native active-request sweep interval：60 秒

P1 與 P2 先以不同 request IDs 在不同 workers 建立正 prefill load，且初始
`pending_count=0`。之後只送一個 target X。X 出現時兩 workers 各有
`active_requests=1`、`potential_prefill_tokens=7`，Dynamo 轉為
`pending_count=1`、`pending_isl_tokens=7`；X 尚未出現在 tracker booking state。
這建立的是 real native pending selection，不是 fabricated accounting。

### 內部 408 與外部 200 必須分開

`LLMRouter` direct ingress 在約 1 秒後記錄 internal HTTP 408（1000.7 ms）並
呼叫 `request_task.cancel()`。HAProxy 的 production
`ingress_request_router_fallback` 隨後將原 request 送至正常 LLMServer replica，
其 0.9 ms 回應使外部 client 收到 HTTP 200，總時間為 1.002706164 秒。

外部 200 不是 KV-aware booking 成功證據。X 從未進入 Python tracker booking
state，且沒有 native `Selected worker`/selection result for X。

## 事後 observation correction

原始 final runner 保持不變，原始分類仍為 `CANCELLATION_NOT_ESTABLISHED`。但
完整未壓縮 snapshots 顯示，timeout 後 production tracker 的 `_pending_tasks`
出現唯一 task：

```text
Task 259382507479744
  → KVTokenTracker._schedule.<locals>._run 259382517836736
  → KVTokenTracker._free_abandoned_reservation 259382507152352
       self       = production tracker 259382932763328
       request_id = ray-67554-d3b2-target-0001
       reserve    = Future 259382507685488 (pending, not cancelled)
  → FutureIter
```

同一物件鏈從 snapshot line 42 持續到 line 2809，共 2,768 個 samples。production
source 只會在 `select_worker()` 捕捉 caller `CancelledError` 後排程這個 cleanup，
所以這建立 cancellation 已到達 handler。

observer 漏判的近因是 `_code(candidate) is _CLEANUP_CODE`：module-level expected
code-object ID 為 `259382941767824`，active coroutine code-object ID 為
`259383360385264`。檔案、qualified name、first line 494、exact tracker、X 與
reserve Future 全部吻合，只有跨 Serve/cloudpickle serialization 不穩定的 Python
object identity 不同。詳見 `OBSERVATION_CORRECTION.md`。

## Native outcome 與 expiry 邊界

cleanup coroutine 停在 `await reserve`；Future 在最後快照仍
`done=false, cancelled=false`。因此：

- production cancellation handler：已到達；
- exact cleanup(X) coroutine：已存在且仍 pending；
- native selection success for X：未建立；
- `free_reservation(X)`：未直接觀察，亦不可由 accounting 推論；
- cleanup terminal outcome：未建立。

約 60 秒 sweep 時 P1/P2 的 native active reservations 被 expiry 釋放；兩 worker
final active load 都為零，但 `pending_count=1`。剩餘 native state 可歸因於唯一
post-prime request X 的 pending selection，而不是 P1/P2 或已建立的 X booking。

Python tracker 的 stale-request TTL 是 3,600 秒，且 mock 不送 completion events；
故 P1/P2 仍可留在 Python `_requests`。這與 native expiry 是兩種狀態，不可拿來
證明 cleanup(X)。

最後 snapshot 先於 shutdown。shutdown 之後 LongPoll 移除 workers 並停止 actor，
但沒有 post-shutdown native snapshot；process termination 不是
`free_reservation(X)` 證據。

## 與 2026-10-30 controlled H3 的關係

舊封存在明確 gated ordering 下得到 **CONTROLLED PARENT GREEN → HEAD RED**：
延遲 A 的真實 PyO3 completion delivery、retire W1、讓 B 以同一 ID 在 W2 booking，
再讓 A cleanup 執行 `free(X)`；HEAD 移除 B accounting，Parent 保留。distinct-ID
control 保留 B。

該 counterexample 對受控 ordering 仍有效。D2/D3A/D3B-2 增加 production
deployment、HTTP、membership、queue 與 cancellation-handler reachability 證據，
但沒有把 Same-ID B、worker retirement 或 stale free 帶進這次 deployment run。
所以不能宣稱已自然重現原 bug，也不能估計自然發生頻率。

## 方法與證據界線

- D2/D3A/D3B/D3B-2 是單次、bounded、本機 deployment runs。
- D3B-2 observer 只讀 tracker tasks、frames、Future state 與 Dynamo loads。
- 沒有 wrap/invoke native select/free/delete，沒有 PyO3 callback gate，沒有 native
  task pause，沒有 scheduling sleep/gate，也沒有 fabricated reservation。
- 大型 logs、28.6 MB snapshots、Ray session、venv、native binaries 與模型 fixture
  均留在原始私人 evidence directories，不收入本 package。
- 本 package 是可稽核索引與最小摘錄，不是 self-contained reproducer。

