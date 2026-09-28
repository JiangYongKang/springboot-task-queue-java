# Spring Boot 本地任务消息队列

一个**不依赖任何外部中间件**的任务消息队列：消息、消费组偏移量、在途交付、死信全部落在本地
嵌入式 H2 文件数据库中，服务重启后状态不丢失。提供 HTTP 接口完成投递、拉取、确认、重试
上报与死信查询，并通过数据库事务 + 行级锁保证并发与重启下的语义稳定。

## 1. 核心语义

### 1.1 投递（publish）

- `POST /api/queue/messages`，请求体 `{"dedupKey": "...", "payload": "..."}`。
- 每条消息带**全局唯一去重标识 `dedupKey`**，服务端对其建唯一约束。
- 相同 `dedupKey` 的重复投递（包括并发投递）**不会产生多条可消费记录**：
  接口返回首条消息的 `offset`，并以 `duplicated=true` 标识命中去重。
- `dedupKey` 为空时服务端自动生成 UUID。
- 消息写入后获得单调递增的 `offset`（即消息主键，从 1 开始）。

### 1.2 拉取与消费组（pull）

- `POST /api/queue/groups/{group}/pull`，返回一条消息和一次性 `deliveryId`。
- 消费状态**按消费组独立维护**：同一条消息可被不同组各消费一次，互不影响。
- 拉取即建立租约（lease），消息状态由 `PENDING` 转为 `INFLIGHT`。
- **同一时刻同一条消息不能被同一组的两个消费者同时消费**：
  同一消费组的所有认领事务都在该组的计数器行上 `SELECT ... FOR UPDATE` 串行化，
  并按 offset 升序认领；消息行也会加行锁。并发拉取只会把不同消息分给不同消费者。

### 1.3 确认（ack）与不可静默忽略的拒绝

- `POST /api/queue/groups/{group}/ack`，请求体 `{"deliveryId": "..."}`。
- 确认后消息进入终态 `ACKED`，租约释放。
- 重复确认、跨组确认等**不会被静默吞掉**，而是返回可区分的拒绝原因（`accepted=false, reason=...`）：

| reason | 触发条件 |
| --- | --- |
| `DELIVERY_NOT_FOUND` | deliveryId 不存在（从未存在，或已被新一轮投递取代） |
| `GROUP_MISMATCH` | deliveryId 属于**另一个消费组** |
| `MESSAGE_NOT_INFLIGHT` | deliveryId 存在，但消息当前不在 `INFLIGHT`（例如已超时回到 `RETRY_WAIT`） |
| `DUPLICATE_ACK` | 该 deliveryId 对应的消息已经被确认过 |

### 1.4 偏移量与断点续拉

- 每个消费组维护一个 `committedOffset`，语义为：
  **“该 offset 及之前的所有消息对本组都已到达终态（`ACKED` 或 `DEAD`）”**。
- 偏移量**只增不减、严格按序推进**：若中间某条消息仍在 `INFLIGHT`/`RETRY_WAIT`，
  即使后面的消息先确认，`committedOffset` 也不会越过它。
- 组内用计数器行 + 行锁维护“窗口内非终态消息数”和“已观测高水位”，
  因此并发确认不会出现偏移量回退或推进丢失。
- 新组首次拉取时自动建立偏移量记录（初始为 0）；
  未被任何组拉取过的消息不会影响该组偏移量。
- 偏移量随数据库持久化，重启后可继续断点续拉。查询：
  - `GET /api/queue/groups/{group}/offset`
  - `GET /api/queue/offsets`（所有组）

### 1.5 失败重试与退避（nack）

- `POST /api/queue/groups/{group}/nack`，请求体 `{"deliveryId": "...", "error": "..."}`。
- 消费失败上报后消息进入 `RETRY_WAIT`，在退避时间到达前对拉取不可见。
- 每次重新投递 `attempt` 加 1，并生成**新的 deliveryId**（旧 deliveryId 立即失效）。
- 退避为可配置的指数退避：第 `n` 次投递（`n ≥ 2`）前的等待时间为
  `backoff-millis * backoff-multiplier^(n-2)`。

### 1.6 死信（dead letter）

- 当投递次数达到 `taskqueue.max-attempts` 仍失败（主动 nack 或租约超时回收），
  消息进入终态 `DEAD`，**不再可被拉取**。
- 同时向死信表写入一条记录，**保留原始投递信息**：
  消息 offset、dedupKey、原始 payload、消费组、累计尝试次数、最后一次错误、
  原始发布时间、进入死信时间。
- `DEAD` 与 `ACKED` 一样视为终态，会计入偏移量推进。
- 查询：`GET /api/queue/groups/{group}/dead-letters?limit=50`。

### 1.7 确认超时与重新投递

- 每次拉取产生的租约有截止时间 `taskqueue.ack-timeout-millis`。
- 后台 `LeaseReaper` 每 `taskqueue.reaper-interval-millis` 扫描一次：
  超过截止时间仍未确认的消息，按剩余重试次数转回 `RETRY_WAIT` 或进入死信，
  整个状态流转在单组单事务内完成，与并发 ack 不会互相覆盖。
- 因此“消费者宕机/未确认”的消息会被自动重新投递，形成投递—确认闭环。
- 对已经超时的旧 deliveryId 再 ack：若消息尚在 `RETRY_WAIT` 返回
  `MESSAGE_NOT_INFLIGHT`；若已重新投递则旧 id 返回 `DELIVERY_NOT_FOUND`，
  保证一条消息不会被重复消费两次。

## 2. 消息状态流转

```
                         拉取(pull)
  (无组记录, 新消息) ───────────────► INFLIGHT ◄─────── 拉取(退避到期)
                                      │  │  ▲                │
                          有效 ack ────┘  │  └──── RETRY_WAIT ┘
                                        │        ▲   │
                                        ▼        │   │ 超时回收 / 未达上限 nack
                                      ACKED      │   ▼
                                        ▲        └──(退避等待, 对 pull 不可见)
                                        │
   INFLIGHT ──nack 达上限 / 超时回收达上限──► DEAD ── 同时写入 queue_dead_letters
```

组内每 (group, message) 只有一行状态记录，终态为 `ACKED` 或 `DEAD`。

## 3. 持久化与重启恢复

- 默认数据源为本地文件 H2：`jdbc:h2:file:./data/taskqueue`（见 `application.properties`）。
- 未确认消息（`INFLIGHT`）、组偏移量、重试状态、死信记录都在文件库中，**重启不丢**。
- 重启后：
  - 仍在租约期内的消息继续保持 `INFLIGHT`，不会被重复投递；租约记录仍可被确认。
  - 租约已在停机期间过期的消息，由启动后的定时回收器转回 `RETRY_WAIT`
    （退避到期后重新投递）或达上限后进入死信。
  - 偏移量与死信记录原样恢复。
- 建表为幂等的 `CREATE TABLE IF NOT EXISTS`，首次启动自动初始化。

## 4. 配置项（application.properties）

| 配置 | 默认值 | 含义 |
| --- | --- | --- |
| `taskqueue.max-attempts` | `3` | 最大投递次数，达到后进入死信 |
| `taskqueue.backoff-millis` | `1000` | 首次重试基础退避（毫秒） |
| `taskqueue.backoff-multiplier` | `2.0` | 指数退避乘数 |
| `taskqueue.ack-timeout-millis` | `30000` | 拉取后未确认的租约时长（毫秒） |
| `taskqueue.reaper-interval-millis` | `5000` | 超时回收扫描间隔（毫秒） |

## 5. HTTP 接口一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/queue/messages` | 投递消息（按 dedupKey 去重） |
| POST | `/api/queue/groups/{group}/pull` | 拉取一条消息（含 deliveryId/attempt） |
| POST | `/api/queue/groups/{group}/ack` | 确认 |
| POST | `/api/queue/groups/{group}/nack` | 上报失败（重试或死信） |
| GET  | `/api/queue/groups/{group}/offset` | 查看组偏移量 |
| GET  | `/api/queue/offsets` | 查看所有组偏移量 |
| GET  | `/api/queue/groups/{group}/dead-letters?limit=` | 查看死信 |

## 6. 本地验证

### 6.1 运行测试

```bash
mvn test
```

测试覆盖（均使用真实 Spring 事务与真实 H2 锁，断言无同组重复消费、无消息丢失、无计数错乱，
日志中打印消息标识、消费组、offset 与状态迁移过程）：

- `DuplicatePublishTest`：串行/并发重复投递只产生一条可消费记录。
- `ConcurrentPullTest`：8 个消费者并发拉取 20 条消息，验证同组无重复消费、
  每消息唯一 deliveryId、偏移量精确推进到 20。
- `LeaseTimeoutTest`：确认超时后重新投递、旧 deliveryId 被区分原因拒绝、
  超过上限进入死信；同时覆盖 `DELIVERY_NOT_FOUND` / `GROUP_MISMATCH` /
  `MESSAGE_NOT_INFLIGHT` / `DUPLICATE_ACK` 四类拒绝。
- `RetryAndDeadLetterTest`：退避期内不可见、退避后按 attempt 重投、
  达上限进入死信并保留原始发布信息。
- `OffsetAdvanceTest`：乱序确认下偏移量不回退、不越过未完成消息。
- `RestartRecoveryTest`：用文件库模拟进程重启，验证未确认消息、偏移量、
  死信不丢失，过期租约在重启后可恢复重新投递。
- `QueueHttpEndToEndTest`：完整 HTTP 链路。

### 6.2 手动启动并用 curl 验证

```bash
mvn spring-boot:run            # 默认 8080；如端口被占用可加参数：
# mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=19091
```

```bash
B=http://localhost:8080/api/queue

# 投递（重复 dedupKey 返回相同 offset 且 duplicated=true）
curl -s -XPOST $B/messages -H 'Content-Type: application/json' \
  -d '{"dedupKey":"job-1","payload":"hello"}'

# 拉取
curl -s -XPOST $B/groups/G1/pull

# 用返回的 deliveryId 确认
curl -s -XPOST $B/groups/G1/ack -H 'Content-Type: application/json' \
  -d '{"deliveryId":"<deliveryId>"}'

# 上报失败（重试）
curl -s -XPOST $B/groups/G1/nack -H 'Content-Type: application/json' \
  -d '{"deliveryId":"<deliveryId>","error":"boom"}'

# 偏移量 / 死信
curl -s $B/groups/G1/offset
curl -s $B/groups/G1/dead-letters
```

本地文件库目录 `./data/` 已加入 `.gitignore`；删除该目录即可得到全新空队列。

## 7. 实现要点

- 表：`queue_messages`（消息 + dedupKey 唯一约束）、`queue_group_messages`
  （每组每消息状态/尝试次数/deliveryId/租约/退避可见时间）、
  `queue_group_offsets`（committedOffset + 非终态计数 + 高水位）、
  `queue_dead_letters`（死信快照）。
- 同组认领、确认、回收都在该组 offset 行的行锁临界区内更新计数器与偏移量，
  从而串行化同组并发操作，避免重复消费、偏移量回退和计数错乱；不同组互不阻塞。
- 所有状态流转（INFLIGHT→ACKED/RETRY_WAIT/DEAD、死信落库、计数更新）均在单事务内完成。
