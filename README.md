# Spring Boot 本地任务消息队列

无需任何外部中间件（无 Kafka/RabbitMQ/Redis、无独立数据库），仅靠一个 Spring Boot
进程加一个本地 JSON 快照文件，实现一套具备**投递确认闭环、按组偏移量、失败重试、
死信、去重与重启恢复**的本地任务消息队列，并通过 HTTP 对外提供接口。

- Java 21 / Spring Boot 3.4 / 只依赖 spring-boot-starter-web
- 并发安全：队列所有状态变更在单进程内一把锁串行化，PENDING→INFLIGHT 是持锁的
  原子“检查并翻转”，同一组同一条消息同一时刻只会被一个消费者拿到。
- 持久化：每次状态翻转后整体快照落盘（临时文件 fsync + 原子 rename），
  消息日志、按组偏移量、各组每条消息的投递状态、死信记录重启不丢失。

## 1. 快速开始

```bash
mvn spring-boot:run
# 或打包后运行（可用命令行参数覆盖任意配置）
mvn package -DskipTests
java -jar target/springboot-task-queue-java-1.0-SNAPSHOT.jar \
  --server.port=8080 \
  --taskqueue.visibility-timeout-ms=2000 \
  --taskqueue.max-attempts=3
```

启动随机端口（默认 `server.port=0`，日志中可见实际端口）。固定端口加
`--server.port=8080`。

## 2. HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/queue/publish` | 投递消息，body：`{"dedupKey":"...","payload":"..."}` |
| GET  | `/api/queue/groups/{group}/pull?maxMessages=10` | 消费者组拉取（含未消费的全部组内消息），返回消息并加确认租约 |
| POST | `/api/queue/groups/{group}/messages/{offset}/ack` | 消费成功确认 |
| POST | `/api/queue/groups/{group}/messages/{offset}/nack` | 显式上报失败，body：`{"error":"..."}`（可为空） |
| GET  | `/api/queue/dead-letters` | 查询全部死信（保留原始投递信息） |
| GET  | `/api/queue/groups/{group}/offset` | 查询组已提交偏移量 |

拉取响应字段：`offset`（组内共享的单调位点）、`dedupKey`、`payload`、
`attempt`（第几次投递，从 1 开始）、`leaseExpireAt`（确认租约到期时间戳）。

### curl 示例

```bash
B=http://127.0.0.1:8080/api/queue

# 投递
curl -X POST $B/publish -H 'Content-Type: application/json' \
  -d '{"dedupKey":"task-A","payload":"do something"}'

# 消费者组 worker-1 拉取
curl "$B/groups/worker-1/pull?maxMessages=10"

# 确认 / 否定确认
curl -X POST $B/groups/worker-1/messages/0/ack
curl -X POST $B/groups/worker-1/messages/0/nack \
  -H 'Content-Type: application/json' -d '{"error":"upstream 500"}'

# 查看偏移量与死信
curl $B/groups/worker-1/offset
curl $B/dead-letters
```

## 3. 投递与确认语义

1. **投递**：消息获得全局单调递增的 `offset`（从 0 开始），并要求调用方提供全局唯一
   的 `dedupKey`。`dedupKey` 已存在时返回 `409 DUPLICATE_DEDUP_KEY` 及已存在的
   `existingOffset`，**不会产生第二条可消费记录**。
2. **拉取与互斥**：某消费者组拉到消息后，该消息在该组内由 `PENDING` 原子翻转为
   `INFLIGHT`，并设置确认租约。租约期内同组其他拉取拿不到这条消息；不同消费者组
   各自维护独立的投递状态，互不影响（类似 Kafka 不同 group 各自消费全量）。
3. **确认闭环**：
   - 正常 `ack` 后消息在该组内进入终态 `ACKED`，并推进组的**连续确认水位**
     （committed offset）。
   - **重复确认**不静默成功：返回 `409 ACK_REJECTED`，原因 `ALREADY_ACKED`。
   - **跨组/未知位点确认**：原因 `GROUP_OR_MESSAGE_UNKNOWN`。
   - **已死信后确认**：原因 `MESSAGE_DEAD`。
   - **租约过期后确认**（消息已被回收重投给别人）：原因 `LEASE_EXPIRED`；
     消息当前不在途（已 nack 回退）时：原因 `NOT_INFLIGHT`。
   以上拒绝原因在响应体 `reason` 字段中可区分。

## 4. 重试与死信

- 消息未在租约（`taskqueue.visibility-timeout-ms`）内确认时，后台 reaper
  （扫描间隔 `taskqueue.reaper-interval-ms`）自动回收；消费者也可主动 `nack`。
- 每次失败后投递次数 `attempt` 加 1。`attempt < max-attempts` 时按退避策略把消息
  置回 `PENDING` 并记录 `nextVisibleAt`，退避结束前不会被再次拉取；退避期间位点不变。
- `attempt >= max-attempts`（默认 5，配置 `taskqueue.max-attempts`）时进入死信，
  状态 `DEAD`，**保留原始投递信息**：`offset`、`dedupKey`、`consumerGroup`、
  原始 `payload`、`publishedAt`、累计 `attempts`、最后失败原因 `lastError`、
  进入死信时间 `deadAt`。死信消息不再可拉取，可经 `/api/queue/dead-letters` 查询。
- 退避时长由 `taskqueue.backoff-base-ms` 与 `taskqueue.backoff-strategy` 决定：
  - `FIXED`：`base`
  - `LINEAR`：`base * 失败尝试次数`
  - `EXPONENTIAL`（默认）：`base * 2^(失败尝试次数-1)`
- 状态流转：`PENDING -> INFLIGHT -> ACKED`（成功路径）；
  失败在未超限时 `INFLIGHT -> PENDING(退避)` 循环，超限 `INFLIGHT -> DEAD`。

## 5. 偏移量与恢复规则

- **偏移量按消费者组维护**，语义为“该组从位点 0 起连续确认到的水位”：只由连续的
  `ACKED` 前缀推进，只增不减。中间一条消息未确认/在退避/已死信会形成空洞，水位停在
  空洞前；空洞之后的消息仍可正常投递与确认（拉取扫描从水位开始但会跳过非 PENDING
  及退避中的消息），因此支持**断点续拉**，既不丢消息也不会让位点回退。
- 每次状态变更（投递、拉取翻转、ack、nack、超时回收、死信）后立即把完整快照
  原子覆盖写到 `taskqueue.data-dir/taskqueue-state.json`。
- **重启恢复**（应用启动时自动执行一次 `recover`）：
  - 消息日志、各组 committed offset、死信记录按磁盘值原样恢复；
  - 上次进程遗留的所有 `INFLIGHT`（崩溃/被 kill 时未确认）消息统一翻转为
    `PENDING` 并按当前尝试次数施加一次退避，随后可被重新拉取，不丢失也不重复建记录；
  - `dedupKey` 索引从消息日志重建，重启后重复投递仍会被拒绝。

## 6. 本地验证方法

```bash
# 全部单测/集成测试（共 12 个），日志打印 dedupKey/consumerGroup/offset/状态迁移
mvn test

# 仅核心语义测试
mvn test -Dtest=TaskQueueServiceTest
# 仅 HTTP 接口测试
mvn test -Dtest=QueueApiTest
```

测试覆盖的场景：

1. 重复投递只产生一条可消费记录（`DuplicateDedupKeyException` / 409）；
2. 20 个并发消费者 × 200 条消息，断言同组无重复消费、无消息丢失；
3. 不确认导致租约超时，reaper 重新投递且 `attempt` 递增、位点不变；
4. 重复 ack（`ALREADY_ACKED`）、跨组/未知位点 ack（`GROUP_OR_MESSAGE_UNKNOWN`）、
   死信后 ack（`MESSAGE_DEAD`）等可区分拒绝；
5. 重试达到上限进入死信，且死信保留原始 payload / dedupKey / 尝试次数；
6. 模拟重启（新实例从同一快照 recover）：committed offset、死信保留，
   未确认消息重新可拉取；
7. 多个消费者组对同一消息独立消费、偏移量各自独立。

手工端到端：启动服务后按第 2 节 curl 顺序操作；把 `visibility-timeout-ms` 调小
（如 2000）后拉取但不 ack，观察日志中 `INFLIGHT->PENDING` 重投与 `INFLIGHT->DEAD`
死信过程；kill 进程后重启可观察到 `recover ... INFLIGHT->PENDING`。

## 7. 配置项（`application.properties`，均可通过命令行 `--taskqueue.xxx=` 覆盖）

| 配置 | 默认值 | 含义 |
|---|---|---|
| `taskqueue.data-dir` | `data/taskqueue` | 快照目录 |
| `taskqueue.visibility-timeout-ms` | `10000` | 确认租约时长 |
| `taskqueue.max-attempts` | `5` | 最大投递次数，超限进死信 |
| `taskqueue.backoff-base-ms` | `1000` | 退避基础时长 |
| `taskqueue.backoff-strategy` | `EXPONENTIAL` | FIXED/LINEAR/EXPONENTIAL |
| `taskqueue.reaper-interval-ms` | `1000` | 超时回收扫描间隔 |

## 8. 适用边界

本实现面向**单机本地**开发/演示/轻量任务场景：吞吐受单锁与每次变更整体快照落盘
限制，且快照只存在本机磁盘，不提供多节点高可用。多实例部署需要换用外部消息中间件。
