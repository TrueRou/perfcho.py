# 中心运行时架构

## 拓扑

perfcho 是一个可信中心应用，不拆分微服务，也不接受外部状态导入。运行时由三个进程角色组成：

| 角色 | 职责 | 连接 |
| --- | --- | --- |
| API/实时网关 | Stable、Lazer 与实时协议适配，认证、命令和查询 | PostgreSQL、Redis DB 0 |
| Outbox Relay | 扫描到期 Delivery 并投递 Taskiq | PostgreSQL、Redis DB 1 |
| Taskiq Worker | 执行投影、计算、通知和维护任务 | PostgreSQL、Redis DB 1，按任务需要访问 DB 0 |

这些角色共享应用服务、SQLAlchemy Model 和部署密钥。进程数量不构成业务节点身份，数据库中不保存节点、Lease、Epoch 或签名。

## Redis 约定

状态键统一以 `perfcho:state` 开头，后续段落使用领域、对象和状态名称，例如：

```text
perfcho:state:presence:account:{account_id}
perfcho:state:multiplayer:session:{session_id}:members
perfcho:state:multiplayer:session:{session_id}:ready
perfcho:state:spectator:account:{account_id}:frames
perfcho:state:ratelimit:{scope}:{subject}
```

在线 Presence 采用续期 TTL；房间瞬时状态不超过 Session 最大重连窗口；帧数据使用短 TTL 和有界长度；限流键 TTL 等于限流窗口。禁止创建无 TTL 的状态键。Key 格式或值结构变化时增加版本段，避免新旧进程误读。

## 可靠任务流程

1. 应用服务显式提供非空消费者列表，按 `(consumer, partition_key)` 获取事务级 Advisory Lock，再写入业务事实、`events.outbox_events` 和每个版本化消费者对应的 `events.outbox_deliveries`。Position 在锁内分配，因此同分区顺序与提交顺序一致。
2. 提交后 Relay 使用 `FOR UPDATE SKIP LOCKED` 领取到期 Delivery，为本次入队生成 Delivery Token，并向 Taskiq Redis Stream 投递 `(event_id, consumer, delivery_token)`。
3. Worker 锁定 Delivery。已完成、已进入 Dead Letter 或 Token 已过期的记录直接返回，保证重复消息和旧租约无副作用。
4. Relay 只领取每个 `(consumer, partition_key)` 最早的未完成事件，保证同一投影分区按 Outbox Position 串行处理。
5. Worker 加载 Outbox Event 并调用注册消费者。消费者接收 Partition Key，投影、Checkpoint 和 Delivery 完成在同一事务提交。
6. 入队与业务执行分别计数。Worker 不可用时 Relay 可以重新入队但不会消耗业务执行次数；消费者实际失败达到上限后才标记 Dead Letter。

Taskiq 使用 `when_executed` 确认和 Redis Stream Pending Entry 降低运输损失，但业务恢复不依赖 Broker Ack。Relay 会重新领取租约过期且未完成的 Delivery，因此消费者必须幂等。

## 故障语义

| 故障 | 行为 |
| --- | --- |
| Redis DB 0 重启 | 在线状态丢失，客户端重连并从 PostgreSQL 事实恢复。 |
| Redis DB 1 重启或 Stream 丢失 | 未完成 Delivery 租约到期后重新投递。 |
| Relay 在入队后崩溃 | 可能产生重复 Taskiq 消息，由 Delivery 主键去重。 |
| Worker 在业务提交前崩溃 | 事务回滚，Delivery 重新投递。 |
| Worker 在业务提交后、Broker Ack 前崩溃 | 重复消息发现 Delivery 已完成并直接返回。 |
| PostgreSQL 不可用 | 持久命令、Relay 与 Worker 停止；Redis 状态不能提升为业务事实。 |
