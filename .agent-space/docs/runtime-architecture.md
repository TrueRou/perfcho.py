# 中心运行时架构

## 拓扑

perfcho 是一个可信中心应用，不拆分微服务，也不接受外部状态导入。运行时由两个进程角色组成：

| 角色 | 职责 | 连接 |
| --- | --- | --- |
| API/实时网关 | Stable、Lazer 与实时协议适配，认证、命令和查询 | PostgreSQL、Redis DB 0 |
| Taskiq Worker | 通过两个独立 Relay Loop 扫描到期 Delivery/Calculation Job，并运行 PostgreSQL 排名快照维护循环，同时执行投影、外部 Calculator 编排和通知任务 | PostgreSQL、Redis DB 1、S3，按任务访问 Calculator HTTP、权威内容上游或 DB 0 |

这些角色共享应用服务、SQLAlchemy Model 和部署密钥。API lifespan 在进程启动时组合一次 Stable 服务和 Bot Registry；请求只取得这个组合，所有数据库操作仍通过各自的短事务 UoW，不共享 AsyncSession 或 ORM Entity。进程数量不构成业务节点身份；数据库只保存持久工作的短期 Lease/Fencing Token，不把 Worker 进程登记为长期业务节点。

## 部署映射

本地开发由 VS Code Compound 同时启动 API 和 Taskiq Worker。启动前任务负责同步锁定依赖、启动并等待开发 `compose.yaml` 中的 PostgreSQL/Redis/MinIO，以及幂等初始化对象存储桶；两个应用角色连接 PostgreSQL 时通过共享 Advisory Lock 串行执行 SQLAlchemy 自动建表。`perfcho.worker` 是唯一 Worker 组合根，启动后创建 Outbox 与 Performance 两个相互隔离的 Relay Loop，以及由 `system.maintenance_states` 去重的每日 Rank Snapshot 循环；调试结束不自动销毁基础设施和开发数据卷。

根目录 `compose.prod.yaml` 是生产拓扑。PostgreSQL、带认证的 Redis、内部 MinIO 与 perfcho-pp Calculator 在 Compose 网络内运行，`minio-init` 幂等创建 bucket，Taskiq Worker 在 Calculator 容器启动后运行；Calculator 通过内部 `http://minio:9000` 访问 Worker 生成的短期签名 URL。API 默认仅向宿主机回环地址发布端口，由同机反向代理提供 TLS；部署凭据由 `.env.production.example` 模板提供。开发 `compose.yaml` 只启动宿主机进程所需的 PostgreSQL、Redis 与 MinIO，`.env.example` 提供对应的本地连接 URL 和凭据。

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

Stable Poll 先用一次持久 Session touch 完成 Token 身份解析，再解析 Redis epoch 并执行 fenced heartbeat；不会在同一 Poll 重复解析身份。严格单个空载荷 ID `4` 在 Mailbox 为空时可持有 fenced Lease 做 200–500 ms 短等待，普通入队与观战帧扇出通过按 Session Fence 隔离的 Redis List Signal 跨进程唤醒；超时空响应不发送 ID `8`，不会形成永久请求链。Presence 列表通过一次 Hash pipeline 解码所有已索引账户，缺失索引成员使用一次批量 ZREM 清理，不对每个账户追加 Redis 请求。

## 可靠任务流程

1. 应用服务显式提供非空消费者列表，按 `(consumer, partition_key)` 获取事务级 Advisory Lock，再写入业务事实、`events.outbox_events` 和每个版本化消费者对应的 `events.outbox_deliveries`。Position 在锁内分配，因此同分区顺序与提交顺序一致。
2. 提交后 Relay 使用 `FOR UPDATE SKIP LOCKED` 领取到期 Delivery，为本次入队生成 Delivery Token，并向 Taskiq Redis Stream 投递 `(event_id, consumer, delivery_token)`；一批投递完成后，所有 enqueue outcome 在一个 PostgreSQL 事务中批量写回，取消时未尝试尾部批量释放。
3. Worker 锁定 Delivery。已完成、已进入 Dead Letter、Token 不匹配或任务开始时 Lease 已过期的记录直接返回，保证重复消息和旧租约无副作用。
4. Relay 只领取每个 `(consumer, partition_key)` 最早的未完成事件，保证同一投影分区按 Outbox Position 串行处理。
5. Worker 加载 Outbox Event 并调用注册消费者。消费者接收 Partition Key，投影、Checkpoint 和 Delivery 完成在同一事务提交。
6. 入队与业务执行分别计数。Worker 不可用时 Relay 可以重新入队但不会消耗业务执行次数；消费者实际失败达到上限后才标记 Dead Letter。Dead Delivery 阻塞同一 `(consumer, partition_key)` 的后续事件，防止越过失败事实推进 Checkpoint。

Taskiq 使用 `when_executed` 确认和 Redis Stream Pending Entry 降低运输损失，但业务恢复不依赖 Broker Ack。Relay 会重新领取租约过期且未完成的 Delivery，因此消费者必须幂等。

Performance Calculation 不作为普通 Outbox Consumer 执行，避免在 Delivery 行锁事务内等待 HTTP。成绩事务直接写 PostgreSQL Job；独立 Relay 租赁 Job 并投递 `(job_id, lease_token)`。Worker 用第一个短事务保证同一 Token 只开始一次业务 Attempt、固化 Input Digest，并从任务真正开始时刷新执行 Lease；事务外生成 S3 签名 URL 并按 Formula Calculator Code 调用 C#/Rust。第二个短事务只有在 Fence 与执行 Lease 仍有效时才写 Difficulty、PP、完成事件和 Job 状态。相同 `(score_id, release_id)` 重复计算必须得到相同 Output Digest。

每日 Rank Snapshot 只在 `ranking-projector.v1` 没有未完成 Delivery 时执行。多个 Worker 使用事务级 Advisory Lock 串行检查 `system.maintenance_states`，再以单个集合查询和窗口函数原子写入当天所有活动 Policy 的 Global/Country Rank；失败事务整体回滚，下一轮继续尝试。

成绩相关 Outbox Delivery 使用 `account:{account_id}:scoreboard:{scoreboard_id}` 分区，保留同账户的统计与排行榜顺序，同时允许同一 Scoreboard 的不同账户并行。Multiplayer Results 对 RoundResult、SessionStanding、Playlist 和 Room summary 使用批量 Upsert。Content Sync 以 `CONTENT_SYNC_MAX_CONCURRENCY` 限制下载、摘要和对象存储写入并发，完成全部外部 I/O 后才开启发布事务。

## 故障语义

| 故障 | 行为 |
| --- | --- |
| Redis DB 0 重启 | 在线状态丢失，客户端重连并从 PostgreSQL 事实恢复。 |
| Redis DB 1 重启或 Stream 丢失 | 未完成 Delivery 租约到期后重新投递。 |
| Relay 在入队后崩溃 | 可能产生重复 Taskiq 消息，由 Delivery 主键去重。 |
| Worker 在业务提交前崩溃 | 事务回滚，Delivery 重新投递。 |
| Worker 在业务提交后、Broker Ack 前崩溃 | 重复消息发现 Delivery 已完成并直接返回。 |
| PostgreSQL 不可用 | 持久命令、Relay 与 Worker 停止；Redis 状态不能提升为业务事实。 |
