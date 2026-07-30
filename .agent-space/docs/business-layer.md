# 业务层后续交付契约

## 范围

本文档保留本次数据库交付刻意排除的完整业务层计划。未来实现必须使用规范持久化模型，不能复制任一参考后端的架构。Stable 与 Lazer 只是围绕同一组应用命令和查询的协议适配器。

## 目标模块

| 模块 | 命令与查询 | 主要 Schema |
| --- | --- | --- |
| Account | 注册、激活、改名、修改邮箱、资料和偏好、匿名化 | `core`、`events`、`audit` |
| Identity | 密码登录、Stable 登录、OAuth、Refresh、撤销、TOTP、可信设备 | `iam`、`core`、`events` |
| Authorization | 权限与权益计算、角色授予和撤销、Supporter 过期 | `authz`、`audit`、`events` |
| Content | 按 ID/MD5 查图、同步谱面集、发布修订、收藏、评分、标签、评论 | `content`、`events` |
| Scoring | 签发 Attempt、接收两端成绩、校验 Attestation/Replay 和调度后续任务 | `scoring`、`moderation`、`events` |
| Performance | Formula/Release、Difficulty/PP 计算、持久 Job 和多结果查询 | `scoring`、`events` |
| Ranking | 投影最佳成绩、用户统计、排名历史、失败进度和回放查看 | `scoring`、`events` |
| Social | 关注、屏蔽、团队成员与成就判断 | `social`、`events` |
| Community | 频道权限、私信、已读游标、通知扇出和外部投递 | `community`、`moderation`、`events` |
| Multiplayer | 房间、Session、谱单、回合、成绩绑定、图池与在线状态 | `multiplayer`、`scoring`、Redis |
| Matchmaking | 小队、队列 Ticket、Assignment 接受和评分更新 | `multiplayer`、`events` |
| Moderation | 建案、隔离成绩、施加和撤销处罚、申诉、证据和审计 | `moderation`、`scoring`、`audit` |

每个模块对外暴露应用命令与查询服务。协议 Router 只能依赖这些接口，不能直接导入 SQLAlchemy Model 或自行管理事务。

## 事务模式

1. 在协议边界完成解析与认证。
2. 把 Stable/Lazer 字段规范化为带 Request ID 和 Idempotency ID 的类型化命令。
3. 应用服务为命令创建一个 `AsyncSession` 事务。
4. 锁定聚合，或使用带预期版本的条件更新。
5. 根据权威数据验证业务策略。
6. 在同一事务写入事实、`events.outbox_events` 和对应 `events.outbox_deliveries`。
7. 提交后才能发送数据包、HTTP 响应或外部消息。
8. 由幂等 Projector 更新读模型，请求事务不能等待投影完成。

可预期的并发冲突转换为领域冲突，并允许执行有界重试。一个 `AsyncSession` 只能属于一个请求或任务，不能在 Free-threaded Python 任务之间共享。

## 认证交付

- 使用唯一且有文档的 NFKC、Casefold、空格转下划线规则生成 `name_key`。
- 邮箱单独规范化，通过 `email_key` 保证唯一性。
- 使用一个版本化密码验证策略。Stable 传入 MD5 形式密码 Token；Lazer/Web 输入必须先转换为相同的预验证表示，再执行 Argon2id。
- `iam.auth_sessions` 是撤销边界。Refresh Token 重放需要撤销整个 Token Family 和 Session。
- Opaque Token、Challenge Code、API Key、设备标识和恢复码只能保存带密钥摘要。
- Authorization Code 和 Reset Challenge 使用带未消费与未过期条件的单条 UPDATE 原子消费。
- Stable 单会话限制属于应用规则，Tourney 客户端例外必须显式声明。

验收必须覆盖密码与 Session 撤销、OAuth PKCE、Redirect URI、TOTP、恢复码单次使用、设备信任和并发重复注册。

## 成绩接收交付

1. 把提交谱面解析到不可变 Beatmap Revision。
2. 规范化 Ruleset、Variant 与 Mod，并按 Digest 获取或创建 Mod Set。
3. 根据账户、协议和幂等键插入 Play Attempt。
4. 校验 Checksum、时间、客户端版本、命中统计、计分公式和多人上下文。
5. 原子写入 Score、Hit Statistics、Attestation、Replay Manifest 与 Outbox Event。
6. 调度版本化难度/PP 计算和反作弊检测。
7. 在 Policy/Context 锁下投影 Eligibility 和用户最佳排行榜记录。
8. 通过事件更新用户与谱面统计，并推进 Projector Watermark。

协议适配器负责把 Stable 百分比 Accuracy 转换为规范比例，并把 Legacy Hit Name 映射成动态命中记录。Lazer Mod Settings 使用规范 JSON，Stable Mod Bits 只作为查询辅助。谱面更新或反作弊发现都不能删除成绩；成绩应通过 Policy State 变为 Ineligible 或 Quarantined。

验收必须覆盖重复提交、过期谱面修订、非法 Mod 组合、回放缺失或损坏、计算版本变更、有效性反转、排行榜 Tie 与全量投影重建。

## 多人交付

持久房间使用事件驱动聚合。Socket、Ready/Loading/Skip、实时 Score Frame、Spectator Frame 和 Typing Indicator 保存在 Redis 并设置 TTL。房间配置、Admission、Session、Playlist Revision、Round、Attempt、语义事件、Result 和 Rating 使用 PostgreSQL 权威事实。

所有多人命令由中心实时网关完成认证和规范化，然后直接调用 Multiplayer 应用服务。聚合版本和行锁处理并发，领域表唯一约束处理协议重试。任何实时状态都不能绕过 PostgreSQL 事务直接生成持久结果。

中心成绩模块负责把已接受成绩绑定到 Multiplayer Attempt。模块必须核对账户、冻结谱面修订、Scoreboard、Mod、时间和单次 Token。Taskiq Projector 计算 Round Result、Session Standing、Matchmaking Rating 与 Daily Challenge Streak。

所有房间均由中心服务托管。`rooms.ranked` 只决定结果是否参与全局排名，不表示可信等级；中心保存的房间事实始终具有相同权威性。

验收必须覆盖重复命令、旧聚合版本、Redis 状态丢失后的重连、Rematch、图池版本、Attempt Token 重放、非法成绩绑定、Assignment 超时和 Rating 幂等。

## 异步任务与消息

- Taskiq 使用 Redis Stream Broker，Worker 以 `when_executed` 确认消息，不启用 Result Backend。
- 请求事务禁止直接调用 `.kiq()`；事件消费使用 Transactional Outbox，像 Performance Calculation 这样拥有领域唯一键和跨外部 I/O 状态机的工作在同一业务事务写专用持久 Job。
- Relay 使用 `FOR UPDATE SKIP LOCKED` 领取 Delivery，投递成功后记录 Task ID，租约过期或 Redis 丢失时允许重新投递。
- Worker 按 `(event_id, consumer)` 幂等处理。处理结果、Projection Checkpoint 与 Delivery 完成必须在同一 PostgreSQL 事务提交。
- 固定周期任务可以由 Taskiq Scheduler 唤醒，但业务游标和可恢复状态保存在 `system.maintenance_states`。

## 社区与处罚交付

- Stable Mail 与 Lazer PM 统一映射为 Direct Conversation Channel。
- 消息插入前检查 Block 和 Channel Permission，使用 Client Message UUID 保证重试幂等。
- 已读状态通过 `(channel_id, message_id)` 游标单调推进，未读数量属于派生值。
- Outbox Event 生成 Notification 和 Recipient，外部投递拥有独立重试状态。
- 反作弊 Finding 只能隔离 Eligibility 或创建 Moderation Case，不能直接施加处罚。
- 每次处罚变化追加 Sanction Event；敏感管理命令同时追加 `audit.audit_events`。

验收必须覆盖并发创建私信、重复消息、Block/Friend 策略、游标单调性、通知扇出重试、处罚过期和撤销、Finding Review 与审计不可变性。

## 交付顺序

1. Repository、Unit of Work、规范化 ID、Outbox Writer 与 Permission Evaluator。
2. 两端共用的 Account 与 Identity 工作流。
3. Content Sync 与谱面查找。
4. 成绩接收、回放存储、计算 Worker、Eligibility 与排行榜。
5. Stable Bancho/Web 适配器和 Lazer OAuth/API 适配器。
6. Community、Social、Team、Achievement 与 Notification。
7. Multiplayer Core、Redis 实时状态与 Matchmaking。
8. Moderation Tooling、Reconciliation Job、负载测试与运维仪表盘。

每个阶段都必须包含 PostgreSQL 集成测试、命令幂等测试、Outbox Replay 测试以及显式 Stable/Lazer Contract Matrix，完成后才能进入下一阶段。
