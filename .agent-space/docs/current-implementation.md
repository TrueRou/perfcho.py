# 当前实现总览

最后更新：2026-07-29。本文描述当前代码事实，不把后续计划写成已完成能力。

## 1. 项目目标与边界

perfcho.py 是同时面向 osu! Stable 和 Lazer 的中心后端。架构采用单一可信中心和模块化单体，不拆分微服务：API、实时协议、异步任务和 Outbox Relay 是同一应用的不同进程角色，共享业务模型并直接访问 PostgreSQL、Redis 和对象存储。

当前优先保证最新 Stable 客户端可用，但业务模型应按 Lazer 的资源结构设计；Stable 是对共享应用命令和查询的兼容适配器，而不是另一套业务实现。

当前明确不做：

- 具体反作弊检测器；只保留 Attestation、Evidence 和可替换 Port。
- Tourney 客户端、`ppy.sb` 或 SB Patcher。
- 为尚未存在的投影注册空 Consumer。
- 为不可靠状态建立第二个事实中心。
- 过度压测、真实客户端自动化和长期 Soak；这些应在主流程完成后按风险补充。

## 2. 不可破坏的架构约束

### 2.1 数据职责

- PostgreSQL 保存账户、会话、内容 Revision、消息、成绩、排行榜和多人游戏等持久事实。
- Redis 保存 Session、Presence、Mailbox、Spectator 和未来 Match Slot 等带 TTL 的在线状态。
- S3 兼容对象存储保存 `.osu`、Replay 等二进制对象；PostgreSQL 只保存对象 Key、Hash、大小和媒体类型。
- Taskiq 只负责执行异步任务；任务输入必须可追踪到 PostgreSQL 中已提交的事实。

Redis 丢失后允许在线用户重新登录、重新加入或从 PostgreSQL 恢复状态，不能导致账户、成绩或房间历史丢失。

### 2.2 依赖方向

固定依赖方向为：

```text
HTTP/Bancho/Lazer/Worker Adapter
  -> modules 中的 Command/Query Service 与 Port
    -> infra 中的 SQLAlchemy/Redis/S3/Upstream Adapter
```

`modules` 不导入 FastAPI、Stable Wire Model 或 SQLAlchemy ORM。协议适配器不直接查询 ORM、不调用 `commit()`，也不自行创建 Service。

### 2.3 事务与幂等

- 每个写命令拥有一个短生命周期 `UnitOfWork`。
- 应用服务在同一事务中写入业务事实、命令回执和所需 Outbox 记录。
- 外部 HTTP 和对象存储 I/O 不得占用数据库事务。
- 命令使用 `request_id`、`idempotency_key` 和请求摘要区分重试与冲突。
- 所有 I/O 方法保持异步；Service 通过构造器接收 Repository、Clock、ID Generator 和外部 Port。

## 3. 运行时与组合根

### 3.1 API 进程

入口是 `src/perfcho/main.py`。FastAPI lifespan 创建一个 PostgreSQL Async Engine、一个 Redis Client 和 Session Factory，并在退出时释放。应用组合以下 Router：

- `/`：健康/欢迎响应。
- `/v1`：当前只保留基础版本化 JSON API 结构。
- Stable 无版本前缀 Router：Bancho、Web、Scoring。

`compose_stable_services()` 为每个 Stable 请求构造应用服务。当前实际接线：

- `IdentityService`
- `AuthorizationQueryService`
- `ContentQueryService`、`ContentService`
- `SocialService`
- `CommunityService`
- `ScoringService`
- `ReplayQueryService`、`ReplayService`
- `RankingQueryService`
- `MultiplayerService`
- `RedisRealtimeRepository`
- `RedisMultiplayerStateRepository`
- `S3ObjectStorage`

组合根当前使用 `DeferredPerformanceCalculator`，因此请求事务会接受可信成绩事实，但不会伪造 PP。

### 3.2 Outbox Relay 与 Worker

Outbox Relay 从 PostgreSQL 领取 `OutboxDelivery`，使用 Lease Owner 和 Fencing Token 防止旧 Worker 完成新租约，再将任务投入 Taskiq Redis Stream。Worker 在独立数据库事务中执行 Consumer，语义为至少一次投递：

- 同一 `consumer + partition_key` 严格按 Outbox Position 顺序执行。
- 不同 Partition 可以并行。
- 失败使用有界退避重试；达到上限后进入 Dead Letter 状态。
- Consumer 必须幂等，不能依赖任务恰好执行一次。

当前唯一实际注册的 Consumer：

| Consumer | 事件 | 实现 |
| --- | --- | --- |
| `ranking-projector.v1` | `score.accepted.v1` | 更新 Overall/Exact-Mods 排行榜投影 |

以下名称会被业务服务写入 Outbox，但没有注册处理器，因此相关生产流程尚未形成完整闭环：

- `account-projection.v1`
- `identity-projection.v1`
- `content-projection.v1`
- `social-projection.v1`
- `achievement-projection.v1`
- `community-projection.v1`
- `community-message.v1`

在对应业务投影或外部投递真正实现前，不能添加仅标记成功的空处理器。Relay/Worker 如果领取这些 Delivery，会因找不到 Consumer 重试并最终进入 Dead Letter；这不是静默忽略，部署异步进程前必须先决定实现对应 Consumer，或从生产者中删除没有实际用途的 Delivery。

## 4. 模块能力状态

| 模块 | 当前状态 | 已有能力 | 尚缺内容 |
| --- | --- | --- | --- |
| `common` | 已接线 | Actor、ClientContext、CommandMeta、CommandReceipt、UoW、Clock、ID、ObjectStorage Port | 无独立协议入口 |
| `account` | 已实现未接线 | 注册、用户名规范化、密码凭据、命令幂等、Outbox | 没有公开注册 API，未进入 Stable 组合根 |
| `identity` | 已接线 | Stable MD5 Token 预验证、Argon2id+Pepper、设备摘要、单会话、Web 凭据验证、主动会话关闭、Country Projection | Lazer OAuth 与 Refresh Token 流程未接线 |
| `authorization` | 已接线 | 权限查询、Stable Privilege 位映射、Silence 读取 | 管理命令与授权管理 API 未完成 |
| `content` | 部分接线 | ID/MD5/文件名查询、Direct、收藏、评分、不可变 Revision、官方 API Source、S3 文件 | Query/收藏/评分已接线；`ContentSyncService` 和官方 Source 未进入生产任务或管理入口 |
| `social` | 部分接线 | Follow/Unfollow、Block/Unblock、好友查询、Achievement 事实 | Stable 好友增删已接线；Block 与 Achievement 无协议入口；相关 Consumer 未注册 |
| `community` | 部分接线 | 频道、公开消息、私信、离线消息、Silence 与私信策略检查 | Stable 频道、公开消息、私信、Away Reply 和离线消息已接线；投递 Consumer 未注册 |
| `scoring` | 部分接线 | Canonical Score、Mod 规范化、校验、Attempt/Score/Hit/Replay/Attestation、Replay View、Ranking Query、Stable 基础统计 Query | Stable 全链路和 Multiplayer Attempt 消费已接线；真实 PP/Difficulty、重建与完整统计投影未完成 |
| `realtime` | 已接线 | Redis Session、Presence Index/Filter、Away、Mailbox、Spectator Relation、Frame History、Fence | 没有跨进程 Pub/Sub；即时扇出使用有界 Mailbox |
| `multiplayer` | 已接线 | Canonical Command/Query、SQL Repository、Redis CAS Projection、Room/Slot/Host/Password、Round/Attempt、Stable Lobby/Match Dispatcher | 未做 Tourney/Matchmaking；真实并发集成覆盖仍需在 PostgreSQL/Redis 环境执行 |

## 5. 持久化实现

### 5.1 数据库

导入全部 ORM Model 后，SQLAlchemy Metadata 当前包含 133 张表，按 PostgreSQL Schema 划分 IAM、Core、Authorization、Content、Community、Scoring、Multiplayer、Moderation、Audit、Events 等领域。

数据库表数量不等于应用完成度。Multiplayer 的 Stable Room/Session/Round 子集已经通过 Canonical Service 和 Adapter 接线；Moderation、Matchmaking、Tournament 等表仍主要是约束和未来能力预留。

数据库启动采用显式 Bootstrap，而不是请求期间隐式建表。ORM 关系默认倾向 `lazy="raise"`，防止异步请求中出现隐式查询。

### 5.2 内容与对象存储

`ContentSyncService` 已实现以下算法，但当前没有生产入口：

1. 在数据库事务外通过 osu! OAuth Client Credentials 获取 Beatmapset Snapshot。
2. 下载每个 `.osu` 文件并校验上游 MD5。
3. 计算 SHA-256，以内容寻址 Key 写入对象存储。
4. 开启短事务，发布不可变 Beatmap Revision 并切换 Current Revision。
5. 在同一事务写入 `content.beatmapset-synchronized.v1`。

Stable 文件读取优先从对象存储流式输出；数据库没有当前文件或对象不可用时重定向到配置的官方上游。

### 5.3 成绩与排行榜

Stable 成绩提交路径：

1. 限制 Multipart 总大小、字段数和 Replay 大小。
2. 使用 Stable Build 派生 Rijndael Key，解密成绩与客户端摘要。
3. 验证 Stable Web 凭据、谱面 MD5、命中统计、Accuracy、Grade、Mods、Checksum 和请求幂等。
4. 在数据库事务外将 Replay 以 SHA-256 内容寻址写入对象存储。
5. 在一个事务中写 PlayAttempt、Score、HitStatistic、Replay、Attestation、命令回执和 `score.accepted.v1`。
6. Ranking Consumer 更新符合资格的 Overall/Exact-Mods 最佳成绩投影。
7. 已同步谱面的 Multiplayer Round 会预先签发 Attempt，Stable 提交按 Account 与 Beatmap Revision 解析并在同一成绩事务中消费。

当前排行榜支持 Top、Exact Mods、Friends 和 Country 过滤。Bancho Stats 会查询 Play Count、Total Score、Accuracy、Ranked Score 和 Global Rank；PP/Performance 保持明确的延迟计算状态，不能把零值解释为最终计算结果。

## 6. Redis 在线状态

当前 Key 空间统一使用 `redis_state_prefix`，并为以下数据设置 TTL：

- Stable Realtime Session：账户、会话、Revision、过期时间。
- Presence Snapshot：Bancho Presence 和 Stats 的预编码 Packet。
- Presence Index 与 Client Preference：有界在线列表、订阅 Filter 和 Away Message；均受 Session Fence 与 TTL 保护。
- Mailbox：跨 Poll 的有界 Packet 队列，支持 Lease 与批量确认。
- Spectator Relation：玩家与 Host 的关系和 Fence。
- Spectator Frame History：按 Sequence 保存有界帧历史，并限制总帧数和总字节数。
- Multiplayer Projection：Room、16 个 Stable Slot、Ready/Loaded/Skip/Fail、Lobby Index 和账户唯一房间索引，使用 Lua CAS 与 TTL。

Mailbox Overflow 不可无限扩容。Spectator 通知投递会抑制单个接收者的 Overflow，防止一个慢客户端阻塞 Host；重要持久事实不得只依赖 Mailbox。

## 7. 配置与安全默认值

配置由 `Settings` 从环境变量读取。生产环境必须覆盖开发默认密钥：

- `PASSWORD_PEPPER`
- `TOKEN_HMAC_KEY`
- `DEVICE_HMAC_KEY`
- `MATCH_PASSWORD_HMAC_KEY`
- `S3_ACCESS_KEY`、`S3_SECRET_KEY`
- `OSU_API_CLIENT_ID`、`OSU_API_CLIENT_SECRET`

当前协议基线：

- Stable Build：`b20260711.1`
- Bancho Protocol Version：`19`
- Stable Session 持久有效期：12 小时
- Redis Session/Presence TTL：360 秒
- Mailbox TTL：600 秒
- Mailbox 最大 4096 个 Packet、16 MiB
- Spectator Frame 最大 4096 帧、16 MiB
- Multiplayer Projection TTL：12 小时；最多 4096 个 Stable Room
- Presence All 单次最多 2048 个账户；Lobby 单次最多 100 个 Match

`.env.production.example` 只作为变量清单，不应直接使用示例 Secret 部署。

## 8. 验证基线

当前最近一次完整验证：

- `168 passed, 8 skipped`
- `uv run ruff format .` 通过
- `uv run ruff check --fix .` 通过
- Python `compileall` 通过
- `git diff --check` 通过

8 个跳过用例依赖真实 PostgreSQL、Redis 或 MinIO。启用方式以测试中的 `TEST_DATABASE_URL`、`TEST_REDIS_URL` 和对象存储环境变量为准。

## 9. 当前已知限制

- Multiplayer 已完成 Stable 创建、加入、准备、开始、帧扇出和完成；未知或未同步谱面可联机但不会创建排名 Attempt。当前使用 Event Command ID 和自然幂等，尚未实现面向未来 Lazer 写命令的可重放 Command Receipt。
- `ContentSyncService` 没有 Scheduler、Worker 或管理命令接线。
- 除 Ranking 外，其他 Outbox Consumer 均未实现。
- 登录、好友和消息等已接线路径会产生尚未注册 Consumer 的 Delivery；运行 Relay 时这些 Delivery 会失败并最终进入 Dead Letter。
- `rosu-pp-py` 4.0.2 在 Python 3.14t 环境无可用 Wheel，源码构建持续超时；当前不能输出真实 PP。
- Lazer OAuth/API 适配器尚未实现，当前生产组合根仍以 Stable 请求为主。
- User Stats 已有基础权威聚合和 Country 映射；PP、Country Rank、Grade Count 和失败进度仍缺少完整投影。
- 没有完整的 Ranking Rebuild、Eligibility 反转、统计对账和 Dead Letter 运维工具。
- 反作弊只有证据模型和扩展点，没有任何检测器，这是刻意边界而非遗漏。
