# 当前实现总览

最后更新：2026-08-03。本文描述当前代码事实，不把后续计划写成已完成能力。

## 1. 项目目标与边界

perfcho.py 是同时面向 osu! Stable 和 Lazer 的中心后端。架构采用单一可信中心和模块化单体，不拆分微服务：API/实时协议与 Taskiq Worker 是同一应用的不同进程角色，共享业务模型并直接访问 PostgreSQL、Redis 和对象存储；Outbox 与 Calculation Relay 作为 Worker 后台循环运行。

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
- Stable 无版本前缀 Router：Cho 承载 Bancho 登录与 Poll，Web 统一承载内容、成绩、Replay 和排行榜端点。
- `api/stable/canonize` 负责登录与成绩上传的 Stable Wire 输入解析和规范化；`api/stable/dispatcher/` 包负责 Packet 到 Canonical Service 的协议适配，其中 `packets.py` 负责 Poll 分发，`multiplayer.py` 负责多人 Packet。

`compose_stable_services()` 为每个 Stable 请求构造应用服务。当前实际接线：

- `IdentityService`
- `AuthorizationQueryService`
- `ContentQueryService`、`ContentService`
- `SocialService`
- `CommunityService`
- `ScoringService`
- `PerformanceQueryService`
- `ReplayQueryService`、`ReplayService`
- `RankingQueryService`
- `MultiplayerService`
- `RedisRealtimeRepository`
- `RedisMultiplayerStateRepository`
- `S3ObjectStorage`

组合根按运行时职责拆分在 `infra/wiring/` 包中：`stable.py` 负责 Stable 请求服务，`management.py` 负责协议无关的授权和处罚管理服务，`content.py` 负责进程级内容同步资源，`common.py` 提供共享时钟、UUID Generator 和数据库绑定。Stable 与 Management 不互相依赖，只通过共享基础设施 wiring 使用同一套 `SystemClock`、Authorization Repository 和 Outbox Writer。

成绩接收事务不调用 Calculator。数据库 Bootstrap 默认安装启用的 `official`/`official-difficulty` Formula，二者使用统一的 `perfcho-pp` Calculator Code，并为四个 vanilla ruleset 创建 `2026.07.1` 活动 Release；部署仍需配置对应 Calculator endpoint。`ScoringService` 只调用中性的事务内后续任务调度端口；Performance PostgreSQL Adapter 根据 Scoreboard 上启用的 `CalculationFormula` 及其活动 `CalculationRelease`，原子创建 `calculation_jobs` 中 `(score_id, release_id)` 唯一的 `PerformanceCalculationJob`。同一成绩事务还通过 transaction-aware Achievement Awarder 在 Score 插入后评估显式注册的确定性定义，幂等写入 `social.achievement_unlocks` 和 `social.achievement-unlocked.v1`。没有发布 Formula/Release 时只接受可信成绩事实，不伪造 PP。

### 3.2 Worker 与持久任务 Relay

每个 Taskiq Worker 子进程由 `perfcho.worker` 统一组合数据库、HTTP、S3、应用服务与 Consumer Catalog，并运行相互隔离的 Outbox Delivery 与 Performance Job Relay Loop。Outbox Relay 使用 Lease Owner、`SKIP LOCKED` 和 Fencing Token 防止重复领取及旧 Worker 完成新租约，再将任务投入 Taskiq Redis Stream。Worker 在独立数据库事务中执行 Consumer，语义为至少一次投递：

- 同一 `consumer + partition_key` 严格按 Outbox Position 顺序执行。
- 不同 Partition 可以并行。
- 失败使用有界退避重试；达到上限后进入 Dead Letter 状态。
- Dead Delivery 阻塞同 Consumer Partition 的后续事件，不能跳过失败事实推进 Checkpoint。
- Consumer 必须幂等，不能依赖任务恰好执行一次。

当前实际注册的 Consumer：

| Consumer | 事件 | 实现 |
| --- | --- | --- |
| `account-projector.v1` | `account.registered.v1` | 写入公开 Account Activity，不复制邮箱 |
| `identity-projector.v1` | `identity.session-opened.v1`、`identity.session-closed.v1` | 写入私有 Session Activity，不修改 Redis Presence |
| `content-projector.v1` | `content.beatmapset-synchronized.v1` | 更新 Beatmapset Sync Projection；有实际变更时生成 Creator Activity/Notification |
| `social-projector.v1` | Follow/Unfollow/Block/Unblock v1 | 写入私有 Social Activity |
| `achievement-projector.v1` | `social.achievement-unlocked.v1` | 写入公开 Activity、Notification 和 Recipient |
| `community-projector.v1` | Direct Conversation 与 Channel Membership v1 | 从权威 Channel/Message 重算 Channel Read Projection |
| `community-message-projector.v1` | `community.message-sent.v1` | 更新频道摘要；私信生成 Notification、Recipient 和外部 Dispatch Intent |
| `ranking-projector.v1` | `score.accepted.v1`、`score.performance-calculated.v1` | 按全部活动 Ranking Policy、Mod Policy 与 Calculation Release 更新 Eligibility/Leaderboard 和 `user_ranked_stats` |
| `scoring-stats-projector.v1` | `score.accepted.v1`、`score.replay-viewed.v1` | 更新用户游玩、月度、用户/谱面活动和失败进度统计 |
| `multiplayer-results-projector.v1` | `multiplayer.round-completed.v1`、多人 `score.accepted.v1` | 从权威 Round/Attempt/Score 重建 Round Result、Session Standing 和房间/谱单汇总 |
| `authorization-projector.v1` | Authorization Grant/Revoke v1 | 写入 Staff Activity 并推进管理分区水位 |
| `moderation-projector.v1` | Case/Sanction v1 | 写入 Staff Activity 并推进管理分区水位 |

所有 Consumer 都严格校验 Schema、Aggregate 与 Partition，并在投影事务中单调推进 `ProjectionCheckpoint`。Community Worker 不写 Redis Mailbox，Stable 在线消息仍只由协议适配器提交后同步扇出，避免至少一次任务造成重复消息。

Performance Relay 独立领取 `PerformanceCalculationJob`，不会因 Outbox Relay 单次异常而停顿。Calculation Worker 使用“短事务按 Token 单次开始 Attempt、刷新执行 Lease 并固化 Input Digest -> 事务外生成 S3 签名 URL 并调用 HTTP Calculator -> 短事务按有效 Fencing Token/Lease 写 Difficulty、PP 和完成事件”三段流程，不在 Outbox Consumer 事务内等待网络。Revision 必须已经关联 S3 Asset；URL 生成或读取失败按 Job 错误策略重试或结束。Redis Stream 丢失或 Worker 崩溃后由 PostgreSQL Job Lease 恢复；相同 Release 重算的 Input/Output Digest 不一致会拒绝覆盖。

## 4. 模块能力状态

| 模块 | 当前状态 | 已有能力 | 尚缺内容 |
| --- | --- | --- | --- |
| `common` | 已接线 | Actor、ClientContext、CommandMeta、CommandReceipt、UoW、Clock、ID、ObjectStorage Port | 无独立协议入口 |
| `bot` | 已接线 | 协议无关命令内核、参数 DSL、别名、命令组、帮助、基础命令和 Stable 公开/私信接入；领域命令由各模块提供并在组合根注册 | `pool`/`clan` 对应应用服务尚未实现 |
| `account` | 已接线 | Stable 注册、用户名规范化、密码凭据、命令幂等、Outbox | Lazer 注册适配器尚未接线 |
| `identity` | 已接线 | Stable MD5 Token 预验证、Argon2id+Pepper、设备摘要、单会话、Web 凭据验证、主动会话关闭、Country Projection | Lazer OAuth 与 Refresh Token 流程未接线 |
| `authorization` | 部分接线 | 权限查询、Stable Privilege 位映射，以及带 Receipt/Audit/Outbox 的 Role、Permission、Entitlement Grant/Revoke Service | 管理协议/API 尚未接入独立 Management 组合根 |
| `content` | 部分接线 | ID/MD5/文件名查询、Direct、收藏、评分、不可变 Revision、官方 API Source、S3 文件 | Query/收藏/评分已接线；`ContentSyncService` 和官方 Source 未进入生产任务或管理入口 |
| `social` | 部分接线 | Follow/Unfollow、Block/Unblock、好友查询、Achievement 事实、成绩事务内确定性解锁、Activity/Notification Consumer | Stable 好友增删和成绩解锁 Chart 已接线；Block、Achievement 和 Activity/Notification 查询无独立协议入口 |
| `community` | 部分接线 | 频道、公开消息、私信、离线消息、Silence、私信策略、频道摘要与通知 Consumer | Stable 消息已接线；Notification Query 和外部邮件/Push Dispatcher 未实现 |
| `scoring` | 部分接线 | Canonical Score、Mod 规范化、校验、Attempt/Score/Hit/Replay/Attestation、中性后续任务调度端口、规范统计投影、Replay View、Ranking Query、Stable 统计 Query | 全量重建、Eligibility 反转和统计对账仍未完成 |
| `performance` | 已接线 | Formula/Release、持久计算 Job、Lease/Fencing、版本化 HTTP Calculator、Input/Output Digest、Multi-PP Query 与完成事件 | 尚未发布真实 C#/Rust Formula Release、Backfill 和管理 Command |
| `realtime` | 已接线 | Redis Session、Presence Index/Filter、Away、Mailbox、Spectator Relation、Frame History、Fence | 没有跨进程 Pub/Sub；即时扇出使用有界 Mailbox |
| `multiplayer` | 已接线 | Canonical Command/Query、SQL Repository、数据库 Public ID Epoch、Redis CAS Projection、Room/Slot/Host/Password、Round/Attempt、Stable Lobby/Match Dispatcher，以及支持迟到成绩的结果 Projector | 未做 Tourney/Matchmaking；START 与最后一次 Slot Mod CAS 的严格跨存储线性化仍需专门 Reservation/Fence |
| `moderation` | 已实现未接线 | 建案、Case Entry、处罚施加/延期/撤销、Sanction Event、Command Receipt、Audit 与 Outbox | 独立 Management 组合根已完成，但尚无认证后的管理协议入口 |
| `audit` | 已实现未接线 | 事务绑定 Audit Writer；Authorization/Moderation 敏感命令原子写 `audit_events` | 尚无审计查询 API 和保留策略任务 |

## 5. 持久化实现

### 5.1 数据库

导入全部 ORM Model 后，SQLAlchemy Metadata 当前包含 137 张表，按 PostgreSQL Schema 划分 IAM、Core、Authorization、Content、Community、Scoring、Multiplayer、Moderation、Audit、Events 等领域。Outbox Consumer 使用 `content.beatmapset_sync_projections`、`community.channel_read_projections` 和规范 Scoring 统计表作为可重建读模型。

数据库表数量不等于应用完成度。Stable Room/Session/Round 已通过 Canonical Service 和 Adapter 接线，完成事件和迟到成绩共同驱动 Multiplayer Result 投影。Moderation/Authorization 管理写侧和 Audit 已由独立 Management 组合根装配，但尚无外部管理协议；Matchmaking 与 Tournament 仍是未来能力预留。

数据库启动采用显式 Bootstrap，而不是请求期间隐式建表；每张目录表只在本次 Bootstrap 开始时为空时初始化，已有表数据不会被默认种子覆盖。ORM 关系默认倾向 `lazy="raise"`，防止异步请求中出现隐式查询。

### 5.2 内容与对象存储

`ContentSyncService` 已通过 Stable `osu-osz2-getscores.php` 按需接入生产路径：

1. 本地 MD5 命中时直接读取；响应发送后才按 `next_check_at` 尝试领取上游刷新，不让已缓存谱面的排行榜请求等待外部 I/O。
2. 本地 MD5 缺失时，优先使用 Stable 提供的 Beatmapset ID；缺失 ID 时通过官方 checksum、filename lookup 解析 Set，并在当前请求内完成首次同步。
3. 在数据库事务外通过 osu! OAuth Client Credentials 获取 Beatmapset Snapshot，下载每个 `.osu` 并校验上游 MD5。
4. 计算 SHA-256，以内容寻址 Key 写入对象存储。
5. 开启短事务，发布不可变 Beatmap Revision、切换 Current Revision，并写入 `content.beatmapset-synchronized.v1`。
6. 多进程到期刷新通过 PostgreSQL `next_check_at` 原子领取短 Lease；Beatmapset 行锁、单调上游时间和包含删除 Tombstone 的同版本 Revision 比较阻止晚到快照回滚 Current Revision。
7. 刷新频率按谱面更新时间和排名状态动态退避，最长 24 小时；失败回写受原 Lease 到期时间 Fence 保护。

Stable 文件读取优先从对象存储流式输出；数据库没有当前文件或对象不可用时重定向到配置的官方上游。

### 5.3 成绩与排行榜

Stable 成绩提交路径：

1. 在 Starlette Multipart Parser 前流式限制请求总字节，并限制字段数、Replay 大小与 24 字节最小结构；无 `Content-Length`/Chunked 请求同样受限。
2. 使用 Stable Build 派生 Rijndael Key，解密成绩与客户端摘要。
3. 验证 Stable Web 凭据、谱面 MD5、客户端字段格式、命中统计、普通 FC/Combo、可证明对象数、Accuracy、Grade、Mods，并按 bancho.py 公式常量时间验证 Online Checksum 和完整事实请求摘要。
4. 在数据库事务外将 Replay 以 SHA-256 内容寻址写入对象存储。
5. 在一个事务中写 PlayAttempt、Score、HitStatistic、Replay、Attestation、可确定的 Achievement Unlock、每个活动 Formula Release 的计算 Job、命令回执及对应 Outbox Event。
6. Calculation Worker 按 Formula 的 Calculator Code 和 S3 签名 URL 调用 C#/Rust HTTP 引擎，成功后写 `BeatmapDifficultyAttribute`、`ScorePerformance` 与 `score.performance-calculated.v1`；Worker 不读取或转发 Beatmap 字节。
7. Ranking Consumer 对全部活动 Policy 更新符合资格的 Overall/Exact-Mods 最佳成绩投影；Stable Query 只读取每个 Scoreboard 的默认 Policy。
8. 已同步谱面的 Multiplayer Round 会预先签发 Attempt，Stable 提交按 Account 与 Beatmap Revision 解析并在同一成绩事务中消费。

当前排行榜支持 Top、Exact Mods、Friends 和 Country 过滤；Exact Mods 会合并所有可表示为同一 Stable Bitmask 的 Canonical ModSet。Vanilla 的 Ranked/Approved/Qualified/Loved 使用 Total Score；RX/AP 只接受指定 Release 的 PP，缺失时保持 `performance_pending` 而不回退。Bancho Stats 由 `scoring-stats-projector.v1` 和 `ranking-projector.v1` 分别写入 `UserPlayStat` 与 `UserRankedStat`，查询动态计算当前 Default Policy 的 Global Rank。成绩上传只提交 Score 事实和 Outbox，不同步写 Redis Presence/Mailbox；后续 `REQUEST_STATUS_UPDATE` 读取最新 Stats，没有结果时不能把零值解释为最终计算结果。

Formula 是对外可选 PP 系统，唯一绑定一个 Calculator Code，但可通过关联表覆盖多个 Scoreboard；Calculator 可以承载多个 Formula。Release 按 `(formula_id, ruleset)` 单活且版本不可变，Performance Release 固定依赖一个 Difficulty Release。同一 Score 可在 `score_performances` 保存任意多个 Formula/历史 Release 结果，`PerformanceQueryService` 返回全部结果，Ranking Policy 始终引用精确 Release。

字段级 Multipart 请求、响应、错误分类、安全边界和 Release 发布步骤见 [Multi-PP Formula 与外部 Calculator 对接规范](performance-calculation.md)。

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

Session 登录 deadline 由 Redis Lua 在 Redis `TIME` 基准下限制为配置的 Session TTL，并以 durable expiry 为上限；因此应用与 Redis 存在小幅时钟偏差时会提前到 Redis TTL 到期，不会因边界计算失败而返回 `INVALID_EXPIRY`。Stable 登录随后使用脚本返回的实际 Session expiry 写入 Presence。

## 7. 配置与安全默认值

配置只由 `Settings` 从环境变量读取；未被运行时读取的 `system.server_settings` 已删除，避免数据库和环境形成配置双中心。本地开发使用 `.env.example` 提供的固定密钥和依赖凭据；部署时必须通过 `.env.production.example` 覆盖全部 `replace-with-*` 密钥，`.env.example` 的固定值不得用于部署：

- `PASSWORD_PEPPER`、`TOKEN_HMAC_KEY`
- `DEVICE_HMAC_KEY`、`MATCH_PASSWORD_HMAC_KEY`
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

部署与本地开发共用根目录 `compose.yaml` 单一拓扑，通过不同环境文件区分：`.env.example` 提供可工作的本地默认值，`.env.production.example` 是部署模板并强制替换全部 Secret。

## 8. 验证基线

当前共享工作树完整验证：

- 无外部服务环境：`430 passed, 23 skipped, 11 xfailed`
- 启用本地真实 PostgreSQL 与 Redis：`288 passed`
- Consumer 与生产者定向测试：`33 passed, 3 skipped`
- Consumer 真实 PostgreSQL 全链路：`2 passed`
- `uv run ruff format .` 通过
- `uv run ruff check --fix .` 通过
- Python `compileall` 通过
- `git diff --check` 通过

默认跳过用例依赖真实 PostgreSQL、Redis 或对象存储。本次未配置外部服务，因此 PostgreSQL/Redis 集成用例保持显式跳过；`288 passed` 是上一轮启用本地 PostgreSQL 与 Redis 的历史基线，不代表本次已复跑。

## 9. 当前已知限制

- 运行日志已统一为生产 JSON/开发文本事件格式；未预期失败事件通过 Loguru 输出原始异常消息和完整 traceback（不输出局部变量），预期 Stable 会话重连事件只保留错误类型与错误码，Taskiq Worker 执行日志不输出 task id 并带具体 relay task name，Outbox Delivery 日志带事件类型和完整 payload，API 请求、Worker Relay、Outbox、Performance、Stable 和迁移阶段均有生命周期事件，热路径使用采样和限频。

- Multiplayer 已完成 Stable 创建、加入、准备、开始、房内 `#multiplayer` 消息、帧扇出和完成；虚拟房间消息按当前权威 RoomState 临时投递，不查询同名普通公开频道。未知或未同步谱面可联机但不会创建排名 Attempt。当前使用 Event Command ID 和自然幂等，尚未实现面向未来 Lazer 写命令的可重放 Command Receipt。
- Multiplayer 完成事件与多人 Score Accepted 事件会幂等重建结果；Round 完成后宽限期内迟到的已验证成绩可修正 Result/Standing/Summary。Abort 不生成 Round Result。
- Authorization/Moderation 管理命令和 Audit 已实现并有独立组合根，但没有对外管理 API；在认证、授权和错误映射完成前不能视为生产可调用入口。
- `ContentSyncService` 已有 Stable 排行榜按需补全与响应后到期刷新，但仍没有主动 Scheduler、Worker 或管理命令入口。
- Activity、Notification、Beatmapset Sync 和 Channel Read 投影已由 Outbox Consumer 生成，但尚无 Lazer Query API、分页读取服务或全量 Rebuild。
- `NotificationDispatch` 当前只生成邮件/Push 待投递意图，尚无外部 Provider Sender；Stable Redis Mailbox 继续由同步适配器负责。
- `rosu-pp-py` 4.0.2 在 Python 3.14t 环境无可用 Wheel，源码构建持续超时；当前不能输出真实 PP。
- Lazer OAuth/API 适配器尚未实现，当前生产组合根仍以 Stable 请求为主。
- User Stats 已由规范拆分投影覆盖 Play Count、Total Score、Accuracy、Ranked Score、默认 PP、Grade Count 和失败进度；Worker 每日生成 Global/Country `RankSnapshot`，仍缺少全量重建和统计对账工具。
- 没有完整的 Ranking Rebuild、Eligibility 反转、统计对账和 Dead Letter 运维工具。
- 反作弊只有证据模型和扩展点，没有任何检测器，这是刻意边界而非遗漏。
- Achievement evaluator 使用显式 `(code, version)` 注册表；当前内置仅有 `score_total_at_least@1`。迁移得到的 `legacy_bancho_condition` 不执行字符串条件，因此在增加经过审核的结构化转换器前保持不可执行，不能据此伪造解锁。
- Stable Attestation 尚不能把 Client Hash/设备分量与登录设备事实权威关联，也没有 Storyboard 内容摘要和完整 Replay Frame 验证；这些字段保持 `pending`，当前只权威验证 Online Checksum、谱面 Hash 与结构边界。

## 10. osu.py 真实客户端 E2E

`tools/fakeclient` 固定使用 `osu.py` 1.5.4 提交 `31a51dc323ae151fe711bb0cb22bd266abdaa500`，通过真实 HTTP
驱动普通 Stable 客户端。独立 Compose 拓扑启动 PostgreSQL、Redis 和 MinIO，Uvicorn 与 Taskiq 作为真实子进程
运行。场景覆盖登录/Poll、Presence、Social/Chat、Spectator、非 Tourney Multiplayer、Direct/Web、评论、成绩、
Ranking Worker 和 Replay。公开头像、封面、试听、季节背景、菜单内容及 `.osz` 使用配置化上游转发，不进入 MinIO。
