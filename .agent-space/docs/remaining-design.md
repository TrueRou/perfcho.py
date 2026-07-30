# 剩余设计与交付路线

最后更新：2026-07-29。本文记录尚未完成的设计约束和建议实施顺序，不代表对应功能已经可用。

## 1. 优先级

建议按以下顺序继续：

1. PP/Difficulty Worker、Calculation Release、剩余统计投影和 Ranking Rebuild；当前按交付要求允许暂缓。
2. Content Sync 生产任务、缺失 Outbox Consumer 和运维对账。
3. Lazer OAuth/API Adapter，复用已完成的 Multiplayer/Scoring 共享 Command/Query。
4. Multiplayer 并发/恢复集成验证和 Lazer Room Adapter；Tourney/Matchmaking 暂不实现。
5. Moderation 与管理工具；反作弊仍只保留 Port。

Stable Multiplayer 主流程已经接入共享 Aggregate。后续 Lazer Room API 应直接复用该 Aggregate，不能复制 Stable Match 状态机；PP 继续保持 Deferred，直到获得可复现且支持当前运行时的计算引擎。

## 2. Multiplayer 设计

本节的核心结构已实现：Canonical Service、SQLAlchemy Repository、Redis CAS Projection、Stable Dispatcher、已同步谱面的 Round/Attempt 与 Stable Score 消费均已接线。保留本节作为架构约束；尚未完成的是启用真实 PostgreSQL/Redis 后的并发故障注入、跨进程恢复验证和 Lazer Adapter。

### 2.1 Canonical 概念

Canonical 层使用 Lazer-facing 概念：

- `Room`：持久房间，拥有公开 ID、名称、容量、可见性和配置。
- `MultiplayerSession`：一次实际在线承载生命周期，拥有 Host、Team Mode、Scoring Mode 和 Aggregate Version。
- `Participant`：账户的持久准入、Ban 和参与者类型。
- `SessionPresence`：每次 Join/Leave 历史。
- `PlaylistItem` 与 `PlaylistRevision`：房间地图配置和不可变 Revision。
- `Round` 与 `RoundParticipant`：一次同步游玩的冻结配置、Slot、Team 和 Mod。
- `MultiplayerAttempt`：授权某账户为指定 Round 提交一次成绩的短期 Token。
- `MultiplayerEvent`：按 Room Sequence 排序的语义事件，用于恢复、审计和协议扇出。

Stable `Match` 只是上述概念的 Wire Projection：Stable Match ID 映射 `Room.public_id`；16 个 Slot 是 Stable Adapter 的容量限制，不应成为 Canonical Room 的全局限制。

### 2.2 PostgreSQL 与 Redis 边界

PostgreSQL 保存：

- Room、Session 和 Host 变更。
- 参与者准入、Join/Leave 历史和 Ban。
- Playlist Revision、Round 冻结配置和 Round 生命周期。
- Multiplayer Attempt、与 Score 的绑定、结果和有序 Multiplayer Event。
- 有序 Multiplayer Event；跨协议可重放 Command Receipt 与 Outbox Delivery 仍是后续增强项。

Redis 保存：

- 当前 Session 的 Stable 16 Slot Projection。
- Slot Status、Ready、Loaded、Skipped、当前 Team/Mods 等高频在线状态。
- Match Revision、Lease Owner、Fencing Token 和 TTL。
- Lobby Snapshot、Match Mailbox 或按 Session 分区的即时广播状态。

Redis Match State 必须能够从 PostgreSQL 的当前 Session、Presence 和最新 Event 重建。Round Start、Participant Join/Leave、Host Transfer、Attempt Issue 和 Round Complete 等持久边界不能只写 Redis。

### 2.3 并发控制

- Host/Room 写命令带 `expected_version`；SQL 行锁与 Aggregate Version 共同拒绝旧命令。
- Redis 更新使用 CAS/Lua，比较 Session ID、Aggregate Version 和 Fence。
- PostgreSQL 提交成功后才发布新的 Redis Projection；如果 Redis 更新失败，通过 Event/Outbox 重建。
- 同一账户同时只能占用一个有效 Slot；同一 Slot 同时只能有一个账户。
- 旧 Host、旧 Session 或延迟 Poll 的命令必须因 Version/Fence 过期而失败。
- Create/Join/Part 具有自然幂等和有序 Event；Round/Attempt 依赖唯一约束与状态机防止重复事实。Stable 协议没有可靠客户端 Command ID，因此不能承诺跨任意时间的同 Payload 去重。

### 2.4 首批 Command 和 Query

建议先实现以下协议无关接口：

```text
CreateRoom
JoinRoom
LeaveRoom
UpdateRoomSettings
TransferHost
MoveParticipant
SetParticipantTeam
SetParticipantMods
SetParticipantReady
LockSlot
StartRound
MarkParticipantLoaded
SubmitRoundFrame
MarkParticipantComplete
AbortRound
CloseSession

ListPublicRooms
GetRoomSnapshot
GetCurrentRound
```

`SubmitRoundFrame` 的高频 Frame 不进入 PostgreSQL；只将必要的 Round 生命周期与最终结果持久化。Stable Score Frame 可通过 Realtime Mailbox 扇出，不能把每帧写成 Outbox Event。

### 2.5 Round Start

`StartRound` 必须在一个事务内：

1. 验证 Actor 是当前 Host 或拥有权威管理权限。
2. 验证 Session Active、没有进行中 Round、地图 Revision 有效、参与者状态满足开始条件。
3. 冻结 Playlist Revision、Scoring Mode、Required Mods、Slot、Team 和个人 Mods。
4. 创建 Round、RoundParticipant 和每位玩家的 MultiplayerAttempt。
5. 写入有序 Multiplayer Event；未来 Lazer Command 还应加入可重放 Command Receipt。
6. 提交后更新 Redis Round Projection 并向参与者广播 Start。

Lazer Attempt Token 只应返回给对应账户，数据库只保存 Digest。Stable 协议没有 Attempt Token 字段，因此 Adapter 按已认证 Account 与 Beatmap Revision 解析最近有效 Attempt，再由 `SqlAlchemyMultiplayerSubmissionValidator` 在成绩事务中锁定、校验并消费；不能仅从 Match Packet 猜测合法性。

### 2.6 Stable Packet 接线状态

1. Lobby、生命周期、设置、准备、Round、Invite 和断线 Host 迁移均已接线。
2. Redis Projection 缺失时从 PostgreSQL Session Presence 重建；Version 不一致时进行 CAS 修复。
3. Tourney Packet 和 Matchmaking 明确不在当前交付范围。

每一阶段都应先调用 Canonical Service，再由 Stable Builder 生成 Packet。禁止让 Dispatcher 直接写 SQLAlchemy Model 或把 Redis Hash 当作 Aggregate。

### 2.7 Multiplayer 测试门槛

- Aggregate 规则与权限单元测试。
- Repository 的 PostgreSQL 约束、乐观锁和命令幂等集成测试。
- Redis CAS、Fence、TTL、Slot 唯一性和重建测试。
- Stable Packet Parse/Build/Dispatcher 合同测试。
- 两个并发 Join 同一 Slot、旧 Host 更新、重复 Start/Complete、Round 中断恢复测试。
- Multiplayer Attempt 与 Stable Score Submission 的真实 PostgreSQL 端到端消费测试。

## 3. PP、Difficulty 与 Ranking

### 3.1 当前阻塞

`rosu-pp-py==4.0.2` 在当前 Python 3.14t 平台没有 Wheel，源码构建连续超时。中心应用已经不依赖 Python 绑定，支持按 Formula 调用独立 C#/Rust HTTP Calculator；当前阻塞改为发布真实 Calculator 制品、登记不可变 Formula/Release 并完成跨引擎金样本验证。

### 3.2 已实现基础

- `CalculationFormula` 表达可选择的 PP/Difficulty 系统，Calculator Code 映射部署时 HTTP URL；一个 Calculator 可承载多个 Formula。
- `CalculationRelease` 绑定 Formula、Ruleset、版本、Artifact/Configuration Digest；Performance Release 固定依赖 Difficulty Release。
- 成绩事务创建每个活动 Formula Release 的持久 Job；Worker 使用 Lease/Fencing、输入/输出摘要和有界重试。
- `ScorePerformance` 按 `(score_id, release_id)` 保留多 Formula 及历史 Release 结果，不覆盖旧值。
- Worker 原子写 Difficulty、PP 和完成 Outbox Event，再触发全部活动 Ranking Policy；Stable 只读取 Default Policy。

### 3.3 尚缺发布与运维

- 实现并部署 osu!lazer C# 与自研 Rust Calculator 的 `/v1/performance/calculate` 合同。
- 增加 Formula/Release 管理 Command，使用真实 OCI/Binary SHA-256 发布和切换 Release；禁止 `latest`。
- 建立官方金样本、C#/Rust 差异阈值、超时率、Pending Age、Dead Job 和非确定输出告警。
- Release 切换需要批量 Backfill 新 Job、覆盖率校验、Ranking Projection Generation 重建后再切 Policy。

### 3.4 Ranking 补全

- 全量 Rebuild：从权威 Score 和 Eligibility 重建 LeaderboardEntry。
- Eligibility 反转：封禁、谱面状态、Policy 或 Attestation 变化时删除/替换最佳成绩。
- User Stats：Play Count、Total Score、Ranked Score、Accuracy 和基础 Global Rank 已有只读聚合；仍需 PP、Country Rank、Grade Count 和可重建投影。
- 失败进度：Beatmap Fail Histogram 和 Activity Projection。
- 对账：发现 Score 与 Ranking Projection 偏差并可重放指定 Partition。

Rebuild 必须写入新 Projection Generation，完成后原子切换，不能在生产表上长时间原地清空重建。

## 4. Content Sync 与对象存储运维

### 4.1 接线方案

- 提供管理员 Command 或 Taskiq Task 调用 `ContentSyncService.synchronize()`。
- 增加按 Beatmapset 分区的幂等 Key，避免并发同步同一 Set。
- OAuth Token 在 Source 内缓存到过期前安全窗口，不写数据库明文。
- 周期同步只负责发现待同步集合；每个 Set 使用独立任务和短数据库事务。
- 实现 `content-projection.v1` 后再让生产任务持续产生该 Delivery。

### 4.2 对账与 GC

- 扫描数据库引用对象不存在的情况并告警/重取。
- 扫描超过保留期且无数据库引用的内容寻址对象并删除。
- Replay 先写对象后写数据库，失败会产生孤儿对象，必须纳入同一 GC。
- 删除只能以数据库引用快照和安全保留窗口为依据，不能只按对象创建时间。

## 5. Outbox Consumer 后续

Account、Identity、Content、Social、Achievement、Community 和 Ranking Consumer 已实现并由 Taskiq Worker 显式注册。当前副作用限定在 PostgreSQL：Activity、Notification/Recipient/Dispatch Intent、Beatmapset Sync Projection、Channel Read Projection、Eligibility 和 Leaderboard；Worker 不重复写 Stable Redis Mailbox。

后续仍需：

- 为 Activity、Notification、Channel Read 和 Beatmapset Sync Projection 增加 Canonical Query、Lazer API 与游标分页。
- 实现邮件/Push `NotificationDispatch` Sender，使用独立租约、幂等 Provider Key 与有界重试；外部 I/O 不能放进 Outbox Consumer 数据库事务。
- 为所有可重建投影提供 Generation 化全量 Rebuild、指定 Partition Replay 和权威事实对账。
- 设计事件 Retention 前先处理 Projection 对 `events.outbox_events` 的来源引用。

如果后续事件没有必要的异步副作用，应删除无效 Delivery 设计，而不是注册只推进 Checkpoint 的空 Consumer。

## 6. Lazer Adapter

### 6.1 原则

- Lazer API 使用版本化 Pydantic Request/Response，不返回 ORM Entity。
- OAuth、Scope 和 Token Family 使用已有 IAM 模型，应用服务不抛 `HTTPException`。
- Lazer Room/Playlist/Score 调用与 Stable 相同的 Canonical Command/Query。
- Stable Password Token 兼容只存在 Identity Adapter，不能进入 Lazer Contract。
- Stable Legacy Mod Bit 在适配边界转换为 Canonical Mod；Lazer 直接提交结构化 Mod。

### 6.2 建议首批接口

1. OAuth Authorization/Token/Refresh/Revoke 和当前用户。
2. Beatmap/Beatmapset Query、收藏和评分。
3. Score Submission、Replay 和 Leaderboard Query。
4. Room List、Room Detail、Join/Leave、Playlist 和 Round。

Canonical Multiplayer 已可供 Stable 使用；Lazer Room API 应直接接入现有 Command/Query，避免形成第二套状态机。

## 7. Moderation、反作弊与运维

- Moderation 最先实现 Silence、Account Restriction、Score Eligibility 变更和审计 Command。
- 权威授权在应用服务中执行，FastAPI Dependency 只负责认证和输入规范化。
- 反作弊只定义 Attestation/Evidence 输入、Detector Port 和 Review 状态，不实现具体检测器。
- 不引入 Circleguard，也不在请求事务内执行高成本 Replay 分析。
- 运维至少需要 Outbox Dead Letter 重放、Projection Rebuild、Session 清理、对象存储对账和数据库 Bootstrap 检查。

## 8. 完成定义

一项功能只有同时满足以下条件才能从“未接线”改为“已接线”：

- Canonical Model、Command/Query 和应用错误完成。
- SQLAlchemy/Redis/S3 等 Adapter 满足 Port，事务边界清晰。
- 生产组合根和协议入口实际调用该能力。
- Outbox 中声明的每个 Consumer 均真实注册，或明确没有 Delivery。
- 单元、合同和必要集成测试通过。
- 对应支持矩阵、进度文档和环境变量示例同步更新。
