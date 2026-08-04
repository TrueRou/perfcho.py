# 实现进度

最后更新：2026-08-04。

状态定义见[文档索引](README.md)：已接线、已实现未接线、仅设计必须严格区分。完整事实见[当前实现总览](current-implementation.md)，Stable 路由与 Packet 见[支持矩阵](stable-adapter.md)，后续约束见[剩余设计](remaining-design.md)。

## 已完成并接线

- 应用内核：协议无关 Actor/Client/Command、显式 Unit of Work、命令回执和 Transactional Outbox。
- Outbox Consumer：Account/Identity/Content/Social/Achievement/Community/Ranking/Multiplayer Results/Management 通过显式 Consumer Catalog 组合，幂等写投影与 Projection Checkpoint；Dead Delivery 保持分区阻塞。
- Account/Identity/Authorization：Stable MD5 预验证、Argon2id+Pepper、Stable 单会话、Web 凭据证明、权限位查询，以及带 Receipt/Audit/Outbox 的授权管理 Service。
- Content：ID/MD5/文件名查询、Direct 搜索、收藏、评分、不可变 Revision、S3 `.osu`、官方 checksum/filename lookup、Stable 首次缺图同步与响应后到期刷新。
- Social/Community：关注、屏蔽、频道、公开消息、私信与离线消息规范服务及 SQLAlchemy Repository；成绩接收通过同事务 Achievement Awarder 幂等写解锁和 Outbox。
- Stable Bancho：登录、Bootstrap、Poll、Presence、频道、公开消息、好友和 Mailbox。
- Bot 命令：协议无关注册表、参数 DSL、帮助和 Stable 公开/私信接入；基础命令由 Bot 提供，`mp`/`pool` 与 `clan` 分别由 Multiplayer、Social 模块提供并在组合根注册。
- Stable Web：账户注册、好友、Beatmap Info、Direct、收藏、评分、更新探针、下载重定向和本地谱面文件。
- Scoring：Canonical Score、Legacy Mod 规范化、命中/Accuracy/Grade 校验、命令与 Attempt 幂等、Score/Hit/Replay/Attestation 原子写入、账户/多人提交校验和中性后续任务调度端口。
- Performance：独立业务模块与适配器，包含 Formula/Scoreboard、Bootstrap 默认 `official`/`official-difficulty` 目录、不可变 Difficulty/Performance Release、每 Score/Release 持久 Job、独立 Job Relay、单 Token Attempt、Lease/Fencing、版本化 HTTP Calculator、Input/Output Digest、多结果 Query 与完成事件。
- 数据库 Bootstrap：仅初始化启动时为空的目录表，保留已有表中的应用数据，并保持并发启动锁与幂等行为；运行时配置只使用环境 `Settings`，不保留未被读取的数据库配置副本。
- Stable Scoring：Rijndael modular submission、Replay staging/download、仅包含本次新解锁且重放为空的提交 Chart 和 `osu-osz2-getscores.php`。
- Ranking：成绩接受与 Performance 完成 Consumer、活动 Policy/Mod Policy Eligibility、Overall/Exact-Mods 最佳成绩投影及 Top/Mods/Friends/Country 查询。
- Statistics：`scoring-stats-projector.v1` 维护用户游玩、月度、用户/谱面活动和失败进度；Ranking Projector 维护 `UserRankedStat`，Worker 每日生成 `RankSnapshot`。
- Spectator：关系 Fence、Join/Leave/Fellow 通知、有界 Frame 历史和 Mailbox 扇出。
- Stable Social/Presence：私信、离线通知、Away Reply、私信策略、Presence All/Filter、主动 Logout 和下线广播。
- Realtime TTL：登录 Session expiry 由 Redis 原子脚本按 Redis 服务端时间截断，容忍应用与 Redis 的小幅时钟偏差；Stable Presence 使用脚本返回的实际 deadline。
- Multiplayer：Canonical Room/Session/Slot/Round/Attempt、PostgreSQL 权威事实、Redis CAS Projection、Stable Lobby/Create/Join/Part/Settings/Ready/Start/Frame/Complete/Invite 全流程；Round Score Frame 覆盖为权威 Slot ID，发送者在当前 Poll 直接回显且其他参与者通过 Mailbox 接收；完成调用者在同一 Poll 收到 Complete 与重置状态；完成事件和迟到成绩幂等生成 Round Result、Standing 与房间/谱单汇总。
- Management：Moderation 建案/条目/处罚施加/延期/撤销、Authorization Grant/Revoke、Audit、Receipt 与 Outbox 已由 `infra/wiring/management.py` 独立组合根装配；共享基础设施 wiring 位于 `infra/wiring/common.py`。
- Stable 房间进入时同步发送 `CHANNEL_KICK #lobby` 与 `CHANNEL_JOIN_SUCCESS #multiplayer`；未加入频道发送公开消息返回稳定通知，不会穿透为 HTTP 500。
- Stable Stats：按客户端当前 Ruleset/Variant 从 `UserPlayStat` 与默认 Policy 的 `UserRankedStat` 读取 Play Count、Total/Ranked Score、Accuracy、Global Rank 和 Performance；Level 由客户端根据 Total Score 推导，不跨模式聚合。
- Stable 成绩提交后的 Stats：成绩上传只提交 Score 与 Outbox；Vanilla 谱面榜保持 Total Score 指标，个人统计按 Policy 固定的 Performance Release 从每张谱面最高合资格 PP 计算加权总 PP 和 PP Global Rank。Performance 完成即使未替换总分 PB 也会刷新投影，后续 Stable 状态请求再更新 Redis Presence。
- bancho.py v5.2.2 离线迁移：Preflight/Apply/Verify、严格身份归并、批次 Checkpoint、`.osu`/`.osr` S3 迁移、Legacy Formula Provenance 和 bcrypt(MD5) 首次登录升级；运维步骤见[迁移文档](bancho-migration.md)。
- 统一应用环境：开发使用 `.env.example` + `compose.yaml` 启动 PostgreSQL/Redis/MinIO 并由宿主机运行 API/Worker；生产使用 `.env.production.example` + `compose.prod.yaml` 运行 PostgreSQL/认证 Redis/MinIO/perfcho-pp/API/Worker。

## 已实现但未完成生产接线

- Content 主动 Scheduler、Worker 与管理同步入口尚未实现；当前生产入口是 Stable Get Scores 的按需补全与响应后到期刷新。
- Social Block/Unblock 与 Achievement 事实已经存在；成绩触发解锁已进入 Stable Chart，但 Achievement 列表和非成绩触发器没有 Stable/Lazer 入口。
- Activity/Notification、Beatmapset Sync Projection 和 Channel Read Projection 已生产接线；对应 Lazer Query/API、全量重建和外部 Notification Sender 尚未实现。
- Authorization/Moderation/Audit 已实现服务、SQL Adapter、Consumer 和独立组合根，但尚无认证后的管理 API，因此仍属于“已实现未接线”。

## 验证状态

- 日志基础设施、请求关联、API/Worker 生命周期、Outbox/Performance 状态、Stable 热路径和迁移阶段事件已接线；Taskiq Worker 日志标记具体 relay task name 且不输出 task id，Outbox Delivery 记录事件类型和完整 payload；生产 JSON 使用字段白名单，异常与协议载荷不写入日志。

- Consumer 与生产者定向测试：`33 passed, 3 skipped`；Consumer 真实 PostgreSQL 全链路：`2 passed`。
- 当前全量无外部依赖测试：`454 passed, 25 skipped`；本次未配置真实 PostgreSQL/Redis，相关集成用例显式跳过；上一轮外部依赖基线为 `288 passed`。
- Ruff format/check 和 Python compileall 通过。
- 迁移工具单元与 PostgreSQL 全领域夹具：`11 passed`，覆盖账户、社交、私信、内容、成绩、Replay、Ranking、成就和赛事图池。
- PostgreSQL、Redis 和 MinIO 的真实集成用例由 `TEST_DATABASE_URL`、`TEST_REDIS_URL` 和对应对象存储环境启用；未配置时显式跳过。

## 剩余交付

- 部署真实 osu!lazer C# 与自研 Rust Calculator，替换 Bootstrap 默认身份为已登记的不可变 Formula/Release，补金样本、批量 Backfill、Release 切换和监控；中心 Worker/持久 Job 已实现。
- Content Sync 生产任务、对象存储对账与孤儿对象 GC。
- Lazer OAuth/API 适配器；对外协议仍复用 Lazer-first Canonical 命令面。
- Ranking 全量重建、Eligibility 反转与统计对账工具。
- Activity/Notification 与 Channel/Content Projection 的 Query API、全量重建和外部邮件/Push Sender。
- Multiplayer 并发、恢复、数据库分配的 Public ID Epoch Fence 和同图 Rematch Attempt 选择已在真实 PostgreSQL/Redis 环境验证；Tourney 和 Matchmaking 不在当前范围。
- Management API、审计查询/保留、对账与运维工具；反作弊只保留 Port，不实现检测器。

## 最近实现基线

- 大批 Content、Scoring、Ranking 与 Stable Web 代码已进入提交 `ceea237 feat: finish scoring and context domain`。
- Spectator、Multiplayer、Stable 剩余协议和本文档更新当前位于该提交之后的工作树，提交前不得丢失或回退其他协作者改动。
