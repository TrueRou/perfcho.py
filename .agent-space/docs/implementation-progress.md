# 实现进度

最后更新：2026-07-29。

状态定义见[文档索引](README.md)：已接线、已实现未接线、仅设计必须严格区分。完整事实见[当前实现总览](current-implementation.md)，Stable 路由与 Packet 见[支持矩阵](stable-adapter.md)，后续约束见[剩余设计](remaining-design.md)。

## 已完成并接线

- 应用内核：协议无关 Actor/Client/Command、显式 Unit of Work、命令回执和 Transactional Outbox。
- Account/Identity/Authorization：Stable MD5 预验证、Argon2id+Pepper、Stable 单会话、Web 凭据证明和权限位投影。
- Content：ID/MD5/文件名查询、Direct 搜索、收藏、评分、不可变 Revision 读取、S3 对象存储和 `.osu` 流式读取。
- Social/Community：关注、屏蔽、频道、公开消息、私信与离线消息规范服务及 SQLAlchemy Repository。
- Stable Bancho：登录、Bootstrap、Poll、Presence、频道、公开消息、好友和 Mailbox。
- Stable Web：好友、Beatmap Info、Direct、收藏、评分、更新探针、下载重定向和本地谱面文件。
- Scoring：Canonical Score、Legacy Mod 规范化、命中/Accuracy/Grade 校验、命令与 Attempt 幂等、Score/Hit/Replay/Attestation 原子写入和账户/多人提交校验。
- Stable Scoring：Rijndael modular submission、Replay staging/download、提交 Chart 和 `osu-osz2-getscores.php`。
- Ranking：`score.accepted.v1` Outbox Consumer、Eligibility、Overall/Exact-Mods 最佳成绩投影及 Top/Mods/Friends/Country 查询。
- Spectator：关系 Fence、Join/Leave/Fellow 通知、有界 Frame 历史和 Mailbox 扇出。
- Stable Social/Presence：私信、离线通知、Away Reply、私信策略、Presence All/Filter、主动 Logout 和下线广播。
- Multiplayer：Canonical Room/Session/Slot/Round/Attempt、PostgreSQL 权威事实、Redis CAS Projection、Stable Lobby/Create/Join/Part/Settings/Ready/Start/Frame/Complete/Invite 全流程。
- Stable Stats：Play Count、Total/Ranked Score、Accuracy、Global Rank 基础 Query 和 Country ID 映射；Performance 保持 Deferred。

## 已实现但未完成生产接线

- Account 注册服务已有事务、幂等和 Outbox，但没有公开 API，也未进入 Stable 组合根。
- `ContentSyncService`、官方 osu! API OAuth Source 和不可变 Revision 发布算法已有测试，但没有 Scheduler、Worker 或管理入口。
- Social Block/Unblock 与 Achievement 事实已经存在，但没有 Stable/Lazer 入口。
- Account、Identity、Content、Social、Achievement、Community 服务会声明 Outbox Delivery；当前只有 Ranking Consumer 实际注册，其他流程尚未闭环。

## 验证状态

- 当前完整测试：`168 passed, 8 skipped`。
- Ruff format/check 和 Python compileall 通过。
- PostgreSQL、Redis 和 MinIO 的真实集成用例由 `TEST_DATABASE_URL`、`TEST_REDIS_URL` 和对应对象存储环境启用；未配置时显式跳过。

## 剩余交付

- PP/Difficulty Worker 与 Calculation Release。`rosu-pp-py` 4.0.2 在 Python 3.14t 上无 wheel，当前源码构建超时，因此请求事务使用明确的 Deferred Calculator，不能伪造 PP。
- Content Sync 生产任务、对象存储对账与孤儿对象 GC。
- Lazer OAuth/API 适配器；对外协议仍复用 Lazer-first Canonical 命令面。
- Ranking 全量重建、Eligibility 反转、Country Rank、Grade Count 和失败进度 Projector。
- Community/Social/Content Outbox Consumer 的实际投影或外部投递，不能以空 Consumer 代替。
- Multiplayer 仍需在启用 PostgreSQL/Redis 的环境执行并发与恢复集成用例；Tourney 和 Matchmaking 不在当前范围。
- Moderation、对账与运维工具；反作弊只保留 Port，不实现检测器。

## 最近实现基线

- 大批 Content、Scoring、Ranking 与 Stable Web 代码已进入提交 `ceea237 feat: finish scoring and context domain`。
- Spectator、Multiplayer、Stable 剩余协议和本文档更新当前位于该提交之后的工作树，提交前不得丢失或回退其他协作者改动。
