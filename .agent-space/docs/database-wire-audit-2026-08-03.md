# 数据库表 Wire 审计（2026-08-03）

## 范围

本次使用多个 Subagent 分别从 ORM Model、Stable Router/Dispatcher、Application Service、SQL Adapter、Outbox Consumer 和测试反向核对全部表。统计投影由并行任务负责；具体反作弊 Detector/Run/Finding 按项目边界排除。

当前 Metadata 为 12 个 Schema、137 张表：`core 8`、`iam 18`、`authz 7`、`content 15`、`scoring 26`、`community 11`、`social 8`、`multiplayer 29`、`moderation 8`、`events 4`、`audit 1`、`system 2`。`tests/test_stable_database_contracts.py` 精确锁定该分布，新增表必须同步更新本审计。

## Stable 已接线

- Account/Identity/Authz 读侧：账户、名称、邮件、资料、偏好、密码、设备、Session、Stable Token、Auth Attempt、Role/Permission/Entitlement 查询。
- Content：Source、Beatmapset、Beatmap、不可变 Revision、同步水位、收藏、评分、评论、媒体资产。
- Social/Community：Follow、Block Policy、Channel、Direct Conversation、Membership、Message、Read Cursor。
- Scoring：Scoreboard、Mod、Attempt、Score、Hit、Replay、Performance Job、Ranking 和对应 Outbox；统计表状态见统计专项文档。
- Multiplayer：Room、Session、Participant/Presence、Playlist、Round、Attempt、Event，以及 Result/Standing/房间与谱单汇总 Projector。
- Events/System：Outbox Event/Delivery、Projection Checkpoint、Command Receipt、Maintenance State。

## 本次补齐

1. 成绩事务通过 transaction-aware Achievement Awarder 评估显式注册的版本化规则，幂等写 `social.achievement_unlocks`；Stable `achievements-new` 只返回本次新解锁，重放为空。
2. `multiplayer-results-projector.v1` 由 Round Complete 和多人 Score Accepted 共同驱动，支持迟到成绩，写 `round_results`、`session_standings`、`playlist_item_user_summaries`、`room_user_summaries`。
3. Authorization/Moderation 写侧、Command Receipt、Audit Writer 和 Management Consumer 已实现；独立 Management 组合根已装配，但外部管理协议仍未接线。
4. 删除未被运行时读取的 `system.server_settings` 及 Bootstrap Seed，环境 `Settings` 成为唯一配置源。

## 排除与后续

- OAuth/MFA、Tag、Badge、Team 管理、Tourney、Matchmaking、Daily Challenge 等属于 Lazer/未来能力，不作为 Stable 缺陷。
- Anticheat Detector、Run、Finding 和 Case Finding 按当前边界排除；Score Attestation 继续由 Stable 成绩事务写入。
- 下一步是 Management API、Content 主动同步、Notification Sender、投影 Rebuild/对账和对象存储 GC；不能因表或 Service 已存在就标记为生产已接线。
