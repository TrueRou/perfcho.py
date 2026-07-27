# 数据库架构

## 设计目的

数据库是 Stable 与 Lazer 协议适配器共用的权威状态存储。客户端特有的数据包或 HTTP 数据结构不能泄漏到规范领域模型中。PostgreSQL 负责持久化身份、授权、谱面、成绩、处罚、聊天和多人历史；Redis 与 Worker 本地内存只能保存可丢失的投影和在线状态。

初始结构包含 13 个 PostgreSQL Schema、133 张表。

## 领域清单

| Schema | 职责与主要表 |
| --- | --- |
| `core` | 账户主体、名称和邮箱历史、公开资料、私有偏好、媒体资源与徽章。 |
| `iam` | 密码凭据、OAuth 客户端、会话、Token、MFA、设备与登录审计。 |
| `authz` | 权限、角色、直接授权和 Supporter 等非权限权益。 |
| `moderation` | 处罚案件、案件记录、处罚事件和反作弊检测结果。 |
| `content` | 内容源、谱面集、谱面、不可变谱面修订、状态、收藏、评分、标签与评论。 |
| `scoring` | Scoreboard、Mod、游玩尝试、成绩、回放、PP、排行榜与统计投影。 |
| `social` | 关注、屏蔽、团队、团队成员和成就。 |
| `community` | 频道、私信、消息、已读状态、通知与外部投递。 |
| `multiplayer` | 房间、托管会话、谱单、回合、赛事图池、匹配与每日挑战。 |
| `service` | 服务身份、可信或边缘节点、节点公钥与幂等命令回执。 |
| `events` | Transactional Outbox、用户动态与投影水位。 |
| `audit` | 敏感管理和安全操作的不可变审计日志。 |
| `system` | 服务端结构化配置与可恢复维护任务状态。 |

每张表的具体用途直接记录在对应 SQLAlchemy Model 的英文类级 Docstring 中，Model 是表用途说明的唯一代码来源。

## 标识与类型策略

- `core.accounts.id` 使用正整数 Identity，因为 Stable 数据包使用有符号 32 位用户 ID，两端也都会公开数字用户 ID。
- 高频追加记录使用 `bigint GENERATED ALWAYS AS IDENTITY`。
- 安全工作流和领域聚合使用应用生成的 UUIDv7。
- 业务时间统一使用 `timestamptz`；客户端时间只作为验证证据，不能成为权威创建时间。
- Hash 与设备指纹使用二进制列；设备标识必须保存服务端带密钥的 HMAC，禁止保存原始硬件 ID。
- IP 地址使用 PostgreSQL `inet`，版本化证据与快照使用 `jsonb`。
- 状态值采用带 CHECK 的字符串枚举，避免大量 PostgreSQL 原生 Enum 带来的演进成本。

## 事实与投影

以下数据属于不可变事实或仅追加历史：谱面修订、游玩尝试、成绩、命中统计、回放清单、处罚事件、多人事件、认证尝试、匹配评分变化和审计事件。不能为了简化当前视图而覆盖这些记录。

以下数据属于可重建投影：

- 特定计算版本下的谱面难度属性与 Score Performance；
- 排名策略下的成绩有效性与排行榜最佳成绩；
- 用户、谱面、月份、失败进度、谱单、房间和每日挑战统计；
- 排名快照、用户动态、通知及其投递状态。

每个异步投影器通过 `events.projection_checkpoints` 记录处理水位。投影写入和水位推进必须处于同一事务。处罚和成绩有效性判断不能只依赖可重建投影。

## 关键不变量

- 每个账户只能有一个当前规范化名称和一个当前主邮箱，历史名称和邮箱永久保留。
- 密码验证器、OAuth Secret、Token、Challenge Code 和恢复码都不能保存明文。
- 谱面修订不可变，每张谱面最多一个当前修订；上游更新后旧成绩仍引用原修订。
- 一次协议提交对应一个 `scoring.play_attempts`，并且最多产生一个 `scoring.scores`。
- 成绩与尝试的账户、谱面修订、Scoreboard 和 Mod Set 由复合外键保持一致。
- Accuracy 使用 `[0, 1]` 比例，Stable 百分比在协议边界转换。
- PP 和星数必须标记生成它们的 Calculation Release。
- 排行榜记录按策略、谱面、范围、Mod Filter 和账户唯一；动态排名不写回成绩事实。
- 一个有序账户对只能对应一个私信频道；已读状态使用每账户频道游标。
- 关注和屏蔽不能指向自己；创建屏蔽关系的业务事务需要删除冲突的关注关系。
- 多人回合冻结具体谱单或图池修订，后续编辑不能改变历史结果。
- 权威多人结果只能绑定由可信成绩服务接受的成绩。
- 处罚变更与敏感管理操作必须产生仅追加事件和审计记录。

## 删除与保留

账户通过停用或匿名化处理，不进行物理删除。成绩、消息、处罚、安全历史和审计数据使用限制型外键；Token Scope、通知接收者等纯附属记录可以级联删除。

数据保留必须由显式、可审计的后台任务执行。在引入分区前，应先确认认证尝试、回放查看、多人事件、通知和审计日志的实际增长量，并建立自动创建分区的运维任务。初始基线不创建没有运维保障的分区。

## 不可信节点边界

边缘多人节点不持有 PostgreSQL 凭据。它通过短期服务凭据和有效的 Fencing Lease 向核心命令网关认证。每条命令必须包含节点身份、公钥版本、Lease Epoch、预期聚合版本、Payload Digest、签名与幂等键。

`service.node_command_receipts` 记录被接受和拒绝的命令。同一幂等键携带不同 Digest 必须被视为安全事件。节点签名只能证明“哪个节点报告了什么”，不能证明成绩真实。

玩家向可信成绩服务直接提交成绩。成绩服务验证账户、冻结的谱面修订、Scoreboard、Mod、时间与 Attestation 后，才允许把成绩绑定到多人 Attempt。

## Stable 与 Lazer 映射

| 客户端概念 | 规范数据模型 |
| --- | --- |
| Stable 数字用户与 Lazer 用户 | `core.accounts` 和当前 `core.account_names` |
| Stable 密码 MD5 输入与 Lazer 密码输入 | 一个版本化 `iam.password_credentials` 验证策略 |
| Lazer OAuth/API Key 与 Stable 会话 Token | `iam.auth_sessions`、`iam.auth_tokens` 与 Token Scope |
| Stable Mode、RX、AP 与 Lazer Ruleset | `scoring.scoreboards` 的 `ruleset + variant` |
| Stable Mod 位掩码与 Lazer 结构化 Mod | `scoring.mod_sets` 的规范 JSON 与 Legacy Bits |
| Stable 谱面 MD5 与 Lazer Beatmap ID | 逻辑 `content.beatmaps` 下的不可变 `beatmap_revisions` |
| Stable/Lazer 成绩提交 | `play_attempts`、`scores`、动态命中统计与 Attestation |
| Stable Mail 与 Lazer 私信 | Direct Conversation Channel 与 Message |
| Stable Match 与 Lazer Room/Playlist | Room、Session、Playlist Revision、Round 与 Attempt |
| Stable Restriction/Silence 与 Lazer Account History | Moderation Case、Sanction 与 Sanction Event |
