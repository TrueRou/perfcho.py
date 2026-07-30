# 数据库架构

## 设计目的

PostgreSQL 是 Stable 与 Lazer 协议适配器共用的持久事实存储。客户端特有的数据包或 HTTP 数据结构不能泄漏到规范领域模型中。Redis 保存带 TTL 的在线与实时状态，并通过 Redis Stream 承载 Taskiq 消息；Worker 本地内存只能保存单次任务所需数据。

系统只有一个可信中心应用。API、Outbox Relay 和 Taskiq Worker 是同一代码库的进程角色，均直接连接 PostgreSQL 与 Redis，不注册服务节点，也不存在边缘状态导入。

初始结构包含 12 个 PostgreSQL Schema、129 张表。

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

以下数据属于不可变事实或仅追加历史：谱面修订、游玩尝试、成绩、命中统计、回放清单、处罚事件、多人事件、认证尝试、匹配评分变化、Outbox Event 和审计事件。不能为了简化当前视图而覆盖这些记录。

以下数据属于可重建投影：

- 特定计算版本下的谱面难度属性与 Score Performance；
- 排名策略下的成绩有效性与排行榜最佳成绩；
- 用户、谱面、月份、失败进度、谱单、房间和每日挑战统计；
- 排名快照、用户动态、通知及其投递状态。

每个异步投影器通过 `events.projection_checkpoints` 记录处理水位。`events.outbox_deliveries` 按版本化消费者记录投递、租约、重试和完成状态。投影写入、Delivery 完成和水位推进必须处于同一事务。处罚和成绩有效性判断不能只依赖可重建投影。

## 状态归属

| 数据 | 存储 | 规则 |
| --- | --- | --- |
| 账户、Token 生命周期、谱面、成绩、房间配置、回合结果、匹配评分 | PostgreSQL | 权威且可审计，必须通过事务更新。 |
| Outbox Event、消费者投递状态、任务恢复游标 | PostgreSQL | Redis 丢失后用于重新投递或继续执行。 |
| 在线连接、当前房间成员、Ready/Loading/Skip、实时 Score/Spectator Frame、Typing、限流 | Redis DB 0 | 必须带 TTL；丢失后由客户端重连或 PostgreSQL 事实重建。 |
| Taskiq Stream、Consumer Group 与 Pending Entry | Redis DB 1 | 传输层状态；启用 AOF 和 `noeviction`，但不能替代 Outbox Delivery。 |
| 当前请求或任务的临时对象 | 进程内存 | 不跨请求共享，不作为恢复来源。 |

## 关键不变量

- 每个账户只能有一个当前规范化名称和一个当前主邮箱，历史名称和邮箱永久保留。
- 密码验证器、OAuth Secret、Token、Challenge Code 和恢复码都不能保存明文。
- 谱面修订不可变，每张谱面最多一个当前修订；上游更新后旧成绩仍引用原修订。
- 一次协议提交对应一个 `scoring.play_attempts`，并且最多产生一个 `scoring.scores`。
- 成绩与尝试的账户、谱面修订、Scoreboard 和 Mod Set 由复合外键保持一致。
- Accuracy 使用 `[0, 1]` 比例，Stable 百分比在协议边界转换。
- PP 和星数必须标记生成它们的 Calculation Release。
- Formula 唯一绑定 Calculator Code 并声明适用 Scoreboard；Release 按 Formula 和 Ruleset 单活，Performance Release 必须固定 Difficulty Release。
- 每个 `(score_id, release_id)` 最多一个 Calculation Job 与一个 Score Performance；不同 Formula 和历史 Release 的值允许并存。
- 排行榜记录按策略、谱面、范围、Mod Filter 和账户唯一；动态排名不写回成绩事实。
- 一个有序账户对只能对应一个私信频道；已读状态使用每账户频道游标。
- 关注和屏蔽不能指向自己；创建屏蔽关系的业务事务需要删除冲突的关注关系。
- 多人回合冻结具体谱单或图池修订，后续编辑不能改变历史结果。
- 多人结果只能绑定由中心成绩模块接受并验证的成绩。
- 处罚变更与敏感管理操作必须产生仅追加事件和审计记录。

## 删除与保留

账户通过停用或匿名化处理，不进行物理删除。成绩、消息、处罚、安全历史和审计数据使用限制型外键；Token Scope、通知接收者等纯附属记录可以级联删除。

数据保留必须由显式、可审计的后台任务执行。在引入分区前，应先确认认证尝试、回放查看、多人事件、通知和审计日志的实际增长量，并建立自动创建分区的运维任务。初始基线不创建没有运维保障的分区。

## 中心运行边界

所有协议请求在中心 API 或实时网关完成认证和规范化，随后直接调用应用服务。所有状态由中心进程产生，不接受外部状态导入。并发控制使用 PostgreSQL 唯一约束、行锁和聚合版本；协议重试幂等键保存在对应领域表中。

业务事务只写 PostgreSQL 事实、Outbox Event 和 Delivery。提交后由独立 Relay 投递 Taskiq；Worker 通过 Delivery 主键和领域约束处理至少一次消息。PostgreSQL 与 Redis 都是中心应用的启动依赖；Relay 在 Redis 恢复后根据未完成 Delivery 继续投递。

Calculation Job 同样由 PostgreSQL 持久状态恢复，但与 Outbox Delivery 分表管理，因为外部 Calculator I/O 不能发生在 Outbox Consumer 事务中。Calculator 不连接中心 PostgreSQL/S3，也不领取任务；它只接受带 Release 指纹、Input Digest 和 `.osu` 内容的纯计算请求，中心 Worker 验证响应并拥有最终写权限。

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
