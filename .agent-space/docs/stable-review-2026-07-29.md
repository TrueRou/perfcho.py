# Stable 实现复核与修复记录

最后更新：2026-07-29。

本文保存 2026-07-29 对当前 Stable 实现的完整复核结果、修复状态和残余边界。业务与 Wire 行为参考仓库内 `.agent-space/stable-python` 的 bancho.py 实现；架构、事务和依赖方向仍以 perfcho 文档为准。

状态定义：

- **已修复**：生产组合根和协议入口已接线，并有回归测试。
- **已缓解**：代码内风险已降低，但仍依赖部署控制或后续运维能力。
- **设计边界**：不是本次伪装成已完成的能力，必须继续显式记录。

## 1. 登录、Poll 与协议边界

| ID | 原问题与影响 | 状态 | 修复与验证位置 |
| --- | --- | --- | --- |
| ST-AUTH-001 | Poll 只续 Session，Presence、Preference、Away、频道和 Spectator 在在线约 360 秒后失效 | 已修复 | `infra/redis/scripts.py` 原子 heartbeat；`test_redis_realtime.py` |
| ST-AUTH-002 | Stable Moderator/Owner 位错误，管理员 Presence 编码溢出并在持久 Session 创建后 500 | 已修复 | `modules/authorization/stable.py` 使用 `1/2/4/8/16`；authorization/composition 测试 |
| ST-AUTH-003 | 客户端崩溃后旧持久 Session 最多阻塞重登 12 小时 | 已修复 | `AuthSession.last_activity_at`、账户锁、120 秒 stale grace；identity PostgreSQL 测试 |
| ST-AUTH-004 | 登录身份事务提交后 Bootstrap 失败会留下客户端未获得 Token 的活动 Session | 已修复 | `api/stable/router/bancho.py` 对所有后置步骤执行精确 Token 补偿关闭；Bancho 测试 |
| ST-AUTH-005 | 登录只返回本人，不返回其他在线用户 | 已修复 | 登录读取有界 Presence Index，双向发送 Presence/Stats；Bancho Bootstrap 测试 |
| ST-AUTH-006 | Redis Session 丢失后静默重建不完整状态 | 已修复 | Poll 关闭对应持久 Session 并返回 Restart，要求客户端重新登录；Bancho 测试 |
| ST-AUTH-007 | Stable 登录后 300-800ms 伪 Logout 会立即关闭会话 | 已修复 | `ResolvedStableSession.opened_at` 和首秒 Logout 抑制；`test_stable_remainder.py` |
| ST-AUTH-008 | 并发 Poll 在业务副作用后才争用 Mailbox Lease，可能 500 并重复消息 | 已修复 | Dispatch 前获取 fenced Poll Lease；冲突返回受控空 Poll；Bancho 测试 |
| ST-AUTH-009 | Token HMAC Key 与 Match Password HMAC Key 在组合根接反 | 已修复 | `composition.py` 分离 Token、Match Password 与 Admission Token Key；composition 测试 |
| ST-AUTH-010 | Redis Mailbox 按账户跨 Session 复用，旧包会泄漏到新 Session | 已修复 | Mailbox enqueue/lease/ack/release 全部要求 `SessionFence`，relogin 清理旧 epoch；Redis 测试 |
| ST-AUTH-011 | Stable Web 只检查 PostgreSQL Session，断线后 MD5 Token 仍可用 | 已修复 | Web、Scoring、Replay 和 Ranking 同时验证匹配的 Redis realtime epoch；Web/Scoring 测试 |
| ST-AUTH-012 | 不存在账户跳过 KDF，可按响应时间枚举账户 | 已修复 | Dummy Argon2 verification，且 Web 先查活动 Session candidate；identity/security 测试 |
| ST-AUTH-013 | 无在线容量约束导致 Presence/Broadcast 固定批次遗漏高 ID 用户 | 已修复 | 登录以 `stable_presence_batch_size` 为显式在线容量，满载时补偿 Session 并拒绝登录 |
| ST-AUTH-014 | 登录频道人数固定 0、忽略 auto-join，Silence 固定 0 | 已修复 | 实际加入 auto-join 频道，查询活动人数与全局 Silence 剩余秒数；Bancho/Community 测试 |
| ST-AUTH-015 | 普通用户未收到兼容 Supporter 位，客户端隐藏已实现 Direct | 已修复 | Login Privileges 包 OR Supporter，Presence 保留真实权限 |
| ST-AUTH-016 | 无条件信任 `CF-Connecting-IP`/`X-Real-IP`，可伪造证据或用非法值触发 500 | 已修复 | `api/stable/client_ip.py` 只信任配置 CIDR，并严格解析地址；Router 测试 |
| ST-PROTO-001 | `USER_STATS_REQUEST` 重复 ID 可把 1 MiB Poll 放大为数十 GB 输出并 OOM | 已修复 | ID 去重、排除请求者、单包数量上限和累计响应预算；`test_stable_remainder.py` |
| ST-PROTO-002 | Dispatcher 没有统一响应上限 | 已修复 | `stable_max_response_bytes`，只追加完整 Packet，预算耗尽即停止 |
| ST-PROTO-003 | CHANGE_ACTION/ReplayAction 接受非法枚举并向其他客户端扇出 | 已修复 | Action `0..13`、mode `0..3`、ReplayAction `0..8`；Codec/Dispatcher 测试 |
| ST-PROTO-004 | Mania RX、非 osu! AP 等非法辅助 Mod 进入错误 Stats 维度 | 已修复 | Stable adapter 规范化并清除不适用辅助 Mod |
| ST-PROTO-005 | `LOGOUT` 与 `USER_PRESENCE_REQUEST_ALL` 错把固定 i32 payload 当可选 | 已修复 | Dispatcher 强制读取并耗尽 4 字节 payload；协议测试 |
| ST-PROTO-006 | 多类 ApplicationError 穿透成 JSON 500 | 已修复 | Dispatcher/Router 映射为 Stable 二进制通知、Join Fail 或约定文本 |

## 2. Community、Social 与 Presence

| ID | 原问题与影响 | 状态 | 修复与验证位置 |
| --- | --- | --- | --- |
| ST-COMM-001 | 未加入频道也能持久化并广播公开消息，普通 Channel Join 可绕过 `#lobby` | 已修复 | Community 权威活动成员校验；普通 join 拒绝 `#lobby`；Community/Dispatcher 测试 |
| ST-COMM-002 | 公开消息提交后单个 Mailbox Overflow 导致部分投递和 Poll 500 | 已修复 | 持久消息幂等，收件人逐个隔离 Overflow，失败不回滚已提交事实 |
| ST-COMM-003 | 公开消息没有按接收者 Block 过滤 | 已修复 | Social 批量查询 blocking recipients，避免 N+1 |
| ST-COMM-004 | Stable 重试公开/私信时每次生成随机 UUID，产生重复持久消息 | 已修复 | Session、Packet 和短重试窗口导出的确定 client message UUID；replay 不重复扇出 |
| ST-COMM-005 | Friends-only DM 被实现成 mutual follow | 已修复 | 按收件人是否单向 follow 发送者判断；Community 测试 |
| ST-COMM-006 | 只检查发送者 Silence，目标 Silence 不返回 Packet 101 | 已修复 | `TargetAccountSilenced` 应用错误映射 `TARGET_IS_SILENCED` |
| ST-COMM-007 | 登录离线私信不推进已读，前 100 条重复并阻塞后续消息 | 已修复 | Keyset 分页、conversation cursor 和 `/web/osu-markasread.php`；Web/Community 测试 |
| ST-COMM-008 | 离线消息丢失参考实现时间戳 | 已修复 | Login 格式化持久 `created_at` 后再构造消息 Packet |
| ST-COMM-009 | Friend Add 无效/Blocked 目标导致 Poll 500；自己的 Block 无法被 Add Friend 替换 | 已修复 | 领域错误稳定映射；仅解除发起者自己的 Block 后 follow，保留对方 Block |
| ST-COMM-010 | Join/Part 后其他成员频道人数长期错误 | 已修复 | Active membership query 返回真实人数，并向仍在线成员广播 Channel Info |
| ST-COMM-011 | CHANGE_ACTION 不同步 Presence mode/global rank，REQUEST_STATUS_UPDATE 不回写 Snapshot | 已修复 | Presence 与 Stats 同步生成并 fenced 保存；Dispatcher 测试 |
| ST-COMM-012 | Presence/Mailbox 扇出没有 recipient epoch | 已修复 | 所有发送路径从 Presence 取得 `SessionFence` 后入队 |

## 3. Spectator

| ID | 原问题与影响 | 状态 | 修复与验证位置 |
| --- | --- | --- | --- |
| ST-SPEC-001 | Relation/Frame 不绑定双方 Session，旧 Poll 可跨 relogin attach、detach 或 publish | 已修复 | Relation 保存双方 `SessionFence` 和 UUID relation ID；Redis/Spectator 测试 |
| ST-SPEC-002 | Relation 删除后 revision 从 1 重启，存在 ABA stale detach | 已修复 | 独立持久 revision counter、relation UUID 和精确条件 detach |
| ST-SPEC-003 | Frame History 满后拒绝新帧，实时扇出永久停止 | 已修复 | 原子滚动淘汰最旧历史并继续 live fanout；单帧超限才拒绝 |
| ST-SPEC-004 | History 只返回最旧前缀，遗漏最新帧 | 已修复 | 返回最新有界窗口和内部 cursor 元数据 |
| ST-SPEC-005 | Attach 与单独读 History 之间会重复或缺帧 | 已修复 | Attach 原子返回 Relation 与 history-to-live handoff snapshot |
| ST-SPEC-006 | 活动 Relation 不随 heartbeat 续期 | 已修复 | 双向 Relation 由双方 Session heartbeat 受 fence 续期 |
| ST-SPEC-007 | u16 Sequence 被当作永久单调整数，`65535 -> 0` 后停止 | 已修复 | Session epoch 内部 64-bit cursor，接受合法 u16 wrap |
| ST-SPEC-008 | 重复 START 同一 Host 重发全部历史和 Fellow | 已修复 | 按 Stable ready signal 语义处理，不重新附加和重放历史 |
| ST-SPEC-009 | Spectator live fanout 与 history write 分步，故障会产生洞 | 已修复 | Redis 单次原子 publish 完成滚动历史和 fenced mailbox fanout |
| ST-SPEC-010 | Stale detach 仍发送 LEFT，关系和客户端视图不一致 | 已修复 | 仅 exact detach 返回 `True` 时发送 Host/Fellow 离开通知 |

## 4. Multiplayer

| ID | 原问题与影响 | 状态 | 修复与验证位置 |
| --- | --- | --- | --- |
| ST-MP-001 | PostgreSQL 不保证账户只能有一个 active room | 已修复 | 全局 partial unique index、重复数据 repair、IntegrityError 领域映射；PG 并发测试 |
| ST-MP-002 | PostgreSQL 提交后 Redis 失败被伪装成整个命令失败 | 已修复 | `DurableRoomSnapshot` 与 `DURABLE_RECOVERY` 状态，后续按持久事实恢复 |
| ST-MP-003 | Redis CAS 只看 state revision，Public ID 复用后旧写可覆盖新房间 | 已修复 | Redis v2 CAS 携带 room/session epoch fence；Redis 测试 |
| ST-MP-004 | Redis 丢失后正在进行的 Round 恢复成未开始 | 已修复 | Snapshot 恢复 active Round、冻结参与者、Slot/Team/Mods |
| ST-MP-005 | 同一 Session 可产生多个 active Round | 已修复 | `uq_rounds_session_active`、旧数据 repair、并发错误映射 |
| ST-MP-006 | Round 中 Settings 更新清除进行中状态 | 已修复 | Canonical Service 在 active Round 拒绝 Settings 变更 |
| ST-MP-007 | Free Mod 成绩不核对 RoundParticipant 个人冻结 ModSet | 已修复 | Scoring 事务锁定并校验 participant mod set |
| ST-MP-008 | Attempt 不核对成绩时间、Round/Session 状态 | 已修复 | 校验 started/ended 窗口、active/complete/abort 状态和 2 分钟提交宽限 |
| ST-MP-009 | 异常断线不关闭 durable presence，Redis 恢复会复活幽灵成员 | 已修复 | `CleanupPresence` 按 auth session fence 清理；Logout 已接线 |
| ST-MP-010 | Host 离开时新 Host 收不到 `MATCH_TRANSFER_HOST`，最后离开不完整 dispose | 已修复 | Stable adapter 发送 Transfer/Update 或 Dispose；Multiplayer 测试 |
| ST-MP-011 | 关键 Match Packet Overflow 被静默吞掉 | 已修复 | 返回 delivery warning 和当前 Room snapshot，接收端可恢复 |
| ST-MP-012 | TeamVs 默认队伍、Free Mod 迁移、Beatmap `-1`、Slot Mod 规范化错误 | 已修复 | Stable-to-canonical 转换和 Redis slot transition 测试 |
| ST-MP-013 | 每个 MATCH_SCORE_UPDATE 查询 PostgreSQL | 已修复 | 热路径只读取 fenced Redis room projection |
| ST-MP-014 | Service 不执行 `multiplayer.play/host`、Restriction 策略 | 已修复 | 协议无关 `MultiplayerAccessPolicy` 在 Canonical Service 内执行 |
| ST-MP-015 | JOIN_MATCH 不离开 `#lobby`，收到重复和无关更新 | 已修复 | Join 成功后 fenced leave lobby |
| ST-MP-016 | 密码房邀请不包含可用凭据 | 已修复 | 绑定 recipient/room/session/expiry 的 HMAC admission token，不保存明文密码 |
| ST-MP-017 | Command metadata 未真正幂等，连续 rematch key 冲突 | 已修复 | 规范 idempotency key 导出 command ID，Round epoch 区分 rematch |
| ST-MP-018 | Lobby Room ZSET 无 TTL | 已修复 | Redis v2 对 Room/Account/Index 同步设置和刷新 TTL |
| ST-MP-019 | MATCH_COMPLETE 发给未参与本轮用户并清除 NO_BEATMAP | 已修复 | 只向冻结 Round participants 发送 Complete，保留其他 Slot 状态 |

## 5. Stable Web、Scoring 与 Ranking

| ID | 原问题与影响 | 状态 | 修复与验证位置 |
| --- | --- | --- | --- |
| ST-SCORE-001 | Ranking ended_at Unix 微秒写入 `NUMERIC(20,5)` 必然溢出 | 已修复 | `NUMERIC(30,5)` 和无浮点 epoch 微秒；Bootstrap schema repair；PG 测试 |
| ST-SCORE-002 | Online Checksum 只保存不重算，伪造成绩可成为权威事实 | 已修复 | 按 bancho.py 公式重算并 `compare_digest`；Score submission 测试 |
| ST-SCORE-003 | `bmk`、`sbk`、`c1` 和 Build Marker 仅保存不验证 | 已修复 | 谱面 Hash、格式、双设备分量和 Build Marker 子校验；Attestation 仍正确保持 pending |
| ST-SCORE-004 | Multipart 无 Content-Length 时先无界 spool，再检查 Replay | 已修复 | 在 MultiPartParser 前流式累计总字节；Chunked 同样受限 |
| ST-SCORE-005 | Replay 可为空/损坏却标记 ready | 已修复 | 最大值、24 字节最小结构和有界读取；完整 Replay 语义分析仍属设计边界 |
| ST-SCORE-006 | Replay SHA/storage key 全局唯一导致相同内容的第二个 Score 500 | 已修复 | 多个 Score manifest 可引用同一内容寻址对象；旧约束 repair 删除 |
| ST-SCORE-007 | Score request digest 漏掉 `x/ft/st/bmk/sbk/c1` | 已修复 | 长度前缀覆盖全部事实字段和 Replay digest |
| ST-SCORE-008 | `st/ft` 可触发 timedelta Overflow，多类预期错误泄漏 JSON 500 | 已修复 | 数值/时间窗口限制和完整 Stable 文本错误映射 |
| ST-SCORE-009 | 命中数、Combo、普通 FC 与谱面对象边界不足 | 已修复 | Ruleset 对象数、最大 Combo、Pass/Fail 和时间一致性校验 |
| ST-SCORE-010 | 过期/中止 Multiplayer Attempt 可绑定后续同图单人游玩 | 已修复 | Attempt 时间与 Round/Session 生命周期校验，事务内单次消费 |
| ST-SCORE-011 | RX/AP 暂无 PP 时静默改用 Total Score 排名 | 已修复 | PP Policy 缺结果标记 `performance_pending`，不伪造排名 |
| ST-SCORE-012 | Stable Loved/Qualified 排行策略不明确 | 已修复 | Vanilla 明确允许 Ranked/Approved/Qualified/Loved；RX/AP 仅 Ranked/Approved |
| ST-SCORE-013 | Exact Mods 对同 legacy bitmask 的多个 Lazer ModSet 随机选一条 | 已修复 | 合并全部相同 `legacy_bits` 的 Canonical ModSet，再按账户选最佳 |
| ST-SCORE-014 | Stable Rating 错误绑定 Beatmapset | 已修复 | 绑定 logical Beatmap；旧 Revision vote 迁移、去重并重建约束 |
| ST-SCORE-015 | Beatmap Info 固定全 `N`，Web Friends 缺系统账户 1 | 已修复 | 查询真实 Vanilla Grade projection；Friend List 固定含系统账户并去重 |
| ST-SCORE-016 | 默认 `.osz` 上游要求官方登录，Direct 下载失败 | 已修复 | 遗留官方默认值转换到可公开下载端点，自定义上游仍可配置 |
| ST-SCORE-017 | Web 认证对离线账户执行高成本 KDF，且不绑定 realtime epoch | 已修复 | 先查活动 Session candidate、dummy verification、Redis epoch 校验 |

## 6. 已知设计与运维边界

以下内容没有被伪装成已完成能力：

| ID | 边界 | 当前处理 |
| --- | --- | --- |
| ST-BOUND-001 | HTTP 响应真正送达客户端之前无法获得传输级 ACK | Mailbox 使用独占 Lease 和 fenced ACK；断线仍可能产生 at-most-once 窗口，重要事实不得只依赖 Mailbox |
| ST-BOUND-002 | Client Hash/`c1` 尚未与登录设备事实做权威关联 | Attestation 保持 `pending`，Evidence 只标记已完成的子校验 |
| ST-BOUND-003 | 没有完整 Replay Frame 解析和反作弊检测 | 当前只校验大小与最小结构；后续 Detection/GC 不得阻塞成绩事实事务 |
| ST-BOUND-004 | S3 写成功而数据库事务失败会留下孤儿对象 | 需要对象存储对账与 GC；数据库事务内禁止执行 S3 I/O |
| ST-BOUND-005 | 真实 PP Formula/Calculation Release 尚未部署 | RX/AP 排行保持空或 `performance_pending`，不退化为 Score 排名 |
| ST-BOUND-006 | Stable Local 排行没有独立投影 | `leaderboard_type=0` 仍明确退化 Overall |
| ST-BOUND-007 | Web KDF 仍需要入口层请求速率限制 | 代码已消除离线账户直接 KDF 与账户枚举；生产反向代理仍必须限制认证请求速率 |
| ST-BOUND-008 | `.osz` 尚未本地缓存 | 当前使用可配置公开上游重定向 |

## 7. 数据库升级要求

`bootstrap_database()` 现在会在同一 advisory lock 下幂等执行以下 repair：

- 回填并约束 `iam.auth_sessions.last_activity_at`。
- 回填 `multiplayer.session_presences.connection_session_id`。
- 关闭历史重复 active presence，再创建账户级 partial unique index。
- 将历史重复 active Round 标记 aborted，再创建 Session 级 partial unique index。
- 放宽未知谱面 Round 的 source 约束。
- 扩展 Ranking Numeric 精度、删除 Replay 错误唯一约束并迁移 Stable Rating。

部署现有数据库前应备份并在维护窗口运行 Bootstrap；`create_all()` 本身不会修改旧表。

## 8. 验证记录

- 单元与无外部依赖测试覆盖 Stable Login/Poll、Codec、Dispatcher、Web、Scoring、Spectator 和 Multiplayer。
- PostgreSQL focused 测试覆盖 Session 并发、Ranking、Round、Attempt 和 Schema repair。
- Redis focused 测试覆盖 Session Fence、Mailbox、Presence、Spectator 与 Multiplayer CAS。
- 无外部服务完整测试：`260 passed, 15 skipped`。
- 启用本地真实 PostgreSQL 与 Redis 的完整测试：`275 passed`。
- 完整验证命令：`uv run ruff format .`、`uv run ruff check --fix .`、`uv run pytest`、`python -m compileall src tests`、`git diff --check`。

本文件是问题历史，不因修复完成而删除条目。若问题复发，应保留原 ID 并追加回归时间、原因与新测试。
