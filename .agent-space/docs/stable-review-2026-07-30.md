# Stable 二次复核与修复记录

最后更新：2026-07-30。

本文记录 2026-07-30 对 Stable 实现的二次复核。四个专项 subagent 分别检查登录/Poll、Scoring/Web、Multiplayer/Spectator 和架构/测试；主流程随后逐项对照当前代码、Python 版本、参考 Stable 实现和真实 PostgreSQL/Redis 测试。未经复现或代码核对的 subagent 结论不作为实现事实。

## 1. 已确认并修复

| ID | 问题 | 修复 |
| --- | --- | --- |
| ST30-PROTO-001 | `stable_max_response_bytes` 只限制 Dispatcher 本地输出，最终 Poll 直接追加整批 Mailbox，可能超过响应上限并确认未受预算约束的消息 | `api/stable/router/cho.py` 按剩余预算选择完整 Mailbox 项，只确认实际返回的最高序列；未返回项保留到后续 Poll |
| ST30-AUTH-001 | 登录容量检查与 Presence 写入分离，并发登录可同时通过检查；成功并发登录还可能互相遗漏 Bootstrap Presence | Redis `SET_PRESENCE` 原子清理过期成员、检查容量并写入 Presence；登录写入后重新读取完整在线快照，容量竞态走精确 Session 补偿 |
| ST30-AUTH-002 | Multiplayer Admission Token 与身份 Token 复用 `token_hmac_key` | 新增独立 `admission_hmac_key`，同步开发/生产环境示例和组合根测试 |
| ST30-MP-001 | Public ID 复用 fence 依赖 UUIDv7 字符串顺序；该顺序只保证单进程生成器单调，不能作为多进程共享 epoch | PostgreSQL `Room.public_id_epoch` 使用数据库 Identity 分配全局单调值；Redis Room blob、Account Index 和 CAS 全部携带并比较该 epoch，逆序 UUID 不再影响新旧房间胜负 |
| ST30-MP-002 | 同图快速 rematch 时，Submission Context 只取最新 Attempt，迟到但仍在宽限期的上一轮成绩可能错误绑定新一轮并被拒绝 | Resolver 接收成绩 `started_at/ended_at`，只选择时间区间包含该 Play Attempt 的 Round；Scoring 事务仍执行最终锁定、维度校验和单次消费 |
| ST30-MP-003 | Active Round 期间仍允许修改个人 Free Mod，Redis 展示值可能与 PostgreSQL 冻结值分离 | Canonical `MultiplayerService` 在 Active Round 拒绝个人 Mod 变更 |
| ST30-ARCH-001 | Stable Dispatcher 和 Multiplayer Packet Handler 位于 `modules/realtime/stable`，却导入 `infra.composition.StableServices` | 两个处理器移动到 `api/stable`；`modules/realtime/stable` 只保留 Wire Model、Codec 和 Builder，`modules/realtime` 不再导入 `infra` |
| ST30-SCORE-001 | Replay 事务外上传的孤儿对象边界只记录在设计文档，代码现场不明显 | 上传点增加简短说明；仍坚持对象存储 I/O 不进入数据库事务，后续由对账/GC 清理孤儿对象 |

## 2. 核验后排除的误报

以下初始报告项与当前代码不符，不能据此修改实现：

- `except A, B:` 不是 Python 2 遗留语法。项目要求 Python 3.14，PEP 758 已允许多异常类型省略括号；当前解释器、Ruff 和 compileall 均通过。
- Stable Online Checksum 字段顺序与 `.agent-space/stable-python/objects/score.py` 一致，已有公式回归测试；没有发现“所有成绩都会被拒绝”的问题。
- `start_round()` 已捕获 `uq_rounds_session_active` 的 `IntegrityError` 并映射为 `MatchStateRejected`，事务由 UoW 回滚。
- Spectator Attach 与 Frame Publish 均为 Redis Lua 脚本，Redis 串行执行保证 history-to-live handoff：Publish 先发生则进入 History，Attach 先发生则进入 Live Mailbox。
- Multiplayer 和 Logout 的 Dispatcher 返回原本就经过 `_extend_response()`；真实缺口是 Dispatcher 之后追加 Mailbox 的最终响应预算，已按 ST30-PROTO-001 修复。
- 加密 Score、Client Hash 和 IV 的 Base64 文本均有 16 KiB 上限，解码后不会出现报告所称的无界内存放大；非法 IV 会由 Rijndael 构造或解密异常映射为 Stable 错误。
- Score 认证使用解密用户名查找账户，并使用认证后的当前用户名重算 Online Checksum；伪造另一用户名不能通过凭据与 Checksum 双重校验。
- Replay 先写对象存储、后写数据库是有意的事务边界，不应通过在 PostgreSQL 事务中执行 S3 I/O“修复”。

## 3. 保留边界

下列事项仍需后续设计或运维能力，不伪装成已完成：

- `START` 读取 Redis Slot/Mod 后到 PostgreSQL 冻结 Round 之间仍存在跨存储并发窗口。Active Round 后修改已被拒绝，但要对“START 与最后一次 Mod CAS 同时发生”建立严格线性化，需要专门的 Redis Round-Starting Fence/Reservation 与失败补偿协议。
- 登录失败、Realtime 丢失和短 Mailbox Lease 释放包含 best-effort cleanup。Redis 状态可由 TTL 收敛；PostgreSQL Session 关闭失败仍需要结构化日志、指标和运维告警。
- 单个 Mailbox 项如果大于整个 Poll 响应预算会保留到过期，避免截断或错误确认。生产配置和所有入队路径应保证单项不超过 Stable 响应上限，后续可在 Mailbox Port 增加显式单项限制。
- Replay 对象存储对账与孤儿 GC 尚未实现。

## 4. 验证结果

- Ruff：`uv run ruff format .`、`uv run ruff check --fix .` 通过。
- Python：`uv run python -m compileall -q src tests` 通过。
- 无外部依赖全量测试：`270 passed, 18 skipped`。
- 启用真实 PostgreSQL 与 Redis：`288 passed`。
- 新增回归覆盖最终 Poll 预算与 Mailbox ACK、原子 Presence 容量、独立 HMAC Key、逆序 UUID 下的 Public ID Epoch、同图 Rematch Attempt 选择和 Active Round Free Mod 拒绝。

本文件是二次核验基线。后续若发现回归，应保留 ID 并追加原因、修复和验证位置。
