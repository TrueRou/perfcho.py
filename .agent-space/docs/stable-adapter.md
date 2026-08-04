# Stable 适配器支持矩阵

最后更新：2026-08-03。

本文只描述当前生产 Router 和 Dispatcher 的真实支持范围。Stable 是协议适配层，所有权威校验仍应在共享应用服务中完成。

## 1. 协议基线

- 目标客户端：最新 Stable `b20260711.1`。
- Bancho Protocol Version：`19`。
- Bancho 使用 `POST /`，`User-Agent` 必须为 `osu!`。
- 无 `osu-token` Header 时执行登录；有 Token 时执行二进制 Poll。
- Web 路由使用 Stable 用户名和 MD5 Password Token，通过 `IdentityService.verify_stable_web()` 统一验证，并要求匹配的 Redis Realtime Session Epoch 在线。
- 二进制 Body、Multipart、Replay、Packet、String、List 和 Frame Bundle 均有明确上限。
- 每个 Packet 已按长度隔离，未知或尚未支持的 Packet 会安全跳过，不会让当前 Poll 失去帧同步。

## 2. Bancho 登录与 Poll

### 2.1 登录

登录当前完成：

1. 校验 `User-Agent`、Body 上限、Stable 登录字段和目标 Build。
2. 使用 Canonical `StableLogin` Command 验证账户、凭据、设备和单会话约束。
3. 查询 Stable Privilege、公开频道、好友和未读离线私信。
4. 在 Redis 创建带 Revision 和 TTL 的 Realtime Session 与 Presence。
5. 返回 Protocol Version、Login Reply、Privileges、Welcome、Channel List、Friend List、Silence、Presence、Stats 和离线消息。

错误语义包括无效请求、旧客户端、凭据错误和重复在线会话。Bancho Token 对应持久 Auth Session，Redis 只承担在线 Lease，不替代持久会话事实。

### 2.2 Poll

每次 Poll 会：

1. 解析并验证持久 Stable Session。
2. 校验 Redis Session 的账户、Session ID 和 Revision。
3. 顺序处理本次请求中的客户端 Packet。
4. 刷新 Session/Presence 有效期，但不超过持久会话到期时间。
5. 领取 Mailbox 批次，与本次请求产生的响应一起返回并确认。

若本次请求处理 `LOGOUT`，登出会先关闭持久会话并 Fence Redis Session；Poll 随即结束，不再对已移除的 Mailbox Lease 执行确认或释放。

Poll 在处理客户端 Packet 前获取 fenced Mailbox Lease，避免并发 Poll 在业务副作用完成后才发生冲突。最终响应把本地输出和 Mailbox 一起计入 `stable_max_response_bytes`，只确认实际返回的完整 Mailbox 项，超出预算的后缀保留到后续 Poll。

客户端源码把 ID `4` 定义为 `Osu_Pong`，成功的空 HTTP Poll 同样会刷新连接计时器。ID `8` 是服务端主动发起的 `Bancho_Ping`；把两者错误地做成请求/响应对会令客户端立即再次发送 ID `4`，并可能触发客户端权限变化退出检查。

## 3. 客户端 Packet 支持

| Client Packet | 状态 | 当前行为 |
| --- | --- | --- |
| `PING` | 已接线 | 消费客户端 keepalive 并返回空 Poll；服务端不会对客户端 ID `4` 回发 ID `8` |
| `REQUEST_STATUS_UPDATE` | 已接线 | 从权威 Score/Ranking Query 返回基础 Stats；PP 保持 Deferred |
| `CHANGE_ACTION` | 已接线 | 查询当前基础 Stats，更新 Redis Presence 并按订阅 Filter 扇出 |
| `USER_STATS_REQUEST` | 已接线 | 从 Redis Presence Snapshot 提取指定账户 Stats |
| `USER_PRESENCE_REQUEST` | 已接线 | 从 Redis Presence Snapshot 提取指定账户 Presence |
| `USER_PRESENCE_REQUEST_ALL` | 已接线 | 从有界 Redis Presence Index 返回在线用户 Presence |
| `RECEIVE_UPDATES` | 已接线 | 保存受 Session Fence/TTL 保护的 All/Friends/Nil Filter |
| `START_SPECTATING` | 已接线 | 建立 Spectator Relation，通知 Host/Fellow，并补发有界 Frame 历史 |
| `STOP_SPECTATING` | 已接线 | 移除关系并通知 Host/Fellow |
| `SPECTATE_FRAMES` | 已接线 | 验证 Sequence，写 Redis 历史并扇出到 Spectator Mailbox |
| `CANT_SPECTATE` | 已接线 | 通知 Host 和同场 Spectator |
| `CHANNEL_JOIN` | 已接线 | 通过 Community Service 校验并返回 Join/Info；`#lobby` 仅确认已由 `JOIN_LOBBY` 建立的成员状态 |
| `CHANNEL_PART` | 已接线 | 退出频道并返回 Kick |
| `SEND_PUBLIC_MESSAGE` | 已接线 | 持久化普通公开消息；虚拟 `#multiplayer` 按当前 RoomState 向房内成员扇出；命令回复由 bot 发送 |
| `SEND_PRIVATE_MESSAGE` | 已接线 | 名称解析、持久消息、Block/Friends/Silence、在线/离线和 Away Reply；发给 `BanchoBot` 时执行命令 |
| `SET_AWAY_MESSAGE` | 已接线 | 保存当前 Session 的有界 Away Message |
| `TOGGLE_BLOCK_NON_FRIEND_DMS` | 已接线 | 更新权威 User Preference 私信策略 |
| `FRIEND_ADD` | 已接线 | 创建 Follow 事实并返回最新 Friend List |
| `FRIEND_REMOVE` | 已接线 | 删除 Follow 事实并返回最新 Friend List |
| `LOGOUT` | 已接线 | 离开 Spectator/Match、关闭持久会话、Fence Redis 并广播下线 |
| Lobby/Match 全套非 Tourney Packet | 已接线 | Join/Part/Create/Join/Slot/Host/Team/Mods/Ready/Map/Start/Load/Frame/Fail/Skip/Complete/Invite |

当前明确不接线 Tourney/Special Match Packet。`ERROR_REPORT`、`IRC_ONLY` 和 `BEATMAP_INFO_REQUEST` 会有界消费并保持 Packet 对齐，但没有业务副作用。

## 4. Stable Web 路由

| 方法与路径 | 状态 | 用途 |
| --- | --- | --- |
| `POST /users` | 已接线 | 校验或原子注册 Stable 账户；`check=0` 提交，非零值仅检查 |
| `GET /web/osu-getfriends.php` | 已接线 | 返回系统账户与当前账户的好友 ID |
| `GET /web/osu-markasread.php` | 已接线 | 按私信对象推进权威 Conversation Read Cursor |
| `POST /web/osu-getbeatmapinfo.php` | 已接线 | 按文件名和 Beatmap ID 批量返回 Song Select 信息与真实 Vanilla 最佳投影 Grade |
| `GET /web/osu-search.php` | 已接线 | 从本地内容索引执行 Direct 搜索和分页 |
| `GET /web/osu-search-set.php` | 已接线 | 按 Set ID、Beatmap ID 或 MD5 返回单个 Direct Set |
| `GET /web/osu-getfavourites.php` | 已接线 | 返回公开收藏的 Beatmapset ID |
| `GET /web/osu-addfavourite.php` | 已接线 | 幂等添加收藏 |
| `GET /web/osu-rate.php` | 已接线 | 执行评分资格检查、提交和平均分查询 |
| `GET /web/bancho_connect.php` | 已接线 | 接受客户端连接探针，不创建会话 |
| `GET /web/check-updates.php` | 已接线 | 返回与当前受支持 Build 对应的最小有效更新 Manifest |
| `POST /web/osu-comment.php` | 已接线 | 持久读写谱面、谱面集和 Replay 时间轴评论 |
| `POST /difficulty-rating` | 已接线 | 返回本地已投影的基础 Star Rating |
| `GET /{account_id}`、`/thumb/*`、`/preview/*` | 已接线 | 配置化转发头像、封面和试听，不写入对象存储 |
| `GET /web/osu-getseasonal.php`、`/menu-content.json` | 已接线 | 配置化转发公开客户端元数据 |
| `GET /web/maps/{map_filename}` | 已接线 | 从 S3 流式返回当前 `.osu`，不可用时重定向上游 |
| `GET /d/{beatmapset_selector}` | 已接线 | 重定向可配置的公开 Beatmapset 上游，支持 `n` 无视频后缀 |
| `POST /web/osu-submit-modular-selector.php` | 已接线 | 解密、验证、暂存 Replay 并原子接受 Stable 成绩 |
| `GET /web/osu-getreplay.php` | 已接线 | 流式返回 Replay 并幂等记录非 Owner 查看事实 |
| `GET /web/osu-osz2-getscores.php` | 已接线 | 按需补全上游内容并返回本地 Ranking Projection 的排行榜 |

## 5. 内容适配语义

- Beatmap Query 支持 External Beatmap ID、MD5、文件名和 Beatmapset ID。
- Get Scores 本地 MD5 命中时不等待上游；响应发送后才按持久刷新水位检查更新。本地缺失时使用 Set ID，或官方 checksum/filename lookup，同步完整 Beatmapset 后重新判定请求 MD5。
- `1|false` 只表示同步后确认客户端 MD5 已是历史修订、客户端需要更新；`-1|false` 表示上游同样无法定位谱面。上游或对象存储故障返回 503，不伪装为这两种内容状态。
- Direct 搜索只查询本地已同步内容，不会在请求内调用官方 API。
- `Newest`、`Top+Rated`、`Most+Played` 当前被当作空搜索词，不代表对应排序投影已经实现。
- Direct Beatmapset 下载当前重定向到可配置上游，不缓存 `.osz`；遗留官方默认值会在 Router 中转换为无需请求内官方认证的公开 NeriNyan 下载端点。
- 头像、封面、试听、季节背景、菜单元数据和 `.osz` 只转发公开上游，不保存到 perfcho 的 S3/MinIO。
- `.osu` 文件读取支持本地对象存储；缺失时重定向到配置的上游 URL。
- 收藏和评分写入 PostgreSQL；评分范围固定为 1 到 10，Stable MD5 评分绑定逻辑 Beatmap，不再错误绑定整个 Beatmapset 或某个可替换 Revision。

## 6. 成绩提交语义

### 6.1 已校验内容

- Stable Build 和 Rijndael 密钥派生。
- Base64、IV、加密字段数量和 Replay 大小。
- 用户名、MD5 Password Token 和 Submission 用户一致性。
- Beatmap MD5 对应当前不可变 Revision。
- Ruleset、Variant、Legacy Mod Bit 和 Mod 组合。
- Hit Statistic、普通榜 Combo/FC、Accuracy、Grade、Pass/Fail、可证明对象数边界与有界结束时间。
- 按 bancho.py 公式重算并常量时间比较的 Stable Online Checksum；公式使用认证后的当前用户名、提交的客户端 Hash 与 Storyboard Hash。兼容未提交 `sbk` 的 Stable 客户端时跳过 checksum 值比较，但仍校验 checksum 格式；提交 `sbk` 时保持严格比较。
- `bmk` 与提交谱面 MD5 一致、`sbk` MD5 格式、`c1` 双设备分量、客户端版本标记、Replay 24 字节最小结构、请求摘要和幂等冲突。
- 解密后的 Stable 成绩字符串严格包含 19 项；末两项分别为 `YYYYMMDD` 客户端版本和非负 `client_flags`，与 multipart 的 `osuver` 及 canonical 成绩事实一致。
- 已同步谱面的 Round Start 会冻结 Playlist/Scoreboard/Mod Policy/Slot/Team/Mods 并签发 Multiplayer Attempt；Stable 路由解析最近有效 Context，`ScoringService` 在成绩事务中消费。
- 未同步或未知谱面允许进行非排名联机，但不会伪造 Round Attempt。

### 6.2 写入原子性

下列事实在同一 PostgreSQL 事务提交：

- Play Attempt
- Score
- Hit Statistics
- Replay Manifest
- Score Attestation 与 Evidence
- Command Receipt
- `score.accepted.v1` Outbox Event 与 Ranking/Scoring Stats Delivery
- 可确定且本次首次满足的 `social.achievement_unlocks` 与 `social.achievement-unlocked.v1` Delivery

Replay 二进制先在事务外按 SHA-256 写入对象存储。数据库失败时可能留下无引用对象，后续应由对象存储对账/GC 清理；不能在数据库事务里执行 S3 I/O。

成绩事务提交后，Stable Web 适配器只返回提交结果和可选的只读 Overall Chart；`achievements-new` 仅序列化该事务首次创建的解锁，命令重放保持为空。适配器不同步写在线 Redis Presence/Mailbox。`score.accepted.v1`、`score.performance-calculated.v1`、Achievement 和 Replay View 事件由 Outbox/Worker 处理，分别更新 `UserPlayStat`、`UserRankedStat`、Activity/Notification 和排行榜；后续 `REQUEST_STATUS_UPDATE` 再读取规范统计投影。PC 与总分、Ranked Score、Global Rank 和 PP 都以这些投影为准，投影尚未完成时保持延迟语义。

Attestation 整体仍保持 `pending`：当前接口无法权威比较提交的 Client Hash/`c1` 与登录设备事实，也没有权威 Storyboard 内容摘要或完整 Replay Frame/反作弊分析。Evidence 只把实际完成的格式、谱面 Hash 和 Online Checksum 子校验标为 verified，不能把请求摘要冒充客户端 Checksum。

### 6.3 排行榜

Stable `leaderboard_type` 当前支持：

- `0`：Local；当前尚无 Local 投影，实际退化为 Overall
- `1`：Top / Overall
- `2`：Exact Mods
- `3`：Friends，并包含请求者自己的 Personal Best
- `4`：Country

编辑器请求返回空排行但保留谱面头信息。谱面不存在、客户端 MD5 过期、Set ID 不匹配和非法 Mod 组合均映射为 Stable 约定响应；服务端首次补全本身不代表客户端需要更新。

Vanilla Policy 以 Total Score 排名，并明确允许 Ranked、Approved、Qualified 与 Loved。Relax/Autopilot Policy 只允许 Ranked/Approved 且以指定 Calculation Release 的 PP 排名；PP 缺失时 Eligibility 为 `performance_pending`，不会静默回退为 Total Score。真实 PP Release 尚未配置时 RX/AP 排行为空，这不能解释为最终 Performance 结果。

## 7. Social、Community 与 Spectator

### 7.1 好友

Stable Friend 对应单向 Follow。添加和删除具有自然幂等语义；Friend List 返回当前有效 Follow，并保留系统账户 `1` 的兼容项。

### 7.2 频道和消息

- 频道可见性、成员资格、Silence 和 Block 由应用服务校验。
- 普通公开消息先写 PostgreSQL，再对当前在线成员写 Mailbox。Stable 虚拟 `#multiplayer` 没有全局 Channel ID，按权威 RoomState 校验发送者和收件人后执行临时 Mailbox 扇出，不能走普通公开频道查询，否则会跨房间混淆同名 target。
- 登录可以读取未读离线私信并返回 Packet。
- 在线私信写入 Redis Mailbox，离线私信保留 PostgreSQL 事实并通知发送者；Away Message 仅属于当前在线 Session。

### 7.3 Spectator

- Redis 保存 Host 与 Spectator 关系，不写 PostgreSQL。
- Relation 使用 Session Revision/Fence，旧 Session 不能覆盖新 Session。
- Frame 使用 Session Epoch 内部 Cursor 写入滚动有界历史，接受 Stable u16 Sequence 回绕；无效 Frame 会被拒绝。
- 新 Spectator 通过原子 History-to-Live Handoff 收到当前最新保留窗口，并通知 Host 和 Fellow Spectator。
- Host 下线或 TTL 到期后关系允许自然失效。

### 7.4 Multiplayer

- PostgreSQL 保存 Room、Session、Participant/Presence、Host、Round、Attempt 和有序 Event；Redis 保存 16 Slot 高频投影。
- Stable Public Match ID 限制为 1 到 32767，结束后允许复用；Active Room 使用部分唯一索引。
- Match Password 使用独立 HMAC Key、随机 Salt 和常量时间比较；Redis 只保存 `requires_password`。
- PostgreSQL 为每个 Room 分配全局单调 `public_id_epoch`；Redis CAS 同时使用 Public ID Epoch、Room/Session Identity 和 State Revision Fence，Public ID 复用不依赖跨进程 UUID 顺序。PostgreSQL 已提交而 Redis 不可用时返回 Durable Recovery Snapshot，后续房间解析按持久 Version、Round 和 Presence 集合修复投影。
- Round 期间拒绝个人 Free Mod 变更；成绩提交按 Play Attempt 的开始/结束时间选择同图 Rematch 中正确的冻结 Attempt，再由 Scoring 事务执行最终校验和单次消费。
- Stable 客户端在 `JOIN_LOBBY` 后仍发送 `CHANNEL_JOIN #lobby`，Adapter 必须确认该虚拟频道；创建或加入房间时由服务端先发送 `CHANNEL_KICK #lobby`，再发送 `CHANNEL_JOIN_SUCCESS #multiplayer`，避免客户端继续向已离开的 lobby 发消息。
- `#multiplayer` 消息使用发送者当前权威 RoomState 解析实际房间，执行全局 Silence、Block 和长度校验，只投递给该房间当前 Slot 成员；客户端 target 本身不是持久公开频道名称。
- `MATCH_COMPLETE` 事务写入版本化完成事件和 `multiplayer-results-projector.v1` Delivery；Projector 从权威 Attempt/Score 生成 Round Result、Session Standing 和房间/谱单汇总。宽限期内迟到的 Stable 成绩通过多人 `score.accepted.v1` 再次幂等重建，Abort 不生成 Round Result。

## 8. 协议适配规则

- Router 只解析 Stable 输入、调用一个应用操作并序列化 Stable 响应。
- Stable 的字段别名、空响应、错误字符串和 Legacy Mod 位只存在于适配层。
- Service 抛应用错误；Router/Dispatcher 负责映射为 Stable 错误语义。
- Stable Match 结构不能直接成为 Canonical Multiplayer Aggregate；必须由 Adapter 转换。
- 新增 Packet 时必须同时增加 Codec/Dispatcher/Builder 合同测试，并更新本文矩阵。
