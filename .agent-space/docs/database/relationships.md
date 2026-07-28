# 领域关系

下列图用于表达聚合边界，不展示全部审计边和投影边。列级结构以 SQLAlchemy Model 为准。

## 身份与授权

```mermaid
erDiagram
    ACCOUNTS ||--o{ ACCOUNT_NAMES : "名称历史"
    ACCOUNTS ||--o{ ACCOUNT_EMAILS : "邮箱历史"
    ACCOUNTS ||--o| USER_PROFILES : "公开资料"
    ACCOUNTS ||--o| USER_PREFERENCES : "私有偏好"
    ACCOUNTS ||--o| PASSWORD_CREDENTIALS : "密码认证"
    ACCOUNTS ||--o{ AUTH_SESSIONS : "创建会话"
    AUTH_SESSIONS ||--o{ AUTH_TOKENS : "签发 Token"
    ACCOUNTS ||--o{ ACCOUNT_DEVICES : "使用设备"
    DEVICES ||--o{ ACCOUNT_DEVICES : "关联账户"
    ROLES ||--o{ ROLE_PERMISSIONS : "包含权限"
    PERMISSIONS ||--o{ ROLE_PERMISSIONS : "归属角色"
    ACCOUNTS ||--o{ ACCOUNT_ROLE_GRANTS : "获得角色"
```

## 谱面与成绩

```mermaid
erDiagram
    SOURCES ||--o{ BEATMAPSETS : "划分命名空间"
    BEATMAPSETS ||--o{ BEATMAPS : "包含难度"
    BEATMAPS ||--o{ BEATMAP_REVISIONS : "文件修订"
    ACCOUNTS ||--o{ PLAY_ATTEMPTS : "发起游玩"
    BEATMAP_REVISIONS ||--o{ PLAY_ATTEMPTS : "目标谱面"
    PLAY_ATTEMPTS ||--o| SCORES : "产生成绩"
    SCORES ||--o{ SCORE_HIT_STATISTICS : "命中统计"
    SCORES ||--o{ SCORE_PERFORMANCES : "计算 PP"
    SCORES ||--o| REPLAYS : "拥有回放"
    RANKING_POLICIES ||--o{ SCORE_ELIGIBILITY : "判断有效性"
    RANKING_POLICIES ||--o{ LEADERBOARD_ENTRIES : "生成排行榜"
    SCORES ||--o{ LEADERBOARD_ENTRIES : "成为最佳成绩"
```

## 中心多人领域

```mermaid
erDiagram
    ROOMS ||--o{ SESSIONS : "产生托管周期"
    ROOMS ||--o{ ROOM_PARTICIPANTS : "接纳参与者"
    ROOMS ||--o{ PLAYLIST_ITEMS : "包含谱单"
    PLAYLIST_ITEMS ||--o{ PLAYLIST_REVISIONS : "配置修订"
    SESSIONS ||--o{ ROUNDS : "冻结回合"
    ROUNDS ||--o{ ROUND_PARTICIPANTS : "冻结参与者"
    ROUNDS ||--o{ ATTEMPTS : "授权成绩机会"
    ATTEMPTS ||--o| PLAY_ATTEMPTS : "绑定提交"
    ATTEMPTS ||--o| SCORES : "绑定验证成绩"
    ROUNDS ||--o{ ROUND_RESULTS : "投影结果"
    SESSIONS ||--o{ SESSION_STANDINGS : "投影积分"
```

当前连接、房间即时成员状态、Ready/Loading/Skip 与实时帧位于 Redis，不进入关系图。PostgreSQL 中的 Presence 只表示需要保留的加入和离开历史。

## 事件投递

```mermaid
erDiagram
    OUTBOX_EVENTS ||--o{ OUTBOX_DELIVERIES : "分发给消费者"
    OUTBOX_EVENTS ||--o{ ACTIVITY_EVENTS : "投影动态"
    OUTBOX_EVENTS ||--o{ PROJECTION_CHECKPOINTS : "推进水位"
```

Relay 从 `OUTBOX_DELIVERIES` 领取到期记录并投递 Taskiq。Worker 完成领域处理后，在同一 PostgreSQL 事务中更新投影、Checkpoint 和 Delivery。

## 社区与处罚

```mermaid
erDiagram
    CHANNELS ||--o{ MESSAGES : "包含消息"
    CHANNELS ||--o{ CHANNEL_MEMBERSHIPS : "包含成员"
    CHANNELS ||--o{ CHANNEL_USER_STATES : "记录已读游标"
    CHANNELS ||--o| DIRECT_CONVERSATIONS : "特化为私信"
    NOTIFICATIONS ||--o{ NOTIFICATION_RECIPIENTS : "发送给用户"
    ACCOUNTS ||--o{ CASES : "成为调查对象"
    CASES ||--o{ CASE_ENTRIES : "包含记录"
    CASES ||--o{ SANCTIONS : "授权处罚"
    SANCTIONS ||--o{ SANCTION_EVENTS : "状态变化"
    ANTICHEAT_RUNS ||--o{ ANTICHEAT_FINDINGS : "产生发现"
    CASES ||--o{ CASE_FINDINGS : "关联发现"
```
