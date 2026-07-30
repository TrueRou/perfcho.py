# bancho.py v5.2.2 迁移

`tools/bancho_migration/` 是一次性、可恢复的离线迁移工具。它从 bancho.py v5.2.2 MySQL 读取事实，写入当前 PostgreSQL Canonical Model，并将 `.data/osu` 与 `.data/osr` 的有效文件写入当前 S3。

## 边界

- 支持账户、名称、邮件、资料、偏好、权限、Supporter、当前处罚、设备、登录记录、好友、屏蔽、Clan、频道、私信、谱面、成绩、统计、成就、审核日志和赛事图池。
- 明确不读取 `sb_patcher_scores_meta`、`scores_suspicion`、`performance_reports`，不创建 SB Finding 或 SB Evidence。
- 不迁移旧明文 API Key。报告只记录它是否存在及不可逆 SHA-256，用户必须重新签发 Key。
- 旧 bcrypt(MD5) 密码作为 `bcrypt_md5` 凭据导入；首次 Stable 登录成功后，在创建会话的同一事务中原子升级为当前 Argon2id+Pepper。
- 已有目标身份和凭据优先。名称、邮件和源 ID 产生歧义时，预检失败，必须通过显式 Override 处理。

## 前置条件

1. 停止 bancho、perfcho API、Outbox Relay、Taskiq Worker 及其他写入进程。
2. 对 MySQL、PostgreSQL 和 S3 做一致备份，并验证恢复步骤。
3. 确认源 MySQL 用户只有读取权限；`apply` 自身也会开启只读一致性快照。
4. 初始化 S3 Bucket，并配置 `S3_ENDPOINT_URL`、`S3_REGION`、`S3_ACCESS_KEY`、`S3_SECRET_KEY`、`S3_BUCKET`。
5. 设置源服务器原来解释 MySQL `DATETIME` 的 IANA 时区。不要在不确定时默认使用本机时区。

迁移连接可通过环境变量提供：

```bash
export BANCHO_DATABASE_URL='mysql+pymysql://reader:secret@127.0.0.1/bancho'
export DATABASE_URL='postgresql+asyncpg://perfcho:secret@127.0.0.1/perfcho'
```

## 执行

先运行不写 PostgreSQL 业务数据的预检。预检会在 S3 的 `migration-probes/` 下写入并立即删除一个小对象，用于验证 Bucket 的写入和删除权限：

```bash
uv run python -m tools.bancho_migration preflight \
  --migration-id prod-2026-07-29 \
  --data-dir /srv/bancho \
  --source-timezone Asia/Shanghai \
  --report /srv/migration/preflight.json
```

处理报告中的所有 `error`。文件缺失或摘要错误作为 Warning 报告，对应谱面、回放和依赖记录会被跳过。账户归并歧义是 Error，禁止部分猜测。

Override 文件只支持经过审核的账户决定：

```json
{
  "accounts": {
    "42": {"target_account_id": 9001},
    "43": {"display_name": "Resolved Name", "email": "resolved@example.com"},
    "44": {"skip": true}
  }
}
```

预检通过后执行迁移：

```bash
uv run python -m tools.bancho_migration apply \
  --migration-id prod-2026-07-29 \
  --data-dir /srv/bancho \
  --source-timezone Asia/Shanghai \
  --overrides /srv/migration/overrides.json \
  --report /srv/migration/apply.json \
  --confirm-offline
```

`apply` 会创建缺失 Schema/Table、升级 IAM Password Credential 约束、获取 PostgreSQL Advisory Lock，并在 `system.maintenance_states` 保存阶段和源主键游标。每个游标与其业务批次在同一事务提交。

中断后使用完全相同的 `migration-id`、数据目录、时区、批大小和 Override 文件重新执行 `apply`。源结构/行数指纹或配置摘要不同会拒绝恢复。不要删除 Checkpoint 后直接重跑；需要从头开始时，应先恢复迁移前 PostgreSQL/S3 备份，再使用新的 Migration ID。

完成后再次对账：

```bash
uv run python -m tools.bancho_migration verify \
  --migration-id prod-2026-07-29 \
  --data-dir /srv/bancho \
  --source-timezone Asia/Shanghai \
  --report /srv/migration/verify.json
```

确认 JSON Report 没有 Error，抽查账户登录、谱面下载、Replay 下载、排行榜和赛事图池后再启动应用写入进程。迁移不自动删除旧源库或 `.data` 文件。

## 一致性规则

- `.osu` 必须匹配源 MD5，并同时保存 SHA-256；不制造缺失 Revision。
- `.osr` 计算 SHA-256 后按账户与内容摘要写入对象存储；缺失 Replay 不影响 Score 事实。
- `verify` 会重新流式读取已迁移的谱面与 Replay 对象，并对照本地源文件、Target Manifest、Size 和 SHA-256。
- Legacy PP/Star Rating 绑定禁用的 `legacy-bancho-*` Formula 和不可变 Release，不冒充当前 Calculator 结果。
- 每个导入 Score 有确定性 PlayAttempt、基础已验证 Attestation 和历史 `score.accepted.v1` 事件；Ranking 使用当前活动 Policy 重建。
- SB/旧遥测表即使存在，也只在报告中注明被排除，永远不会查询或映射。
