# 数据库运维

## 本地依赖

启动 PostgreSQL 17 与 Redis 8，并应用数据库结构：

```bash
docker compose up -d postgres redis
uv run alembic upgrade head
```

PostgreSQL 监听 `127.0.0.1:55432`。Redis 监听 `127.0.0.1:56379`，DB 0 保存在线状态，DB 1 承载 Taskiq Stream。开发库为 `perfcho`，集成测试库为 `perfcho_test`。只有需要覆盖本地默认值时才创建 `.env`。

当前 `0001` 是重新生成的中心化基线，不提供旧结构升级路径。已有旧开发库必须显式删除并重新创建；不要对需要保留数据的数据库执行这一操作。

## Migration 流程

1. 同时修改 SQLAlchemy Metadata 与测试。
2. 创建显式 Alembic Revision。当前 `0001_initial_schema.py` 从本次中心化基线起禁止修改。
3. 检查约束名、Server Default、Schema 限定、部分索引条件和 Downgrade 顺序。
4. 在空数据库以及上一 Revision 的数据库副本上执行 Upgrade。
5. 执行 `uv run alembic check`，必须不存在 Metadata Drift。
6. Downgrade 只用于集成测试；生产回滚通常应使用新的前向修复 Migration。

应用启动只检查数据库连通性。Migration 必须作为部署任务执行一次，并在新应用实例接收流量前完成。

## 集成验证

```bash
docker compose up -d postgres redis
TEST_DATABASE_URL=postgresql+asyncpg://perfcho:perfcho@127.0.0.1:55432/perfcho_test \
  uv run pytest -m postgres
```

集成测试会清理测试 Schema、升级至 Head、检查 Seed 和关键约束、降级至 Base，然后再次升级。SQLite 不受支持，因为它无法验证 Schema、`jsonb`、`inet`、部分索引、Identity 行为和 PostgreSQL CHECK 表达式。

## 进程启动

```bash
uv run uvicorn perfcho.main:asgi_app --host 127.0.0.1 --port 8000
uv run taskiq worker perfcho.infra.taskiq:broker perfcho.tasks.outbox --ack-type when_executed
uv run python -m perfcho.infra.outbox
```

API、Worker 和 Relay 是同一可信中心应用的进程角色。它们使用相同代码和数据库结构，不注册节点身份。生产环境分别监管进程并进行独立健康检查。

## Redis 运维

- 所有 `perfcho:state:*` 键必须设置 TTL，禁止把账户、成绩、Token 生命周期或任务完成状态只保存在 Redis。
- 单实例启用 AOF 与 `noeviction`。DB 编号只提供命名隔离，不提供内存或故障隔离；连接配置保留独立 URL，未来可直接拆分实例。
- Redis 清空后，在线客户端需要重连；Relay 根据 PostgreSQL 未完成 Delivery 重建 Taskiq 投递。
- 监控内存、过期键数量、Stream 长度、Pending Entry、最老 Delivery 延迟和 Dead Letter 数量。

## 备份与恢复

- 使用 PostgreSQL 原生逻辑或物理备份，并包含所有领域 Schema 和 `alembic_version`。
- 回放与媒体对象存储必须和 Manifest 数据库事务点一起备份。
- 恢复测试除了行数外，还必须验证 Replay Hash 与 Media Storage Key。
- 加密和 HMAC Key 在 PostgreSQL 外管理；备份必须保存对应 Key Version 的恢复关系。
- Redis 在线状态不进入业务备份；AOF 只用于缩短队列恢复时间，PostgreSQL Outbox 才是未完成任务的恢复依据。

## 后续业务层必需的运维任务

- 过期 Session、Token、Challenge、Redis 在线状态与 Matchmaking Ticket；
- 使用 `FOR UPDATE SKIP LOCKED`、租约和有界重试发布 Outbox Delivery；
- 重建并核对成绩、排行榜和统计投影；
- 检查对象存储 Manifest，并标记丢失的 Replay 或 Media Asset；
- 同步上游谱面，但不替换不可变谱面修订；
- 幂等结算每日挑战和排名快照；
- 按审批后的保留策略匿名化账户并清理安全数据；
- 监控 Identity 序列余量、索引膨胀、Autovacuum 延迟和 Projector Watermark。
