# 数据库运维

## 本地依赖

启动 PostgreSQL 17、Redis 8 与 MinIO，并初始化对象存储桶：

```bash
docker compose up -d --wait postgres redis minio
docker compose run --rm --no-deps minio-init
```

PostgreSQL 监听 `127.0.0.1:55432`。Redis 监听 `127.0.0.1:56379`，DB 0 保存在线状态，DB 1 承载 Taskiq Stream。MinIO API 监听 `127.0.0.1:59000`，控制台监听 `127.0.0.1:59001`。开发库为 `perfcho`，集成测试库为 `perfcho_test`。只有需要覆盖本地默认值时才创建 `.env`。

VS Code 中选择 `perfcho: all processes` 后按 F5，会并行执行依赖同步与 Compose 基础设施启动，待 PostgreSQL、Redis、MinIO 健康后幂等初始化对象存储桶，最后同时调试 API、Outbox Relay 和 Taskiq Worker。结束调试只停止三个应用进程，基础设施保持运行；不再需要时执行 `docker compose down`。

SQLAlchemy Metadata 是应用内数据库结构契约。API、Relay、Taskiq 任一角色连接数据库时，都会在 PostgreSQL 事务级 Advisory Lock 内创建缺失的领域 Schema，并调用 `MetaData.create_all()` 创建缺失表。多个角色同时首次启动时只有一个执行初始化，其余角色等待后复查现有结构。

`create_all()` 不会修改已存在的列、约束或索引。涉及现有结构的模型变更仍需制定显式 SQL 发布与回滚方案，不能把自动建表当作结构演进工具。

Multi-PP 基线新增 `calculation_formulas`、`calculation_formula_scoreboards`、`performance_calculation_jobs`，并修改 `calculation_releases`、`score_performances`、`ranking_policies`。已有开发数据库必须删除并重建 Scoring Schema，或在发布前编写对应 ALTER/数据回填 SQL；仅重启应用不会升级这些旧表。Formula/Release 当前不由 Bootstrap 伪造，部署必须使用真实 Calculator 制品 SHA-256 和配置摘要登记后才能产生 Job。

## 集成验证

```bash
docker compose up -d postgres redis
TEST_DATABASE_URL=postgresql+asyncpg://perfcho:perfcho@127.0.0.1:55432/perfcho_test \
  uv run pytest -m postgres
```

PostgreSQL 标记测试会清空测试 Schema，调用应用数据库引擎两次，并验证全部映射表都已创建且初始化幂等。SQLite 不受支持，因为它无法验证 Schema、`jsonb`、`inet`、部分索引、Identity 行为和 PostgreSQL CHECK 表达式。

## 进程启动

```bash
uv run uvicorn perfcho.main:asgi_app --host 127.0.0.1 --port 8000
uv run taskiq worker perfcho.infra.taskiq:broker perfcho.tasks.outbox perfcho.tasks.performance --ack-type when_executed
uv run python -m perfcho.infra.outbox
```

API、Worker 和 Relay 是同一可信中心应用的进程角色。它们使用相同代码和数据库结构，不注册节点身份。生产环境分别监管进程并进行独立健康检查。

## 生产 Compose

`compose.prod.yaml` 是独立生产拓扑，禁止与包含本地端口和测试库初始化脚本的 `compose.yaml` 合并。根据 `.env.production.example` 创建不提交到 Git 的 `.env.production`，使用 `openssl rand -hex 32` 分别生成数据库、Redis、Password Pepper、Token HMAC 与 Device HMAC 密钥，并配置外部 S3 兼容对象存储凭据，然后执行：

```bash
docker compose --env-file .env.production -f compose.prod.yaml up -d --build
docker compose --env-file .env.production -f compose.prod.yaml ps
```

生产拓扑等待 PostgreSQL/Redis 健康后启动 API/Relay/Taskiq，最先获得数据库初始化锁的角色通过 SQLAlchemy 创建缺失的 Schema 和表。三个应用角色使用同一 Python 3.14t 镜像并独立监管；API 提供 HTTP 健康检查，Relay 与 Taskiq 由主进程退出状态触发重启，并结合最老 Delivery 延迟、Dead Letter 和 Redis Pending Entry 监控判断业务健康。

PostgreSQL 与 Redis 不发布宿主机端口。生产对象存储是 Compose 外部依赖，必须与 PostgreSQL Manifest 的事务点一致备份。API 默认只发布到 `127.0.0.1:8000`，由同机反向代理终止 TLS；必须显式修改 `APP_BIND_ADDRESS` 才能监听其他地址。`perfcho-postgres` 和 `perfcho-redis` 是生产持久卷，执行 `down` 时禁止附带 `--volumes`，除非已确认永久删除数据。

## Redis 运维

- 所有 `perfcho:state:*` 键必须设置 TTL，禁止把账户、成绩、Token 生命周期或任务完成状态只保存在 Redis。
- 单实例启用 AOF 与 `noeviction`。DB 编号只提供命名隔离，不提供内存或故障隔离；连接配置保留独立 URL，未来可直接拆分实例。
- Redis 清空后，在线客户端需要重连；Relay 根据 PostgreSQL 未完成 Delivery 重建 Taskiq 投递。
- 监控内存、过期键数量、Stream 长度、Pending Entry、最老 Delivery 延迟和 Dead Letter 数量。

## 备份与恢复

- 使用 PostgreSQL 原生逻辑或物理备份，并包含所有领域 Schema。
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
