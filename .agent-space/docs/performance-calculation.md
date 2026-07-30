# Multi-PP Formula 与外部 Calculator 对接规范

最后更新：2026-07-30。

本文定义 perfcho 多 PP 系统的方法论、持久化身份、任务与事务语义，以及 C#/Rust 外部 Calculator 必须实现的 HTTP 合同。代码事实对应 `modules/performance`、`infra/db/repositories/performance/`、`infra/db/relays/performance_job.py`、`infra/upstream/performance_calculator.py`、`tasks/performance_calculation.py`、`worker.py` 和 Scoring ORM。

## 1. 目标与非目标

目标：

- 一个 Score 可以在多个 Formula、同一 Formula 的多个历史 Release 下保留不同 PP。
- osu!lazer C# 官方算法和 perfcho Rust 自研算法通过同一协议接入。
- 成绩接收不依赖 Calculator 可用性，计算失败不能回滚已接受 Score。
- 每个结果都能追溯 Formula、Calculator、Release、Difficulty Release、输入和输出摘要。
- Release 更新不覆盖历史结果，Ranking Policy 始终引用精确 Release。

非目标：

- Calculator 不拥有 Score、Ranking 或 Job 状态。
- Calculator 不连接 perfcho PostgreSQL 或 Redis，不直接写 `score_performances`；它只通过短期签名 URL 读取 S3 Beatmap，也可以自行按 SHA-256 缓存。
- Calculator 不消费 Taskiq/Redis 消息，也不主动扫描未完成 Job。
- 不在 C# 与 Rust 之间对同一 Release 自动降级；不同实现必须使用不同 Formula 或 Release。
- 当前不提供 Formula/Release 管理 HTTP API，也不 Bootstrap 虚假的默认 Release。

## 2. 核心方法论

### 2.1 三层身份

| 概念 | 责任 | 示例 |
| --- | --- | --- |
| Calculator | 一组可部署、可路由的计算实现，使用稳定 Code 标识 | `osu-lazer-dotnet`、`perfcho-rust` |
| Formula | 用户、Ranking 和产品语义可选择的计算系统 | `official`、`custom-relax`、`custom-aim` |
| Calculation Release | Formula 在某个 Ruleset 下的不可变算法制品与配置 | `official/osu/2026.07.1` |

一个 Formula 固定一个 Calculator Code。一个 Calculator 可以承载多个 Formula，例如同一个 Rust 进程按 `formula_code` 路由多个自研算法。

Formula 通过 `calculation_formula_scoreboards` 声明适用 Scoreboard。Release 按 `(formula_id, ruleset)` 单活，但所有历史 Release 永久保留。Performance Release 必须固定一个 Difficulty Release，当前实现要求二者属于同一个 Calculator Code。

### 2.2 何时创建新 Formula 或新 Release

以下变化创建新 Formula：

- PP 的产品语义、排名含义或目标人群改变。
- 官方、Relax、自研 Aim 等结果需要被用户同时选择或展示。
- 结果之间不承诺同一算法的版本演进关系。
- Calculator 家族改变，并且不再承诺与原 Formula 保持同一语义。

以下变化创建同一 Formula 的新 Release：

- C#/Rust 代码、osu! ruleset 依赖、Beatmap Parser 或计算依赖升级。
- 公式参数、Mod 处理、Legacy/Lazer Score 解释、舍入或 Breakdown Schema 改变。
- Difficulty 算法或其配置改变。
- OCI Image、Native Binary 或依赖锁文件改变。

禁止使用可变的 `latest` 表示 Release。`version` 是可读标识，`artifact_digest` 和 `configuration_digest` 才是不可变制品身份；二者均保存原始 32 字节 SHA-256。

### 2.3 多值与唯一性

| 数据 | 唯一键 | 语义 |
| --- | --- | --- |
| Performance Job | `(score_id, release_id)` | 同一 Score/Release 最多一个持久任务 |
| Score Performance | `(score_id, release_id)` | 同一 Score/Release 最多一个最终结果 |
| Difficulty Attribute | `(beatmap_revision_id, scoreboard_id, mod_set_id, release_id)` | 同一 Difficulty Release 输入最多一个结果 |

不能增加 `(score_id, formula_id)` 唯一约束。同一 Formula 升级 Release 后，新旧 PP 必须并存以支持回滚、对比、重建和审计。

## 3. 中心编排流程

### 3.1 成绩事务

Score 接受事务不调用任何 Calculator。它完成 Score、Hit Statistics、Replay、Attestation 等权威事实后，选择满足以下条件的所有 Performance Release：

- Formula `kind = performance` 且 `enabled = true`。
- Formula 已关联当前 Scoreboard。
- Release `active = true` 且 Ruleset 与 Scoreboard 一致。
- Performance Release 已固定 `difficulty_release_id`。

事务为每个目标 Release 幂等插入一个 Job，然后提交 `score.accepted.v1`。没有可用 Formula/Release 时 Score 仍正常接受，PP 保持 Pending，不能写零值伪装完成。

### 3.2 Worker Relay 与 Taskiq

独立 Performance Relay 使用 `FOR UPDATE SKIP LOCKED` 批量领取 Pending Job，或重新领取 Lease 已过期的 Running Job。每次领取生成新的 `lease_token`，Redis 只传输：

```text
job_id + lease_token
```

PostgreSQL Job 是恢复来源。Redis Stream 丢失、入队重复或 Worker 崩溃不会丢失业务工作。入队失败只增加 `enqueue_count`，不消耗业务 `attempt_count`。

### 3.3 三段事务

Worker 必须按以下顺序执行：

1. 短事务锁定 Job，验证 Fencing Token 与 Lease，同一 Token 只增加一次 `attempt_count`，记录 `attempt_started_at`，从任务真正开始时刷新执行 Lease，加载不可变 Score/Release/Beatmap 输入并保存 Input Digest，然后提交。
2. 无数据库事务地生成短期 S3 GET URL 并调用外部 Calculator；Calculator 自己拉取或命中本地 SHA-256 Cache，Worker 不读取 Beatmap 字节。
3. 短事务重新锁定 Job，验证 Fencing Token、执行 Lease、Score/Release 维度和 Input Digest，写 Difficulty、Score Performance、Job 完成状态和 `score.performance-calculated.v1`，然后提交。

perfcho Worker 始终不读取或转发 Beatmap 字节。Revision 必须已经关联 S3 Asset，Calculator 请求只接收短期 `beatmap_url`。URL 生成失败、对象缺失或 Calculator 无法读取时不切换数据来源，而是按错误分类重试或结束 Job。

禁止在普通 Outbox Consumer 事务中调用 Calculator。Outbox Consumer 会锁定 Delivery，网络调用会放大事务时长、连接占用和分区头部阻塞。

### 3.4 完成与 Ranking

完成事件包含：

```json
{
  "score_id": 100,
  "scoreboard_id": 1,
  "formula_id": "019f...",
  "formula_code": "official",
  "release_id": "019f...",
  "pp": "321.12345",
  "output_digest": "64-char-lowercase-hex"
}
```

`ranking-projector.v1` 同时消费 `score.accepted.v1` 和 `score.performance-calculated.v1`，并投影该 Scoreboard 的全部活动 Ranking Policy。PP Policy 只读取自身 `calculation_release_id` 对应的结果；缺失时记为 `performance_pending`，不能回退到其他 Formula。Stable 查询只读取 `is_default = true` 的 Policy。

### 3.5 多 PP 查询语义

`PerformanceQueryService.list_for_score(score_id)` 返回该 Score 的全部 Formula/Release 结果，包括 Formula Code/Name、Calculator、Release Version、Release 是否活动、PP 和 Breakdown。它不会选取“创建时间最新的一条”。

协议适配器应采用以下一种显式选择方式：

- 展示比较页面时返回全部 Formula，并按 Formula Code 和 Release Version 分组。
- Formula 页面按明确 `formula_code` 选择其活动 Release。
- 排行榜和 Stable 默认值通过 Ranking Policy 选择精确 Release。
- 历史回放或审计按明确 `release_id` 读取，不跟随当前活动 Release。

当前尚未提供公开 Multi-PP HTTP Query Route；外部 Calculator 合同与客户端查询合同是两个独立边界，不能让 Calculator 承担查询职责。

## 4. 外部 HTTP 合同

### 4.1 路由与部署配置

Calculator 必须实现：

```http
POST /v1/performance/calculate
Content-Type: multipart/form-data
```

perfcho 通过 Calculator Code 查找基础 URL：

```env
PERFORMANCE_CALCULATOR_URLS={"osu-lazer-dotnet":"http://calculator-dotnet:6001","perfcho-rust":"http://calculator-rust:6002"}
PERFORMANCE_HTTP_TIMEOUT_SECONDS=30
PERFORMANCE_BEATMAP_URL_EXPIRY_SECONDS=600
S3_PRESIGN_ENDPOINT_URL=http://minio:9000
```

实际请求 URL 为 `{base_url}/v1/performance/calculate`。Code 未配置是不可重试部署错误，Job 进入 Dead 状态，不会改用其他 Calculator。

### 4.2 Multipart Part

请求只包含 `metadata`，不得提供 `beatmap` 或其他 Part：

| Part Name | Filename | Content-Type | 内容 |
| --- | --- | --- | --- |
| `metadata` | `metadata.json` | `application/json` | UTF-8 JSON 计算输入 |

Calculator 不需要长期 S3 凭据。它通过 Metadata 的 `beatmap_url` 直接读取对象，并可按 `beatmap_sha256` 缓存解析结果；URL 必须只在内部网络可达且短期有效。Calculator 必须验证下载内容与 `beatmap_sha256` 一致，对象缺失或摘要不一致时不得计算。

Calculator Cache 由实现自行管理，但 Key 必须遵守以下边界：

- 原始 `.osu` 字节可以只按 `beatmap_sha256` 缓存。
- Parsed Beatmap 至少按 `(beatmap_sha256, parser/artifact version)` 缓存，不能跨不兼容 Parser Release 复用。
- Difficulty Attributes 至少按 `(beatmap_sha256, difficulty_release_id, ruleset, variant, canonical mods)` 缓存。
- Score Performance 仍由 perfcho 按 Score/Release 持久化；Calculator 不应建立用户或 Score 权威缓存。
- Cache Miss、Cache Eviction 和进程重启不得改变计算结果，只影响延迟。

本地合同测试可使用：

```bash
curl --fail-with-body \
  -X POST http://127.0.0.1:6001/v1/performance/calculate \
  -F 'metadata=@metadata.json;type=application/json'
```

### 4.3 Metadata 请求

```json
{
  "schema_version": 1,
  "job_id": "019f0000-0000-7000-8000-000000000001",
  "score_id": 100,
  "formula_id": "019f0000-0000-7000-8000-000000000002",
  "formula_code": "official",
  "calculator": "osu-lazer-dotnet",
  "release_id": "019f0000-0000-7000-8000-000000000003",
  "release_version": "2026.07.1",
  "artifact_digest": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "release_configuration": {
    "score_system": "lazer"
  },
  "difficulty_formula_id": "019f0000-0000-7000-8000-000000000004",
  "difficulty_formula_code": "official-difficulty",
  "difficulty_release_id": "019f0000-0000-7000-8000-000000000005",
  "difficulty_release_version": "2026.07.1-difficulty",
  "difficulty_artifact_digest": "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
  "difficulty_release_configuration": {},
  "input_digest": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  "beatmap_revision_id": 501,
  "beatmap_sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
  "beatmap_url": "http://minio:9000/perfcho/beatmaps/501.osu?X-Amz-Signature=...",
  "ruleset": "osu",
  "variant": "vanilla",
  "mod_set_id": 21,
  "mods": [
    {
      "acronym": "DT",
      "settings": {
        "speed_change": 1.5
      }
    }
  ],
  "client_family": "lazer",
  "score": {
    "total_score": 987654,
    "classic_score": 654321,
    "accuracy": "0.987500000",
    "max_combo": 1234,
    "outcome": "passed",
    "hits": [
      {
        "hit_result": "great",
        "actual": 500,
        "maximum": null
      },
      {
        "hit_result": "miss",
        "actual": 2,
        "maximum": null
      }
    ]
  }
}
```

示例中的 `input_digest` 是 32 字节 SHA-256 的 64 个小写十六进制字符。

字段规则：

| 字段 | 规则 |
| --- | --- |
| `schema_version` | 当前必须为整数 `1` |
| UUID 字段 | 标准 UUID 字符串；Calculator 不应把它们转换为业务主键 |
| Digest 字段 | 32 字节 SHA-256 的 64 字符小写 Hex，不带 `sha256:` 前缀 |
| `ruleset` | `osu`、`taiko`、`fruits`、`mania` |
| `variant` | `vanilla`、`relax`、`autopilot` |
| `mods` | Lazer-first Canonical Mod JSON；`settings` 缺失等价于空对象 |
| `client_family` | 当前来源协议事实；至少可能为 `stable` 或 `lazer` |
| `accuracy` | `[0, 1]` 的十进制字符串，不是百分比 |
| `hits[].maximum` | 整数或 `null`；结果名称是可扩展 Snake Case，不能只硬编码 osu!standard 四种 Hit |
| `release_configuration` | Formula Release 的不可变 JSON 配置，Calculator 必须按发布约定解释 |
| `beatmap_url` | 必填的非空短期签名 GET URL；是可变运输字段，不属于算法输入 |

`grade`、`perfect`、Client Flags、Online Checksum、Replay 和 Account ID 不属于当前 PP 输入，不会发送给 Calculator。需要 Replay 的反作弊或自研分析应使用独立任务边界，不能偷偷扩展 v1 请求。

### 4.4 Input Digest

Input Digest 由 perfcho 计算并持久化。摘要覆盖 Formula/Release/Difficulty 身份与配置、Beatmap Revision/SHA-256、Ruleset、Variant、Mod Set、Canonical Mods、Client Family、Score 数值和 Hit Statistics。

以下运输或数据库实例身份不进入摘要：

- `job_id`
- `score_id`
- `input_digest` 自身
- Beatmap S3 Storage Key
- 每次请求生成的 `beatmap_url`

perfcho 使用 UTF-8、JSON Key 排序、无额外空白和 ASCII Escape 生成摘要输入，再计算 SHA-256。Calculator 必须原样回显 `input_digest`。由于跨语言浮点 JSON 序列化可能不同，v1 不强制 Calculator 重新生成该摘要；它应以字段校验、Beatmap SHA-256 和自身确定性测试验证输入。

### 4.5 成功响应

成功响应使用 JSON，建议返回 HTTP `200`：

```json
{
  "schema_version": 1,
  "calculator": "osu-lazer-dotnet",
  "release_version": "2026.07.1",
  "artifact_digest": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "difficulty_release_version": "2026.07.1-difficulty",
  "difficulty_artifact_digest": "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
  "input_digest": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  "difficulty": {
    "star_rating": "6.543219",
    "max_combo": 1234,
    "attributes": {
      "aim": 3.2001,
      "speed": 2.9912
    }
  },
  "performance": {
    "pp": "321.123456",
    "breakdown": {
      "aim": 200.1,
      "speed": 121.023456
    }
  }
}
```

perfcho 会逐项验证：

- `schema_version`
- `calculator`
- `release_version` 和 `artifact_digest`
- `difficulty_release_version` 和 `difficulty_artifact_digest`
- `input_digest`

任一字段缺失或不匹配均为不可重试的无效响应。`pp` 和 `star_rating` 必须是非空十进制字符串、有限且非负；`max_combo` 必须是非负整数；`attributes` 和 `breakdown` 必须是 JSON Object。

perfcho 将 PP 和 Star Rating 量化到五位小数后持久化。外部服务不能依赖传入更多小数被原样保存。

### 4.6 Output Digest 与确定性

Calculator 不返回 Output Digest。perfcho 对已经校验和五位量化的以下结果生成 SHA-256：

- PP
- Star Rating
- Difficulty Max Combo
- Difficulty Attributes
- Performance Breakdown

相同 `(score_id, release_id)` 或相同 Difficulty Key 已存在结果时，所有值和 Digest 必须一致。不一致表示同一个 Release 产生了非确定输出，中心应用拒绝覆盖并将 Job 置为 Dead。不得用“最后一次结果”覆盖第一次结果。

## 5. 错误与重试合同

| 场景 | Calculator/HTTP 行为 | perfcho 行为 |
| --- | --- | --- |
| 正常完成 | `200` + 合法 JSON | 原子写结果和完成事件 |
| 暂时过载 | `429` | 有界指数退避重试 |
| Calculator 暂时故障 | `5xx` | 有界指数退避重试 |
| 网络错误或超时 | 连接失败/超时 | 有界指数退避重试 |
| S3 暂时不可访问 | Calculator 返回 `502` 或 `503` | 使用新签名 URL 有界重试 |
| S3 对象缺失或 Beatmap SHA-256 不匹配 | Calculator 返回 `422` | 不重试，Job Dead |
| Revision 未关联 Asset 或签名 URL 无法生成 | 不调用 Calculator | 有界重试，达到上限后 Job Dead |
| 请求字段或 Mods 不支持 | `400` 或 `422` | 不重试，Job Dead |
| 身份或权限错误 | `401` 或 `403` | 不重试，Job Dead |
| Release 指纹不匹配 | `409` | 当前实现不重试，Job Dead |
| 非 JSON、字段缺失、NaN/Infinity、负值 | 无效成功响应 | 不重试，Job Dead |
| 同 Release 输出不一致 | 任意合法响应 | 拒绝覆盖，Job Dead 并告警 |

达到 `PERFORMANCE_CALCULATION_MAX_ATTEMPTS` 后，Relay 会把仍可重试的 Job 转为 Dead。Calculator 必须是无副作用纯函数；请求可能因超时、Worker 崩溃或 Broker Ack 丢失而重复到达。

## 6. C# 与 Rust 实现要求

每种实现都应遵循相同处理顺序：

1. 限制 Multipart 总大小和 Part 数，只接受唯一的 `metadata` Part。
2. 严格解析 Metadata v1，拒绝未知 Formula、Ruleset、Variant 或不支持的 Mod Settings。
3. 验证 `calculator` 与当前进程身份一致。
4. 验证 Performance/Difficulty Version 和 Artifact Digest 与当前部署制品一致。
5. 先按 SHA-256 查询本地 Cache；Cache Miss 时读取 `beatmap_url` 并验证内容 SHA-256。
6. 临时 S3 或网络故障返回 `502/503`，对象缺失或摘要不匹配返回 `422`，不得请求 Worker 上传字节。
7. 按 `formula_code` 和两个 Release Configuration 构造算法输入。
8. 在同一次请求中产生 Difficulty 和 Performance，避免不同 Parser/依赖组合。
9. 返回有限非负值和版本指纹，不保存 Job、Score 或用户状态；Beatmap Cache 只能按内容摘要保存可重建内容。

C# 官方实现应锁定 osu! ruleset 源码/NuGet 依赖和运行时版本，并把最终容器或发布制品 SHA-256 登记为 Artifact Digest。Rust 自研实现应锁定 Cargo.lock、Feature、Target 和二进制/容器 Digest。仅记录 Git Branch 或 SemVer 不足以证明运行制品。

同一 Formula 的不同 Release 应保持相同 Breakdown 字段语义；必须改变字段语义时，应升级 Release Configuration 中的 Breakdown Schema 标识。不同 Formula 的 Breakdown 可以不同，调用方不得跨 Formula 假设字段相同。

## 7. 安全边界

当前 `HttpPerformanceCalculator` 不发送 Authorization Header，也没有 mTLS 配置。因此当前强制部署要求是：

- Calculator 只监听受控内部网络或本机回环地址。
- 使用防火墙、容器 Network Policy 或 Sidecar 边界阻止公网访问。
- Calculator 不信任客户端来源，只有 perfcho Worker 可以访问。
- 日志不得记录完整 Metadata、Replay、凭据或原始用户标识；建议只记录 Job ID、Formula Code、Release Version、Input Digest 前缀、耗时和状态。
- Presigned URL 属于短期访问凭据，不得写入日志、指标 Label、错误响应或持久 Cache Metadata。

生产公网或跨集群部署前必须先扩展客户端支持 mTLS 或服务 Token，并把认证失败、密钥轮换和证书生命周期纳入合同。不能在未修改 perfcho 适配器的情况下自行要求一个未发送的 Header。

## 8. 推荐运维接口

以下接口当前不是 perfcho 强制调用合同，但建议 Calculator 提供：

| 路由 | 用途 |
| --- | --- |
| `GET /healthz` | 仅表示进程可接受请求，不检查 perfcho 数据库 |
| `GET /v1/capabilities` | 返回 Calculator Code、支持的 Formula/Ruleset/Variant、Release Version 和 Artifact Digest |

Capabilities 可用于部署前探测，但不能替代每个计算响应中的指纹回显。健康检查不得动态下载“最新版算法”或改变进程内算法版本。

建议监控：

- Pending Job 数量和最老 `available_at` 年龄
- Running Lease 过期数量
- Dead Job 数量及 `last_error` 分类
- 每个 Calculator/Formula 的请求数、延迟、超时率、`4xx`、`5xx`
- Input/Output 不一致次数
- Formula/Release 计算覆盖率
- Ranking Policy 所需 Release 的缺失结果数

## 9. Release 发布流程

1. 构建固定依赖的 C# 或 Rust 制品，计算 Artifact SHA-256。
2. 使用金样本验证 Beatmap Parsing、Mods、Difficulty、PP、舍入和 Breakdown。
3. 创建或确认 Formula，固定 Calculator Code，并关联适用 Scoreboard。
4. 创建 Difficulty Release，再创建引用它的 Performance Release；初始保持非活动。
5. 在目标环境配置 `PERFORMANCE_CALCULATOR_URLS`，验证制品返回的指纹。
6. 对有限 Score 创建 Canary/Shadow Job，确认成功率和结果确定性。
7. 批量 Backfill 新 Release，等待目标 Score 覆盖率达标并处理 Dead Job。
8. 创建或升级 Ranking Policy，使其引用精确 Performance Release；重建新 Projection Generation。
9. 原子切换默认 Policy 或活动 Projection，保留旧 Release、旧 Performance 和回滚路径。

当前尚未实现 Formula/Release 管理 Command、Backfill Command 和 Capabilities 探测。正式发布前应实现这些入口，避免直接手工修改生产表。现有数据库还需注意 `create_all()` 不会修改旧表，结构升级必须使用显式迁移。

## 10. 对接验收清单

- Calculator 能处理 v1 Multipart 请求并拒绝多余/缺失 Part。
- 请求只通过 `beatmap_url` 拉取并按 SHA-256 Cache，不接受 perfcho 上传字节。
- S3 临时故障返回 `502/503`，对象缺失或 Digest 不匹配返回 `422`。
- C#/Rust 返回的 Calculator、Release、Difficulty 和 Artifact 指纹完全匹配。
- Calculator 校验 Beatmap SHA-256，不依赖文件名识别谱面。
- Stable/Lazer Canonical Mods 和带 Settings 的 Mod 均有合同测试。
- 四个 Ruleset 的动态 Hit Result Name 不被错误截断或硬编码。
- PP/Star Rating 使用十进制字符串，NaN、Infinity、负值被拒绝。
- 相同请求重复执行得到相同五位量化结果和 JSON Breakdown。
- `429/5xx/timeout` 可恢复，`400/401/403/409/422` 不会无限重试。
- Calculator 下线时成绩提交仍成功，Job 保持 Pending/Retrying。
- Worker 在 HTTP 前后崩溃都能通过 Lease/Fencing 恢复且不重复写结果。
- 同一 Score 同时存在 C# 官方和 Rust 自研 Formula 结果，查询不会选择任意“最新值”。
- Ranking Policy 只消费指定 Release，默认 Stable Policy 不受 Shadow Formula 影响。
