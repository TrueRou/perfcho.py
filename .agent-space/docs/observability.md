# 运行日志与可观测性

## 输出

perfcho 的 API、Worker 和迁移命令使用同一个事件日志契约：

- 生产默认输出 JSON；开发默认输出可读文本。
- API/Worker 日志写 stdout，迁移命令日志写 stderr；迁移命令的最终报告摘要仍写 stdout。
- 每条记录包含 `event_schema`、`service`、`process_role`、`pid`、`event`、级别，以及事件白名单中的字段。
- 开发文本中的 Worker 标签显示为 `worker[PID]`；PID 从每条记录的实际发送进程获取。
- Request、Command、Outbox Event、Job、Session、Room 等内部标识符可以保留，用于和 PostgreSQL 事实关联。
- Worker 执行 Taskiq 任务时，标准库日志的事件名使用实际 relay task name，并附带 `relay_task`；Taskiq 通用的 task id 执行日志不输出，任务外的第三方库日志仍使用 `library.log`。
- Outbox Delivery 日志记录 `event_type`、聚合身份、`schema_version`、`source_position` 和 `payload_fields`；只有重试或 Dead Letter 失败日志包含完整 `outbox_payload`，成功热路径不重复序列化事件正文。
- Relay 批次事件记录 `claimed`、`enqueued` 和 `enqueue_failed`；enqueue outcome 的批量落库失败单独记录 `outcome_persist_failed`，便于区分 Broker 投递问题与 PostgreSQL 回写问题。
- 未预期异常通过 Loguru 保留类型、原始消息和完整 traceback，开发文本与生产 JSON 使用同一异常信息。预期的协议状态转换（例如 Stable 会话过期、Realtime TTL 到期和短 Mailbox Lease 冲突）只记录错误类型与错误码，不附带 exception context，避免把正常重连显示为 traceback。`backtrace` 开启，`diagnose` 关闭，避免把调用栈局部变量中的凭据写入日志。

日志必须通过 `perfcho.infra.logging.log_event()` 发送。该函数会丢弃未注册字段；确需诊断的异常必须传入 `exception=error`，预期状态转换只传 `error_code` 和 `error_type`。不要直接把命令、请求、ORM 对象或协议载荷交给 Logger。

## 级别

- `INFO`：进程 ready/stopped、登录成功、房间和回合生命周期、成绩/计算等关键持久状态提交，以及成功的 Outbox Delivery 摘要。
- `WARNING`：外部依赖失败、有限重试、Redis 降级、容量问题和部分清理失败。
- `ERROR`：未预期异常、启动失败、Dead Letter、不可恢复的迁移或不变量失败。
- `DEBUG`：幂等重放、读操作、对象存储成功、Packet/Frame 汇总和高频状态变化。

空 Relay 轮询、Taskiq Redis 空消息拉取、Redis 心跳、Presence 查询、Slot 更新和逐帧数据不会产生日志。

性能观测应按 `(consumer, partition_key)` 统计 Delivery 延迟和 Dead Letter；成绩分区包含账户与 Scoreboard，禁止把账户 ID、Token、请求正文或 Presence payload 作为指标标签。Content Sync 使用并发上限配置，观测应关注上游 fetch、对象存储 put 和最终短事务的独立耗时。

## 采样与限频

成功 HTTP 请求、Bancho Poll、Packet 汇总和消息成功状态分别受配置采样率控制。服务器生成的采样 UUID 与客户端可传入的 `X-Request-ID` 分离，客户端不能操纵采样结果。预期认证失败、输入拒绝和 Redis 故障按低基数事件限频；未预期的 5xx、慢请求和 Dead Letter 不采样。

## 禁止字段

禁止主动记录用户名、邮箱、IP、User-Agent、密码、密码 Token、Session/Broker/Admission/房间 Token、设备分量、请求 Query、请求/响应正文、聊天内容、Away 文本、Replay/Frame 字节、S3 Key、签名 URL、DSN、Secret 和 SQL。异常消息与 traceback 必须完整保留，因此抛出异常时不得把上述敏感值拼入异常消息。

迁移 JSON 报告是离线诊断产物，不是实时日志；报告文件使用 `0600` 权限。报告中的诊断也不能被用于替代日志脱敏规则。

## 事件边界

请求中间件在最终响应 Body 发送后记录完成事件，流式对象读取失败不能记录为成功。应用服务只在事务提交后记录持久状态；Relay 和 Performance Job 只有在失败状态事务提交后记录 retry/dead。低层 PostgreSQL、Redis、S3 和 HTTP Adapter 记录连接、Provider 状态码、操作和耗时，业务语义由上层 Service 记录。
