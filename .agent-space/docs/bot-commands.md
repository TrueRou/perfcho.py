# Bot 命令系统

## 范围

Bot 命令内核位于 `src/perfcho/modules/bot/`，是协议无关的注册与执行能力。Stable 适配器只负责把已认证的聊天消息转换为 `BotInvocation`，并把 `CommandResult` 编码为 Stable 数据包。

默认目录包含：

- 基础命令：`help`、`roll`、`server`、`reconnect`、`quit`
- 多人命令组：`mp`
- 图池命令组：`pool`
- Clan 命令组：`clan`

`pool` 和 `clan` 中尚无对应的规范应用服务时，命令保留参数校验并返回明确的未实现响应，不直接访问数据库。

命令所有权按领域划分：

- `modules/bot/commands.py`：Bot 自身的基础命令
- `modules/multiplayer/commands.py`：`mp` 与赛事图池命令
- `modules/social/commands.py`：`clan` 协议兼容命令
- `infra/wiring/stable.py`：组合 Stable 进程使用的各领域 CommandDefinition/CommandGroup

`BotCommandService` 不导入任何业务领域，也不保存 Multiplayer、Content、Social 或 Authorization 服务集合。领域命令工厂通过闭包绑定自己的应用服务或窄查询函数，`CommandContext` 只携带调用事实和 Registry。

## 命令框架

`CommandRegistry` 负责大小写不敏感的触发器、别名、命令组、帮助文本、权限钩子和异常边界。`CommandBuilder` 支持以下参数形式：

- `<value:type>`：必选参数
- `[value:type]`：可选参数
- `[...values:type]`：剩余参数
- `-f`、`--flag=value` 和带类型值的选项

命令 handler 只能通过所属领域工厂绑定的应用服务执行业务操作。命令模块不导入 FastAPI、Stable Wire Model 或 ORM。

## Stable 语义

公开频道中的命令消息先按普通公开消息持久化，再执行命令；回复发送给当前频道成员。发给 `BanchoBot` 的私信先通过 CommunityService 的权限、Block、Silence 和幂等检查，再执行命令。Bot 回复本身作为实时协议消息发送，不绕过应用服务写入数据库。

`reconnect` 和 `quit` 通过结果中的生命周期指令交给 Stable 适配器处理。命令执行耗时只写结构化日志，不拼接到用户可见回复中。
