# perfcho.py 文档索引

最后更新：2026-07-29。

本目录记录项目的架构约束、已经落地的事实、协议适配范围和后续设计。阅读时应区分以下三类状态：

- **已接线**：生产组合根或进程入口已经构造并调用该能力。
- **已实现未接线**：模型、服务或基础设施适配器已经存在并有测试，但生产入口尚未使用。
- **仅设计**：文档、数据库预留或 Wire Model 已存在，应用服务和生产流程尚未完成。

代码是实现事实的最终来源。若文档与代码冲突，应先核对代码并在同一次修改中修正文档，不能靠兼容层掩盖偏差。

## 推荐阅读顺序

1. [项目背景](../background.md)：目标、参考资料边界和模块化单体总原则。
2. [当前实现总览](current-implementation.md)：当前代码有哪些进程、模块、持久化边界和已知限制。
3. [Stable 适配器支持矩阵](stable-adapter.md)：当前支持的 HTTP 路由、Bancho Packet、状态语义和缺口。
4. [剩余设计与交付路线](remaining-design.md)：Multiplayer 后续验证、PP、Lazer 和投影体系的实施顺序。
5. [实现进度](implementation-progress.md)：适合快速检查的简表和最近验证结果。

## 深入设计

- [业务层架构](business-layer.md)：Command、Query、Actor、Unit of Work、Outbox 和模块依赖规则。
- [运行时架构](runtime-architecture.md)：API、Worker、Outbox Relay、PostgreSQL、Redis 和对象存储的进程关系。
- [数据库架构](database/architecture.md)：Schema、标识符、不可变事实和数据库设计原则。
- [数据库关系](database/relationships.md)：核心实体关系和跨 Schema 依赖。
- [数据库运维](database/operations.md)：Bootstrap、迁移、备份、恢复和维护约束。

## 维护规则

- 新功能必须同时更新 `implementation-progress.md` 和对应支持矩阵。
- “实现了类或表”不等于“功能可用”；只有组合根和协议入口完成接线后才能标为已接线。
- 新 Outbox 事件必须记录事件名、Consumer 名和实际注册位置。未注册 Consumer 的事件生产者不能视为完成。
- Stable 和 Lazer 必须复用协议无关的应用命令与查询；禁止在适配器中复制业务流程。
- PostgreSQL 只记录持久事实，Redis 只记录可丢失且可重建的在线状态。
- 修改代码后运行 `uv run ruff format .` 和 `uv run ruff check --fix .`；涉及行为时还应运行对应测试或完整 `uv run pytest`。
