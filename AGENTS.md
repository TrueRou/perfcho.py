# Agent Guidelines

## If user ASK you to IMPLEMENT something

go to check .agent-space's background.md to gain all the information you need to understand the requirements and context. Then, implement the requested feature or functionality according to the overall requirements, background, and goals.

## After Every Code Change

Run ruff formatting and auto-fix after every file modification:

```bash
uv run ruff format .
uv run ruff check --fix .
```

## Refactoring

- When refactoring, always update the corresponding tests in `tests/` to match the new structure/signatures.
- Do not leave tests that reference old APIs or removed code.
- Backward compatibility is generally not required — rename, restructure, and break interfaces freely when it improves the codebase.
