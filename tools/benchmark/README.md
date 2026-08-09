# Benchmark Tooling

This package contains domain-neutral benchmark infrastructure and domain-specific workload adapters.

## Design Boundaries

- `models.py` defines benchmark configuration, scenario protocols, and result models.
- `runner.py` manages warmup, concurrent workers, deadlines, and error counting.
- `metrics.py` calculates percentiles, throughput, and error rates.
- `report.py` writes consistent JSON and Markdown reports.
- `scoring.py` provides scoring leaderboard workloads using an isolated PostgreSQL schema.

To add another domain, implement `BenchmarkScenario`. The runner and reporting layers should not require changes.

## Running Benchmarks

Run from `workspace/perfcho.py`:

```bash
uv run python -m tools.benchmark \
  --players 50000 \
  --concurrency 50 \
  --warmup 5 \
  --duration 30 \
  --output artifacts/benchmark/scoring
```

Pass `--redis-url` to include the Redis ZSET comparison workloads:

```bash
uv run python -m tools.benchmark \
  --redis-url redis://127.0.0.1:56379/0
```

The command writes:

- `artifacts/benchmark/scoring.md`: a concise human-readable report.
- `artifacts/benchmark/scoring.json`: structured benchmark results.

By default, the tool creates and removes a unique `perfbench_scoring_<id>` schema. Do not run benchmarks against a
production database. Use an isolated database that matches the production topology as closely as possible.

## Scoring Workloads

- `postgres-personal-rank` measures the current `COUNT WHERE performance > value` query shape.
- `postgres-top-n` measures the PostgreSQL global Top 50 query.
- `redis-personal-rank` measures Redis ZSET `ZREVRANK`.
- `redis-top-n` measures Redis ZSET `ZREVRANGE WITHSCORES`.

This tool provides database and Redis microbenchmarks. It does not replace HTTP load testing. Production decisions
should also consider PostgreSQL `EXPLAIN (ANALYZE, BUFFERS)`, connection pool wait time, CPU utilization, Redis resource
usage, projection lag, and end-to-end API latency.
