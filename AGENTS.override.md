# Codex Instructions for AIstock

This file is for Codex only.

## Ownership

- Do not modify AGENTS.md unless the user explicitly requests it; it may be owned by another tool.
- Use docs/codex_project_memory.md as the Codex project memory for this repository.
- Before architecture analysis, backend changes, frontend changes, data pipeline changes, or trading-related changes, read docs/codex_project_memory.md first.
- Keep future Codex-specific notes in docs/codex_project_memory.md, not in AGENTS.md.

## Project Snapshot

AIstock is an A-share quant research, experiment, and future live-trading platform.

Main services:

- TDX Go backend: tdx-api-main/web/server.go, usually port 19080.
- FastAPI backend: backend/main.py, usually port 8001.
- Next.js frontend: frontend, usually port 3000.

Main backend areas:

- backend/routers: API layer.
- backend/services: business logic.
- backend/data_service: unified data access.
- backend/db/pg_pool.py: PostgreSQL / TimescaleDB connection pool.
- backend/infra: QMT, DeepSeek, WSL Qlib, compute node clients.
- backend/qlib_exporter: Qlib dataset export.

Major subsystems:

- QuantEvolver / QE: backend/services/quantevolver and related routers.
- RD-Agent integration: backend/services/rdagent_* and backend/routers/rdagent*.py.
- Paper trading: backend/services/paper_trading.
- QMT / xtquant: backend/infra/qmt_client.py and backend/routers/qmt.py.
- RL execution: rl_execution.
- Monitoring: monitoring/docker-compose.yml.

For detailed architecture, always read docs/codex_project_memory.md.
