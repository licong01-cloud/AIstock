# AIstock Codex Project Memory

## Purpose

AIstock is an A-share quantitative research and experiment platform, with future live-trading capabilities. The platform combines local market data, strategy research, AI-assisted factor or strategy generation, paper trading, QMT integration, and frontend dashboards.

This memory is maintained for Codex. Do not use AGENTS.md for Codex-specific notes unless the user explicitly asks for that file to be changed.

## Service Architecture

### 1. TDX Go Backend

- Path: tdx-api-main/web/server.go
- Typical port: 19080
- Role: TDX data service and market data bridge.

### 2. FastAPI Backend

- Path: backend/main.py
- Typical port: 8001
- Role: main application API, orchestration layer, quant workflow backend, local data access, AI workflow backend, and trading adapter gateway.

Important directories:

- backend/routers: FastAPI router layer and API endpoints.
- backend/services: business services and workflow logic.
- backend/data_service: unified data access layer.
- backend/db/pg_pool.py: PostgreSQL / TimescaleDB connection pool.
- backend/infra: external infrastructure clients, including QMT, DeepSeek, WSL Qlib, and compute node clients.
- backend/qlib_exporter: Qlib snapshot and bin export tools.

### 3. Next.js Frontend

- Path: frontend
- Typical port: 3000
- Main routes: frontend/src/app
- Major sections include quantevolver, rdagent, local-data, qmt, paper-trading, watchlist, and analysis.

## Core Subsystems

### QuantEvolver / QE

- backend/services/quantevolver
- backend/routers/quantevolver.py
- backend/routers/quantevolver_evolution.py
- Purpose: AI-assisted strategy or factor evolution workflows.

### RD-Agent Integration

- backend/services/rdagent_*
- backend/routers/rdagent*.py
- rdagent_assets
- Purpose: integration with RD-Agent workflows and generated research assets.

### Paper Trading

- backend/services/paper_trading
- Purpose: simulated trading and strategy validation before live execution.

### QMT / xtquant

- backend/infra/qmt_client.py
- backend/routers/qmt.py
- Purpose: QMT client integration for future live or semi-live trading workflows.

### RL Execution

- rl_execution
- Purpose: reinforcement-learning related execution research.

### Monitoring

- monitoring/docker-compose.yml
- Purpose: Prometheus / Grafana monitoring stack.

## Engineering Rules for Codex

- Do not modify AGENTS.md unless explicitly requested.
- Prefer AGENTS.override.md and docs/codex_project_memory.md for Codex-specific project notes.
- Before making structural changes, inspect the relevant router, service, data access, and frontend route together.
- Treat trading-related code as high-risk: preserve existing behavior unless the user explicitly requests a behavioral change.
- Distinguish research, paper trading, and real execution paths. Do not assume live-trading safety.
- Prefer small, reviewable changes with tests or clear manual verification steps.
- For project-wide searches, prefer rg / rg --files.

## Known Current Workspace Notes

- The existing AGENTS.md in the project root may belong to another programming tool. Avoid editing it.
- If Codex is launched from F:\Dev\AIstock, this AGENTS.override.md file should be read as the Codex-specific project instruction file.
- If Codex is launched from another directory, use --add-dir F:\Dev\AIstock for write access, but project instructions may not be loaded automatically unless the project directory is the working directory.
- Trading Core v2 / Paper Trading v2 restart-safe continuation plan is recorded in `docs/architecture/paper_trading_v2_remaining_execution_plan.md`. Before continuing this work after a restart, read that document. Do not install Torch yet; V24 is deprecated and V25 execution strategy is still under development.
