# AIstock - Project Memory

## Architecture Overview


## Agent Workflow Entrypoints

- Read `docs/codex_project_memory.md` once for AIstock repo/workflow/runtime work, then use exactly one task-specific skill or Claude command; do not load unrelated standards or scenario instructions by default.
- Broad or ambiguous requests start with the lightweight router: Codex `.codex/skills/aistock-task-router/SKILL.md`; Claude `.claude/commands/aistock-task-router.md`.
- BUG fixes use `scripts/aistock_issue_workflow.py` through the issue skill; do not hand-write BUG JSON or skip GitHub sync.
- New non-trivial features use the feature skill/command and `FEATURE-WORKFLOW-001` acceptance ids; read the approved design only after the task is confirmed as feature delivery.
- Codex/Claude keep minimal local validation and delegate broad UI/API/business-flow or cross-module suites to Validation Center/CI/Nightly through the validation-delegation lane.
- Never report simplified, POC, mock-only, static-success, partial, or silent-fallback delivery as complete unless the user explicitly approved the deviation and the acceptance matrix records it.


3-tier full-stack A-share quantitative trading platform.

**3 Services (start_all_ai_stock.bat):**
- TDX Go Backend → port 19080 (`tdx-api-main/web/server.go`)
- FastAPI Backend → port 8001 (`backend/main.py`)
- Next.js 14 Frontend → port 3000 (`frontend/`)

**Database:** TimescaleDB (PostgreSQL)

## Directory Structure Quick Reference

```
AIstock/
├── backend/                    # Python FastAPI backend (CORE)
│   ├── main.py                 # App entry, lifespan, scheduler startup
│   ├── routers/                # 34 API route modules (REST /api/v1/*)
│   ├── services/               # Business logic layer (37+ modules)
│   │   ├── analysis_service.py         # Stock analysis orchestration
│   │   ├── cloud_screening_service.py  # Eastmoney cloud screening
│   │   ├── quantevolver/               # QE subsystem (27 files)
│   │   │   ├── factor_analysis_service.py
│   │   │   ├── model_analysis_service.py
│   │   │   ├── correlation_engine.py
│   │   │   ├── evolution_service.py
│   │   │   └── portfolio_architecture.py
│   │   ├── paper_trading/              # Paper trading (14 files)
│   │   │   ├── signal_generator.py
│   │   │   ├── execution_engine.py
│   │   │   ├── performance_calculator.py
│   │   │   └── live_ic_tracker.py
│   │   ├── rdagent_sync_service.py     # RD-Agent task sync
│   │   ├── rdagent_catalog_service.py  # RD-Agent catalog ETL
│   │   └── rdagent_asset_service.py    # RD-Agent asset management
│   ├── data_service/           # Unified multi-source data access (16 modules)
│   │   ├── tdx_adapter.py
│   │   ├── tushare_adapter.py
│   │   ├── xtquant_adapter.py
│   │   └── timescaledb_adapter.py
│   ├── core/                   # Core implementations
│   ├── data_access/            # Unified data access facade
│   ├── db/                     # DB schema, migrations, connection pool
│   │   └── pg_pool.py          # psycopg2 ThreadedConnectionPool
│   ├── models/                 # Pydantic models
│   ├── agents/                 # AI agent implementations
│   ├── infra/                  # External clients
│   │   ├── qmt_client.py       # MiniQMT client (1199 lines)
│   │   ├── deepseek_client.py  # DeepSeek LLM client
│   │   ├── wsl_qlib_runner.py  # WSL-based Qlib training
│   │   ├── realtime_quote_subscriber.py
│   │   └── risk_control.py
│   ├── ingestion/              # Data ingestion
│   │   └── tdx_scheduler.py    # Background ingestion (2111 lines)
│   ├── strategies/             # Trading strategies (EMA, MA cross, trend, volatility)
│   ├── execution_algos/        # Execution algos (VWAP, TWAP, POV, AC, SBB)
│   ├── qlib_exporter/          # PostgreSQL → Qlib binary export (14 files)
│   ├── quant_models/           # HMM, LSTM, ARIMA, DeepAR
│   ├── quant_datasets/         # Dataset classes for quant models
│   ├── rebalance_strategies/   # TopK Dropout, TopK Dropout RC
│   ├── schedulers/             # Background schedulers (6 total)
│   ├── repositories/           # Data repository implementations
│   ├── plugins/factors/        # Factor plugins
│   ├── schema_registry/        # Prompt pack & schema registry
│   └── scripts/                # Backend utility scripts
│
├── frontend/                   # Next.js 14 (App Router)
│   └── src/
│       ├── app/                # 25+ pages
│       │   ├── analysis/           # Multi-agent stock analysis
│       │   ├── analysis-trend/     # Trend analysis
│       │   ├── watchlist/          # Watchlist
│       │   ├── cloud-screening/    # Cloud stock screening
│       │   ├── portfolio/          # Portfolio
│       │   ├── smart-monitor/      # AI monitoring
│       │   ├── quantevolver/       # QE UI (12 sub-pages)
│       │   ├── paper-trading/      # Paper trading (5 sub-pages)
│       │   ├── qmt/                # QMT positions/strategies
│       │   └── rdagent/            # RD-Agent dispatch/tasks
│       ├── components/         # UI components (simple-ui, ui)
│       ├── lib/                # Utility libraries
│       └── types/              # TypeScript types
│
├── scripts/                    # 185+ standalone utility scripts
├── monitoring/                 # Prometheus + Grafana (Docker Compose)
├── docs/                       # 113+ design/analysis docs
├── xtquant/                    # XTPythonClient SDK (.pyd + Python wrapper)
├── model_training/             # HMM model training
├── qe_strategies/              # QuantEvolver strategy implementations
├── tdx-api-main/               # Go TDX HTTP backend (port 19080)
│
├── ai_agents.py                # Multi-agent orchestrator (9 agents)
├── deepseek_client.py          # DeepSeek LLM API wrapper
├── config.py                   # Environment config loader
├── app_pg.py                   # PostgreSQL helper
├── sector_strategy_*.py        # Sector strategy engine + PDF/MD reports
├── start_all_ai_stock.bat      # One-click startup (3 services)
└── requirements.txt
```

## Key API Routes (/api/v1/)

| Prefix | Router File | Function |
|--------|-----------|----------|
| `/api/v1/analysis` | `analysis.py` | Multi-agent stock analysis |
| `/api/v1/watchlist` | `watchlist.py` | Watchlist management |
| `/api/v1/cloud-screening` | `cloud_screening.py` | Eastmoney screening |
| `/api/v1/monitor` | `monitor.py` | Real-time monitoring |
| `/api/v1/portfolio` | `portfolio.py` | Portfolio management |
| `/api/v1/sector-strategy` | `sector_strategy.py` | Sector analysis |
| `/api/v1/qmt` | `qmt.py` | QMT trading |
| `/api/v1/quant` | `quant.py` | Quant data |
| `/api/v1/quantevolver` | `quantevolver.py` | QE factor/model/strategy |
| `/api/v1/quantevolver/evolution` | `quantevolver_evolution.py` | Auto-evolution |
| `/api/v1/rdagent` | `rdagent.py` | RD-Agent integration |
| `/api/v1/rdagent-catalog-admin` | `rdagent_catalog_admin.py` | Catalog management |
| `/api/v1/paper-trading` | `paper_trading.py` | Paper trading |
| `/api/v1/dispatch` | `dispatch.py` | Multi-node dispatch |
| `/api/v1/hmm-training` | `hmm_training.py` | HMM training |
| `/api/v1/rl-execution` | `rl_execution.py` | RL execution |
| `/api/v1/stocks` | `stocks.py` | Stock data queries |
| `/api/v1/news` | `news.py` | Market news |
| `/api/v1/tasks` | `tasks.py` | Task management |
| `/api/v1/settings` | `settings.py` | System settings |
| `/api/v1/config` | `config_env.py` | Environment config |

## Multi-Agent AI Analysis (ai_agents.py)

9 specialized agents running in parallel (ThreadPoolExecutor):

1. **Technical Analyst** - MA, RSI, MACD, Bollinger, KDJ
2. **Fundamental Analyst** - ROE, ROA, margins, quarterly reports
3. **Fund Flow Analyst** - Main force capital, margin trading
4. **Risk Manager** - Lock-up expiry, shareholder reduction
5. **Market Sentiment Analyst** - ARBR, PE/PB, fear/greed
6. **News Analyst** - Real-time news from QStock
7. **Research Report Analyst** - Broker reports, ratings
8. **Announcement Analyst** - Company announcements, PDF parsing
9. **Chip Analyst** - Chip distribution, concentration

→ Chief Analyst synthesizes all reports → final investment decision

## Data Pipeline

```
TDX Binary → Go Backend (19080) → TimescaleDB → FastAPI Data Service → Frontend
Tushare API → TushareSyncEngine → TimescaleDB → ...
Akshare → Akshare Adapters → TimescaleDB → ...
xtquant → xtquant_adapter → Real-time + TimescaleDB
Eastmoney Cloud → cloud_screening_svc → Frontend (pass-through)
DeepSeek LLM → deepseek_client → Analysis Reports (PDF/MD)
```

Data Service Layer priority: xtquant → TDX → error (no synthetic data)

## Background Schedulers (started in backend/main.py lifespan)

6 schedulers: data ingestion, strategy, paper trading, correlation, node health, HMM, evolution scanner

## Tech Stack

- **Backend**: Python 3.12/3.13, FastAPI, psycopg2, pandas, numpy
- **Frontend**: Next.js 14, TypeScript 5.5, Ant Design 6, TailwindCSS, Plotly.js
- **Database**: PostgreSQL / TimescaleDB
- **LLM**: DeepSeek API (OpenAI SDK)
- **Trading**: miniQMT / xtquant SDK
- **Quant**: Qlib, PyTorch, Optuna, MLflow
- **Monitoring**: Prometheus + Grafana + Docker Compose
- **Data Sources**: TDX, Tushare, Akshare, xtquant

## Conventions

- API versioning: `/api/v1/` prefix
- Layered architecture: Router → Service → Data Service → DB
- Config via `.env` + `config.py`
- DB connection pool: `backend/db/pg_pool.py`
- Qlib binary export: `backend/qlib_exporter/`
