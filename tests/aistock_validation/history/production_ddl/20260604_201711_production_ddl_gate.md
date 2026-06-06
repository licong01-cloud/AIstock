# Production DDL Gate - 20260604_201711

## Scope

- User authorization: explicit request in current session to complete all DDL before user restarts backend.
- Git HEAD: `2f76d1cd3b80`
- Target DB: `127.0.0.1:5432/aistock` user `postgres` password_set=True
- Production runtime touched: false; backend/frontend/TDX were not started, stopped, or restarted by this gate.
- psql availability: not available; execution used psycopg2 with committed SQL/bootstrap DDL.

## Applied DDL

- `trading_core_v2_bootstrap` -> `backend.db.init_trading_core_v2_schema::init_trading_core_v2_schema`: applied in 0.863s
- `paper_v2_minqmt_session_sources_20260526` -> `backend\migrations\paper_v2_minqmt_session_sources_20260526.sql`: applied in 0.054s
- `paper_v2_miniqmt_auto_run_20260527` -> `backend\migrations\paper_v2_miniqmt_auto_run_20260527.sql`: applied in 0.043s
- `ra_upgrade_001_memory_tree` -> `backend\db\migrations\ra_upgrade\001_memory_tree.sql`: applied in 0.054s
- `ra_upgrade_002_agent_teams` -> `backend\db\migrations\ra_upgrade\002_agent_teams.sql`: applied in 0.019s
- `ra_upgrade_003_qe_autonomy` -> `backend\db\migrations\ra_upgrade\003_qe_autonomy.sql`: applied in 0.044s
- `price_guard_stage1_advisory_20260602` -> `backend\db\migrations\add_price_guard_stage1_advisory_20260602.sql`: applied in 0.035s
- `advisory_program_lifecycle_20260604` -> `backend\db\migrations\add_advisory_program_lifecycle_20260604.sql`: applied in 0.059s
- `research_assistant_schema_bootstrap_post_ra_migrations` -> `backend.db.init_research_assistant_schema_20260521::init_research_assistant_schema`: applied in 0.809s

## Verification

- Before failures: 0
- After failures: 0
- Result: applied_and_verified

All expected RA, Paper v2, Simulation Runtime, PriceGuard/advisory tables, columns, indexes, constraints, triggers, and sampled comments were verified present.

## Files / Bootstraps

- `backend/db/init_trading_core_v2_schema.py`
- `backend/migrations/paper_v2_minqmt_session_sources_20260526.sql`
- `backend/migrations/paper_v2_miniqmt_auto_run_20260527.sql`
- `backend/db/init_research_assistant_schema_20260521.py`
- `backend/db/migrations/ra_upgrade/001_memory_tree.sql`
- `backend/db/migrations/ra_upgrade/002_agent_teams.sql`
- `backend/db/migrations/ra_upgrade/003_qe_autonomy.sql`
- `backend/db/migrations/add_price_guard_stage1_advisory_20260602.sql`
- `backend/db/migrations/add_advisory_program_lifecycle_20260604.sql`

## Gates

- production_ddl_gate=applied_and_verified
- production_backend_dependency_gate=noop
- production_frontend_dependency_gate=noop
- production_backend_8001_touched=false
- production_frontend_3000_touched=false
- tdx_19080_touched=false
