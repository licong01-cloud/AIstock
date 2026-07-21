"""Bounded DEV reference-data seed for HMM Phase 1 benchmark acceptance (GO DEV SEED FULL).

Authorized scope (eight canonical relations, DEV 127.0.0.1:5433/aistock_dev only):

  1. market.kline_daily_raw       WHERE trade_date >= '2024-06-03'   (full universe)
  2. market.sw_index_member       full table (complete PIT spans)
  3. public.model_train_configs   WHERE config_id IN <10 canonical batch configs>
  4. public.model_train_snapshots WHERE snapshot_id IN <10 canonical batch snapshots>
  5. infra.compute_nodes          WHERE node_id IN ('wsl2-5080', 'rdagent-node1')
  6. public.qe_experiments        WHERE experiment_id IN <base + L10 of benchmark task>
  7. public.qe_evolution_tasks    WHERE task_id = 'qe_20260705_004409_4437'
  8. public.qe_evolution_loops    WHERE loop_id = 'qe_20260705_004409_4437_Loop10'

Relations 6 (qe_experiments, 2 rows) and 3 (model_train_configs, 10 rows) were
authorized by explicit NEED-HUMAN approval on 2026-07-21 after the FK chain
qe_evolution_tasks.base_experiment_id / qe_evolution_loops.experiment_id and
model_train_snapshots.config_id made them hard prerequisites.  The two
qe_experiments rows have NULL task_id/round_id, so no further tables follow.

Safety contract (binding):
- production source is explicitly read-only (default_transaction_read_only=on)
- DEV target asserted to be port 5433 with 'dev' in dbname; no other target accepted
- no TRUNCATE / DELETE / DROP / DDL on DEV; insert-only with conflict classification
- per-table independent transaction; first failure stops the whole run
- divergent primary-key conflicts are listed and stop the run; never overwritten
- source row counts must match the approved estimates within +10%
- idempotent: a second run classifies every row as reused/identical and inserts nothing

Run: python scripts/dev_db/seed_hmm_benchmark_reference_20260721.py
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2.extras import Json, RealDictCursor, execute_values

ENV_FILE = Path("F:/Dev/AIstock/.env")
BENCHMARK_SNAPSHOT_ID = "bbec3863-fb67-445f-938e-66f092d18696"
BENCHMARK_TASK_ID = "qe_20260705_004409_4437"
BENCHMARK_LOOP_ID = "qe_20260705_004409_4437_Loop10"
BENCHMARK_EXPERIMENT_IDS = (
    "qe_20260705_004409_4437_base",
    "qe_20260705_004409_4437_L10",
)
# 2026-07-22 user-approved ruling: the workspace-fallback cold/warm benchmark
# runs on the only approved+verifiable fallback loop qe_20260502_131502_9b54/Loop1
# (its LABEL0 uses a 10-period horizon, so the approved spec sets
# label_horizon_days=10).  The evaluation path resolves the workspace node via
# DEV qe_evolution_tasks and requires the source loop row to be completed, so
# that task/loop/experiment metadata must be seeded as reference data too.
FALLBACK_TASK_ID = "qe_20260502_131502_9b54"
FALLBACK_LOOP_ID = "qe_20260502_131502_9b54_Loop1"
FALLBACK_EXPERIMENT_IDS = (
    "qe_20260502_131502_9b54_base",
    "qe_20260502_131502_9b54_L1",
)
# Canonical 10-candidate batch hmmb_63d536bca43c480293de45d7d6952dba:
# every candidate resolves coefficients from one of these snapshots.
BENCHMARK_SNAPSHOT_IDS = (
    "19c918f3-f27e-413b-8d46-0bb71f8eb548",
    "38d8cd16-8fac-4d88-9293-4c42dcb50218",
    "41e5cea2-a8be-47ee-a3ca-831c9609be16",
    "60f055b8-03f2-4ee4-8130-6c5f0c6ad595",
    "6ea64754-003d-48d8-ad9e-d0e7857716c8",
    "8834983a-7a44-4073-8108-d509faa92a31",
    "a48ab231-e43e-41a8-9855-0e908b7913d1",
    "bbec3863-fb67-445f-938e-66f092d18696",
    "bf4eda9d-d252-46f8-a063-fb3f95f49a1e",
    "d2da20b1-f3c5-410b-aee9-9d71dff4e846",
)
BENCHMARK_CONFIG_IDS = (
    "22902343-f3f2-4b6e-a8ba-5eb08821aa85",
    "22640944-da5f-4e53-bc59-7cc3c22dd231",
    "444c14d8-87ef-43dd-8442-7e45c74d7e05",
    "8eea5ce8-9b1d-4d39-b041-9903b140fefd",
    "b99c907b-873a-4173-a4ee-5eab266f8c49",
    "bd305cc0-6d9e-4036-8a27-4a777cbb0d5e",
    "ce4952c1-4b0d-46a7-81f2-ae1d4a249555",
    "da8f18dc-53d3-4243-a04a-07c69be89f06",
    "ef7608e2-4b59-41af-947a-fcef0478e7c0",
    "efccb64a-3f25-4c6c-b8df-615cdf03a3bb",
)
SNAPSHOT_ID_SQL = ", ".join(f"'{sid}'" for sid in BENCHMARK_SNAPSHOT_IDS)
CONFIG_ID_SQL = ", ".join(f"'{cid}'" for cid in BENCHMARK_CONFIG_IDS)
EXPERIMENT_ID_SQL = ", ".join(f"'{eid}'" for eid in BENCHMARK_EXPERIMENT_IDS + FALLBACK_EXPERIMENT_IDS)
BENCHMARK_NODE_IDS = ("wsl2-5080", "rdagent-node1")
KLINE_MIN_TRADE_DATE = "2024-06-03"
BATCH_SIZE = 50_000
ESTIMATE_TOLERANCE = 0.10


@dataclass(frozen=True)
class TableSeed:
    table: str
    where: str
    order_by: str
    pk_columns: tuple[str, ...]
    estimated_rows: int
    purpose: str
    digest_exclude: tuple[str, ...] = ()


SEEDS: tuple[TableSeed, ...] = (
    TableSeed(
        table="market.kline_daily_raw",
        where=f"trade_date >= DATE '{KLINE_MIN_TRADE_DATE}'",
        order_by="ts_code, trade_date",
        pk_columns=("ts_code", "trade_date"),
        estimated_rows=2_796_553,
        purpose=(
            "market forward returns for benchmark window 2024-07-01..2026-04-27 "
            "+ label horizon 20 + market horizon 10/20 + latest-common watermark"
        ),
    ),
    TableSeed(
        table="market.sw_index_member",
        where="TRUE",
        order_by="l2_code, ts_code, in_date",
        pk_columns=("l2_code", "ts_code", "in_date"),
        estimated_rows=7_053,
        purpose="complete PIT sector membership spans (in_date/out_date), no current-only subset",
    ),
    TableSeed(
        table="public.model_train_configs",
        where=f"config_id IN ({CONFIG_ID_SQL})",
        order_by="config_id",
        pk_columns=("config_id",),
        estimated_rows=10,
        purpose="FK parents for the 10 canonical-batch snapshots (authorized 2026-07-21)",
    ),
    TableSeed(
        table="public.model_train_snapshots",
        where=f"snapshot_id IN ({SNAPSHOT_ID_SQL})",
        order_by="snapshot_id",
        pk_columns=("snapshot_id",),
        estimated_rows=10,
        purpose="candidate coefficient snapshots for the canonical 10-candidate batch",
    ),
    TableSeed(
        table="infra.compute_nodes",
        where="node_id IN ('wsl2-5080', 'rdagent-node1')",
        order_by="node_id",
        pk_columns=("node_id",),
        estimated_rows=2,
        purpose="authoritative QE workspace node resolution for the cold/fallback download path",
        digest_exclude=(
            # Approved 2026-07-21 telemetry exception: these columns drift with
            # the live node heartbeat and never participate in the stable digest.
            "last_heartbeat",
            "updated_at",
            "current_task_id",
            "metrics_snapshot",
        ),
    ),
    TableSeed(
        table="public.qe_experiments",
        where=f"experiment_id IN ({EXPERIMENT_ID_SQL})",
        order_by="experiment_id",
        pk_columns=("experiment_id",),
        estimated_rows=4,
        purpose=(
            "FK parents for qe_evolution_tasks.base_experiment_id and "
            "qe_evolution_loops.experiment_id (authorized 2026-07-21; the rows "
            "have NULL task_id/round_id so no further tables follow; "
            "2026-07-22 ruling adds the approved fallback loop's two rows)"
        ),
    ),
    TableSeed(
        table="public.qe_evolution_tasks",
        where=f"task_id IN ('{BENCHMARK_TASK_ID}', '{FALLBACK_TASK_ID}')",
        order_by="task_id",
        pk_columns=("task_id",),
        estimated_rows=2,
        purpose="benchmark + approved fallback QE task rows for workspace node resolution",
    ),
    TableSeed(
        table="public.qe_evolution_loops",
        where=f"loop_id IN ('{BENCHMARK_LOOP_ID}', '{FALLBACK_LOOP_ID}')",
        order_by="loop_id",
        pk_columns=("loop_id",),
        estimated_rows=2,
        purpose=(
            "benchmark loop row (qe_20260705_004409_4437/Loop10) and approved "
            "fallback loop row (qe_20260502_131502_9b54/Loop1)"
        ),
    ),
)


@dataclass
class TableReceipt:
    table: str
    source_rows: int = 0
    candidate_rows: int = 0
    inserted_rows: int = 0
    reused_rows: int = 0
    identical_conflict_rows: int = 0
    divergent_conflict_rows: int = 0
    rejected_rows: int = 0
    extra_dev_rows: int = 0
    refreshed_rows: int = 0
    verification: dict[str, Any] = field(default_factory=dict)


def parse_env() -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def connect(env: dict[str, str]) -> tuple[Any, Any]:
    prod_cfg = {
        "host": env["TDX_DB_HOST"],
        "port": int(env["TDX_DB_PORT"]),
        "dbname": env["TDX_DB_NAME"],
        "user": env["TDX_DB_USER"],
        "password": env["TDX_DB_PASSWORD"],
        "connect_timeout": 10,
    }
    dev_cfg = {
        "host": env["TDX_DB_DEV_HOST"],
        "port": int(env["TDX_DB_DEV_PORT"]),
        "dbname": env["TDX_DB_DEV_NAME"],
        "user": env["TDX_DB_DEV_USER"],
        "password": env["TDX_DB_DEV_PASSWORD"],
        "connect_timeout": 10,
    }
    if prod_cfg["port"] == dev_cfg["port"]:
        raise SystemExit("refusing to run: source and target resolve to the same port")
    if dev_cfg["port"] != 5433 or "dev" not in dev_cfg["dbname"].lower():
        raise SystemExit(
            f"refusing to run: target {dev_cfg['host']}:{dev_cfg['port']}/{dev_cfg['dbname']} "
            "is not the authorized DEV database"
        )
    prod = psycopg2.connect(
        **prod_cfg, options="-c default_transaction_read_only=on -c timezone=UTC"
    )
    # Read-only is enforced by default_transaction_read_only; a transaction is
    # still required for the server-side streaming cursors below.  UTC makes
    # timestamptz text rendering (and thus the content digest) instance-stable.
    prod.autocommit = False
    dev = psycopg2.connect(**dev_cfg, options="-c timezone=UTC")
    dev.autocommit = False
    return prod, dev


def _adapt_row(row: Any) -> list[Any]:
    """Adapt jsonb-originated values for parameterized INSERT into jsonb columns."""

    return [Json(value) if isinstance(value, (dict, list)) else value for value in row]


COMPUTE_NODES_TABLE = "infra.compute_nodes"
# Approved 2026-07-21 telemetry-refresh exception for infra.compute_nodes only:
# - these columns may be refreshed from the read-only source for existing rows
COMPUTE_NODES_VOLATILE_REFRESH = ("last_heartbeat", "updated_at", "status")
# - these columns are DEV-owned: never copied from production (task-occupancy
#   and live telemetry must not leak into DEV); new rows get neutral values
COMPUTE_NODES_NEUTRAL_COLUMNS = ("current_task_id", "metrics_snapshot")
COMPUTE_NODES_NEUTRAL_VALUES: dict[str, Any] = {
    "current_task_id": None,
    "metrics_snapshot": {},
}


def _apply_compute_nodes_neutral(row: Any, columns: list[str]) -> list[Any]:
    """Return insert values with DEV-owned telemetry columns set to neutral."""

    values = list(row)
    for col, neutral in COMPUTE_NODES_NEUTRAL_VALUES.items():
        if col in columns:
            values[columns.index(col)] = neutral
    return values


def _digest_column_list(seed: TableSeed, columns: list[str]) -> list[str]:
    excluded = set(seed.digest_exclude)
    return [col for col in columns if col not in excluded]


def _digest_sql(seed: TableSeed, columns: list[str]) -> str:
    digest_columns = _digest_column_list(seed, columns)
    if len(digest_columns) == len(columns):
        return "t::text"
    return f"ROW({', '.join('t.' + col for col in digest_columns)})::text"


def table_columns(conn: Any, table: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema || '.' || table_name = %s ORDER BY ordinal_position",
            (table,),
        )
        return [row[0] for row in cur.fetchall()]


def print_plan(seed: TableSeed, columns: list[str]) -> None:
    print("=" * 78)
    print(f"SEED PLAN {seed.table}")
    print("  source:           127.0.0.1:5432/aistock (read-only)")
    print(f"  where:            {seed.where}")
    print(f"  order:            {seed.order_by}")
    print(f"  estimated rows:   {seed.estimated_rows}")
    print(f"  primary key:      {', '.join(seed.pk_columns)}")
    print(f"  insert strategy:  INSERT ... ON CONFLICT ({', '.join(seed.pk_columns)}) DO NOTHING")
    print("  conflict policy:  identical=reused, divergent=list+STOP, extra DEV rows=report only")
    if seed.table == COMPUTE_NODES_TABLE:
        print(
            "  telemetry policy: refresh last_heartbeat/updated_at/status with before/after "
            "report; preserve DEV current_task_id/metrics_snapshot; stable-column "
            "divergence=list+STOP (approved 2026-07-21)"
        )
    if seed.digest_exclude:
        print(f"  digest excludes:  {', '.join(seed.digest_exclude)}")
    print(f"  purpose:          {seed.purpose}")
    print(f"  columns ({len(columns)}):  {', '.join(columns)}")
    sys.stdout.flush()


def seed_empty_target(
    prod: Any,
    dev: Any,
    seed: TableSeed,
    columns: list[str],
    receipt: TableReceipt,
) -> None:
    """Stream source rows into an empty DEV scope in batches."""

    column_list = ", ".join(columns)
    pk_list = ", ".join(seed.pk_columns)
    with prod.cursor(name=f"seed_{seed.table.replace('.', '_')}") as cur:
        cur.itersize = BATCH_SIZE
        cur.execute(
            f"SELECT {column_list} FROM {seed.table} WHERE {seed.where} "
            f"ORDER BY {seed.order_by}"
        )
        while True:
            rows = cur.fetchmany(BATCH_SIZE)
            if not rows:
                break
            if seed.table == COMPUTE_NODES_TABLE:
                # DEV-owned telemetry starts neutral; stable fields come from
                # the authoritative source (approved 2026-07-21 exception).
                adapted = [
                    _adapt_row(_apply_compute_nodes_neutral(row, columns)) for row in rows
                ]
            else:
                adapted = [_adapt_row(row) for row in rows]
            with dev.cursor() as dcur:
                execute_values(
                    dcur,
                    f"INSERT INTO {seed.table} ({column_list}) VALUES %s "
                    f"ON CONFLICT ({pk_list}) DO NOTHING",
                    adapted,
                    page_size=10_000,
                )
            receipt.candidate_rows += len(rows)
            print(
                f"  ... {seed.table}: scanned {receipt.candidate_rows}/"
                f"{receipt.source_rows}"
            )
            sys.stdout.flush()


def _classify_compute_nodes_row(
    dev: Any,
    seed: TableSeed,
    col_idx: dict[str, int],
    columns: list[str],
    prod_row: Any,
    dev_row: Any,
    receipt: TableReceipt,
    divergent: list[dict[str, Any]],
) -> None:
    """Approved 2026-07-21 telemetry-refresh exception for infra.compute_nodes.

    Stable columns must match exactly; any stable divergence is a divergent
    conflict and stops the run.  Volatile refresh columns (last_heartbeat,
    updated_at, status) are refreshed from the read-only source with explicit
    before/after reporting.  DEV-owned columns (current_task_id,
    metrics_snapshot) are never overwritten.
    """

    volatile = set(COMPUTE_NODES_VOLATILE_REFRESH)
    neutral = set(COMPUTE_NODES_NEUTRAL_COLUMNS)
    pk = {col: prod_row[col_idx[col]] for col in seed.pk_columns}
    stable_diffs = {
        col: {"prod": str(prod_row[col_idx[col]]), "dev": str(dev_row[col_idx[col]])}
        for col in columns
        if col not in volatile | neutral
        and prod_row[col_idx[col]] != dev_row[col_idx[col]]
    }
    if stable_diffs:
        receipt.divergent_conflict_rows += 1
        divergent.append({"pk": pk, "stable_column_divergence": stable_diffs})
        return
    refresh = {
        col: {
            "before": str(dev_row[col_idx[col]]),
            "after": str(prod_row[col_idx[col]]),
        }
        for col in COMPUTE_NODES_VOLATILE_REFRESH
        if col in col_idx and prod_row[col_idx[col]] != dev_row[col_idx[col]]
    }
    if refresh:
        assignments = ", ".join(f"{col} = %s" for col in refresh)
        params = [prod_row[col_idx[col]] for col in refresh]
        params.extend(prod_row[col_idx[col]] for col in seed.pk_columns)
        with dev.cursor() as dcur:
            dcur.execute(
                f"UPDATE {seed.table} SET {assignments} "
                f"WHERE {' AND '.join(f'{col} = %s' for col in seed.pk_columns)}",
                params,
            )
        receipt.refreshed_rows += 1
        print(
            f"  ... {seed.table}: telemetry refresh {pk} "
            f"(current_task_id/metrics_snapshot preserved): "
            + json.dumps(refresh, ensure_ascii=False)
        )
    else:
        receipt.identical_conflict_rows += 1
    receipt.reused_rows += 1


def classify_existing_target(
    prod: Any,
    dev: Any,
    seed: TableSeed,
    columns: list[str],
    receipt: TableReceipt,
) -> None:
    """Idempotent rerun: merge-compare both sides ordered by the primary key."""

    column_list = ", ".join(columns)
    pk_list = ", ".join(seed.pk_columns)
    order = seed.order_by
    prod_cur = prod.cursor(name=f"cmp_prod_{seed.table.replace('.', '_')}")
    prod_cur.itersize = BATCH_SIZE
    prod_cur.execute(
        f"SELECT {column_list} FROM {seed.table} WHERE {seed.where} ORDER BY {order}"
    )
    dev_cur = dev.cursor(name=f"cmp_dev_{seed.table.replace('.', '_')}")
    dev_cur.itersize = BATCH_SIZE
    dev_cur.execute(
        f"SELECT {column_list} FROM {seed.table} WHERE {seed.where} ORDER BY {order}"
    )

    pk_idx = tuple(columns.index(col) for col in seed.pk_columns)
    col_idx = {col: idx for idx, col in enumerate(columns)}

    def key(row: Any) -> tuple[Any, ...]:
        return tuple(row[i] for i in pk_idx)

    divergent: list[dict[str, Any]] = []
    fetch_chunk = 10_000

    def buffered_rows(cursor: Any) -> Any:
        while True:
            chunk = cursor.fetchmany(fetch_chunk)
            if not chunk:
                return
            yield from chunk

    prod_iter = iter(buffered_rows(prod_cur))
    dev_iter = iter(buffered_rows(dev_cur))

    def advance(iterator: Any, done: bool) -> tuple[Any, bool]:
        if done:
            return None, True
        row = next(iterator, None)
        return (row, False) if row is not None else (None, True)

    prod_row, prod_done = advance(prod_iter, False)
    dev_row, dev_done = advance(dev_iter, False)
    scanned = 0
    while not (prod_done and dev_done):
        scanned += 1
        if scanned % 500_000 == 0:
            print(f"  ... {seed.table}: compared {scanned} rows")
            sys.stdout.flush()
        if dev_done or (not prod_done and key(prod_row) < key(dev_row)):
            receipt.candidate_rows += 1
            values = (
                _apply_compute_nodes_neutral(prod_row, columns)
                if seed.table == COMPUTE_NODES_TABLE
                else list(prod_row)
            )
            with dev.cursor() as dcur:
                dcur.execute(
                    f"INSERT INTO {seed.table} ({column_list}) VALUES "
                    f"({', '.join(['%s'] * len(columns))}) "
                    f"ON CONFLICT ({pk_list}) DO NOTHING",
                    _adapt_row(values),
                )
                receipt.inserted_rows += max(dcur.rowcount, 0)
            if seed.table == COMPUTE_NODES_TABLE:
                print(
                    f"  ... {seed.table}: inserted {key(prod_row)} with neutral "
                    "current_task_id/metrics_snapshot (production task-occupancy "
                    "is never copied to DEV)"
                )
            prod_row, prod_done = advance(prod_iter, prod_done)
        elif prod_done or key(dev_row) < key(prod_row):
            receipt.extra_dev_rows += 1
            dev_row, dev_done = advance(dev_iter, dev_done)
        else:
            receipt.candidate_rows += 1
            if seed.table == COMPUTE_NODES_TABLE:
                _classify_compute_nodes_row(
                    dev, seed, col_idx, columns, prod_row, dev_row, receipt, divergent
                )
            elif prod_row == dev_row:
                receipt.reused_rows += 1
                receipt.identical_conflict_rows += 1
            else:
                receipt.divergent_conflict_rows += 1
                divergent.append(
                    {
                        "pk": dict(zip(seed.pk_columns, key(prod_row), strict=True)),
                        "prod": {col: str(val) for col, val in zip(columns, prod_row, strict=True)},
                        "dev": {col: str(val) for col, val in zip(columns, dev_row, strict=True)},
                    }
                )
            prod_row, prod_done = advance(prod_iter, prod_done)
            dev_row, dev_done = advance(dev_iter, dev_done)
    prod_cur.close()
    dev_cur.close()
    if divergent:
        print(json.dumps({"divergent_conflicts": divergent[:20]}, indent=2, default=str))
        raise SystemExit(
            f"STOP: {seed.table} has {receipt.divergent_conflict_rows} divergent primary-key "
            "conflicts; refusing to overwrite DEV rows"
        )


def verify_table(dev: Any, seed: TableSeed, columns: list[str], receipt: TableReceipt) -> None:
    with dev.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f"SELECT COUNT(*) AS n FROM {seed.table} WHERE {seed.where}")
        total = int(cur.fetchone()["n"])
        receipt.verification["total_rows_in_scope"] = total
        cur.execute(
            f"SELECT COUNT(*) AS n FROM ("
            f"  SELECT {', '.join(seed.pk_columns)} FROM {seed.table} WHERE {seed.where} "
            f"  GROUP BY {', '.join(seed.pk_columns)} HAVING COUNT(*) > 1"
            f") dup"
        )
        receipt.verification["duplicate_pk_groups"] = int(cur.fetchone()["n"])
        if seed.table == "market.kline_daily_raw":
            cur.execute(
                "SELECT MIN(trade_date) AS dmin, MAX(trade_date) AS dmax, "
                "COUNT(DISTINCT ts_code) AS symbols, "
                "COUNT(*) FILTER (WHERE close_li IS NULL) AS null_close "
                f"FROM {seed.table} WHERE {seed.where}"
            )
            receipt.verification.update({k: str(v) for k, v in dict(cur.fetchone()).items()})
        elif seed.table == "market.sw_index_member":
            cur.execute(
                "SELECT MIN(in_date) AS in_min, MAX(in_date) AS in_max, "
                "MAX(out_date) AS out_max, COUNT(DISTINCT ts_code) AS symbols, "
                "COUNT(DISTINCT l2_code) AS l2_codes "
                f"FROM {seed.table} WHERE {seed.where}"
            )
            receipt.verification.update({k: str(v) for k, v in dict(cur.fetchone()).items()})
        elif seed.table == "public.model_train_snapshots":
            cur.execute(
                f"SELECT snapshot_id, config_id, status, model_path FROM {seed.table} "
                f"WHERE {seed.where}"
            )
            receipt.verification.update(
                {k: str(v) for k, v in dict(cur.fetchone()).items()}
            )
        elif seed.table in {"public.qe_evolution_tasks", "public.qe_evolution_loops"}:
            cur.execute(
                f"SELECT COUNT(DISTINCT task_id) AS tasks FROM {seed.table} WHERE {seed.where}"
            )
            receipt.verification["distinct_tasks"] = int(cur.fetchone()["tasks"])
        digest_expr = _digest_sql(seed, columns)
        cur.execute(
            f"SELECT SUM(hashtextextended({digest_expr}, 0)) AS digest "
            f"FROM (SELECT * FROM {seed.table} WHERE {seed.where}) t"
        )
        receipt.verification["dev_content_digest"] = str(cur.fetchone()["digest"])
        if seed.digest_exclude:
            receipt.verification["digest_excluded_columns"] = list(seed.digest_exclude)


def source_digest(prod: Any, seed: TableSeed, columns: list[str]) -> str:
    digest_expr = _digest_sql(seed, columns)
    with prod.cursor() as cur:
        cur.execute(
            f"SELECT SUM(hashtextextended({digest_expr}, 0)) AS digest "
            f"FROM (SELECT * FROM {seed.table} WHERE {seed.where}) t"
        )
        return str(cur.fetchone()[0])


def final_overall_verification(dev: Any) -> dict[str, Any]:
    checks: dict[str, Any] = {}
    with dev.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT COUNT(*) AS gap_days
            FROM market.trading_calendar c
            WHERE c.cal_date BETWEEN DATE '2024-07-01' AND DATE '2026-04-27'
              AND c.is_trading = true
              AND NOT EXISTS (
                  SELECT 1 FROM market.kline_daily_raw k WHERE k.trade_date = c.cal_date
              )
            """
        )
        checks["benchmark_window_trading_days_without_any_kline"] = int(cur.fetchone()["gap_days"])
        cur.execute(
            """
            SELECT c.cal_date
            FROM market.trading_calendar c
            WHERE c.cal_date > DATE '2026-04-27' AND c.is_trading = true
            ORDER BY c.cal_date ASC LIMIT 1 OFFSET 19
            """
        )
        row = cur.fetchone()
        horizon_date = row["cal_date"] if row else None
        cur.execute("SELECT MAX(trade_date) AS kmax FROM market.kline_daily_raw")
        kmax = cur.fetchone()["kmax"]
        checks["label_horizon20_end_date"] = str(horizon_date)
        checks["kline_max_trade_date"] = str(kmax)
        checks["horizon20_computable"] = bool(
            horizon_date is not None and kmax is not None and kmax >= horizon_date
        )
        cur.execute(
            """
            SELECT COUNT(*) AS unmapped
            FROM market.kline_daily_raw k
            WHERE k.trade_date = DATE '2026-04-27'
              AND NOT EXISTS (
                  SELECT 1 FROM market.sw_index_member m
                  WHERE RTRIM(m.ts_code) = RTRIM(k.ts_code)
                    AND m.in_date <= k.trade_date
                    AND (m.out_date IS NULL OR m.out_date > k.trade_date)
              )
            """
        )
        checks["window_end_symbols_without_pit_sector_mapping"] = int(cur.fetchone()["unmapped"])
        cur.execute(
            "SELECT status, config_id FROM public.model_train_snapshots WHERE snapshot_id = %s",
            (BENCHMARK_SNAPSHOT_ID,),
        )
        snap = cur.fetchone()
        checks["snapshot_status"] = str(snap["status"]) if snap else None
        checks["snapshot_config_id"] = str(snap["config_id"]) if snap else None
        cur.execute(
            "SELECT COUNT(*) AS n FROM public.model_train_configs WHERE config_id = %s",
            (str(snap["config_id"]) if snap else "",),
        )
        checks["snapshot_config_fk_satisfied"] = int(cur.fetchone()["n"]) == 1
        cur.execute(
            "SELECT status, COUNT(*) AS n FROM public.model_train_snapshots "
            f"WHERE snapshot_id IN ({SNAPSHOT_ID_SQL}) GROUP BY status ORDER BY status"
        )
        checks["batch_snapshot_status_counts"] = {
            str(row["status"]): int(row["n"]) for row in cur.fetchall()
        }
        cur.execute(
            "SELECT COUNT(*) AS n FROM public.model_train_snapshots "
            f"WHERE snapshot_id IN ({SNAPSHOT_ID_SQL}) "
            "AND config_id NOT IN (SELECT config_id FROM public.model_train_configs)"
        )
        checks["batch_snapshots_missing_config_fk"] = int(cur.fetchone()["n"])
        for table, max_expr in (
            ("market.stock_st", "GREATEST(MAX(start_date), MAX(COALESCE(end_date, start_date)))"),
            ("market.suspend_d", "MAX(trade_date)"),
        ):
            cur.execute(f"SELECT {max_expr} AS dmax FROM {table}")
            checks[f"{table}_max_date"] = str(cur.fetchone()["dmax"])
    return checks


def main() -> int:
    env = parse_env()
    prod, dev = connect(env)
    receipts: list[TableReceipt] = []
    try:
        for seed in SEEDS:
            columns = table_columns(prod, seed.table)
            if table_columns(dev, seed.table) != columns:
                raise SystemExit(f"STOP: {seed.table} column drift between source and DEV")
            print_plan(seed, columns)
            with prod.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {seed.table} WHERE {seed.where}")
                source_rows = int(cur.fetchone()[0])
            cap = int(seed.estimated_rows * (1 + ESTIMATE_TOLERANCE))
            if source_rows > cap:
                raise SystemExit(
                    f"STOP: {seed.table} source rows {source_rows} exceed the approved "
                    f"estimate {seed.estimated_rows} by more than 10%"
                )
            with dev.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {seed.table} WHERE {seed.where}")
                existing = int(cur.fetchone()[0])
            receipt = TableReceipt(table=seed.table, source_rows=source_rows)
            if existing == 0:
                seed_empty_target(prod, dev, seed, columns, receipt)
                with dev.cursor() as cur:
                    cur.execute(f"SELECT COUNT(*) FROM {seed.table} WHERE {seed.where}")
                    receipt.inserted_rows = int(cur.fetchone()[0])
            else:
                classify_existing_target(prod, dev, seed, columns, receipt)
            if receipt.inserted_rows + receipt.reused_rows != receipt.candidate_rows:
                receipt.rejected_rows = (
                    receipt.candidate_rows - receipt.inserted_rows - receipt.reused_rows
                )
                raise SystemExit(
                    f"STOP: {seed.table} has {receipt.rejected_rows} rows neither inserted "
                    "nor classified as reused; refusing to claim success"
                )
            verify_table(dev, seed, columns, receipt)
            digest = source_digest(prod, seed, columns)
            receipt.verification["source_content_digest"] = digest
            receipt.verification["digest_match"] = (
                digest == receipt.verification.get("dev_content_digest")
            )
            dev.commit()
            receipts.append(receipt)
            print(f"RECEIPT {seed.table}: {json.dumps(receipt.__dict__, default=str)}")
            sys.stdout.flush()
        overall = final_overall_verification(dev)
        print(f"OVERALL VERIFICATION: {json.dumps(overall, indent=2, default=str)}")
        failures = [
            name
            for name, ok in (
                ("benchmark_window_trading_days_without_any_kline", overall["benchmark_window_trading_days_without_any_kline"] == 0),
                ("horizon20_computable", overall["horizon20_computable"]),
                ("window_end_symbols_without_pit_sector_mapping", overall["window_end_symbols_without_pit_sector_mapping"] == 0),
                ("snapshot_status", overall["snapshot_status"] in {"completed", "ready"}),
                ("snapshot_config_fk_satisfied", overall["snapshot_config_fk_satisfied"]),
            )
            if not ok
        ]
        digest_failures = [
            receipt.table
            for receipt in receipts
            if receipt.verification.get("digest_match") is not True
        ]
        if failures or digest_failures:
            print(f"SEED VERIFICATION FAILED: {failures} digest_mismatch={digest_failures}")
            return 3
        print("SEED_HMM_BENCHMARK_REFERENCE_OK")
        return 0
    except SystemExit:
        dev.rollback()
        raise
    except Exception:
        dev.rollback()
        raise
    finally:
        prod.close()
        dev.close()


if __name__ == "__main__":
    raise SystemExit(main())
