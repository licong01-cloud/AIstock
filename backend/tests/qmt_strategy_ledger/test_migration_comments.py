from __future__ import annotations

import re
from pathlib import Path


MIGRATION = Path("backend/migrations/qmt_strategy_ledger_20260518.sql")
TABLE_PATTERN = re.compile(
    r"CREATE TABLE IF NOT EXISTS qmt_strategy\.(?P<table>[a-z_]+) \(\n(?P<body>.*?)\n\);",
    re.DOTALL,
)


def _migration_sql() -> str:
    return MIGRATION.read_text(encoding="utf-8")


def _tables_and_columns(sql: str) -> dict[str, list[str]]:
    tables: dict[str, list[str]] = {}
    for match in TABLE_PATTERN.finditer(sql):
        table = match.group("table")
        columns: list[str] = []
        for raw_line in match.group("body").splitlines():
            line = raw_line.strip().rstrip(",")
            if not line:
                continue
            first_token = line.split()[0].upper()
            if first_token in {"CONSTRAINT", "PRIMARY", "UNIQUE", "FOREIGN", "CHECK"}:
                continue
            columns.append(line.split()[0].strip('"'))
        tables[table] = columns
    return tables


def test_qmt_strategy_migration_comments_every_table_and_column() -> None:
    sql = _migration_sql()
    tables = _tables_and_columns(sql)

    assert set(tables) == {
        "cash_ledger",
        "daily_snapshot",
        "order_batch",
        "order_intent",
        "order_ledger",
        "order_status_event",
        "position_lot",
        "reconciliation_issue",
        "reconciliation_run",
        "strategy_package_binding",
        "trade_ledger",
        "unattributed_order",
        "unattributed_trade",
        "virtual_account",
    }

    for table, columns in tables.items():
        assert f"COMMENT ON TABLE qmt_strategy.{table} IS" in sql
        for column in columns:
            assert f"COMMENT ON COLUMN qmt_strategy.{table}.{column} IS" in sql


def test_qmt_strategy_migration_has_required_uniqueness_guards() -> None:
    sql = _migration_sql()

    assert "ux_qmt_strategy_active_binding" in sql
    assert "UNIQUE(account_id, order_remark)" in sql
    assert "UNIQUE(account_id, qmt_order_id)" in sql
    assert "UNIQUE(account_id, trade_date, trade_id)" in sql
    assert "UNIQUE(strategy_id, trade_date)" in sql
