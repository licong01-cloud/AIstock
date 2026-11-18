"""Materialize a sealed LocalSIM correction in DEV and prove exact rollback."""
# ruff: noqa: E402
from __future__ import annotations

import argparse
from collections import defaultdict
from contextlib import closing, contextmanager
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from functools import partial
from pathlib import Path
import sys
from types import SimpleNamespace

import psycopg2
import simplejson
from psycopg2 import sql
from psycopg2.extras import Json, register_default_jsonb

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.paper_trading_v2.repository import PaperTradingV2Repository
from backend.services.simulation_execution.localsim.models import LocalSimExecutionStateV1
from backend.services.simulation_runtime.localsim_control import LocalSimControlPlaneService
from backend.services.simulation_runtime.localsim_reconstruction import verify_day
from backend.services.simulation_runtime.localsim_reconstruction_inputs import file_ref, money, publish, read, require
from backend.services.simulation_runtime.localsim_workbench import LocalSimWorkbenchService, READ_COLUMNS
from backend.services.simulation_runtime.models import canonical_json_sha256
from backend.services.simulation_runtime.successor_models import SimulationAccountStatus, SimulationAccountV1
from backend.services.simulation_runtime.successor_repository import LocalSimSuccessorRepository
from scripts.prepare_localsim_successor_cutover import _settings

# Ordered foreign-key writes. Serial keys are allocated by the target database.
COLUMNS = {
    "run": "run_id portfolio_id trade_date status data_source runtime_config started_at completed_at error_json model_params_origin",
    "orders": "order_id run_id portfolio_id package_id intent_id symbol side quantity order_type limit_price status filled_quantity avg_fill_price metadata created_at updated_at stock_name",
    "fills": "fill_id run_id order_id symbol side quantity price trade_time bar_time reason metadata created_at updated_at intended_price fill_market_context stock_name",
    "cash_ledger": "run_id portfolio_id fill_id trade_date symbol side notional fee cash_delta cash_after created_at stock_name",
    "positions": "run_id portfolio_id trade_date symbol quantity available_quantity avg_cost market_price market_value metadata created_at updated_at stock_name",
    "daily_snapshots": "run_id portfolio_id trade_date cash market_value nav position_count snapshot_time metadata created_at updated_at",
    "order_events": "event_id run_id order_id event_type event_time reason metadata fill_json",
}
KEYS = {
    "run": ("run_id",), "orders": ("order_id",), "fills": ("fill_id",), "cash_ledger": ("fill_id",),
    "positions": ("run_id", "symbol"), "daily_snapshots": ("run_id",), "order_events": ("event_id",),
}
SOURCE_KEYS = {
    "run": "runs", "orders": "orders", "fills": "fills", "cash_ledger": "cash",
    "positions": "positions", "daily_snapshots": "snapshots", "order_events": "events",
}
SOURCE_ORDER = {"cash_ledger": "cash_id", "positions": "position_id", "daily_snapshots": "snapshot_id"}
EXACT_JSON = partial(simplejson.loads, use_decimal=True)


def key(table, row):
    return tuple(row[k] for k in KEYS[table])


def index(table, rows):
    by_key = {key(table, row): row for row in rows}
    require(len(by_key) == len(rows) and all(all(k is not None for k in item) for item in by_key),
            f"IMPORT_DUPLICATE_OR_MISSING_KEY_{table}")
    return by_key


def same_projection(actual, expected):
    if isinstance(actual, bool) or isinstance(expected, bool):
        return actual is expected
    # The frozen workbench projection serializes some database numerics as floats.
    if isinstance(actual, Decimal) and isinstance(expected, float):
        return float(actual) == expected
    if isinstance(actual, (Decimal, int)) and isinstance(expected, str):
        try:
            return Decimal(actual) == Decimal(expected)
        except InvalidOperation:
            return False
    if isinstance(actual, str) and isinstance(expected, str) and len(actual) > 10 and len(expected) > 10:
        try:
            return datetime.fromisoformat(actual) == datetime.fromisoformat(expected)
        except ValueError:
            pass
    return actual == expected


def collect_original(cur, source, history, prefix):
    snapshots = [r["source_snapshot"] for r in prefix]
    run_ids = [r["run_id"] for r in snapshots]
    require(len(set(run_ids)) == len(prefix), "IMPORT_PREFIX_RUN_DUPLICATE")
    require(all(r["source"] == prefix[0]["source"] for r in prefix), "IMPORT_PREFIX_FACT_CLOSURE")
    frames = {}
    for table, field in SOURCE_KEYS.items():
        order = SOURCE_ORDER.get(table, KEYS[table][0])
        row_json = sql.SQL("to_jsonb(t)")
        if table == "run":
            row_json += sql.SQL(" || jsonb_build_object('runtime_identity', md5(COALESCE(runtime_config::text,'')))")
        cur.execute(sql.SQL("SELECT {} FROM {} t WHERE run_id=ANY(%s) ORDER BY {}").format(
            row_json, sql.Identifier("paper_v2", table), sql.Identifier(order)), (run_ids,))
        rows = [row[0] for row in cur.fetchall()]
        expected = snapshots if table == "daily_snapshots" else [r for r in source[field] if r["run_id"] in run_ids]
        by_key = index(table, rows)
        require(by_key.keys() == index(table, expected).keys(), f"IMPORT_PREFIX_ROW_SET_{table}")
        for old in expected:
            raw = by_key[key(table, old)]
            for column, value in old.items():
                require(column in raw and same_projection(raw[column], value), f"IMPORT_PREFIX_ROW_DRIFT_{table}.{column}")
            if "portfolio_id" in raw:
                require(raw["portfolio_id"] == history["source_account_id"], "IMPORT_PREFIX_ACCOUNT")
        frames[table] = rows
    return frames


def corrected_frames(day, manifest, source):
    states = [LocalSimExecutionStateV1.model_validate(x) for x in day["execution_states"]]
    require(len(states) == len(day["orders"]) and all(x.is_terminal for x in states), "IMPORT_CORRECTED_EXECUTION_NOT_CLOSED")
    require(len({x.run_id for x in states}) <= 1, "IMPORT_CORRECTED_RUN_ID")
    run_id = states[0].run_id if states else "correction_" + canonical_json_sha256(source)[:24]
    day_value = day["trade_date"]
    stamp = day["snapshot_time"]
    started = min((state.created_at for state in states), default=datetime.fromisoformat(stamp))
    completed = max((state.updated_at for state in states), default=datetime.fromisoformat(stamp))
    frame = {t: [] for t in COLUMNS}
    frame["run"] = [{"run_id": run_id, "portfolio_id": day["account_version_id"], "trade_date": day_value,
        "status": "SUCCEEDED", "data_source": "DB_HISTORICAL", "started_at": started.isoformat(),
        "completed_at": completed.isoformat(), "model_params_origin": "unavailable",
        "runtime_config": {"correction_source": source, "projection_timestamp_basis": "SEALED_EFFECTIVE_EVENT_TIME"}}]
    frame["orders"] = [{**row, "run_id": run_id} for row in day["orders"]]
    require(all(r["package_id"] == manifest["package_id"] for r in frame["orders"]), "IMPORT_CORRECTED_PACKAGE")
    frame["fills"] = [{**row, "run_id": run_id, "created_at": row["trade_time"], "updated_at": row["trade_time"]}
                      for row in day["fills"]]
    trade_times = {x["fill_id"]: x["trade_time"] for x in day["fills"]}
    frame["cash_ledger"] = [{**row, "run_id": run_id, "created_at": trade_times[row["fill_id"]]}
                            for row in day["cash_entries"]]
    market_value = money(day["nav"]) - money(day["cash"])
    frame["daily_snapshots"] = [{"run_id": run_id, "portfolio_id": day["account_version_id"], "trade_date": day_value,
        "snapshot_time": stamp, "created_at": stamp, "updated_at": stamp, "cash": day["cash"], "nav": day["nav"],
        "market_value": str(market_value), "position_count": len(day["positions"]), "metadata": {"terminal": True,
         "source": "isolated_correction_replay", "correction_source": source}}]
    for symbol, position in day["positions"].items():
        mark = day["marks"][symbol]
        frame["positions"].append({**position, "run_id": run_id, "trade_date": day_value, "market_price": mark,
            "market_value": str(money(mark) * position["quantity"]), "created_at": stamp, "updated_at": stamp,
            "metadata": {"position_trade_date": position["trade_date"]}})
    return frame


def verify_economics(frames, history):
    by_run = {}
    for table, rows in frames.items():
        grouped = defaultdict(list)
        for row in rows:
            grouped[row["run_id"]].append(row)
        by_run[table] = grouped
    snapshots = sorted(frames["daily_snapshots"], key=lambda r: r["trade_date"])
    require(len(snapshots) == len(frames["run"]) == len(history["days"]), "IMPORT_CALENDAR")
    initial = history["initial_capital"]
    previous = {"cash": initial, "nav": initial, "positions": {}}
    for basis, snapshot in zip(history["days"], snapshots, strict=True):
        rid, day = snapshot["run_id"], snapshot["trade_date"]
        require(basis["trade_date"] == day and money(basis["cash"]) == money(snapshot["cash"])
                and money(basis["nav"]) == money(snapshot["nav"]), "IMPORT_NAV_HISTORY")
        positions = {p["symbol"]: p for p in by_run["positions"][rid]}
        require(len(positions) == len(by_run["positions"][rid]) == snapshot["position_count"]
                and all(0 <= p["available_quantity"] <= p["quantity"] and money(p["avg_cost"]) >= 0
                        for p in positions.values()), "IMPORT_POSITION_SNAPSHOT")
        order_ids = {o["order_id"] for o in by_run["orders"][rid]}
        require(all(row["order_id"] in order_ids for t in ("fills", "order_events") for row in by_run[t][rid]),
                "IMPORT_ORDER_FACT_LINK")
        result = {"cash": basis["cash"], "nav": basis["nav"], "valuation_day_pnl": basis["valuation_day_pnl"],
            "cumulative_pnl": basis["cumulative_nav_pnl"], "origin_initial_cash": initial, "positions": positions,
            "marks": {s: p["market_price"] for s, p in positions.items()}, "fills": by_run["fills"][rid],
            "cash_entries": by_run["cash_ledger"][rid]}
        try:
            verify_day(result, previous)
        except ValueError as exc:
            raise ValueError(f"{day}: {exc}") from exc
        previous = result


def rebind(frames, account_id):
    def mapped(key_value):
        return account_id + "_" + canonical_json_sha256([key_value])[:36]

    result = {}
    for table, rows in frames.items():
        converted = []
        for row in rows:
            values = {column: row.get(column) for column in COLUMNS[table].split()}
            for field in ("run_id", "order_id", "fill_id", "event_id", "intent_id"):
                if field in values and values[field] is not None:
                    values[field] = mapped(values[field])
            if "portfolio_id" in values:
                values["portfolio_id"] = account_id
            event = values.get("fill_json")
            if isinstance(event, dict):
                values["fill_json"] = {k: mapped(v) if k in {"run_id", "order_id", "fill_id", "intent_id"} and v is not None
                                       else account_id if k == "portfolio_id" else v for k, v in event.items()}
            converted.append(values)
        index(table, converted)
        result[table] = converted
    return result


def prepare(production_dsn, proof_ref):
    proof = read(proof_ref)
    delivery = read(proof["delivery"])
    dev = read(proof["dev"])
    req = read(delivery["request"])
    require(delivery["status"] == "OFFLINE_CORRECTION_VERIFIED" and dev["delivery"] == proof["delivery"]
            and dev["rolled_back"] and read(delivery["exact_retry"])["recomputed_days"] == 0,
            "IMPORT_ECONOMIC_DELIVERY")
    history = read(delivery["corrected_history"])
    parent = read(delivery["parent_history"])
    require(history["days"][:len(parent["days"])] == parent["days"]
            and history["account_days"] == len(history["days"]) and history["new_capital"] == "0",
            "IMPORT_HISTORY_PARENT")
    prefix = [v for v in history["days"] if v["source_kind"] == "EXISTING_VERIFIED_CLOSE_REUSED"]
    suffix = history["days"][len(prefix):]
    require(bool(prefix) and bool(suffix) and history["days"][:len(prefix)] == prefix
            and all(v["source_kind"] in {"NATIVE_CAUSAL_CORRECTION_REPLAY", "NATIVE_CAUSAL_CORRECTION_CONTINUATION"}
                    for v in suffix), "IMPORT_PREFIX_SUFFIX_SCOPE")
    source = read(prefix[0]["source"])
    account = SimulationAccountV1.model_validate(source["account"])
    manifest = req["manifest"]
    require(account.account_id == history["source_account_id"] and account.package_id == manifest["package_id"]
            and account.manifest_sha256 == manifest["manifest_sha256"]
            and money(account.initial_capital) == money(history["initial_capital"]), "IMPORT_ACCOUNT_IDENTITY")
    with closing(psycopg2.connect(**production_dsn, options="-c default_transaction_read_only=on -c statement_timeout=30000")) as conn:
        register_default_jsonb(conn, loads=EXACT_JSON)
        conn.set_session(readonly=True, isolation_level="REPEATABLE READ")
        with conn.cursor() as cur:
            cur.execute("SHOW transaction_read_only")
            require(cur.fetchone()[0] == "on", "IMPORT_SOURCE_READ_ONLY")
            cur.execute("SELECT cal_date FROM market.trading_calendar WHERE is_trading AND cal_date BETWEEN %s AND %s ORDER BY cal_date",
                        (history["start_trade_date"], history["end_trade_date"]))
            calendar = [str(r[0]) for r in cur.fetchall()]
            require(calendar == [r["trade_date"] for r in history["days"]], "IMPORT_TRADING_CALENDAR")
            frames = collect_original(cur, source, history, prefix)
            cur.execute("SELECT to_jsonb(p) FROM strategy_pkg.package p WHERE package_id=%s", (account.package_id,))
            package_row = cur.fetchone()
            require(package_row is not None and package_row[0]["manifest_sha256"] == account.manifest_sha256,
                    "IMPORT_PACKAGE_DRIFT")
            package = package_row[0]
            cur.execute("SELECT min(cal_date) FROM market.trading_calendar WHERE is_trading AND cal_date>%s",
                        (calendar[-1],))
            next_day = cur.fetchone()[0]
            require(next_day is not None, "IMPORT_NEXT_TRADING_DAY")
    for row in suffix:
        day = read(row["source"])
        require(day["trade_date"] == row["trade_date"] and day["new_capital"] == "0", "IMPORT_CORRECTED_SCOPE")
        for table, records in corrected_frames(day, manifest, row["source"]).items():
            frames[table].extend(records)
    verify_economics(frames, history)
    return account, package, calendar, next_day, frames, history, delivery["corrected_history"]


def row_count(cur, table, run_ids):
    cur.execute(sql.SQL("SELECT count(*) FROM {} WHERE run_id=ANY(%s)").format(sql.Identifier("paper_v2", table)),
                (run_ids,))
    return cur.fetchone()[0]


def insert_frame(cur, table, rows):
    if not rows:
        return
    table_name = sql.Identifier("paper_v2", table)
    columns = sql.SQL(", ").join(sql.Identifier(c) for c in COLUMNS[table].split())
    cur.execute(sql.SQL("INSERT INTO {} ({}) SELECT {} FROM jsonb_populate_recordset(NULL::{}, %s) "
                        "WITH ORDINALITY ORDER BY ordinality").format(
        table_name, columns, columns, table_name), (Json(rows, dumps=simplejson.dumps),))
    require(cur.rowcount == len(rows), f"IMPORT_INSERT_COUNT_{table}")


def equal_rows(cur, table, rows, run_ids):
    table_name = sql.Identifier("paper_v2", table)
    columns = sql.SQL(", ").join(sql.Identifier(c) for c in COLUMNS[table].split())
    cur.execute(sql.SQL("""
        WITH expected AS (SELECT {columns} FROM jsonb_populate_recordset(NULL::{table}, %s)),
             actual AS (SELECT {columns} FROM {table} WHERE run_id=ANY(%s))
        SELECT NOT EXISTS ((SELECT * FROM expected EXCEPT ALL SELECT * FROM actual)
                     UNION ALL (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected))
        """).format(table=table_name, columns=columns), (Json(rows, dumps=simplejson.dumps), run_ids))
    require(cur.fetchone()[0], f"IMPORT_READBACK_CONFLICT_{table}")


def compare_candidate_rows(cur, rows, run_ids):
    for table, frame in rows.items():
        equal_rows(cur, table, frame, run_ids)
    cur.execute("SELECT fill_id FROM paper_v2.cash_ledger WHERE run_id=ANY(%s) ORDER BY cash_id", (run_ids,))
    require([r[0] for r in cur.fetchall()] == [r["fill_id"] for r in rows["cash_ledger"]], "IMPORT_CASH_SEQUENCE")


def materialize_dev(cur, rows, run_ids):
    observed = {table: row_count(cur, table, run_ids) for table in rows}
    if any(observed.values()):
        compare_candidate_rows(cur, rows, run_ids)
        return 0
    for table, frame in rows.items():
        insert_frame(cur, table, frame)
    compare_candidate_rows(cur, rows, run_ids)
    return sum(len(frame) for frame in rows.values())


def verify_conflict_refused(cur, rows, run_ids):
    snapshots = rows["daily_snapshots"]
    conflicting = {**snapshots[-1], "nav": money(snapshots[-1]["nav"]) + Decimal("0.01")}
    try:
        materialize_dev(cur, {**rows, "daily_snapshots": [*snapshots[:-1], conflicting]}, run_ids)
    except ValueError as exc:
        if str(exc) != "LOCALSIM_RECONSTRUCTION_IMPORT_READBACK_CONFLICT_daily_snapshots":
            raise
    else:
        require(False, "IMPORT_EXPECTED_CONFLICT_ACCEPTED")
    compare_candidate_rows(cur, rows, run_ids)


def verify_dev(dsn, prepared, proof_ref):
    require((dsn["host"], dsn["port"], dsn["dbname"]) == ("127.0.0.1", 5433, "aistock_dev"), "IMPORT_DEV_TARGET")
    source, package, calendar, next_day, frames, history, history_ref = prepared
    now = datetime.now(UTC)
    control = LocalSimControlPlaneService
    account = control._build_account(account_name=f"{source.account_name} corrected {history_ref['sha256'][:12]}",
        package_id=source.package_id, manifest_sha256=source.manifest_sha256, admission_receipt_id=source.admission_receipt_id,
        initial_capital=source.initial_capital, lineage_source_legacy_account_id=None, status=SimulationAccountStatus.PAUSED,
        created_by="test_int_localsim_corrected_ledger", now=now)
    scope = control._build_native_ledger_scope(account=account, created_by=account.created_by, now=now)
    require(account.account_id != source.account_id, "IMPORT_SAME_ACCOUNT")
    rows = rebind(frames, account.account_id)
    run_ids = [r["run_id"] for r in rows["run"]]

    with closing(psycopg2.connect(**dsn)) as conn:
        @contextmanager
        def shared():
            yield conn

        scopes = LocalSimSuccessorRepository(conn_factory=shared)
        repo = PaperTradingV2Repository(conn_factory=shared)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT current_database(),pg_is_in_recovery()")
                require(cur.fetchone() == ("aistock_dev", False), "IMPORT_DEV_DATABASE_IDENTITY")
                cur.execute("SET LOCAL statement_timeout='60s'")
                cur.execute("SET LOCAL lock_timeout='3s'")
                cur.execute("SELECT 1 FROM paper_v2.simulation_account_v1 WHERE account_id=%s", (account.account_id,))
                require(cur.fetchone() is None, "IMPORT_DEV_ACCOUNT_EXISTS")
                cur.execute("SELECT manifest_sha256 FROM strategy_pkg.package WHERE package_id=%s", (source.package_id,))
                package_existing = cur.fetchone()
                if package_existing is None:
                    cur.execute("INSERT INTO strategy_pkg.package SELECT r.* FROM jsonb_populate_record(NULL::strategy_pkg.package, %s) r",
                                (Json(package, dumps=simplejson.dumps),))
                else:
                    require(package_existing[0] == source.manifest_sha256, "IMPORT_DEV_PACKAGE_CONFLICT")
                scopes._insert_account(cur, account)
                scopes.save_ledger_scope(scope)
                require(scopes.get_account(account.account_id).status is SimulationAccountStatus.PAUSED
                        and scopes.resolve_ledger_scope_for_account(account.account_id).ledger_scope_hash == scope.ledger_scope_hash,
                        "IMPORT_NATIVE_PAUSED_ACCOUNT")
                require(not any(row_count(cur, t, run_ids) for t in rows), "IMPORT_DEV_RUN_NAMESPACE_EXISTS")
                inserted = materialize_dev(cur, rows, run_ids)
                retry = materialize_dev(cur, rows, run_ids)
                require(inserted == sum(len(v) for v in rows.values()) and retry == 0, "IMPORT_DEV_RETRY_WRITES")
                verify_conflict_refused(cur, rows, run_ids)

                portfolio = SimpleNamespace(portfolio_id=scope.ledger_scope_id, initial_cash=account.initial_capital)
                lots = repo.load_latest_positions(scope.ledger_scope_id, next_day)
                cash = repo.load_latest_cash(portfolio, next_day)
                last = history["days"][-1]
                closing_lots = {p["symbol"]: p for p in rows["positions"] if p["trade_date"] == calendar[-1]}
                require(money(cash) == money(last["cash"]) and set(lots) == set(closing_lots)
                        and all((lot.quantity, lot.available_quantity, money(lot.avg_cost), str(lot.trade_date)) ==
                            (closing_lots[s]["quantity"], closing_lots[s]["available_quantity"], money(closing_lots[s]["avg_cost"]),
                             closing_lots[s]["metadata"]["position_trade_date"])
                            for s, lot in lots.items()), "IMPORT_NEXT_OPENING_READBACK")
                cur.execute("SELECT count(*) FROM paper_v2.simulation_release_binding WHERE strategy_id=%s", (account.account_id,))
                require(cur.fetchone()[0] == 0, "IMPORT_UNEXPECTED_RUNTIME_BINDING")
                data = {"account": account.model_dump(mode="json"), "scope": account.account_id, "native": True,
                        "package": package, "replays": [], "valuation": [], "capital_origin": {},
                        "calendar": [{"cal_date": d} for d in calendar]}
                for table, field in SOURCE_KEYS.items():
                    cur.execute(f"SELECT {READ_COLUMNS[field]} FROM paper_v2.{table} WHERE run_id=ANY(%s)", (run_ids,))
                    names = [c[0] for c in cur.description]
                    data[field] = [dict(zip(names, val, strict=True)) for val in cur.fetchall()]
                summary = LocalSimWorkbenchService(repository=scopes)._summary(data)
                require(summary.valuation_day == calendar[-1] and money(summary.nav) == money(last["nav"])
                        and money(summary.valuation_day_pnl) == money(last["valuation_day_pnl"])
                        and money(summary.cumulative_pnl) == money(last["cumulative_nav_pnl"]), "IMPORT_WORKBENCH_READBACK")
        finally:
            conn.rollback()
    with closing(psycopg2.connect(**dsn, options="-c default_transaction_read_only=on")) as conn, conn.cursor() as cur:
        require(not any(row_count(cur, t, run_ids) for t in rows), "IMPORT_DEV_ECONOMIC_ROLLBACK")
        for table, field in (("simulation_account_v1", "account_id"), ("simulation_ledger_scope_v1", "ledger_scope_id")):
            cur.execute(sql.SQL("SELECT count(*) FROM {} WHERE {}=%s").format(
                sql.Identifier("paper_v2", table), sql.Identifier(field)), (account.account_id,))
            require(cur.fetchone()[0] == 0, "IMPORT_DEV_ACCOUNT_ROLLBACK")
        cur.execute("SELECT manifest_sha256 FROM strategy_pkg.package WHERE package_id=%s", (source.package_id,))
        require(cur.fetchone() == package_existing, "IMPORT_DEV_PACKAGE_ROLLBACK")
    return {"schema_version": "localsim_corrected_ledger_consistency_v1", "status": "DEV_FULL_HISTORY_VERIFIED",
        "proof": proof_ref, "history": history_ref, "source": source.account_id, "dev_candidate_account": account.account_id,
        "account_days": len(calendar), "start": calendar[0], "end": calendar[-1], "next_trade_date": str(next_day),
        "row_counts": {k: len(v) for k, v in rows.items()}, "inserted": inserted, "exact_retry_inserted": retry,
        "same_key_conflict_refused": True, "cash_sequence_verified": True,
        "rollback_verified": True, "dev_target": "127.0.0.1:5433/aistock_dev", "next_opening_cash": str(cash),
        "next_opening_positions": len(lots), "next_opening_position_trade_dates_verified": True,
        "verified_closing_nav": summary.nav, "verified_valuation_day_pnl": summary.valuation_day_pnl,
        "verified_cumulative_pnl": summary.cumulative_pnl, "runtime_binding_created": False, "production_writes": 0,
        "execution_events_coverage": "ORIGINAL_PREFIX_ONLY_CORRECTION_STATE_FILE_REFERENCES_PRESERVED"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--proof", required=True, type=Path)
    parser.add_argument("--proof-sha256", required=True)
    parser.add_argument("--env-file", required=True, type=Path)
    parser.add_argument("--receipt", required=True, type=Path)
    args = parser.parse_args()
    require(args.receipt.resolve().is_relative_to(Path("X:/AIstock_temp").resolve()), "IMPORT_RECEIPT_LOCATION")
    proof = file_ref(args.proof)
    require(proof["sha256"] == args.proof_sha256, "IMPORT_PROOF_SHA")
    dev = _settings("dev", args.env_file)
    prod = _settings("production", args.env_file)
    require((prod["host"], prod["port"], prod["dbname"]) != (dev["host"], dev["port"], dev["dbname"]), "IMPORT_DB_TARGET_ALIAS")
    prepared = prepare(prod, proof)
    print("SOURCE_VERIFIED", len(prepared[2]), "days", flush=True)
    receipt = verify_dev(dev, prepared, proof)
    print(publish(args.receipt, {**receipt, "implementation": file_ref(__file__)}), flush=True)


if __name__ == "__main__":
    main()
