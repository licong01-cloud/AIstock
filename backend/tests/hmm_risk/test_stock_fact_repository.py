from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from backend.services.hmm_risk import stock_fact_repository as subject
from backend.services.hmm_risk.state_model_set import StateModelSetError


class _Cursor:
    def __init__(self, connection, *, name=None) -> None:
        self.connection = connection
        self.name = name
        self.sql = ""
        self.itersize = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, sql, params=None) -> None:
        self.sql = " ".join(str(sql).split())
        self.connection.executed.append((self.name, self.sql, params))

    def fetchone(self):
        if self.sql.startswith("SHOW transaction_read_only"):
            return ("on",)
        if "FROM market.stock_universe_pit_state" in self.sql:
            return (
                "immutable_v1",
                "rule_v1",
                "st_only_active",
                date(2020, 1, 1),
                date(2026, 1, 1),
                "ready",
                False,
                "a" * 64,
                datetime(2026, 1, 2, tzinfo=timezone.utc),
            )
        return None

    def fetchall(self):
        if "information_schema.columns" in self.sql:
            return [("daily_basic", "trade_date", "date")]
        if "FROM market.sw_index_classify" in self.sql:
            rows = [("L1", f"L1-{index:02d}", f"I1-{index:02d}", f"L1 Sector {index}") for index in range(31)]
            rows.extend(("L2", f"L2-{index:03d}", f"I2-{index:03d}", f"L2 Sector {index}") for index in range(131))
            return rows
        if "duplicates WHERE conflict_groups>0" in self.sql:
            return self.connection.duplicates
        return []

    def __iter__(self):
        if self.name == "hmm_risk_mapping_source":
            return iter(self.connection.mapping_rows)
        if self.name in {"hmm_risk_stock_fact_source", "hmm_risk_stock_fact_source_l2"}:
            return iter(self.connection.stock_rows)
        if self.name in {"hmm_risk_missing_price_source", "hmm_risk_missing_price_source_l2"}:
            return iter(self.connection.missing_price_rows)
        return iter(())

    def close(self) -> None:
        pass


class _Connection:
    def __init__(self) -> None:
        self.executed = []
        self.duplicates = []
        self.mapping_rows = []
        self.stock_rows = []
        self.missing_price_rows = []

    def cursor(self, name=None):
        return _Cursor(self, name=name)


def _spec() -> subject.StockFactSourceSpec:
    return subject.StockFactSourceSpec(
        universe_key="immutable_v1",
        universe_rule_version="rule_v1",
        source_start=date(2022, 1, 1),
        source_end=date(2025, 4, 30),
    )


def test_reader_allows_identical_duplicates_but_rejects_conflicting_duplicate_keys() -> None:
    connection = _Connection()
    reader = subject.PostgresStockFactReader(connection, _spec())

    state = reader.validate_source()
    lookup = reader.load_classification_lookup()
    reader.validate_fact_uniqueness()

    assert state["universe_key"] == "immutable_v1"
    assert lookup[("L1", "I1-00")]["index_code"] == "L1-00"
    connection.duplicates = [("moneyflow_ts", 2)]
    with pytest.raises(StateModelSetError, match="conflicting duplicate keys"):
        reader.validate_fact_uniqueness()


def test_reader_streams_normalized_mapping_and_scaled_stock_facts() -> None:
    connection = _Connection()
    connection.mapping_rows = [
        (
            date(2024, 1, 2),
            "000001.SZ",
            "I1-00",
            "I2-000",
            date(2020, 1, 1),
            None,
            date(2020, 1, 1),
            None,
            "L1-00",
            "L1 Sector 0",
            "L2-000",
            "L2 Sector 0",
        )
    ]
    connection.stock_rows = [
        (
            date(2024, 1, 2),
            "000001.SZ",
            "L1-00",
            "L1 Sector 0",
            "L2-000",
            "L2 Sector 0",
            date(2020, 1, 1),
            1,
            10_000,
            11_000,
            9_000,
            10_500,
            100,
            1_000_000,
            date(2024, 1, 1),
            10_000,
            date(2023, 12, 25),
            9_000,
            date(2023, 12, 18),
            8_000,
            100.0,
            date(2024, 1, 1),
            date(2024, 1, 1),
            80.0,
            2.0,
            1.0,
            4.0,
            3.0,
            2.0,
            11.0,
        )
    ]
    reader = subject.PostgresStockFactReader(connection, _spec())

    mapping = next(reader.iter_mapping_source_rows())
    stock = next(reader.iter_stock_fact_rows())
    l2_stock = next(reader.iter_stock_fact_rows(sector_level="L2"))
    assert list(reader.iter_missing_price_rows()) == []

    assert mapping["source_l1_code"] == "I1-00"
    assert mapping["l1_code"] == "L1-00"
    assert stock["close_yuan"] == 10.5
    assert stock["volume_shares"] == 10_000.0
    assert stock["amount_cny"] == 1_000.0
    assert stock["prev_circ_mv_cny"] == 800_000.0
    assert stock["net_mf_amount_cny"] == 20_000.0
    assert l2_stock == stock
    l2_queries = [sql for name, sql, _ in connection.executed if name == "hmm_risk_stock_fact_source_l2"]
    assert len(l2_queries) == 1
    assert "ORDER BY c.trade_date,c.l2_code,c.ts_code,c.l1_code" in l2_queries[0]
    assert all("DISTINCT ON" not in sql.upper() for _, sql, _ in connection.executed)
    assert all(params is None or sql.count("%s") == len(params) for _, sql, params in connection.executed)


def test_reader_requires_circ_mv_from_exact_previous_trading_day() -> None:
    connection = _Connection()
    connection.stock_rows = [
        (
            date(2024, 1, 2),
            "000001.SZ",
            "L1-00",
            "L1 Sector 0",
            "L2-000",
            "L2 Sector 0",
            date(2020, 1, 1),
            1,
            10_000,
            11_000,
            9_000,
            10_500,
            100,
            1_000_000,
            date(2024, 1, 1),
            10_000,
            date(2023, 12, 25),
            9_000,
            date(2023, 12, 18),
            8_000,
            100.0,
            date(2024, 1, 1),
            date(2023, 12, 29),
            80.0,
            2.0,
            1.0,
            4.0,
            3.0,
            2.0,
            11.0,
        )
    ]
    row = next(subject.PostgresStockFactReader(connection, _spec()).iter_stock_fact_rows())
    assert row["prev_circ_mv_cny"] is None


def test_reader_accepts_exact_previous_day_circ_mv_before_current_pit_entry() -> None:
    connection = _Connection()
    connection.stock_rows = [
        (
            date(2024, 1, 2),
            "000001.SZ",
            "L1-00",
            "L1 Sector 0",
            "L2-000",
            "L2 Sector 0",
            date(2024, 1, 2),
            1,
            10_000,
            11_000,
            9_000,
            10_500,
            100,
            1_000_000,
            date(2024, 1, 1),
            10_000,
            date(2023, 12, 25),
            9_000,
            date(2023, 12, 18),
            8_000,
            100.0,
            date(2024, 1, 1),
            date(2024, 1, 1),
            80.0,
            2.0,
            1.0,
            4.0,
            3.0,
            2.0,
            11.0,
        )
    ]
    connection.missing_price_rows = [
        (
            date(2024, 1, 2),
            "000001.SZ",
            "L1-00",
            "L1 Sector 0",
            "L2-000",
            "L2 Sector 0",
            1,
            date(2024, 1, 2),
            100.0,
            date(2024, 1, 1),
            date(2024, 1, 1),
            80.0,
        )
    ]
    reader = subject.PostgresStockFactReader(connection, _spec())

    stock_row = next(reader.iter_stock_fact_rows())
    missing_price_row = next(reader.iter_missing_price_rows())

    assert stock_row["prev_circ_mv_cny"] == 800_000.0
    assert stock_row["prev_close_yuan"] is None
    assert stock_row["prev_close_5_yuan"] is None
    assert stock_row["prev_close_10_yuan"] is None
    assert missing_price_row["prev_circ_mv_cny"] == 800_000.0


class _MappingReader:
    spec = _spec()

    def iter_mapping_source_rows(self):
        for index in range(131):
            l1 = index % 31
            yield {
                "trade_date": date(2024, 1, 2),
                "symbol": f"{index:06d}.SZ",
                "source_l1_code": f"I1-{l1:02d}",
                "source_l2_code": f"I2-{index:03d}",
                "in_date": date(2020, 1, 1),
                "out_date": None,
                "eligible_start": date(2020, 1, 1),
                "eligible_end": None,
                "l1_code": f"L1-{l1:02d}",
                "l1_name": f"L1 Sector {l1}",
                "l2_code": f"L2-{index:03d}",
                "l2_name": f"L2 Sector {index}",
            }


def test_mapping_manifest_freezes_all_source_rows_and_31_131_constituents() -> None:
    manifest, constituents = subject.load_mapping_manifest(_MappingReader())

    assert manifest["source_row_count"] == 131
    assert manifest["canonical_l1_count"] == 31
    assert manifest["canonical_l2_count"] == 131
    assert len(constituents) == 31


class _FactReader:
    spec = _spec()

    def iter_missing_price_rows(self):
        return iter(())

    def iter_stock_fact_rows(self):
        for l1 in range(31):
            for stock in range(10):
                close = 10.0 + stock / 10.0
                yield {
                    "trade_date": date(2024, 1, 2),
                    "symbol": f"{l1:02d}{stock:04d}.SZ",
                    "l1_code": f"L1-{l1:02d}",
                    "l1_name": f"L1 Sector {l1}",
                    "l2_code": f"L2-{l1 * 4:03d}",
                    "l2_name": "L2",
                    "is_suspended": False,
                    "open_yuan": close,
                    "high_yuan": close + 0.1,
                    "low_yuan": close - 0.1,
                    "close_yuan": close,
                    "volume_shares": 1000.0,
                    "amount_cny": 10_000.0,
                    "prev_close_yuan": close / 1.01,
                    "prev_close_5_yuan": close / 1.05,
                    "prev_close_10_yuan": close / 1.10,
                    "total_mv_cny": 1_000_000.0,
                    "prev_circ_mv_cny": 800_000.0,
                    "buy_sm_amount_cny": 100.0,
                    "sell_sm_amount_cny": 90.0,
                    "buy_elg_amount_cny": 200.0,
                    "sell_elg_amount_cny": 180.0,
                    "net_mf_amount_cny": 30.0,
                    "up_limit_yuan": close + 1.0,
                }


def test_daily_aggregate_loader_hashes_raw_rows_and_returns_all_l1() -> None:
    aggregates, manifest = subject.load_daily_aggregates(_FactReader())

    assert len(aggregates) == 31
    assert manifest["raw_row_count"] == 310
    assert manifest["aggregate_row_count"] == 31
    assert len(manifest["raw_jsonl_sha256"]) == 64
