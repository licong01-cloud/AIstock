from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from backend.services.hmm_risk import stock_fact_repository as subject
from backend.services.hmm_risk.security_identity import load_security_source_identity_manifest
from backend.services.hmm_risk.provider_absence import load_provider_absence_manifest
from backend.services.hmm_risk.state_model_set import StateModelSetError, canonical_sha256


class _Cursor:
    def __init__(self, connection, *, name=None) -> None:
        self.connection = connection
        self.name = name
        self.sql = ""
        self.params = None
        self.itersize = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, sql, params=None) -> None:
        self.sql = " ".join(str(sql).split())
        self.params = params
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
        if "SELECT cal_date::date FROM market.trading_calendar" in self.sql:
            start, end = self.params
            rows = []
            current = start
            while current <= end:
                if current.weekday() < 5:
                    rows.append((current,))
                current = date.fromordinal(current.toordinal() + 1)
            return rows
        if "FROM requested LEFT JOIN LATERAL" in self.sql:
            history_start, before_date = self.params[1:3]
            latest = {}
            for row in self.connection.stock_rows:
                source_date = row[22]
                if source_date is not None and history_start <= source_date < before_date:
                    key = row[1]
                    candidate = (key, source_date, row[23])
                    if key not in latest or source_date > latest[key][1]:
                        latest[key] = candidate
            return [latest[key] for key in sorted(latest)]
        if "SELECT trade_date,ts_code,total_mv,circ_mv FROM market.daily_basic" in self.sql:
            start, end, codes = self.params
            facts = {}
            for row in self.connection.stock_rows:
                if row[1] not in codes:
                    continue
                if start <= row[0] <= end:
                    facts[(row[0], row[1])] = (row[0], row[1], row[20], row[23])
                if row[22] is not None and start <= row[22] <= end:
                    facts[(row[22], row[1])] = (row[22], row[1], None, row[23])
            return [facts[key] for key in sorted(facts)]
        if "FROM market.moneyflow_ts" in self.sql and "buy_sm_amount" in self.sql:
            start, end, codes = self.params
            rows = [
                (row[0], row[25], *row[26:31])
                for row in self.connection.stock_rows
                if row[25] is not None and row[25] in codes and start <= row[0] <= end
            ]
            rows.extend(
                (row[0], row[31], *row[26:31])
                for row in self.connection.stock_rows
                if row[31] is not None and row[31] in codes and start <= row[0] <= end
            )
            return rows
        if "SELECT trade_date,ts_code,up_limit FROM market.stk_limit" in self.sql:
            start, end, codes = self.params
            return [
                (row[0], row[1], row[32])
                for row in self.connection.stock_rows
                if row[1] in codes and start <= row[0] <= end
            ]
        return []

    def __iter__(self):
        if self.name == "hmm_risk_mapping_source":
            return iter(self.connection.mapping_rows)
        if self.name and self.name.startswith("hmm_risk_missing_price_base"):
            window_start, window_end = self.params[2:4]
            return iter(row for row in self.connection.missing_rows if window_start <= row[0] <= window_end)
        if self.name and self.name.startswith("hmm_risk_stock_fact_base"):
            window_start, window_end = self.params[-2:]
            return iter(
                (*row[:20], row[21]) for row in self.connection.stock_rows if window_start <= row[0] <= window_end
            )
        if self.name and self.name.startswith("hmm_risk_stock_fact_source"):
            window_start, window_end = self.params[-2:]
            return iter(row for row in self.connection.stock_rows if window_start <= row[0] <= window_end)
        return iter(())

    def close(self) -> None:
        pass


class _Connection:
    def __init__(self) -> None:
        self.executed = []
        self.duplicates = []
        self.mapping_rows = []
        self.missing_rows = []
        self.stock_rows = []

    def cursor(self, name=None):
        return _Cursor(self, name=name)


def _spec(*, circ_mv_history_start: date | None = None) -> subject.StockFactSourceSpec:
    return subject.StockFactSourceSpec(
        universe_key="immutable_v1",
        universe_rule_version="rule_v1",
        source_start=date(2022, 1, 1),
        source_end=date(2025, 4, 30),
        circ_mv_history_start=circ_mv_history_start,
    )


def _identity_manifest():
    path = (
        Path(__file__).resolve().parents[3]
        / "backend"
        / "services"
        / "hmm_risk"
        / "manifests"
        / "security_source_identity_v1.json"
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    return load_security_source_identity_manifest(path, expected_sha256=canonical_sha256(payload))


def _provider_absence_manifest():
    path = Path(__file__).resolve().parents[2] / "services" / "hmm_risk" / "manifests" / "provider_absence_v1.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    return load_provider_absence_manifest(path, expected_sha256=canonical_sha256(payload))


def _reader(
    connection: _Connection,
    *,
    circ_mv_history_start: date | None = None,
) -> subject.PostgresStockFactReader:
    return subject.PostgresStockFactReader(
        connection,
        _spec(circ_mv_history_start=circ_mv_history_start),
        security_identity_manifest=_identity_manifest(),
        provider_absence_manifest=_provider_absence_manifest(),
    )


def test_reader_allows_identical_duplicates_but_rejects_conflicting_duplicate_keys() -> None:
    connection = _Connection()
    reader = _reader(connection)

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
            0,
            "000001.SZ",
            2.0,
            1.0,
            4.0,
            3.0,
            2.0,
            "000001.SZ",
            11.0,
        )
    ]
    reader = _reader(connection)

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
    assert stock["circ_mv_source_date"] == date(2024, 1, 1)
    assert stock["circ_mv_staleness_trading_days"] == 0
    assert stock["net_mf_amount_cny"] == 20_000.0
    assert stock["moneyflow_fact_status"] == "available"
    assert stock["moneyflow_source_identity"]["source_ts_code"] == "000001.SZ"
    assert l2_stock == stock
    l2_queries = [
        sql for name, sql, _ in connection.executed if name and name.startswith("hmm_risk_stock_fact_base_l2_")
    ]
    assert l2_queries
    assert all("ORDER BY c.trade_date,c.l2_code,c.ts_code,c.l1_code" in sql for sql in l2_queries)
    base_queries = [sql for name, sql, _ in connection.executed if name and "stock_fact_base" in name]
    assert all("market.daily_basic" not in sql for sql in base_queries)
    assert all("market.moneyflow_ts" not in sql for sql in base_queries)
    missing_queries = [sql for name, sql, _ in connection.executed if name and "missing_price_base" in name]
    assert missing_queries and all("missing_keys AS MATERIALIZED" in sql for sql in missing_queries)
    assert all("DISTINCT ON" not in sql.upper() for _, sql, _ in connection.executed)
    assert all(params is None or sql.count("%s") == len(params) for _, sql, params in connection.executed)


def test_all_stock_fact_query_paths_use_authoritative_full_day_suspension_before_price_lags() -> None:
    connection = _Connection()
    reader = _reader(connection)

    reader._load_stock_base_rows(
        window_start=date(2024, 1, 1),
        window_end=date(2024, 1, 31),
        fetch_size=100,
        sector_level="L1",
    )
    reader._load_missing_price_base_rows(
        window_start=date(2024, 1, 1),
        window_end=date(2024, 1, 31),
        fetch_size=100,
        sector_level="L1",
    )
    list(
        reader.iter_stock_fact_rows(
            _window_start=date(2024, 1, 1),
            _window_end=date(2024, 1, 31),
        )
    )

    manifest = _identity_manifest()

    class AliasManifest:
        def alias_rows(self, source_dataset: str):
            if source_dataset == "market.daily_basic":
                return [
                    {
                        "canonical_ts_code": "000001.SZ",
                        "source_ts_code": "000002.SZ",
                        "effective_start": "2024-01-01",
                        "effective_end": "2024-01-31",
                        "security_identity_id": "test-alias",
                        "row_hash": "a" * 64,
                    }
                ]
            return manifest.alias_rows(source_dataset)

        def resolve(self, *args, **kwargs):
            return manifest.resolve(*args, **kwargs)

        def evidence(self):
            return manifest.evidence()

    alias_reader = subject.PostgresStockFactReader(
        connection,
        _spec(),
        security_identity_manifest=AliasManifest(),
        provider_absence_manifest=_provider_absence_manifest(),
    )
    list(alias_reader.iter_missing_price_rows())

    stock_queries = [
        sql
        for name, sql, _ in connection.executed
        if name and (name.startswith("hmm_risk_stock_fact_base_") or name.startswith("hmm_risk_stock_fact_source_"))
    ]
    missing_queries = [
        sql
        for name, sql, _ in connection.executed
        if name and (name.startswith("hmm_risk_missing_price_base_") or name == "hmm_risk_missing_price_source")
    ]
    assert len(stock_queries) == 2
    assert len(missing_queries) == 2

    for sql in (*stock_queries, *missing_queries):
        assert "suspension.suspend_type='S'" in sql
        assert "COALESCE(BTRIM(suspension.suspend_timing),'') IN ('','09:30-09:30')" in sql
        assert "resume.suspend_type='R'" not in sql
        assert "NOT ( EXISTS" in sql

    for sql in stock_queries:
        price_base = sql.split("price_base AS (", 1)[1].split("), price_history AS", 1)[0]
        assert "market.suspend_d suspension" in price_base
        assert "COALESCE(price.volume_hand,0)=0" in price_base
        assert "COALESCE(price.amount_li,0)=0" in price_base


def test_full_day_suspension_predicate_rejects_unregistered_sql_aliases() -> None:
    with pytest.raises(ValueError, match="unsupported full-day suspension SQL identity"):
        subject._full_day_suspension_exists_sql(trade_date="unsafe.trade_date", ts_code="unsafe.ts_code")


def test_reader_uses_latest_causal_circ_mv_before_previous_market_day() -> None:
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
            1,
            "000001.SZ",
            2.0,
            1.0,
            4.0,
            3.0,
            2.0,
            "000001.SZ",
            11.0,
        )
    ]
    row = next(_reader(connection).iter_stock_fact_rows())
    assert row["prev_circ_mv_cny"] == 800_000.0
    assert row["circ_mv_source_date"] == date(2023, 12, 29)
    assert row["circ_mv_staleness_trading_days"] == 1

    non_causal = list(connection.stock_rows[0])
    non_causal[22] = date(2024, 1, 2)
    connection.stock_rows[0] = tuple(non_causal)
    rejected = next(_reader(connection).iter_stock_fact_rows())
    assert rejected["prev_circ_mv_cny"] is None
    assert rejected["circ_mv_source_date"] is None
    assert rejected["circ_mv_staleness_trading_days"] is None


def test_reader_preserves_causal_circ_mv_across_current_pit_entry_boundary() -> None:
    connection = _Connection()
    connection.stock_rows = [
        (
            date(2024, 1, 3),
            "000001.SZ",
            "L1-00",
            "L1 Sector 0",
            "L2-000",
            "L2 Sector 0",
            date(2024, 1, 3),
            1,
            10_000,
            11_000,
            9_000,
            10_500,
            100,
            1_000_000,
            date(2024, 1, 2),
            10_000,
            date(2023, 12, 25),
            9_000,
            date(2023, 12, 18),
            8_000,
            100.0,
            date(2024, 1, 2),
            date(2024, 1, 2),
            80.0,
            0,
            "000001.SZ",
            2.0,
            1.0,
            4.0,
            3.0,
            2.0,
            "000001.SZ",
            11.0,
        )
    ]

    row = next(_reader(connection).iter_stock_fact_rows())

    assert row["prev_circ_mv_cny"] == 800_000.0
    assert row["circ_mv_source_date"] == date(2024, 1, 2)
    assert row["circ_mv_staleness_trading_days"] == 0
    assert row["circ_mv_crossed_pit_entry_boundary"] is True
    assert row["circ_mv_history_start"] == date(2022, 1, 1)
    assert row["circ_mv_lookback_contract_version"] == "hmm_risk_causal_circ_mv_source_window_v1"
    assert row["circ_mv_fact_status"] == "available"
    assert row["circ_mv_reason_code"] is None


def test_reader_rejects_circ_mv_before_immutable_source_window() -> None:
    connection = _Connection()
    connection.stock_rows = [
        (
            date(2022, 1, 3),
            "000001.SZ",
            "L1-00",
            "L1 Sector 0",
            "L2-000",
            "L2 Sector 0",
            date(2022, 1, 3),
            1,
            10_000,
            11_000,
            9_000,
            10_500,
            100,
            1_000_000,
            date(2021, 12, 31),
            10_000,
            date(2021, 12, 24),
            9_000,
            date(2021, 12, 17),
            8_000,
            100.0,
            date(2021, 12, 31),
            date(2021, 12, 31),
            80.0,
            0,
            "000001.SZ",
            2.0,
            1.0,
            4.0,
            3.0,
            2.0,
            "000001.SZ",
            11.0,
        )
    ]

    row = next(_reader(connection).iter_stock_fact_rows())

    assert row["prev_circ_mv_cny"] is None
    assert row["circ_mv_source_date"] is None
    assert row["circ_mv_staleness_trading_days"] is None
    assert row["circ_mv_crossed_pit_entry_boundary"] is False
    assert row["circ_mv_history_start"] == date(2022, 1, 1)
    assert row["circ_mv_fact_status"] == "source_unavailable"
    assert row["circ_mv_reason_code"] == "hmm_risk_stock_fact_circ_mv_source_unavailable"


def test_reader_uses_separate_immutable_circ_mv_history_before_observation_window() -> None:
    connection = _Connection()
    connection.stock_rows = [
        (
            date(2022, 1, 3),
            "000001.SZ",
            "L1-00",
            "L1 Sector 0",
            "L2-000",
            "L2 Sector 0",
            date(2022, 1, 3),
            1,
            10_000,
            11_000,
            9_000,
            10_500,
            100,
            1_000_000,
            date(2021, 12, 31),
            10_000,
            date(2021, 12, 24),
            9_000,
            date(2021, 12, 17),
            8_000,
            100.0,
            date(2021, 12, 31),
            date(2021, 12, 31),
            80.0,
            0,
            "000001.SZ",
            2.0,
            1.0,
            4.0,
            3.0,
            2.0,
            "000001.SZ",
            11.0,
        )
    ]

    row = next(
        _reader(
            connection,
            circ_mv_history_start=date(2020, 7, 30),
        ).iter_stock_fact_rows()
    )

    assert row["prev_circ_mv_cny"] == 800_000.0
    assert row["circ_mv_source_date"] == date(2021, 12, 31)
    assert row["circ_mv_crossed_pit_entry_boundary"] is True
    assert row["circ_mv_history_start"] == date(2020, 7, 30)


def test_stock_fact_spec_rejects_circ_mv_history_after_observation_start() -> None:
    with pytest.raises(StateModelSetError, match="history window must start no later"):
        _spec(circ_mv_history_start=date(2022, 1, 2)).validate()


def test_circ_mv_sql_fragments_use_source_history_start_not_pit_entry() -> None:
    for has_alias, expected_count in ((False, 1), (True, 3)):
        _, _, join_sql = subject.PostgresStockFactReader._circ_mv_asof_fragments(
            previous_date="calendar.previous_trade_date",
            history_start="contract.history_start",
            has_alias=has_alias,
        )

        assert join_sql.count("trade_date>=contract.history_start") == expected_count
        assert "eligible_start" not in join_sql


@pytest.mark.parametrize(
    ("raw_value", "expected_status", "expected_reason"),
    [
        (None, "latest_value_missing", "hmm_risk_stock_fact_circ_mv_latest_value_missing"),
        (0, "latest_value_non_positive", "hmm_risk_stock_fact_circ_mv_latest_value_non_positive"),
        (-1, "latest_value_non_positive", "hmm_risk_stock_fact_circ_mv_latest_value_non_positive"),
        (float("nan"), "latest_value_non_finite", "hmm_risk_stock_fact_circ_mv_latest_value_non_finite"),
        ("invalid", "latest_value_non_numeric", "hmm_risk_stock_fact_circ_mv_latest_value_non_numeric"),
    ],
)
def test_circ_mv_invalid_latest_value_preserves_causal_source_without_fallback(
    raw_value,
    expected_status,
    expected_reason,
) -> None:
    evidence = subject._build_circ_mv_evidence(
        raw_value=raw_value,
        source_date=date(2024, 1, 2),
        staleness_trading_days=0,
        trade_date=date(2024, 1, 3),
        pit_eligible_start=date(2024, 1, 3),
        history_start=date(2022, 1, 1),
    )

    assert evidence.accepted_value is None
    assert evidence.source_date == date(2024, 1, 2)
    assert evidence.staleness_trading_days == 0
    assert evidence.crossed_pit_entry_boundary is True
    assert evidence.fact_status == expected_status
    assert evidence.reason_code == expected_reason


def test_reader_resolves_historical_moneyflow_source_without_rewriting_canonical_symbol() -> None:
    connection = _Connection()
    connection.stock_rows = [
        (
            date(2024, 6, 28),
            "302132.SZ",
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
            date(2024, 6, 27),
            10_000,
            date(2024, 6, 21),
            9_000,
            date(2024, 6, 14),
            8_000,
            100.0,
            date(2024, 6, 27),
            date(2024, 6, 27),
            80.0,
            0,
            "300114.SZ",
            2.0,
            1.0,
            4.0,
            3.0,
            2.0,
            None,
            11.0,
        )
    ]

    row = next(_reader(connection).iter_stock_fact_rows())

    assert row["symbol"] == "302132.SZ"
    assert row["moneyflow_source_identity"]["source_ts_code"] == "300114.SZ"
    assert row["moneyflow_source_identity"]["resolution_kind"] == "explicit_effective_alias"
    assert row["moneyflow_fact_status"] == "available"

    conflicting = list(connection.stock_rows[0])
    conflicting[31] = "302132.SZ"
    connection.stock_rows[0] = tuple(conflicting)
    with pytest.raises(StateModelSetError, match="canonical and aliased moneyflow rows coexist"):
        next(_reader(connection).iter_stock_fact_rows())


def test_reader_marks_missing_provider_moneyflow_as_na_with_identity_evidence() -> None:
    connection = _Connection()
    connection.stock_rows = [
        (
            date(2024, 1, 8),
            "603595.SH",
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
            date(2024, 1, 5),
            10_000,
            date(2023, 12, 29),
            9_000,
            date(2023, 12, 22),
            8_000,
            100.0,
            date(2024, 1, 5),
            date(2024, 1, 5),
            80.0,
            0,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            11.0,
        )
    ]

    row = next(_reader(connection).iter_stock_fact_rows())

    assert row["moneyflow_fact_status"] == "provider_absence"
    assert row["net_mf_amount_cny"] is None
    assert row["moneyflow_source_identity"]["source_ts_code"] == "603595.SH"
    assert row["moneyflow_provider_absence"]["provider_audit_receipt_sha256"] == (
        "a96c19313e110e7ea3ce67f33d0027eaef3ef494898f5d8db7362c9e88670fec"
    )
    assert row["moneyflow_provider_absence"]["row_hash"] == (
        "7f7eb116ab9b800995eeea98c7c1d050bea6674702d7b6994906a2bcaee147b6"
    )

    connection.stock_rows[0] = (date(2024, 1, 9), *connection.stock_rows[0][1:])
    with pytest.raises(StateModelSetError, match="provider_absence_unverified"):
        next(_reader(connection).iter_stock_fact_rows())


def test_missing_price_reader_uses_split_causal_daily_basic_without_evaluating_moneyflow() -> None:
    connection = _Connection()
    connection.missing_rows = [
        (
            date(2024, 1, 2),
            "000001.SZ",
            "L1-00",
            "L1 Sector 0",
            "L2-000",
            "L2 Sector 0",
            1,
            date(2020, 1, 1),
            date(2024, 1, 1),
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
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            100.0,
            date(2024, 1, 1),
            date(2024, 1, 1),
            80.0,
            0,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    ]

    row = next(_reader(connection).iter_missing_price_rows())

    assert row["close_yuan"] is None
    assert row["total_mv_cny"] == 1_000_000.0
    assert row["prev_circ_mv_cny"] == 800_000.0
    assert row["circ_mv_source_date"] == date(2024, 1, 1)
    assert row["moneyflow_fact_status"] == "not_evaluated_missing_price"
    assert row["moneyflow_provider_absence"] is None


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
    security_identity_manifest = _identity_manifest()
    provider_absence_manifest = _provider_absence_manifest()

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
                    "circ_mv_source_date": date(2024, 1, 1),
                    "circ_mv_staleness_trading_days": 0,
                    "circ_mv_crossed_pit_entry_boundary": False,
                    "circ_mv_pit_eligible_start": date(2020, 1, 1),
                    "circ_mv_history_start": date(2022, 1, 1),
                    "circ_mv_lookback_contract_version": "hmm_risk_causal_circ_mv_source_window_v1",
                    "circ_mv_fact_status": "available",
                    "circ_mv_reason_code": None,
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
    assert manifest["circ_mv_lookback_contract_version"] == "hmm_risk_causal_circ_mv_source_window_v1"
    assert manifest["circ_mv_pit_boundary_crossing_count"] == 0


def test_daily_aggregate_uses_effective_circ_mv_history_contract_identity() -> None:
    class HistoryReader(_FactReader):
        spec = _spec(circ_mv_history_start=date(2020, 7, 30))

        def iter_stock_fact_rows(self):
            for row in super().iter_stock_fact_rows():
                yield {**row, "circ_mv_history_start": date(2020, 7, 30)}

    _, manifest = subject.load_daily_aggregates(HistoryReader())

    assert manifest["source_window_start"] == "2022-01-01"
    assert manifest["circ_mv_history_start"] == "2020-07-30"


def test_daily_aggregate_manifest_persists_pit_boundary_crossing_receipt() -> None:
    class BoundaryReader(_FactReader):
        def iter_stock_fact_rows(self):
            for index, row in enumerate(super().iter_stock_fact_rows()):
                if index == 0:
                    yield {
                        **row,
                        "circ_mv_crossed_pit_entry_boundary": True,
                        "circ_mv_pit_eligible_start": date(2024, 1, 2),
                    }
                else:
                    yield row

    _, manifest = subject.load_daily_aggregates(BoundaryReader())

    assert manifest["circ_mv_pit_boundary_crossing_count"] == 1
    assert manifest["circ_mv_pit_boundary_crossing_available_count"] == 1
    assert manifest["circ_mv_pit_boundary_crossing_invalid_count"] == 0
    assert len(manifest["circ_mv_pit_boundary_crossing_key_sha256"]) == 64


def test_daily_aggregate_manifest_counts_invalid_latest_crossing_without_fallback() -> None:
    class BoundaryReader(_FactReader):
        def iter_stock_fact_rows(self):
            for index, row in enumerate(super().iter_stock_fact_rows()):
                if index == 0:
                    yield {
                        **row,
                        "prev_circ_mv_cny": None,
                        "circ_mv_crossed_pit_entry_boundary": True,
                        "circ_mv_pit_eligible_start": date(2024, 1, 2),
                        "circ_mv_fact_status": "latest_value_missing",
                        "circ_mv_reason_code": "hmm_risk_stock_fact_circ_mv_latest_value_missing",
                    }
                else:
                    yield row

    _, manifest = subject.load_daily_aggregates(BoundaryReader())

    assert manifest["circ_mv_pit_boundary_crossing_count"] == 1
    assert manifest["circ_mv_pit_boundary_crossing_available_count"] == 0
    assert manifest["circ_mv_pit_boundary_crossing_invalid_count"] == 1


def test_daily_aggregate_manifest_rejects_crossing_with_wrong_history_identity() -> None:
    class BoundaryReader(_FactReader):
        def iter_stock_fact_rows(self):
            for index, row in enumerate(super().iter_stock_fact_rows()):
                if index == 0:
                    yield {
                        **row,
                        "circ_mv_crossed_pit_entry_boundary": True,
                        "circ_mv_pit_eligible_start": date(2024, 1, 2),
                        "circ_mv_history_start": date(2023, 1, 1),
                    }
                else:
                    yield row

    with pytest.raises(
        StateModelSetError,
        match="hmm_risk_stock_fact_circ_mv_evidence_contract_invalid",
    ):
        subject.load_daily_aggregates(BoundaryReader())


def test_daily_aggregate_manifest_rejects_false_crossing_flag_for_derived_boundary() -> None:
    class BoundaryReader(_FactReader):
        def iter_stock_fact_rows(self):
            for index, row in enumerate(super().iter_stock_fact_rows()):
                if index == 0:
                    yield {
                        **row,
                        "circ_mv_crossed_pit_entry_boundary": False,
                        "circ_mv_pit_eligible_start": date(2024, 1, 2),
                    }
                else:
                    yield row

    with pytest.raises(
        StateModelSetError,
        match="hmm_risk_stock_fact_circ_mv_pit_boundary_evidence_invalid",
    ):
        subject.load_daily_aggregates(BoundaryReader())


def test_direct_loader_builds_l1_l2_from_one_database_stream() -> None:
    class DirectReader(_FactReader):
        def __init__(self) -> None:
            self.stock_calls = 0
            self.missing_calls = 0

        def iter_missing_price_rows(self):
            self.missing_calls += 1
            return super().iter_missing_price_rows()

        def iter_stock_fact_rows(self):
            self.stock_calls += 1
            return super().iter_stock_fact_rows()

    reader = DirectReader()
    l1, l1_manifest, l2, l2_manifest = subject.load_direct_daily_aggregates(reader)

    assert reader.stock_calls == 1
    assert reader.missing_calls == 1
    assert len(l1) == 31
    assert len(l2) == 31
    assert l1_manifest["raw_row_count"] == 310
    assert l2_manifest["raw_row_count"] == 310
    assert (
        l1_manifest["moneyflow_provider_absence_key_sha256"] == (l2_manifest["moneyflow_provider_absence_key_sha256"])
    )
    assert l2_manifest["direct_sector_level"] == "L2"
