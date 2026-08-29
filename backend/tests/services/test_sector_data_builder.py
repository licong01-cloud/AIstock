from __future__ import annotations

import datetime as dt
import hashlib
import json
from decimal import Decimal
from pathlib import Path
from typing import Mapping

import pytest

from backend.services import sector_data_builder as module
from backend.services.dataset_release.canonical import canonical_json_bytes
from backend.services.industry_pit.artifact_store import CandidateBundleReadback
from backend.services.industry_pit.contracts import (
    CLASSIFICATION_CANDIDATE_SCHEMA,
    INDEX_MEMBERSHIP_CANDIDATE_SCHEMA,
    AuthorityReceipt,
    AuthorityType,
    KnowledgeTimePolicy,
    ResearchBasis,
    TaxonomyIdentity,
    make_candidate_interval,
)


class _Cursor:
    def __init__(self, preflight=(0, 0, 0, 0, 0, 0, 0), build_rows=7):
        self.preflight = preflight
        self.build_rows = build_rows
        self.executed = []
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params):
        self.executed.append((sql, params))
        self.rowcount = self.build_rows if sql is module._BUILD_DAY_SQL else 0

    def fetchone(self):
        return self.preflight


class _Connection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.commits = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commits += 1


def test_build_date_uses_dynamic_industry_mapping_without_persisted_identity(monkeypatch):
    cursor = _Cursor()
    connection = _Connection(cursor)
    monkeypatch.setattr(module, "get_conn", lambda: connection)

    rows = module.SectorDataBuilder().build_date(dt.date(2026, 7, 22))

    assert rows == 7
    assert connection.commits == 1
    assert [sql for sql, _ in cursor.executed] == [
        module._PREFLIGHT_DAY_SQL,
        module._DELETE_STALE_DAY_SQL,
        module._BUILD_DAY_SQL,
    ]
    for _, params in cursor.executed:
        assert params == {
            "trade_date": dt.date(2026, 7, 22),
            "live_universe_key": module.DEFAULT_ST_PIT_UNIVERSE_KEY,
            "qe_universe_pattern": f"{module.IMMUTABLE_QE_ST_PIT_UNIVERSE_PREFIX}%",
        }
    assert "market.stock_universe_pit_spans" in module._BUILD_DAY_SQL
    assert "JOIN authoritative_universes USING (universe_key)" in module._BUILD_DAY_SQL
    assert "market.sw_index_member" in module._BUILD_DAY_SQL
    assert "l1_code, l2_code, mapping_in_date" not in module._BUILD_DAY_SQL
    assert "mapping_in_date     = EXCLUDED.mapping_in_date" not in module._BUILD_DAY_SQL
    assert "DISTINCT ON" not in module._BUILD_DAY_SQL


@pytest.mark.parametrize(
    ("preflight", "message"),
    [
        ((1, 0, 0, 0, 0, 0, 0), "universe_not_ready=1"),
        ((0, 1, 0, 0, 0, 0, 0), "missing_pit_mappings=1"),
        ((0, 0, 1, 0, 0, 0, 0), "ambiguous_latest_mappings=1"),
        ((0, 0, 0, 1, 0, 0, 0), "invalid_mapping_identities=1"),
        ((0, 0, 0, 0, 1, 0, 0), "missing_sw_daily_facts=1"),
        ((0, 0, 0, 0, 0, 1, 0), "missing_l2_moneyflow_facts=1"),
    ],
)
def test_build_date_fails_loudly_before_mutation(monkeypatch, preflight, message):
    cursor = _Cursor(preflight=preflight)
    connection = _Connection(cursor)
    monkeypatch.setattr(module, "get_conn", lambda: connection)

    with pytest.raises(module.SectorDataBuildContractError, match=message):
        module.SectorDataBuilder().build_date(dt.date(2026, 7, 22))

    assert connection.commits == 0
    assert [sql for sql, _ in cursor.executed] == [module._PREFLIGHT_DAY_SQL]


def test_preflight_exempts_unpublished_l2_from_sw_daily_contract():
    sql = module._PREFLIGHT_DAY_SQL

    assert "unpublished_l2 AS" in sql
    assert "market.sw_index_classify" in sql
    assert "is_pub = '0'" in sql
    assert "pit.l2_code NOT IN (SELECT index_code FROM unpublished_l2)" in sql
    assert "pit.l2_code IN (SELECT index_code FROM unpublished_l2)" in sql


def test_build_date_exempted_unpublished_l2_logs_warning_and_builds(monkeypatch, caplog):
    cursor = _Cursor(preflight=(0, 0, 0, 0, 0, 0, 16))
    connection = _Connection(cursor)
    monkeypatch.setattr(module, "get_conn", lambda: connection)

    with caplog.at_level("WARNING", logger=module.logger.name):
        rows = module.SectorDataBuilder().build_date(dt.date(2026, 7, 22))

    assert rows == 7
    assert connection.commits == 1
    assert [sql for sql, _ in cursor.executed] == [
        module._PREFLIGHT_DAY_SQL,
        module._DELETE_STALE_DAY_SQL,
        module._BUILD_DAY_SQL,
    ]
    assert any(
        "16 stocks exempted" in record.getMessage()
        and "is_pub=0" in record.getMessage()
        for record in caplog.records
    )


def test_schema_and_retirement_keep_sector_data_fact_only():
    root = Path(__file__).resolve().parents[3]
    schema = (root / "scripts/create_sw_sector_tables.py").read_text(encoding="utf-8")
    retirement = (
        root / "backend/db/migrations/sector_data_pit_identity_retirement_v1.sql"
    ).read_text(encoding="utf-8")

    for column in ("l1_code", "l2_code", "mapping_in_date"):
        assert f"{column}              TEXT" not in schema
        assert f"DROP COLUMN IF EXISTS {column}" in retirement

    assert not (root / "backend/db/migrations/sector_data_pit_identity_v1.sql").exists()
    assert "SECTOR_DATA_PERSISTED_PIT_IDENTITY_RETIREMENT_INCOMPLETE" in retirement
    assert "resolve industry identity dynamically from sw_index_member" in retirement


_HASH_A = "a" * 64
_HASH_B = "b" * 64
_HASH_C = "c" * 64
_TRADE_DATE = dt.date(2022, 1, 4)
_CHEMICAL = TaxonomyIdentity(
    l1_code="220000",
    l1_name="基础化工",
    l2_code="220300",
    l2_name="化学制品",
    l3_code="220315",
    l3_name="食品及饲料添加剂",
)
_CHEMICAL_OTHER_L3 = TaxonomyIdentity(
    l1_code="220000",
    l1_name=_CHEMICAL.l1_name,
    l2_code="220300",
    l2_name=_CHEMICAL.l2_name,
    l3_code="220316",
    l3_name="other-l3",
)
_CHEMICAL_CONFLICTING_L2_NAME = TaxonomyIdentity(
    l1_code="220000",
    l1_name=_CHEMICAL.l1_name,
    l2_code="220300",
    l2_name="conflicting-l2-name",
    l3_code="220316",
    l3_name="other-l3",
)
_FOOD = TaxonomyIdentity(
    l1_code="110000",
    l1_name="食品饮料",
    l2_code="110100",
    l2_name="食品加工",
    l3_code="110101",
    l3_name="食品综合",
)


def _receipt(authority_type: AuthorityType, *, denominator: int = 2) -> AuthorityReceipt:
    return AuthorityReceipt(
        authority_type=authority_type,
        authority_schema=(
            CLASSIFICATION_CANDIDATE_SCHEMA
            if authority_type is AuthorityType.CLASSIFICATION
            else INDEX_MEMBERSHIP_CANDIDATE_SCHEMA
        ),
        authority_version=f"test_{authority_type.value}_v1",
        taxonomy_contract_id="sw2021",
        taxonomy_version="2021",
        knowledge_time_policy=KnowledgeTimePolicy.CAUSAL_DAILY_NEXT_TRADE,
        research_basis=ResearchBasis.AS_PUBLISHED_PIT,
        source_ids=("test:source",),
        source_hashes=(_HASH_A,),
        frozen_denominator=denominator,
        denominator_digest=_HASH_B,
    )


def _interval(
    *,
    symbol: str,
    authority_type: AuthorityType,
    receipt: AuthorityReceipt,
    identity: TaxonomyIdentity,
    valid_from: dt.date = dt.date(2021, 8, 2),
    valid_to: dt.date | None = None,
    index_codes: tuple[str, str, str] = ("801120", "801123", "801124"),
):
    authority_identity = (
        {
            "classification_l1_code": identity.l1_code,
            "classification_l2_code": identity.l2_code,
            "classification_l3_code": identity.l3_code,
        }
        if authority_type is AuthorityType.CLASSIFICATION
        else {
            "index_l1_code": index_codes[0],
            "index_l2_code": index_codes[1],
            "index_l3_code": index_codes[2],
        }
    )
    return make_candidate_interval(
        canonical_symbol=symbol,
        authority_type=authority_type,
        taxonomy_contract_id="sw2021",
        taxonomy_version="2021",
        authority_receipt_hash=receipt.receipt_hash,
        valid_from=valid_from,
        valid_to_exclusive=valid_to,
        eligible_from=dt.date(2020, 1, 1),
        eligible_to_exclusive=dt.date(2027, 1, 1),
        causal_use_from=valid_from,
        causal_use_to_exclusive=valid_to,
        known_from=valid_from,
        source_effective_field=(
            "计入日期"
            if authority_type is AuthorityType.CLASSIFICATION
            else "membership_enter_date/membership_exit_date_exclusive"
        ),
        source_last_updated_at=None,
        research_basis=ResearchBasis.AS_PUBLISHED_PIT,
        non_as_known_taxonomy=False,
        identity=identity,
        authority_identity=authority_identity,
        unavailable_reason=None,
        source_ids=("test:source",),
        source_hashes=(_HASH_A,),
        lineage_hashes=(_HASH_C,),
    )


def _authority_bundle(
    *,
    symbols: tuple[str, ...] = ("300741.SZ", "605077.SH"),
    denominator: int | None = None,
    unaligned_before_switch: bool = False,
    identities: Mapping[str, TaxonomyIdentity] | None = None,
):
    denominator = len(symbols) if denominator is None else denominator
    classification_receipt = _receipt(AuthorityType.CLASSIFICATION, denominator=denominator)
    index_receipt = _receipt(AuthorityType.INDEX_MEMBERSHIP, denominator=denominator)
    classification = tuple(
        _interval(
            symbol=symbol,
            authority_type=AuthorityType.CLASSIFICATION,
            receipt=classification_receipt,
            identity=(identities or {}).get(symbol, _CHEMICAL),
        )
        for symbol in symbols
    )
    index_rows = []
    for symbol in symbols:
        if unaligned_before_switch:
            index_rows.append(
                _interval(
                    symbol=symbol,
                    authority_type=AuthorityType.INDEX_MEMBERSHIP,
                    receipt=index_receipt,
                    identity=_FOOD,
                    valid_from=dt.date(2020, 1, 1),
                    valid_to=dt.date(2021, 12, 13),
                    index_codes=("801120", "801121", "801122"),
                )
            )
        index_rows.append(
            _interval(
                symbol=symbol,
                authority_type=AuthorityType.INDEX_MEMBERSHIP,
                receipt=index_receipt,
                identity=(identities or {}).get(symbol, _CHEMICAL),
                valid_from=dt.date(2021, 12, 13),
            )
        )
    return CandidateBundleReadback(
        artifact_root=Path("unused"),
        manifest={"bundle_hash": _HASH_C},
        classification_receipt=classification_receipt,
        index_membership_receipt=index_receipt,
        classification_intervals=classification,
        index_membership_intervals=tuple(index_rows),
        preflight_report={},
    )


def _sw_daily():
    return {
        field: Decimal(index + 1)
        for index, field in enumerate(module.SW_DAILY_FIELDS)
    }


def _moneyflow(multiplier: int = 1):
    return {
        field: Decimal((index + 1) * multiplier)
        for index, field in enumerate(module.MONEYFLOW_FIELDS)
    }


def _opportunity_digest(day: module.SectorDataCandidateDay) -> str:
    digest = hashlib.sha256()
    for row in day.assignments:
        digest.update(
            canonical_json_bytes(
                {
                    "schema_version": module.SECTOR_DATA_OPPORTUNITY_SCHEMA,
                    "trade_date": row["trade_date"],
                    "canonical_symbol": row["canonical_symbol"],
                }
            )
            + b"\n"
        )
    return digest.hexdigest()


def test_candidate_day_is_order_invariant_and_separates_assignment_from_fact():
    builder = module.SectorDataCandidateBuilder(authority_bundle=_authority_bundle())
    forward = module.SectorDataSourceDay(
        trade_date=_TRADE_DATE,
        symbols=("300741.SZ", "605077.SH"),
        sw_daily_by_index_l2={"801123.SI": _sw_daily()},
        moneyflow_by_symbol={
            "300741.SZ": _moneyflow(1),
            "605077.SH": _moneyflow(2),
        },
    )
    reverse = module.SectorDataSourceDay(
        trade_date=_TRADE_DATE,
        symbols=tuple(reversed(forward.symbols)),
        sw_daily_by_index_l2=dict(reversed(tuple(forward.sw_daily_by_index_l2.items()))),
        moneyflow_by_symbol=dict(reversed(tuple(forward.moneyflow_by_symbol.items()))),
    )

    first = builder.build_day(forward)
    second = builder.build_day(reverse)

    assert first == second
    assert len(first.assignments) == 2
    assert len(first.sector_facts) == 1
    assert {row["status"] for row in first.assignments} == {"resolved"}
    assert len({row["sector_fact_row_hash"] for row in first.assignments}) == 1
    fact = first.sector_facts[0]
    assert fact["classification_l2_code"] == "220300"
    assert fact["index_l2_code"] == "801123"
    assert fact["moneyflow_aggregate"]["buy_sm_amount"] == "3"
    assert fact["contributor_coverage"] == {"expected": 2, "resolved": 2, "ratio": "1"}


def test_sector_fact_uses_l2_identity_not_an_arbitrary_l3_member():
    symbols = ("300741.SZ", "605077.SH")
    builder = module.SectorDataCandidateBuilder(
        authority_bundle=_authority_bundle(
            symbols=symbols,
            identities={"605077.SH": _CHEMICAL_OTHER_L3},
        )
    )
    result = builder.build_day(
        module.SectorDataSourceDay(
            trade_date=_TRADE_DATE,
            symbols=tuple(reversed(symbols)),
            sw_daily_by_index_l2={"801123": _sw_daily()},
            moneyflow_by_symbol={symbol: _moneyflow() for symbol in symbols},
        )
    )

    assert len(result.sector_facts) == 1
    fact = result.sector_facts[0]
    assert fact["classification_l2_identity_hash"] != _CHEMICAL.identity_hash
    assert fact["classification_l2_identity_hash"] != _CHEMICAL_OTHER_L3.identity_hash
    assert fact["classification_l2_identity_hash"]
    assert fact["index_l2_identity_hash"]


def test_sector_fact_rejects_conflicting_l2_identity_projection():
    symbols = ("300741.SZ", "605077.SH")
    builder = module.SectorDataCandidateBuilder(
        authority_bundle=_authority_bundle(
            symbols=symbols,
            identities={"605077.SH": _CHEMICAL_CONFLICTING_L2_NAME},
        )
    )

    with pytest.raises(module.SectorDataBuildContractError, match="conflicting authority identities"):
        builder.build_day(
            module.SectorDataSourceDay(
                trade_date=_TRADE_DATE,
                symbols=symbols,
                sw_daily_by_index_l2={"801123": _sw_daily()},
                moneyflow_by_symbol={symbol: _moneyflow() for symbol in symbols},
            )
        )


def test_candidate_day_retains_unaligned_rows_without_silent_join():
    builder = module.SectorDataCandidateBuilder(
        authority_bundle=_authority_bundle(unaligned_before_switch=True)
    )
    source = module.SectorDataSourceDay(
        trade_date=dt.date(2021, 8, 2),
        symbols=("605077.SH", "300741.SZ"),
        sw_daily_by_index_l2={"801121": _sw_daily()},
        moneyflow_by_symbol={"300741.SZ": _moneyflow(), "605077.SH": _moneyflow()},
    )

    result = builder.build_day(source)

    assert len(result.assignments) == 2
    assert result.sector_facts == ()
    assert {row["status"] for row in result.assignments} == {"unaligned"}
    assert all("authority_unaligned" in row["unavailable_reasons"] for row in result.assignments)
    assert all(row["sector_fact_row_hash"] is None for row in result.assignments)


def test_candidate_day_reports_missing_contributor_without_shrinking_denominator():
    builder = module.SectorDataCandidateBuilder(authority_bundle=_authority_bundle())
    result = builder.build_day(
        module.SectorDataSourceDay(
            trade_date=_TRADE_DATE,
            symbols=("300741.SZ", "605077.SH"),
            sw_daily_by_index_l2={"801123": _sw_daily()},
            moneyflow_by_symbol={"300741.SZ": _moneyflow()},
        )
    )

    assert len(result.assignments) == 2
    by_symbol = {row["canonical_symbol"]: row for row in result.assignments}
    assert by_symbol["300741.SZ"]["status"] == "resolved"
    assert by_symbol["605077.SH"]["status"] == "unavailable"
    assert by_symbol["605077.SH"]["unavailable_reasons"] == [
        "contributor_moneyflow_unavailable"
    ]
    assert result.sector_facts[0]["contributor_coverage"] == {
        "expected": 2,
        "resolved": 1,
        "ratio": "0.5",
    }


def test_candidate_day_reports_non_finite_source_values_without_serializing_nan():
    builder = module.SectorDataCandidateBuilder(authority_bundle=_authority_bundle())
    invalid = _moneyflow()
    invalid["net_mf_amount"] = float("nan")
    result = builder.build_day(
        module.SectorDataSourceDay(
            trade_date=_TRADE_DATE,
            symbols=("300741.SZ", "605077.SH"),
            sw_daily_by_index_l2={"801123": _sw_daily()},
            moneyflow_by_symbol={"300741.SZ": invalid, "605077.SH": _moneyflow()},
        )
    )
    by_symbol = {row["canonical_symbol"]: row for row in result.assignments}
    assert by_symbol["300741.SZ"]["status"] == "unavailable"
    assert by_symbol["300741.SZ"]["unavailable_reasons"] == [
        "contributor_moneyflow_invalid"
    ]
    assert by_symbol["605077.SH"]["status"] == "resolved"


def test_candidate_day_rejects_conflicting_normalized_index_rows():
    builder = module.SectorDataCandidateBuilder(authority_bundle=_authority_bundle())
    different = _sw_daily()
    different["close"] = Decimal("999")
    with pytest.raises(module.SectorDataBuildContractError, match="conflicting rows"):
        builder.build_day(
            module.SectorDataSourceDay(
                trade_date=_TRADE_DATE,
                symbols=("300741.SZ", "605077.SH"),
                sw_daily_by_index_l2={"801123": _sw_daily(), "801123.SI": different},
                moneyflow_by_symbol={"300741.SZ": _moneyflow(), "605077.SH": _moneyflow()},
            )
        )


def test_four_mandatory_regression_symbols_share_no_hardcoded_exception():
    symbols = ("300741.SZ", "300858.SZ", "603020.SH", "605077.SH")
    builder = module.SectorDataCandidateBuilder(authority_bundle=_authority_bundle(symbols=symbols))
    result = builder.build_day(
        module.SectorDataSourceDay(
            trade_date=_TRADE_DATE,
            symbols=tuple(reversed(symbols)),
            sw_daily_by_index_l2={"801123": _sw_daily()},
            moneyflow_by_symbol={symbol: _moneyflow() for symbol in symbols},
        )
    )

    assert [row["canonical_symbol"] for row in result.assignments] == sorted(symbols)
    assert {row["classification"]["identity_codes"]["l3_code"] for row in result.assignments} == {
        "220315"
    }
    assert {row["status"] for row in result.assignments} == {"resolved"}


def test_candidate_writer_readback_is_hash_closed_and_refuses_overwrite(tmp_path):
    authority = _authority_bundle()
    builder = module.SectorDataCandidateBuilder(authority_bundle=authority)
    day = builder.build_day(
        module.SectorDataSourceDay(
            trade_date=_TRADE_DATE,
            symbols=("300741.SZ", "605077.SH"),
            sw_daily_by_index_l2={"801123": _sw_daily()},
            moneyflow_by_symbol={"300741.SZ": _moneyflow(), "605077.SH": _moneyflow()},
        )
    )
    root = tmp_path / "candidate"

    readback = module.write_sector_data_candidate(
        artifact_root=root,
        forbidden_roots=(Path(__file__).resolve().parents[3],),
        authority_bundle=authority,
        days=(day,),
        expected_opportunities=2,
        expected_opportunity_digest=_opportunity_digest(day),
        candidate_scope="full",
        producer_commit="1" * 40,
        producer_tree="2" * 40,
    )

    assert readback.assignment_rows == 2
    assert readback.sector_fact_rows == 1
    assert readback.report["closure"]["passed"] is True
    assert readback.manifest["candidate_hash"]
    with pytest.raises(module.SectorDataBuildContractError, match="overwrite"):
        module.write_sector_data_candidate(
            artifact_root=root,
            forbidden_roots=(Path(__file__).resolve().parents[3],),
            authority_bundle=authority,
            days=(day,),
            expected_opportunities=2,
            expected_opportunity_digest=_opportunity_digest(day),
            candidate_scope="full",
            producer_commit="1" * 40,
            producer_tree="2" * 40,
        )


def test_candidate_writer_rejects_wrong_opportunity_identity_digest(tmp_path):
    authority = _authority_bundle()
    builder = module.SectorDataCandidateBuilder(authority_bundle=authority)
    day = builder.build_day(
        module.SectorDataSourceDay(
            trade_date=_TRADE_DATE,
            symbols=("300741.SZ", "605077.SH"),
            sw_daily_by_index_l2={"801123": _sw_daily()},
            moneyflow_by_symbol={"300741.SZ": _moneyflow(), "605077.SH": _moneyflow()},
        )
    )
    root = tmp_path / "candidate"
    with pytest.raises(module.SectorDataBuildContractError, match="opportunity identity"):
        module.write_sector_data_candidate(
            artifact_root=root,
            forbidden_roots=(Path(__file__).resolve().parents[3],),
            authority_bundle=authority,
            days=(day,),
            expected_opportunities=2,
            expected_opportunity_digest="f" * 64,
            candidate_scope="full",
            producer_commit="1" * 40,
            producer_tree="2" * 40,
        )
    assert not root.exists()


def test_candidate_writer_readback_failure_does_not_publish_target(tmp_path):
    authority = _authority_bundle(symbols=("300741.SZ",), denominator=1)
    builder = module.SectorDataCandidateBuilder(authority_bundle=authority)
    day = builder.build_day(
        module.SectorDataSourceDay(
            trade_date=_TRADE_DATE,
            symbols=("300741.SZ",),
            sw_daily_by_index_l2={"801123": _sw_daily()},
            moneyflow_by_symbol={"300741.SZ": _moneyflow()},
        )
    )
    malformed = dict(day.assignments[0])
    malformed["unexpected"] = True
    malformed_day = module.SectorDataCandidateDay(day.trade_date, (malformed,), day.sector_facts)
    root = tmp_path / "candidate"

    with pytest.raises(module.SectorDataBuildContractError, match="fields differ from schema"):
        module.write_sector_data_candidate(
            artifact_root=root,
            forbidden_roots=(Path(__file__).resolve().parents[3],),
            authority_bundle=authority,
            days=(malformed_day,),
            expected_opportunities=1,
            expected_opportunity_digest=_opportunity_digest(malformed_day),
            candidate_scope="full",
            producer_commit="1" * 40,
            producer_tree="2" * 40,
        )
    assert not root.exists()


def test_candidate_readback_rejects_assignment_tampering(tmp_path):
    authority = _authority_bundle()
    builder = module.SectorDataCandidateBuilder(authority_bundle=authority)
    day = builder.build_day(
        module.SectorDataSourceDay(
            trade_date=_TRADE_DATE,
            symbols=("300741.SZ", "605077.SH"),
            sw_daily_by_index_l2={"801123": _sw_daily()},
            moneyflow_by_symbol={"300741.SZ": _moneyflow(), "605077.SH": _moneyflow()},
        )
    )
    root = tmp_path / "candidate"
    module.write_sector_data_candidate(
        artifact_root=root,
        forbidden_roots=(Path(__file__).resolve().parents[3],),
        authority_bundle=authority,
        days=(day,),
        expected_opportunities=2,
        expected_opportunity_digest=_opportunity_digest(day),
        candidate_scope="full",
        producer_commit="1" * 40,
        producer_tree="2" * 40,
    )
    path = root / "assignments.jsonl"
    rows = path.read_text(encoding="utf-8").splitlines()
    value = json.loads(rows[0])
    value["status"] = "unavailable"
    rows[0] = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")

    with pytest.raises(module.SectorDataBuildContractError, match="hash/size mismatch"):
        module.read_sector_data_candidate(
            artifact_root=root,
            forbidden_roots=(Path(__file__).resolve().parents[3],),
        )


def test_candidate_readback_rejects_non_object_closure_with_contract_error(tmp_path):
    authority = _authority_bundle()
    builder = module.SectorDataCandidateBuilder(authority_bundle=authority)
    day = builder.build_day(
        module.SectorDataSourceDay(
            trade_date=_TRADE_DATE,
            symbols=("300741.SZ",),
            sw_daily_by_index_l2={"801123": _sw_daily()},
            moneyflow_by_symbol={"300741.SZ": _moneyflow()},
        )
    )
    root = tmp_path / "candidate"
    module.write_sector_data_candidate(
        artifact_root=root,
        forbidden_roots=(Path(__file__).resolve().parents[3],),
        authority_bundle=authority,
        days=(day,),
        expected_opportunities=1,
        expected_opportunity_digest=_opportunity_digest(day),
        candidate_scope="sample",
        producer_commit="1" * 40,
        producer_tree="2" * 40,
    )
    report_path = root / "candidate_report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["closure"] = []
    encoded = canonical_json_bytes(report) + b"\n"
    report_path.write_bytes(encoded)
    manifest_path = root / "candidate_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["files"]["candidate_report.json"] = {
        "sha256": hashlib.sha256(encoded).hexdigest(),
        "size_bytes": len(encoded),
    }
    manifest_path.write_bytes(canonical_json_bytes(manifest) + b"\n")

    with pytest.raises(module.SectorDataBuildContractError, match="closure"):
        module.read_sector_data_candidate(
            artifact_root=root,
            forbidden_roots=(Path(__file__).resolve().parents[3],),
        )


def test_candidate_readback_rejects_non_integer_file_size(tmp_path):
    authority = _authority_bundle()
    builder = module.SectorDataCandidateBuilder(authority_bundle=authority)
    day = builder.build_day(
        module.SectorDataSourceDay(
            trade_date=_TRADE_DATE,
            symbols=("300741.SZ",),
            sw_daily_by_index_l2={"801123": _sw_daily()},
            moneyflow_by_symbol={"300741.SZ": _moneyflow()},
        )
    )
    root = tmp_path / "candidate"
    module.write_sector_data_candidate(
        artifact_root=root,
        forbidden_roots=(Path(__file__).resolve().parents[3],),
        authority_bundle=authority,
        days=(day,),
        expected_opportunities=1,
        expected_opportunity_digest=_opportunity_digest(day),
        candidate_scope="sample",
        producer_commit="1" * 40,
        producer_tree="2" * 40,
    )
    manifest_path = root / "candidate_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["files"]["assignments.jsonl"]["size_bytes"] = "invalid"
    manifest_path.write_bytes(canonical_json_bytes(manifest) + b"\n")

    with pytest.raises(module.SectorDataBuildContractError, match="file size"):
        module.read_sector_data_candidate(
            artifact_root=root,
            forbidden_roots=(Path(__file__).resolve().parents[3],),
        )
