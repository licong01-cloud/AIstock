import datetime as dt

from backend.services.canonical_equity_pit import CANONICAL_PIT_TERMINAL_EVIDENCE_CONTRACT

from backend.services.event_signal.announcement_issuer_binding import (
    ISSUER_BINDING_LATERAL_SQL,
    IssuerBindingStatus,
    attach_announcement_issuer_bindings,
    resolve_announcement_issuer_binding,
)


def _row(**overrides):
    value = {
        "ts_code": "002070.SZ",
        "ann_date": dt.date(2019, 5, 17),
        "available_at": dt.datetime(2019, 5, 17, 8, 0, tzinfo=dt.timezone.utc),
        "effective_trade_date": dt.date(2019, 5, 20),
        "event_type": "stock_delisting_confirmed",
        "ann_signal_status": "ACTIVE",
        "ann_signal_evidence": {"st_cross_check": {"matched": True, "terminal": True}},
        "classification_detail": {},
        "issuer_candidate_ts_codes": ["002070.SZ"],
        "issuer_candidate_authorities": [
            {
                "ts_code": "002070.SZ",
                "authority_source": "tushare.namechange_interval_v1",
            }
        ],
    }
    value.update(overrides)
    return value


def test_exact_binding_is_actionable_and_digest_is_deterministic() -> None:
    first = resolve_announcement_issuer_binding(_row(), require_terminal_cross_check=True)
    second = resolve_announcement_issuer_binding(_row(), require_terminal_cross_check=True)

    assert first.status is IssuerBindingStatus.EXACT
    assert first.actionable is True
    assert first.fact_status == "ACTIVE"
    assert first.signal_status == "ACTIVE"
    assert first.digest == second.digest


def test_candidate_sql_rejects_security_lifetime_mismatch() -> None:
    assert "stock.list_date <= peer.ann_date" in ISSUER_BINDING_LATERAL_SQL
    assert "stock.delist_date >= peer.ann_date" in ISSUER_BINDING_LATERAL_SQL


def test_candidate_sql_uses_audited_name_authorities_only() -> None:
    assert "market.stock_namechange historical_name" in ISSUER_BINDING_LATERAL_SQL
    assert "historical_name.start_date <= peer.ann_date" in ISSUER_BINDING_LATERAL_SQL
    assert "historical_name.end_date >= peer.ann_date" in ISSUER_BINDING_LATERAL_SQL
    assert "tushare.namechange_interval_v1" in ISSUER_BINDING_LATERAL_SQL
    assert "tushare.stock_basic_terminal_display_v1" in ISSUER_BINDING_LATERAL_SQL
    assert "regexp_replace(stock.name, '\\(退\\)$', '') = peer.name" in ISSUER_BINDING_LATERAL_SQL


def test_provider_alias_is_preserved_but_suppressed() -> None:
    decision = resolve_announcement_issuer_binding(
        _row(ts_code="000001.SZ"),
        require_terminal_cross_check=True,
    )

    assert decision.status is IssuerBindingStatus.DUPLICATE_PROVIDER_ALIAS
    assert decision.resolved_ts_code == "002070.SZ"
    assert decision.fact_status == "SUPERSEDED"
    assert decision.signal_status == "SUPPRESSED"
    assert decision.reason_code == "announcement_issuer_binding_provider_alias"


def test_ambiguous_and_unresolved_bindings_fail_closed() -> None:
    ambiguous = resolve_announcement_issuer_binding(
        _row(
            issuer_candidate_ts_codes=["000001.SZ", "002070.SZ"],
            issuer_candidate_authorities=[
                {"ts_code": "000001.SZ", "authority_source": "tushare.namechange_interval_v1"},
                {"ts_code": "002070.SZ", "authority_source": "tushare.namechange_interval_v1"},
            ],
        )
    )
    unresolved = resolve_announcement_issuer_binding(
        _row(issuer_candidate_ts_codes=[], issuer_candidate_authorities=[])
    )

    assert ambiguous.status is IssuerBindingStatus.AMBIGUOUS
    assert unresolved.status is IssuerBindingStatus.UNRESOLVED
    assert ambiguous.signal_status == unresolved.signal_status == "SUPPRESSED"
    assert ambiguous.fact_status == unresolved.fact_status == "UNKNOWN"


def test_candidate_without_authority_evidence_fails_closed() -> None:
    decision = resolve_announcement_issuer_binding(
        _row(issuer_candidate_authorities=[]),
        require_terminal_cross_check=True,
    )

    assert decision.status is IssuerBindingStatus.UNRESOLVED
    assert decision.reason_code == "announcement_issuer_binding_authority_missing"
    assert decision.signal_status == "SUPPRESSED"


def test_pit_time_inconsistency_is_not_silently_rewritten() -> None:
    decision = resolve_announcement_issuer_binding(
        _row(
            available_at=dt.datetime(2019, 5, 23, 14, 5, tzinfo=dt.timezone.utc),
            effective_trade_date=dt.date(2019, 5, 20),
        )
    )

    assert decision.status is IssuerBindingStatus.PIT_TIME_INCONSISTENT
    assert decision.known_date == dt.date(2019, 5, 23)
    assert decision.effective_trade_date == dt.date(2019, 5, 20)
    assert decision.signal_status == "SUPPRESSED"


def test_confirmed_terminal_requires_independent_cross_check() -> None:
    decision = resolve_announcement_issuer_binding(
        _row(ann_signal_evidence={"st_cross_check": {"matched": False}}),
        require_terminal_cross_check=True,
    )

    assert decision.status is IssuerBindingStatus.TERMINAL_EVIDENCE_UNCONFIRMED
    assert decision.reason_code == "announcement_terminal_evidence_cross_check_missing"
    assert decision.signal_status == "SUPPRESSED"


def test_attach_records_evidence_without_mutating_raw_row() -> None:
    source = _row(ts_code="000001.SZ")
    enriched, counts = attach_announcement_issuer_bindings(
        [source],
        require_terminal_cross_check=True,
    )

    assert "issuer_binding" not in source["classification_detail"]
    assert "issuer_binding_decision" not in source
    assert enriched[0]["ann_signal_status"] == "SUPPRESSED"
    assert enriched[0]["issuer_fact_status"] == "SUPERSEDED"
    assert enriched[0]["ann_signal_evidence"]["issuer_binding"]["resolved_ts_code"] == "002070.SZ"
    assert enriched[0]["ann_signal_evidence"]["issuer_binding"]["candidate_authorities"] == [
        {
            "ts_code": "002070.SZ",
            "authority_source": "tushare.namechange_interval_v1",
        }
    ]
    assert counts == {"DUPLICATE_PROVIDER_ALIAS": 1}


def test_exact_terminal_binding_emits_v2_contract() -> None:
    enriched, counts = attach_announcement_issuer_bindings(
        [_row()],
        require_terminal_cross_check=True,
    )

    evidence = enriched[0]["ann_signal_evidence"]
    assert evidence["terminal_evidence_contract"] == CANONICAL_PIT_TERMINAL_EVIDENCE_CONTRACT
    assert evidence["issuer_binding"]["status"] == "EXACT"
    assert evidence["issuer_binding"]["actionable"] is True
    assert counts == {"EXACT": 1}


def test_suppressed_binding_removes_stale_terminal_contract() -> None:
    source = _row(
        ts_code="000001.SZ",
        ann_signal_evidence={
            "terminal_evidence_contract": "issuer_bound_stock_delisting_v1",
            "st_cross_check": {"matched": True, "terminal": True},
        },
    )

    enriched, _ = attach_announcement_issuer_bindings(
        [source],
        require_terminal_cross_check=True,
    )

    assert enriched[0]["ann_signal_status"] == "SUPPRESSED"
    assert "terminal_evidence_contract" not in enriched[0]["ann_signal_evidence"]


def test_pre_suppressed_exact_binding_does_not_emit_terminal_contract() -> None:
    enriched, _ = attach_announcement_issuer_bindings(
        [_row(ann_signal_status="SUPPRESSED")],
        require_terminal_cross_check=True,
    )

    assert enriched[0]["ann_signal_status"] == "SUPPRESSED"
    assert "terminal_evidence_contract" not in enriched[0]["ann_signal_evidence"]
