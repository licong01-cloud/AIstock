"""Derived-field correctness checks against the reference implementation.

Per Stage 7.3 §7.3.3:
- ``qe_archive.paper_v2_cash_ledger.entry_type`` must match the reference
  ``synthesize_cash_ledger_entry_type`` for every archived row (BUG-006).
- ``qe_archive.paper_v2_reset_audit.reset_type`` must match the reference
  ``synthesize_reset_audit_reset_type`` (BUG-007).
- ``qe_archive.paper_v2_session_day.data_quality`` must be one of the
  allowed enum values (BUG-008 partial fix).
- ``market.regime_label.regime`` for ``simple_quadrant`` must match the
  reference ``classify_simple_quadrant``.
- ``paper_v2.fills`` slippage (when intended_price is non-NULL) must match
  ``compute_slippage_bps``.

The reference impls live in backend/tests/data_quality/_reference.py — they
mirror the dw-foundation handler synthesize logic (commit bd098f8).
"""

from __future__ import annotations

import json

import pytest

psycopg2 = pytest.importorskip("psycopg2")
from psycopg2.extras import RealDictCursor  # noqa: E402  after importorskip

from ._reference import (
    CASH_LEDGER_ENTRY_TYPES,
    RESET_AUDIT_RESET_TYPES,
    REGIME_VALUES,
    SESSION_DAY_QUALITY,
    classify_simple_quadrant,
    compute_slippage_bps,
    synthesize_cash_ledger_entry_type,
    synthesize_reset_audit_reset_type,
)
from .conftest import skip_if_missing_columns


def test_module_collected_smoke():
    assert CASH_LEDGER_ENTRY_TYPES
    assert RESET_AUDIT_RESET_TYPES
    assert SESSION_DAY_QUALITY


def _coerce(value):
    if value is None or isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return None
    return None


def test_cash_ledger_entry_type_matches_synth_reference(
    dev_conn, archive_tables_ready,
):
    """Whole-table contract: every row in qe_archive.paper_v2_cash_ledger
    joined with paper_v2.cash_ledger must have entry_type matching the
    reference synthesize logic. No LIMIT -- the contract applies to all
    rows (per Codex r2 P2.3 review)."""
    with dev_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT a.ledger_entry_id, a.entry_type,
                   s.side, s.notional, s.fee, s.cash_delta
            FROM qe_archive.paper_v2_cash_ledger a
            JOIN paper_v2.cash_ledger s ON s.cash_id::text = a.ledger_entry_id
            """
        )
        rows = list(cur.fetchall())
    if not rows:
        pytest.skip(
            "no joined cash_ledger rows between source and archive; "
            "PaperV2ArchiveHandler.handle has not produced archive rows yet."
        )
    drift = []
    for r in rows:
        expected = synthesize_cash_ledger_entry_type(
            side=r["side"], notional=r["notional"],
            fee=r["fee"], cash_delta=r["cash_delta"],
        )
        if r["entry_type"] != expected:
            drift.append((r["ledger_entry_id"], r["entry_type"], expected))
    assert not drift, (
        f"{len(drift)} cash_ledger rows have entry_type drift; first 5: {drift[:5]}"
    )


def test_cash_ledger_entry_type_in_allowed_enum(
    dev_conn, archive_tables_ready,
):
    """Whole-table contract: every archived entry_type must be in
    CASH_LEDGER_ENTRY_TYPES. No LIMIT (Codex r2 P2.3)."""
    with dev_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT ledger_entry_id, entry_type FROM qe_archive.paper_v2_cash_ledger"
        )
        rows = list(cur.fetchall())
    if not rows:
        pytest.skip("qe_archive.paper_v2_cash_ledger empty.")
    bad = [r for r in rows if r["entry_type"] not in CASH_LEDGER_ENTRY_TYPES]
    assert not bad, (
        f"{len(bad)} cash_ledger rows have illegal entry_type; first 5: "
        f"{[(r['ledger_entry_id'], r['entry_type']) for r in bad[:5]]}"
    )


def test_reset_audit_reset_type_matches_synth_reference(
    dev_conn, archive_tables_ready,
):
    """Per BUG-007: handler.synthesize_reset_audit_reset_type must produce
    the canonical mapping. Where the source table is empty this is a no-op
    pass (the synthesize function is unit-tested separately on dw-foundation)."""
    # qe_archive.paper_v2_reset_audit.audit_id is TEXT (per T12 P1.3 UUID->TEXT
    # round-2 fix BUG-003); paper_v2.reset_audit.audit_id is bigint. Cast at
    # the join boundary to bridge the two type domains.
    with dev_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT a.reset_type, s.rerun_policy, s.deleted_counts
            FROM qe_archive.paper_v2_reset_audit a
            JOIN paper_v2.reset_audit s ON s.audit_id::text = a.audit_id
            """
        )
        rows = list(cur.fetchall())
    if not rows:
        pytest.skip(
            "paper_v2.reset_audit join is empty (Batch A had no reset events); "
            "this is the BUG-007 'integration test deferred' case."
        )
    drift = []
    for r in rows:
        expected = synthesize_reset_audit_reset_type(
            rerun_policy=r["rerun_policy"],
            deleted_counts=_coerce(r["deleted_counts"]),
        )
        if r["reset_type"] != expected:
            drift.append((r["reset_type"], expected, r["rerun_policy"]))
    assert not drift, f"{len(drift)} reset_type drift; first 5: {drift[:5]}"


def test_session_day_data_quality_in_allowed_enum(
    dev_conn, archive_tables_ready,
):
    """Whole-table contract (BUG-008 partial fix): data_quality is
    derived but may still under-report. The enum-domain contract
    remains tight; this test walks every non-NULL row. No LIMIT
    (Codex r2 P2.3)."""
    with dev_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT trade_session_id, data_quality FROM qe_archive.paper_v2_session_day "
            "WHERE data_quality IS NOT NULL"
        )
        rows = list(cur.fetchall())
    if not rows:
        pytest.skip("qe_archive.paper_v2_session_day has no data_quality rows.")
    bad = [r for r in rows if r["data_quality"] not in SESSION_DAY_QUALITY]
    assert not bad, (
        f"{len(bad)} session_day rows have illegal data_quality; first 5: "
        f"{[(r['trade_session_id'], r['data_quality']) for r in bad[:5]]}"
    )


def test_regime_label_simple_quadrant_classification(
    dev_conn,
):
    """``market.regime_label.regime`` (where source_method='simple_quadrant')
    must equal classify_simple_quadrant(signal['ret_pct_5y'],
    signal['vol_pct_5y'])."""
    with dev_conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pg_tables WHERE schemaname='market' AND tablename='regime_label'"
        )
        if cur.fetchone() is None:
            pytest.skip("market.regime_label absent.")
    with dev_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT trade_date, regime, source_signal_json
            FROM market.regime_label
            WHERE source_method = 'simple_quadrant'
            """
        )
        rows = list(cur.fetchall())
    if not rows:
        pytest.skip("no simple_quadrant rows in market.regime_label yet.")
    drift = []
    for r in rows:
        signal = _coerce(r["source_signal_json"]) or {}
        expected, _conf = classify_simple_quadrant(
            ret_pct_5y=signal.get("ret_pct_5y"),
            vol_pct_5y=signal.get("vol_pct_5y"),
        )
        if r["regime"] not in REGIME_VALUES:
            drift.append((r["trade_date"], r["regime"], "illegal enum"))
        elif r["regime"] != expected:
            drift.append((r["trade_date"], r["regime"], expected))
    assert not drift, f"{len(drift)} regime classifications drift; first 5: {drift[:5]}"


def test_slippage_bps_value_matches_d5_formula(
    dev_conn, archive_tables_ready,
):
    """Whole-table contract: for **every** archived fill where
    ``slippage_bps`` is non-NULL, the value must match the D5 raw formula

        ``(fill_price - intended_price) / intended_price * 10000``

    within 0.5 bps tolerance (Decimal rounding). No LIMIT -- the contract
    applies to all rows. Skips cleanly only when the archive has no
    populated slippage_bps rows at all (canonical state today: 100%
    MARKET orders per D5 §502, intended_price structurally NULL).
    """
    skip_if_missing_columns(
        dev_conn, "qe_archive", "paper_v2_fill",
        ("intended_price", "fill_price", "side", "slippage_bps"),
        "T12 paper_v2_fill columns missing on this dev DB.",
    )
    with dev_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT fill_id, intended_price, fill_price, side, slippage_bps
            FROM qe_archive.paper_v2_fill
            WHERE slippage_bps IS NOT NULL
            """
        )
        rows = list(cur.fetchall())
    if not rows:
        pytest.skip(
            "qe_archive.paper_v2_fill has no rows with slippage_bps populated. "
            "Canonical D5 §502 state today (intended_price structurally NULL "
            "for MARKET-only orders). When paper_v2 adds LIMIT orders + the "
            "handler populates slippage_bps, this test activates with no "
            "code change."
        )
    drift = []
    for r in rows:
        expected = compute_slippage_bps(
            intended_price=r["intended_price"],
            fill_price=r["fill_price"],
            # side intentionally omitted -- D5 §507 raw formula has no
            # BUY/SELL branch (per Codex r2 review drawer 46553d25).
        )
        if expected is None:
            # Defensive: shouldn't happen because we filtered on
            # slippage_bps IS NOT NULL and the formula returns NULL only
            # when intended_price/fill_price is NULL.
            drift.append((r["fill_id"], "expected=None (one of inputs NULL)", r["slippage_bps"]))
            continue
        actual = float(r["slippage_bps"])
        if abs(actual - expected) > 0.5:  # 0.5 bps Decimal-rounding tolerance
            drift.append((r["fill_id"], actual, expected))
    assert not drift, (
        f"{len(drift)} slippage_bps rows violate the D5 raw formula; "
        f"first 5 (fill_id, actual, expected): {drift[:5]}"
    )


def test_slippage_bps_market_orders_remain_null(
    dev_conn, archive_tables_ready,
):
    """**Negative contract** (Codex r2 P1.1, second part).

    D5 §507: ``slippage_bps`` is computed **only when**
    ``intended_price IS NOT NULL``, **otherwise NULL**. Therefore any
    archive row where ``intended_price IS NULL AND slippage_bps IS NOT
    NULL`` violates the contract and must surface as a test failure
    (it would mean the handler invented a slippage value out of
    nothing, or attached it to a MARKET fill that has no reference
    price).

    Whole-table check: no LIMIT.
    """
    skip_if_missing_columns(
        dev_conn, "qe_archive", "paper_v2_fill",
        ("intended_price", "slippage_bps"),
        "T12 paper_v2_fill columns missing on this dev DB.",
    )
    with dev_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT fill_id, intended_price, slippage_bps
            FROM qe_archive.paper_v2_fill
            WHERE intended_price IS NULL
              AND slippage_bps IS NOT NULL
            """
        )
        violators = list(cur.fetchall())
    assert not violators, (
        f"{len(violators)} archive fill(s) violate D5 §507 'otherwise NULL': "
        f"intended_price IS NULL but slippage_bps is set. Handler must NOT "
        f"derive slippage for MARKET orders. First 5: "
        f"{[(r['fill_id'], r['slippage_bps']) for r in violators[:5]]}"
    )


def test_slippage_bps_present_for_every_intended_price_row(
    dev_conn, archive_tables_ready,
):
    """**Whole-table strict NULL contract** (Codex Lane E r3, drawer
    a25cd473).

    For every archive row where ``intended_price IS NOT NULL``,
    ``slippage_bps`` MUST also be NOT NULL. The contrapositive
    statement of D5 §507: if intended_price is set then the formula
    must be applied (the formula never returns NULL on non-NULL,
    non-zero inputs).

    This closes the false-negative path Codex r2 review left open: the
    earlier handler-coverage sentinel only checked that *some* archive
    rows had slippage_bps populated. A handler that mis-derived a
    single row to NULL would have slipped through. This test catches
    that on the first such row, whole-table.

    Skips cleanly when no archive rows have ``intended_price IS NOT
    NULL`` (the canonical D5 §502 MARKET-only baseline).
    """
    skip_if_missing_columns(
        dev_conn, "qe_archive", "paper_v2_fill",
        ("intended_price", "slippage_bps"),
        "T12 paper_v2_fill columns missing on this dev DB.",
    )
    with dev_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT fill_id, intended_price, fill_price, side
            FROM qe_archive.paper_v2_fill
            WHERE intended_price IS NOT NULL
              AND slippage_bps IS NULL
            """
        )
        violators = list(cur.fetchall())
    if not violators:
        # Distinguish: are there ANY archive rows with intended_price NOT
        # NULL? If not, the canonical MARKET-only state -> skip.
        with dev_conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM qe_archive.paper_v2_fill "
                "WHERE intended_price IS NOT NULL"
            )
            total = cur.fetchone()[0]
        if total == 0:
            pytest.skip(
                "qe_archive.paper_v2_fill has no rows with intended_price "
                "IS NOT NULL (canonical D5 §502 MARKET-only baseline). "
                "Strict NULL contract activates once LIMIT orders appear."
            )
        return  # all intended-price rows have slippage_bps — contract OK
    assert False, (
        f"{len(violators)} archive fill(s) violate the D5 §507 strict NULL "
        f"contract: intended_price IS NOT NULL but slippage_bps IS NULL. "
        f"Handler must derive slippage_bps for every populated "
        f"intended_price row. First 5 (fill_id, intended_price, fill_price, "
        f"side): "
        f"{[(r['fill_id'], r['intended_price'], r['fill_price'], r['side']) for r in violators[:5]]}"
    )


def test_slippage_bps_handler_derives_when_intended_price_present(
    dev_conn, source_tables_ready, archive_tables_ready,
):
    """**Handler-coverage sentinel** (Codex r2 P1.1, first part).

    When the source ``paper_v2.fills`` table has at least one row with
    ``intended_price IS NOT NULL`` AND the archive has its mirror, the
    archive's ``slippage_bps`` MUST be populated (not all-NULL). If the
    handler regresses and stops deriving slippage_bps, this assertion
    surfaces it instead of letting the value-matching test above skip.

    Skips cleanly when no source rows have ``intended_price`` populated
    (the canonical D5 §502 MARKET-only baseline).
    """
    skip_if_missing_columns(
        dev_conn, "qe_archive", "paper_v2_fill",
        ("intended_price", "slippage_bps"),
        "T12 paper_v2_fill columns missing on this dev DB.",
    )
    with dev_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM paper_v2.fills WHERE intended_price IS NOT NULL"
        )
        source_with_intended = cur.fetchone()[0]
    if source_with_intended == 0:
        pytest.skip(
            "paper_v2.fills has no rows with intended_price IS NOT NULL "
            "(MARKET-only baseline per D5 §502). Handler-derivation sentinel "
            "activates once paper_v2 adds LIMIT orders."
        )
    # Source has rows that SHOULD produce a non-NULL slippage_bps in
    # archive. The archive must reflect this; if the corresponding archive
    # rows exist but slippage_bps is NULL for all of them, fail loudly.
    with dev_conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              count(*) FILTER (WHERE a.intended_price IS NOT NULL) AS arch_with_intended,
              count(*) FILTER (
                WHERE a.intended_price IS NOT NULL AND a.slippage_bps IS NOT NULL
              ) AS arch_with_slippage
            FROM qe_archive.paper_v2_fill a
            """
        )
        arch_with_intended, arch_with_slippage = cur.fetchone()
    if arch_with_intended == 0:
        pytest.skip(
            f"paper_v2.fills has {source_with_intended} row(s) with "
            f"intended_price IS NOT NULL but qe_archive.paper_v2_fill has 0 "
            f"such rows (archive worker has not consumed the relevant outbox "
            f"events yet). Stage 7.3 r1 cross-table sentinel (BUG-NNN) "
            f"surfaces the related gap; deferred here."
        )
    assert arch_with_slippage > 0, (
        f"qe_archive.paper_v2_fill has {arch_with_intended} row(s) with "
        f"intended_price IS NOT NULL but 0 with slippage_bps populated. "
        f"Handler is failing to derive slippage_bps for LIMIT-order fills, "
        f"violating D5 §507. This is the handler regression Codex r2 P1.1 "
        f"asked the sentinel to surface."
    )
