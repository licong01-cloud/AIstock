"""JSONB structural validation for paper_v2 + market.regime_label.

Per Stage 7.3 §7.3.2:
- ``paper_v2.fills.fill_market_context`` must carry the 13 keys defined
  by the T6.1 §5.7 contract, with the right Python types after JSONB
  load (psycopg2 returns dicts directly).
- ``market.regime_label.source_signal_json`` must carry ret_pct_5y +
  vol_pct_5y for ``source_method='simple_quadrant'`` rows.
- Archive outbox event payloads must declare schema_version so handler
  ``validate_payload`` doesn't silently accept new shapes.

Tests skip cleanly when source tables / rows are missing.
"""

from __future__ import annotations

import json

import pytest

psycopg2 = pytest.importorskip("psycopg2")
from psycopg2.extras import RealDictCursor  # noqa: E402  after importorskip

from ._reference import FILL_MARKET_CONTEXT_KEYS
from .conftest import skip_if_missing_columns


T6_1_REASON = (
    "paper_v2.fills.fill_market_context not present; T6.1 capture-field "
    "migration (paper-v2-vnpy-mvp-20260508 branch) has not been merged to "
    "main yet. Test activates once paper-v2 team's T6.1 merge lands."
)


def test_module_collected_smoke():
    assert FILL_MARKET_CONTEXT_KEYS  # sanity


def _coerce_jsonb(value):
    if value is None or isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return None
    return None


def test_fill_market_context_has_all_required_keys(
    dev_conn, source_tables_ready,
):
    """T6.1 §5.7: each non-NULL fill_market_context carries 13 canonical keys."""
    skip_if_missing_columns(
        dev_conn, "paper_v2", "fills", ("fill_market_context",), T6_1_REASON,
    )
    with dev_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT fill_id, fill_market_context
            FROM paper_v2.fills
            WHERE fill_market_context IS NOT NULL
            LIMIT 50
            """
        )
        rows = list(cur.fetchall())
    if not rows:
        pytest.skip(
            "no fills with fill_market_context populated yet; "
            "T6.1 backfill may not have run on this dev DB."
        )
    failures: list[tuple[str, str]] = []
    for r in rows:
        ctx = _coerce_jsonb(r["fill_market_context"])
        if ctx is None:
            failures.append((r["fill_id"], "fill_market_context is not a JSON object"))
            continue
        for key in FILL_MARKET_CONTEXT_KEYS:
            if key not in ctx:
                failures.append((r["fill_id"], f"missing key {key!r}"))
    assert not failures, (
        f"{len(failures)} fill_market_context structural issues; first 5: {failures[:5]}"
    )


def test_fill_market_context_types_match_contract(
    dev_conn, source_tables_ready,
):
    """Types per FILL_MARKET_CONTEXT_KEYS (str / number / bool tolerance)."""
    skip_if_missing_columns(
        dev_conn, "paper_v2", "fills", ("fill_market_context",), T6_1_REASON,
    )
    with dev_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT fill_id, fill_market_context
            FROM paper_v2.fills
            WHERE fill_market_context IS NOT NULL
            LIMIT 50
            """
        )
        rows = list(cur.fetchall())
    if not rows:
        pytest.skip("no fills with fill_market_context populated.")
    type_failures: list[tuple[str, str, type]] = []
    for r in rows:
        ctx = _coerce_jsonb(r["fill_market_context"])
        if not isinstance(ctx, dict):
            continue
        for key, expected_types in FILL_MARKET_CONTEXT_KEYS.items():
            if key not in ctx:
                continue  # covered by the previous test
            value = ctx[key]
            if value is None:
                continue  # nullability tolerated; presence is the strict check
            if not isinstance(value, expected_types):
                type_failures.append((r["fill_id"], key, type(value)))
    assert not type_failures, (
        f"{len(type_failures)} fill_market_context type mismatches; "
        f"first 5: {type_failures[:5]}"
    )


def test_regime_label_simple_quadrant_signal_carries_percentile_inputs(
    dev_conn,
):
    """simple_quadrant rows MUST carry ret_pct_5y + vol_pct_5y inputs so
    the classify_simple_quadrant rule is reproducible from the JSONB
    payload alone."""
    with dev_conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pg_tables WHERE schemaname='market' AND tablename='regime_label'"
        )
        if cur.fetchone() is None:
            pytest.skip("market.regime_label absent (T10 not applied on this dev DB).")
    with dev_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT trade_date, source_method, source_signal_json
            FROM market.regime_label
            WHERE source_method = 'simple_quadrant'
            LIMIT 50
            """
        )
        rows = list(cur.fetchall())
    if not rows:
        pytest.skip("market.regime_label has no simple_quadrant rows yet.")
    missing: list[tuple] = []
    for r in rows:
        signal = _coerce_jsonb(r["source_signal_json"])
        if not isinstance(signal, dict):
            missing.append((r["trade_date"], "not a JSON object"))
            continue
        if "ret_pct_5y" not in signal or "vol_pct_5y" not in signal:
            missing.append((r["trade_date"], list(signal.keys())[:5]))
    assert not missing, (
        f"{len(missing)} simple_quadrant rows missing ret/vol inputs; first 5: {missing[:5]}"
    )


def test_outbox_event_payload_declares_schema_version(
    dev_conn,
):
    """qe_archive.outbox_event payload must declare schema_version so any
    handler refusing to upgrade picks up the new shape rather than parsing
    an unknown JSON blob silently."""
    with dev_conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pg_tables WHERE schemaname='qe_archive' AND tablename='outbox_event'"
        )
        if cur.fetchone() is None:
            pytest.skip("qe_archive.outbox_event absent on dev DB.")
    # The payload column in qe_archive.outbox_event is named ``payload``
    # (the convention used by the dw-foundation T8 schema); some older
    # branches called it ``event_payload``. Tolerate both at probe time.
    with dev_conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='qe_archive' AND table_name='outbox_event' "
            "AND column_name IN ('payload', 'event_payload')"
        )
        cols = {r[0] for r in cur.fetchall()}
    if not cols:
        pytest.skip("qe_archive.outbox_event has no payload column on this dev DB.")
    payload_col = "payload" if "payload" in cols else "event_payload"
    # Per D5 Q2.b: the 4 *archive* event types added under qe_archive MUST
    # declare schema_version. paper.daemon.* telemetry events from T6.2 are
    # routing_class=telemetry (NOT archive) so they're out of scope here.
    # See cross-tool drawer e943f994 + dispatch dispatch_protocol_d5.
    d5_archive_event_types = (
        "paper.portfolio_run.completed",
        "paper.daily_snapshot.captured",
        "paper.config.changed",
        "factor.recompute.completed",
    )
    with dev_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT event_id, event_type, {payload_col} AS event_payload
            FROM qe_archive.outbox_event
            WHERE event_type = ANY(%s)
            ORDER BY created_at DESC
            LIMIT 100
            """,
            (list(d5_archive_event_types),),
        )
        rows = list(cur.fetchall())
    if not rows:
        pytest.skip(
            "no D5 archive-side outbox events (paper.portfolio_run.completed / "
            "paper.daily_snapshot.captured / paper.config.changed / "
            "factor.recompute.completed) on dev DB; telemetry-only event types "
            "(paper.daemon.*) are intentionally out of scope here."
        )
    failures = []
    for r in rows:
        payload = _coerce_jsonb(r["event_payload"])
        if not isinstance(payload, dict):
            failures.append((r["event_id"], r["event_type"], "payload is not a JSON object"))
            continue
        if "schema_version" not in payload:
            failures.append((r["event_id"], r["event_type"], "missing schema_version"))
    assert not failures, (
        f"{len(failures)} D5 archive-side outbox events lack schema_version "
        f"(per D5 Q2.b this is required); first 5: {failures[:5]}"
    )
