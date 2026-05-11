"""Tests for Batch C P1.3 (column dependency pre-flight) and P1.4
(outbox payload routing_class).
"""
from __future__ import annotations

import json

import pytest

from scripts.dev_db.batch_c_synthetic_fixtures import (
    REQUIRED_COLUMNS,
    assert_required_columns,
)


class TestRequiredColumnsPreflight:
    def test_all_required_columns_present_on_dev(self, dev_conn):
        """If dev DB has had T5 + T1 migrations applied (it has, per drawer
        eec88545), assert_required_columns should NOT exit."""
        with dev_conn() as conn:
            with conn.cursor() as cur:
                # Should not raise / sys.exit
                assert_required_columns(cur)

    def test_required_columns_list_covers_t5_and_t1(self):
        """Sanity that REQUIRED_COLUMNS includes the T5 (paper_v2 capture
        fields) and T1 (model_params_origin) columns Codex REV-6 called out.
        """
        cols = {(s, t, c) for s, t, c, _ in REQUIRED_COLUMNS}
        assert ("paper_v2", "fills", "intended_price") in cols
        assert ("paper_v2", "fills", "fill_market_context") in cols
        assert ("paper_v2", "run", "model_params_origin") in cols


class TestOutboxRoutingClass:
    """P1.4 — every Batch C-inserted outbox event must carry
    payload['routing_class']='archive'."""

    def test_existing_dev_seed_outbox_events_have_routing_class(self, dev_conn):
        """If Batch C has been run on dev, all dev_seed_* events should now
        carry routing_class. Skip if Batch C hasn't been run."""
        with dev_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT event_id, payload
                       FROM qe_archive.outbox_event
                       WHERE event_id LIKE 'dev_seed_%'"""
                )
                rows = cur.fetchall()
        if not rows:
            pytest.skip("no dev_seed outbox_event rows; run batch_c first")

        bad = []
        for event_id, payload in rows:
            if isinstance(payload, str):
                payload = json.loads(payload)
            if (payload or {}).get("routing_class") != "archive":
                bad.append(event_id)
        assert not bad, \
            f"{len(bad)} dev_seed events missing routing_class='archive': {bad[:5]}"
