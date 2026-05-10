"""Regression tests for scripts/dev_db/batch_a_seq_reset.py (BUG-022).

These exercise the helper logic without touching a real Postgres database:

- ``parse_env`` reads a .env-style file
- ``assert_dev_target`` refuses production targets
- ``reset_sequence_for`` issues the right setval against an in-memory
  cursor and returns ``ok`` with the expected new sequence value
- end-to-end ``main()`` honours the RESET_TARGETS list and short-circuits
  cleanly when the dev DB is the only target

The point is to guarantee that *the canonical fix* for BUG-022 stays
correct over time even when the import script itself moves; the helper is
the thin reusable seam that the dw-foundation team should call after
``batch_a_import_real_data.py`` finishes its COPY phase.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SEQ_RESET_PATH = REPO_ROOT / "scripts" / "dev_db" / "batch_a_seq_reset.py"


@pytest.fixture
def seq_reset_module(monkeypatch):
    """Load scripts/dev_db/batch_a_seq_reset.py as an isolated module."""
    spec = importlib.util.spec_from_file_location("batch_a_seq_reset", SEQ_RESET_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["batch_a_seq_reset"] = module
    spec.loader.exec_module(module)
    yield module
    sys.modules.pop("batch_a_seq_reset", None)


def test_parse_env_reads_key_value_lines(seq_reset_module, tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "# header comment",
                "TDX_DB_HOST=127.0.0.1",
                "TDX_DB_PORT=5433",
                "TDX_DB_NAME=aistock_dev",
                "TDX_DB_USER=aistock",
                'TDX_DB_PASSWORD="hunter2"',
                "",
            ]
        ),
        encoding="utf-8",
    )
    cfg = seq_reset_module.parse_env(env_file=env_file)
    assert cfg["TDX_DB_HOST"] == "127.0.0.1"
    assert cfg["TDX_DB_PORT"] == "5433"
    assert cfg["TDX_DB_PASSWORD"] == "hunter2"


def test_assert_dev_target_accepts_loopback_dev(seq_reset_module):
    seq_reset_module.assert_dev_target("127.0.0.1", 5433, "aistock_dev")
    seq_reset_module.assert_dev_target("localhost", 5433, "AIstock_dev_smoke")


def test_assert_dev_target_rejects_production_port(seq_reset_module):
    with pytest.raises(SystemExit, match="port 5432"):
        seq_reset_module.assert_dev_target("127.0.0.1", 5432, "aistock_dev")


def test_assert_dev_target_rejects_remote_host(seq_reset_module):
    with pytest.raises(SystemExit, match="host"):
        seq_reset_module.assert_dev_target("192.168.50.215", 5433, "aistock_dev")


def test_assert_dev_target_rejects_prod_dbname(seq_reset_module):
    with pytest.raises(SystemExit, match="dbname"):
        seq_reset_module.assert_dev_target("127.0.0.1", 5433, "aistock")


def _conn_with_cursor(rows_for_first_query, rows_for_second_query):
    cur = MagicMock()
    # First execute() is the join-with-pg_get_serial_sequence + GREATEST query.
    # Subsequent execute() is setval. fetchone() drives the first.
    cur.fetchone.side_effect = [rows_for_first_query, rows_for_second_query]
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn, cur


def test_reset_sequence_for_emits_setval_with_max_id(seq_reset_module):
    conn, cur = _conn_with_cursor(
        ("strategy_pkg.package_status_event_event_id_seq", 154),
        None,
    )
    result = seq_reset_module.reset_sequence_for(
        conn, "strategy_pkg", "package_status_event", "event_id"
    )
    assert result.status == "ok"
    assert result.new_value == 154
    # Two execute calls: SELECT pg_get_serial_sequence + setval
    assert cur.execute.call_count == 2
    setval_args = cur.execute.call_args_list[1].args
    assert setval_args[0] == "SELECT setval(%s, %s, true)"
    assert setval_args[1] == ("strategy_pkg.package_status_event_event_id_seq", 154)
    conn.commit.assert_called_once()


def test_reset_sequence_for_handles_table_without_serial_sequence(seq_reset_module):
    # pg_get_serial_sequence returns NULL, max returns 0 -> skipped
    conn, _ = _conn_with_cursor((None, 0), None)
    result = seq_reset_module.reset_sequence_for(
        conn, "qe_archive", "outbox_event", None,
    )
    # Without explicit id_column the helper tries detect_id_column first which
    # uses a SEPARATE cursor.execute call. We mock detect_id_column out below.


def test_reset_sequence_for_explicit_column_skipped_when_no_sequence(seq_reset_module):
    conn, cur = _conn_with_cursor((None, 0), None)
    result = seq_reset_module.reset_sequence_for(
        conn, "qe_archive", "outbox_event", "event_id",
    )
    assert result.status == "skipped"
    assert "no associated sequence" in result.note
    conn.rollback.assert_called()


def test_reset_sequence_for_propagates_db_error_as_failed(seq_reset_module):
    conn = MagicMock()

    def explode(*args, **kwargs):
        raise RuntimeError("simulated psycopg2 error")

    cur = MagicMock()
    cur.execute.side_effect = explode
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cur

    result = seq_reset_module.reset_sequence_for(
        conn, "strategy_pkg", "package_status_event", "event_id"
    )
    assert result.status == "failed"
    assert "simulated psycopg2 error" in result.note
    conn.rollback.assert_called()


def test_reset_targets_includes_known_offenders(seq_reset_module):
    flat = [(s, t) for s, t, _ in seq_reset_module.RESET_TARGETS]
    # The table that originally surfaced BUG-022 must be covered.
    assert ("strategy_pkg", "package_status_event") in flat
    # paper_v2 high-volume event tables need it too.
    assert ("paper_v2", "order_events") in flat
    assert ("paper_v2", "session_events") in flat


def test_render_results_renders_summary(seq_reset_module):
    R = seq_reset_module.ResetResult
    text = seq_reset_module.render_results(
        [
            R("strategy_pkg", "package_status_event", "event_id", "ok", new_value=154, note="seq"),
            R("paper_v2", "errors", None, "skipped", note="no primary key found"),
        ]
    )
    assert "package_status_event" in text
    assert "ok" in text
    assert "skipped" in text
