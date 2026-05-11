"""T23 (Codex r1 BLOCKED) regression tests:

P1.1 — TARGET_SCHEMAS must include qe_archive so reset_owned_sequences walks
       all 41 qe_archive BIGSERIAL columns (including run_source.id which
       Codex caught with max=35 / seq next=1).

P1.2 — validate_foreign_keys must include qe_archive so the bypass-induced
       orphan rows surface (Codex caught run_source.run_source_run_id_fkey
       orphan_count=16 before this fix landed).

P1.3 — QE_ARCHIVE_SAMPLE.run_source must carry an FK-aware extra_where so
       the import never recreates the orphan condition.
"""
from __future__ import annotations

import pytest

from scripts.dev_db._seq_reset_helpers import (
    TARGET_SCHEMAS,
    list_serial_columns,
    list_foreign_keys,
    reset_owned_sequences,
    validate_foreign_keys,
)
from scripts.dev_db.batch_a_import_real_data import QE_ARCHIVE_SAMPLE


class TestTargetSchemasIncludesQeArchive:
    def test_qe_archive_in_default_target_schemas(self):
        assert "qe_archive" in TARGET_SCHEMAS, \
            f"TARGET_SCHEMAS must include qe_archive (Codex r1 BLOCKED), got {TARGET_SCHEMAS}"

    def test_list_serial_columns_returns_qe_archive_columns(self, dev_conn):
        with dev_conn() as conn:
            cols = list_serial_columns(conn, schemas=("qe_archive",))
        # qe_archive has many BIGSERIAL columns including run_source.id and the
        # 22 T12 paper_v2_*/factor_value tables. Lower bound: 20.
        assert len(cols) >= 20, \
            f"expected >=20 qe_archive serial cols, got {len(cols)}: {cols[:5]}"
        col_names = {(c[1], c[2]) for c in cols}
        assert ("run_source", "id") in col_names, \
            "qe_archive.run_source.id missing from serial column list"

    def test_list_foreign_keys_returns_qe_archive_fks(self, dev_conn):
        with dev_conn() as conn:
            fks = list_foreign_keys(conn, schemas=("qe_archive",))
        # qe_archive has the run_source.run_id FK + 22 T12 FKs to paper_v2_run
        assert len(fks) >= 5, f"expected >=5 qe_archive FKs, got {len(fks)}"


class TestQeArchiveSeqReset:
    def test_reset_advances_qe_archive_run_source_id_seq(self, dev_conn):
        """After reset_owned_sequences walks qe_archive, the run_source.id
        sequence's nextval must equal MAX(id) + 1 (or 2 if no rows yet)."""
        with dev_conn() as conn:
            conn.autocommit = False
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT COALESCE(MAX(id), 0) FROM qe_archive.run_source")
                    max_before = cur.fetchone()[0]
                    cur.execute(
                        "SELECT pg_get_serial_sequence('qe_archive.run_source', 'id')"
                    )
                    seq = cur.fetchone()[0]

                # Run reset
                reset_owned_sequences(conn, schemas=("qe_archive",))

                with conn.cursor() as cur:
                    cur.execute(f"SELECT nextval('{seq}')")
                    next_val = cur.fetchone()[0]
                assert next_val == max(max_before, 1) + 1, \
                    f"after reset, nextval={next_val}; expected {max(max_before, 1) + 1}"
            finally:
                conn.rollback()


class TestQeArchiveFkValidation:
    def test_validate_foreign_keys_detects_qe_archive_orphans_when_present(
        self, dev_conn,
    ):
        """If dev DB has orphan rows in any qe_archive FK, validate_foreign_keys
        must surface them. We don't fabricate orphans; we just verify the sweep
        runs over qe_archive constraints and either reports orphans or clean.
        """
        with dev_conn() as conn:
            conn.autocommit = False
            try:
                report = validate_foreign_keys(conn, schemas=("qe_archive",))
                conn.commit()
            finally:
                pass
        assert len(report.results) > 0, \
            "validate_foreign_keys returned 0 qe_archive results — sweep didn't run"
        # All qe_archive FKs should now be either validated or already_valid.
        # Any orphan_rows means a real integrity issue Codex r1 was right to flag.
        orphans = [r for r in report.results if r.status == "orphan_rows"]
        if orphans:
            msg = "qe_archive orphan rows still present after fix:\n" + "\n".join(
                f"  {r.schema}.{r.table}.{r.constraint}: orphan_count={r.orphan_count}"
                for r in orphans
            )
            pytest.fail(msg)


class TestQeArchiveSampleFkAwareFilter:
    def test_run_source_sample_has_extra_where(self):
        """QE_ARCHIVE_SAMPLE.run_source row must carry an extra_where SQL
        fragment that prunes child rows whose parent run isn't imported."""
        run_source_entry = next(
            (e for e in QE_ARCHIVE_SAMPLE if e[0] == "run_source"), None,
        )
        assert run_source_entry is not None, "run_source missing from QE_ARCHIVE_SAMPLE"
        # 4-tuple (table, time_col, days, extra_where)
        assert len(run_source_entry) == 4, \
            f"QE_ARCHIVE_SAMPLE entries must be 4-tuples, got {len(run_source_entry)}"
        _, _, _, extra = run_source_entry
        assert extra is not None, "run_source must carry FK-aware extra_where"
        assert "qe_archive.run" in extra, \
            f"run_source extra_where must reference parent qe_archive.run; got {extra!r}"

    def test_all_sample_entries_are_4_tuples(self):
        """No entries should still be 3-tuples (would crash the unpack in main)."""
        bad = [e for e in QE_ARCHIVE_SAMPLE if len(e) != 4]
        assert not bad, f"non-4-tuple entries: {bad}"
