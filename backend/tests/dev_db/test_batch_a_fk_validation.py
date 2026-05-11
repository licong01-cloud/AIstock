"""Tests for scripts/dev_db/_seq_reset_helpers.validate_foreign_keys (P1.2).

After Batch A's session_replication_role='replica' bypass, run the FK
validation sweep and assert zero orphan rows / failed validations.
"""
from __future__ import annotations

import pytest

from scripts.dev_db._seq_reset_helpers import (
    list_foreign_keys,
    validate_foreign_keys,
)


class TestFkValidation:
    def test_lists_fks_for_target_schemas(self, dev_conn):
        with dev_conn() as conn:
            fks = list_foreign_keys(conn)
        # paper_v2 has many FKs (run.portfolio_id, fills.run_id, etc.)
        schemas = {fk["schema"] for fk in fks}
        assert "paper_v2" in schemas, f"expected paper_v2 in {schemas}"
        assert len(fks) >= 5, f"expected at least 5 FKs in target schemas, got {len(fks)}"

    def test_validate_foreign_keys_returns_no_failures(self, dev_conn):
        """The post-Batch-A integrity sweep must report ZERO failures.

        If this test fails the dev DB has either:
          - a NOT VALIDATED FK that VALIDATE CONSTRAINT couldn't cure
          - actual orphan rows (the bypass let bad data in)
        Either is a P0 issue Codex REV-6 was right to call out.
        """
        with dev_conn() as conn:
            conn.autocommit = False
            try:
                report = validate_foreign_keys(conn)
                conn.commit()
            except Exception:
                conn.rollback()
                raise

        failures = [r for r in report.results
                    if r.status in ("failed", "orphan_rows")]
        if failures:
            msg = "FK integrity failures:\n" + "\n".join(
                f"  {r.schema}.{r.table}.{r.constraint}: status={r.status} "
                f"orphans={r.orphan_count} note={r.note}"
                for r in failures
            )
            pytest.fail(msg)

        # Also assert SOMETHING was checked (positive coverage signal)
        assert len(report.results) > 0, "FK sweep returned 0 results"

    def test_no_unvalidated_fks_remain(self, dev_conn):
        """After running validate_foreign_keys, every FK in target schemas
        must have convalidated=true."""
        with dev_conn() as conn:
            conn.autocommit = False
            try:
                validate_foreign_keys(conn)
                conn.commit()

                fks = list_foreign_keys(conn)
                unvalidated = [fk for fk in fks if not fk["convalidated"]]
            finally:
                pass
        assert not unvalidated, \
            f"{len(unvalidated)} FK(s) still NOT VALIDATED: " \
            + ", ".join(f"{f['schema']}.{f['table']}.{f['conname']}" for f in unvalidated[:5])
