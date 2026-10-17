"""Offline source-boundary and repair-plan integrity checks."""
import pytest

from scripts.repair_local_sync_incident import ANCHOR, DAYS, SCHEMA, digest, groups


@pytest.mark.parametrize("local_anchor,expected", [("1.0", 3), ("0.9", 0), (142.0699, 3)])
def test_adj_append_requires_matching_authoritative_boundary(local_anchor, expected):
    batches = []
    for day in (ANCHOR, *DAYS):
        rows = [{"ts_code": "000001.SZ", "trade_date": day,
                 "adj_factor": 142.0699 if isinstance(local_anchor, float) else 1.0}]
        batches.append({"api": "adj_factor", "day": day, "rows": rows,
                        "sha256": digest(rows), "stable_reads": 2})
    limits = [{"ts_code": "920196.BJ", "trade_date": DAYS[-1],
               "pre_close": 14.21, "up_limit": 99999.99, "down_limit": 0}]
    batches.append({"api": "stk_limit", "day": DAYS[-1], "rows": limits,
                    "sha256": digest(limits), "stable_reads": 2})
    payload = {"schema_version": SCHEMA, "batches": batches, "listings": {},
               "local_adj_boundary": [{"ts_code": "000001.SZ", "trade_date": "2026-10-13", "adj_factor": local_anchor}]}
    rows, _, _, unresolved = groups(payload)
    assert len(rows) == expected
    assert len(unresolved) == 3 - expected
    batches[0]["rows"][0]["adj_factor"] = 2
    with pytest.raises(ValueError, match="source hash"):
        groups(payload)
