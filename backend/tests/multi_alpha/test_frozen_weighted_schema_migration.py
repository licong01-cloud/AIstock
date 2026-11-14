"""Keep the API scheme domain aligned with its latest persistence migration.

This is a read-only CI regression guard. Real DDL and transactional result
insertion are verified separately in the existing DEV database.
"""

import re
from pathlib import Path

import pytest

from backend.services.multi_alpha.combine_backtest import SUPPORTED_WEIGHTING_SCHEMES


@pytest.mark.parametrize(
    "constraint",
    ["ck_macb_scheme_supported", "ck_macb_loo_scheme_supported"],
)
def test_every_api_scheme_can_be_persisted_by_latest_migration(constraint):
    migrations = Path(__file__).resolve().parents[2] / "migrations"
    candidates = list(migrations.glob("multi_alpha_*schemes_*.sql"))
    latest = max(candidates, key=lambda path: re.search(r"(\d{8})\.sql$", path.name)[1])
    sql = latest.read_text(encoding="utf-8-sig")
    clause = re.search(
        rf"ADD\s+CONSTRAINT\s+{constraint}\s+CHECK\s*\(\s*"
        r"weighting_scheme\s+IN\s*\((.*?)\)\s*\)",
        sql,
        re.IGNORECASE | re.DOTALL,
    )
    assert clause is not None, f"{latest.name} does not define {constraint}"
    persisted_schemes = re.findall(r"'([^']+)'", clause[1])
    assert len(persisted_schemes) == len(set(persisted_schemes))
    assert set(persisted_schemes) == set(SUPPORTED_WEIGHTING_SCHEMES)
