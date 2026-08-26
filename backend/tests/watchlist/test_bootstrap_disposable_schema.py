from __future__ import annotations

import pytest

from backend.tests.watchlist import bootstrap_disposable_schema as bootstrap


def test_disposable_target_contract_is_removed() -> None:
    with pytest.raises(RuntimeError, match="existing DEV database validation lane"):
        bootstrap._require_disposable_ci_target()


def test_bootstrap_fails_closed_without_database_side_effects() -> None:
    with pytest.raises(RuntimeError, match="existing DEV database validation lane"):
        bootstrap.bootstrap_disposable_schema()
