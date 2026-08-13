from __future__ import annotations

from pathlib import Path

import pytest

from backend.services.dataset_release.profile import DatasetProfile, load_dataset_profile


ROOT = Path(__file__).resolve().parents[3]
PROFILE_PATH = ROOT / "configs" / "datasets" / "qe_backtest_monthly_v1.yaml"


@pytest.fixture
def dataset_profile() -> DatasetProfile:
    return load_dataset_profile(PROFILE_PATH)
