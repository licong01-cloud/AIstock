from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

from backend.services.hmm_risk import formal_state_input as subject
from backend.services.hmm_risk import rotation_l1_input_bundle as reader
from backend.services.hmm_risk.contracts import ALL_CORE_FEATURES
from backend.services.hmm_risk.formal_state_executor import frozen_release_binding
from backend.services.hmm_risk.formal_state_model import FormalStateError


def test_file_constructor_supplies_approved_frozen_binding_without_active_or_fit(monkeypatch, tmp_path):
    def first_read(root, **kwargs):
        assert root == tmp_path
        assert kwargs["frozen_release_binding"] == frozen_release_binding()
        assert kwargs["data_window_end"] == date(2025, 4, 30)
        assert kwargs["security_identity_manifest"] == tmp_path / "security.json"
        assert kwargs["provider_absence_manifest"] == tmp_path / "absence.json"
        raise FormalStateError("hmm_risk_formal_input_invalid", "explicit first file read reached")

    monkeypatch.setattr(reader, "load_rotation_l1_direct_v2_source_assets", first_read)
    monkeypatch.setattr(reader, "load_active_hmm_dataset_identity", lambda *_: pytest.fail("active fallback accessed"))
    with pytest.raises(FormalStateError, match="explicit first file read"):
        subject.prepare_file_request(
            candidate_root=tmp_path,
            security_identity_manifest=tmp_path / "security.json",
            provider_absence_manifest=tmp_path / "absence.json",
            industry_authority={},
            work_parent=tmp_path / "work",
            producer_commit="a" * 40,
        )
    assert not (tmp_path / "work").exists()


@pytest.mark.parametrize("fault", [None, "duplicate", "sector_missing", "calendar_missing", "nine_dimensions"])
def test_full_feature_frame_closes_calendar_and_sector_denominator(fault):
    calendar = ["2024-07-01", "2024-07-02"]
    codes = ["801001.SI", "801002.SI"]
    index = pd.MultiIndex.from_product([pd.to_datetime(calendar), codes], names=["date", "sector"])
    columns = [*ALL_CORE_FEATURES, "benchmark_return"]
    panel = pd.DataFrame(np.arange(4 * len(columns)).reshape(4, len(columns)), index=index, columns=columns)
    if fault == "duplicate":
        panel = pd.concat([panel, panel.iloc[:1]])
    elif fault == "sector_missing":
        panel = panel.loc[panel.index.get_level_values(1) != codes[1]]
    elif fault == "calendar_missing":
        panel = panel.iloc[:2]
    elif fault == "nine_dimensions":
        panel = panel.iloc[:, :9]
    if fault is not None:
        with pytest.raises(FormalStateError):
            subject._frame(panel, codes=codes, calendar=calendar)
    else:
        restored = subject._frame(panel.iloc[::-1], codes=codes, calendar=calendar)
        assert restored.equals(panel) and len(restored.columns) == 21
