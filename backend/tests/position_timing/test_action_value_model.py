from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services.position_timing.action_value import ActionValueError, FEATURE_ORDER, TZ, cutoff_on
from backend.services.position_timing.action_value_model import (
    HEADS, fit_local_model, monthly_training_windows, numeric_matrix,
    read_local_model, training_rows_asof, write_local_model,
)


def training_fixture():
    rng = np.random.default_rng(17)
    frame = pd.DataFrame(rng.normal(size=(500, len(FEATURE_ORDER))), columns=FEATURE_ORDER)
    frame["holding_age_missing"] = 0.0
    frame["entry_cost_missing"] = 0.0
    frame["objective"] = np.tile(HEADS, 250)
    frame["net_action_value_bps"] = frame["return_20d_bps"] * 2
    frame["decision_as_of"] = cutoff_on(date(2024, 1, 2))
    frame["label_available_at"] = cutoff_on(date(2024, 2, 2))
    return frame


@pytest.fixture(scope="module")
def fitted():
    return fit_local_model(training_fixture(), cutoff=cutoff_on(date(2024, 3, 29)),
                           available_at=cutoff_on(date(2024, 4, 1)), source_sha256="1" * 64,
                           request_sha256="2" * 64, source_commit="a" * 40)


def test_model_train_only_matured_labels_and_fail_naive():
    rows = training_fixture()
    rows.loc[0, "label_available_at"] = cutoff_on(date(2025, 1, 1))
    eligible = training_rows_asof(rows, cutoff_on(date(2024, 3, 29)))
    assert len(eligible) == 499 and 0 not in eligible.index
    rows["decision_as_of"] = datetime(2024, 1, 2)
    with pytest.raises(ActionValueError, match="TIME_INVALID"):
        training_rows_asof(rows, cutoff_on(date(2024, 3, 29)))


def test_training_medians_not_influenced_by_prediction():
    frame = training_fixture().loc[:, FEATURE_ORDER]
    _, medians = numeric_matrix(frame)
    changed = frame.copy()
    changed.loc[0, "return_1d_bps"] = np.nan
    matrix, actual = numeric_matrix(changed, medians)
    assert matrix.loc[0, "return_1d_bps"] == medians["return_1d_bps"]
    assert actual == medians
    with pytest.raises(ActionValueError, match="ALL_MISSING"):
        numeric_matrix(frame.assign(return_1d_bps=np.nan))


def test_unknown_cost_age_preserve_masks_not_fake_observations():
    frame = training_fixture().loc[:, FEATURE_ORDER]
    frame["holding_age"] = np.nan
    frame["holding_age_missing"] = 1.0
    matrix, medians = numeric_matrix(frame)
    assert matrix.holding_age.eq(0).all() and matrix.holding_age_missing.eq(1).all()
    assert medians["holding_age"] == 0
    frame["holding_age_missing"] = 0
    with pytest.raises(ActionValueError, match="MASK_INVALID"):
        numeric_matrix(frame)


def test_local_native_text_roundtrip_and_future_publish_refused(fitted, tmp_path: Path):
    folder = write_local_model(fitted, timing_root=tmp_path)
    assert {p.suffix for p in folder.iterdir()} == {".txt", ".json"}
    loaded = read_local_model(timing_root=tmp_path, model_sha256=fitted.metadata["model_sha256"],
                              decision_as_of=cutoff_on(date(2024, 4, 2)), allow_historical=True)
    frame = training_fixture().loc[:9, FEATURE_ORDER]
    objectives = list(training_fixture().objective.iloc[:10])
    np.testing.assert_allclose(
        loaded.predict(frame, objectives, decision_as_of=cutoff_on(date(2024, 4, 2))),
        fitted.predict(frame, objectives, decision_as_of=cutoff_on(date(2024, 4, 2))),
    )
    assert write_local_model(fitted, timing_root=tmp_path) == folder
    with pytest.raises(ActionValueError, match="NOT_AVAILABLE"):
        read_local_model(timing_root=tmp_path, model_sha256=fitted.metadata["model_sha256"],
                         decision_as_of=cutoff_on(date(2024, 3, 29)), allow_historical=True)
    with pytest.raises(ActionValueError, match="NOT_SERVABLE"):
        read_local_model(timing_root=tmp_path, model_sha256=fitted.metadata["model_sha256"],
                         decision_as_of=cutoff_on(date(2024, 4, 2)))


def test_model_corruption_and_path_escape_fail_closed(fitted, tmp_path: Path):
    folder = write_local_model(fitted, timing_root=tmp_path)
    (folder / f"{HEADS[0]}.txt").write_text("corrupt", encoding="utf-8")
    with pytest.raises(ActionValueError, match="TEXT_IDENTITY"):
        read_local_model(timing_root=tmp_path, model_sha256=fitted.metadata["model_sha256"],
                         decision_as_of=cutoff_on(date(2024, 4, 2)), allow_historical=True)
    with pytest.raises(ValueError):
        read_local_model(timing_root=tmp_path, model_sha256="../../evil",
                         decision_as_of=cutoff_on(date(2024, 4, 2)))


def test_missing_current_core_does_not_silently_impute(fitted):
    rows = training_fixture().loc[:1, FEATURE_ORDER].copy()
    rows["return_1d_bps"] = np.nan
    with pytest.raises(ActionValueError, match="CURRENT_CORE"):
        fitted.predict(rows, list(HEADS), decision_as_of=cutoff_on(date(2024, 4, 2)))
    rows["return_1d_bps"] = np.inf
    with pytest.raises(ActionValueError, match="CURRENT_CORE"):
        fitted.predict(rows, list(HEADS), decision_as_of=cutoff_on(date(2024, 4, 2)))


def test_partially_missing_state_requires_per_row_mask():
    frame = training_fixture().loc[:, FEATURE_ORDER]
    frame.loc[0, "holding_age"] = np.nan
    with pytest.raises(ActionValueError, match="MASK_INVALID"):
        numeric_matrix(frame)
    frame.loc[0, "holding_age_missing"] = 1
    matrix, _ = numeric_matrix(frame)
    assert np.isfinite(matrix.holding_age).all()


def test_monthly_windows_not_future_or_same_day_deploy():
    days = tuple(pd.bdate_range("2018-08-01", periods=900).date)
    windows = monthly_training_windows(days)
    assert windows
    for window in windows:
        assert window["available_at"] > window["cutoff"]
        assert window["first_target_trade_date"] > window["available_at"].date()
        assert days.index(window["cutoff"].date()) >= 755
    with pytest.raises(ActionValueError, match="SCHEDULE_DRIFT"):
        monthly_training_windows(days, initial_sessions=10)


def test_live_training_and_publication_cannot_be_backdated(tmp_path):
    started = datetime.now(TZ)
    model = fit_local_model(training_fixture(), cutoff=cutoff_on(date(2024, 3, 29)),
                            available_at=cutoff_on(date(2024, 4, 1)), source_sha256="1" * 64,
                            request_sha256="2" * 64, source_commit="a" * 40, temporal_mode="LIVE_FINAL_FIT")
    trained_at = datetime.fromisoformat(model.metadata["trained_at"])
    assert trained_at >= started
    assert datetime.fromisoformat(model.metadata["available_at"]) >= trained_at
    frame = training_fixture().loc[:1, FEATURE_ORDER]
    with pytest.raises(ActionValueError, match="NOT_PUBLISHED"):
        model.predict(frame, HEADS, decision_as_of=datetime.now(TZ))
    folder = write_local_model(model, timing_root=tmp_path)
    original = (folder / "publication.json").read_bytes()
    assert write_local_model(model, timing_root=tmp_path) == folder
    assert (folder / "publication.json").read_bytes() == original
    assert model.published_at >= trained_at
    loaded = read_local_model(timing_root=tmp_path, model_sha256=model.metadata["model_sha256"], decision_as_of=datetime.now(TZ))
    assert loaded.predict(frame, HEADS, decision_as_of=datetime.now(TZ)).shape == (2,)
    with pytest.raises(ActionValueError, match="NOT_AVAILABLE"):
        read_local_model(timing_root=tmp_path, model_sha256=model.metadata["model_sha256"], decision_as_of=started-timedelta(seconds=1))

