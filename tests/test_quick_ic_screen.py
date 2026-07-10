import json
from pathlib import Path
import sys
import types

import numpy as np
import pandas as pd
import pytest

from scripts import quick_ic_screen


def _valid_split_manifest(**overrides: object) -> dict[str, object]:
    return {
        "manifest_version": 1,
        "trial_id": "trial-a2-001",
        "split_id": "sector-h20-validation-v1",
        "split_role": "validation",
        "signal_start": "2025-01-02",
        "signal_end": "2025-06-30",
        "label_horizon_days": 20,
        "purge_days": 20,
        "embargo_days": 0,
        "expected_direction": 1,
        "data_snapshot_sha256": "a" * 64,
        **overrides,
    }


def _install_fake_qlib_reader(
    monkeypatch: pytest.MonkeyPatch, frame: pd.DataFrame
) -> None:
    modules = {
        "rdagent": types.ModuleType("rdagent"),
        "rdagent.app": types.ModuleType("rdagent.app"),
        "rdagent.app.factor_metrics": types.ModuleType("rdagent.app.factor_metrics"),
        "rdagent.app.factor_metrics.qlib_data_reader": types.ModuleType(
            "rdagent.app.factor_metrics.qlib_data_reader"
        ),
    }
    modules["rdagent.app.factor_metrics.qlib_data_reader"].read_close_prices = (
        lambda start_date: frame
    )
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)


def test_build_forward_returns_uses_t_plus_one_entry() -> None:
    close = pd.DataFrame({"000001.SZ": np.arange(1.0, 31.0)})

    h1 = quick_ic_screen.build_forward_returns(close, 1)
    h20 = quick_ic_screen.build_forward_returns(close, 20)

    assert h1.iloc[0, 0] == pytest.approx(close.iloc[2, 0] / close.iloc[1, 0] - 1)
    assert h20.iloc[0, 0] == pytest.approx(close.iloc[21, 0] / close.iloc[1, 0] - 1)
    assert h20.iloc[-21:, 0].isna().all()


def test_build_forward_returns_rejects_non_positive_horizon() -> None:
    close = pd.DataFrame({"000001.SZ": [1.0, 2.0, 3.0]})

    with pytest.raises(ValueError, match="positive integer"):
        quick_ic_screen.build_forward_returns(close, 0)


def test_hac_icir_is_nullable_for_insufficient_or_degenerate_series() -> None:
    assert quick_ic_screen._hac_icir([0.1] * 19, lag=19) is None
    assert quick_ic_screen._hac_icir([0.1] * 25, lag=19) is None

    values = [0.01 + (index % 5) * 0.002 for index in range(40)]
    assert np.isfinite(quick_ic_screen._hac_icir(values, lag=19))
    with pytest.raises(ValueError, match="non-negative"):
        quick_ic_screen._hac_icir(values, lag=-1)


def test_frozen_direction_prevents_absolute_value_pass() -> None:
    assert quick_ic_screen._classify_ic(0.02, 0.02, expected_direction=1) == "PASS"
    assert quick_ic_screen._classify_ic(0.02, 0.02, expected_direction=-1) == "KILL"
    assert quick_ic_screen._classify_ic(-0.02, -0.02, expected_direction=None) == "PASS"
    assert quick_ic_screen._classify_ic(0.006, 0.011, expected_direction=1) == "MARGINAL"
    assert quick_ic_screen._classify_ic(0.001, 0.001, expected_direction=None) == "KILL"
    with pytest.raises(ValueError, match="expected_direction"):
        quick_ic_screen._classify_ic(0.02, 0.02, expected_direction=0)


def test_split_manifest_freezes_h20_contract(tmp_path: Path) -> None:
    manifest_path = tmp_path / "split.json"
    manifest_path.write_text(
        json.dumps(_valid_split_manifest()),
        encoding="utf-8",
    )

    receipt = quick_ic_screen.load_split_manifest(
        manifest_path, horizon=20, cli_direction=1
    )

    assert receipt["split_role"] == "validation"
    assert receipt["expected_direction"] == 1
    assert receipt["purge_days"] == 20
    assert len(receipt["manifest_sha256"]) == 64


def test_split_manifest_rejects_insufficient_purge(tmp_path: Path) -> None:
    manifest_path = tmp_path / "split.json"
    manifest_path.write_text(
        json.dumps(_valid_split_manifest(purge_days=19)),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="purge_days"):
        quick_ic_screen.load_split_manifest(manifest_path, horizon=20, cli_direction=1)


@pytest.mark.parametrize(
    ("overrides", "horizon", "direction", "match"),
    [
        ({"manifest_version": 2}, 20, 1, "manifest_version"),
        ({"split_role": "holdout"}, 20, 1, "split_role"),
        ({"label_horizon_days": 10}, 20, 1, "label_horizon_days"),
        ({"expected_direction": 0}, 20, None, "expected_direction"),
        ({"expected_direction": -1}, 20, 1, "conflicts"),
        ({"embargo_days": -1}, 20, 1, "embargo_days"),
        ({"signal_start": "2025-07-01"}, 20, 1, "signal_end"),
        ({"data_snapshot_sha256": "not-a-digest"}, 20, 1, "data_snapshot_sha256"),
    ],
)
def test_split_manifest_rejects_invalid_contracts(
    tmp_path: Path,
    overrides: dict[str, object],
    horizon: int,
    direction: int | None,
    match: str,
) -> None:
    manifest_path = tmp_path / "split.json"
    manifest_path.write_text(
        json.dumps(_valid_split_manifest(**overrides)), encoding="utf-8"
    )

    with pytest.raises(ValueError, match=match):
        quick_ic_screen.load_split_manifest(
            manifest_path, horizon=horizon, cli_direction=direction
        )


def test_split_manifest_rejects_missing_fields(tmp_path: Path) -> None:
    manifest = _valid_split_manifest()
    manifest.pop("trial_id")
    manifest_path = tmp_path / "split.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="missing required fields"):
        quick_ic_screen.load_split_manifest(manifest_path, horizon=20, cli_direction=1)


def test_quick_ic_returns_contract_for_insufficient_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dates = pd.date_range("2024-07-01", periods=19, freq="B")
    instruments = [f"{index:06d}.SZ" for index in range(100)]
    index = pd.MultiIndex.from_product(
        [dates, instruments], names=["datetime", "instrument"]
    )
    factor_frame = pd.DataFrame({"m_short": np.arange(len(index), dtype=float)}, index=index)
    returns = pd.DataFrame(1.0, index=dates, columns=instruments)
    monkeypatch.setattr(quick_ic_screen.pd, "read_hdf", lambda _path: factor_frame)

    result = quick_ic_screen.quick_ic(Path("unused"), returns, returns, horizon=20)

    assert result["verdict"] == "KILL"
    assert result["return_horizon_label"] == "T21T1"
    assert "insufficient data" in result["reason"]


def test_quick_ic_rejects_invalid_window(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ValueError, match="eval_end"):
        quick_ic_screen.quick_ic(
            Path("unused"),
            pd.DataFrame(),
            pd.DataFrame(),
            eval_start="2025-02-01",
            eval_end="2025-01-01",
        )


def test_main_uses_split_receipt_and_emits_formal_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    manifest_path = tmp_path / "split.json"
    manifest_path.write_text(json.dumps(_valid_split_manifest()), encoding="utf-8")
    workspace = tmp_path / "factor"
    workspace.mkdir()
    (workspace / "result.h5").touch()

    close_index = pd.MultiIndex.from_product(
        [pd.date_range("2024-01-02", periods=3, freq="B"), ["000001.SZ"]],
        names=["datetime", "instrument"],
    )
    _install_fake_qlib_reader(
        monkeypatch, pd.DataFrame({"close": [1.0, 1.1, 1.2]}, index=close_index)
    )
    monkeypatch.setattr(
        quick_ic_screen,
        "quick_ic",
        lambda *args, **kwargs: {
            "factor_name": "m_formal",
            "verdict": "PASS",
            "ic_mean": 0.02,
            "rank_ic": 0.02,
            "divergence_flag": "",
        },
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "quick_ic_screen.py",
            "--horizon",
            "20",
            "--split-manifest",
            str(manifest_path),
            str(workspace),
        ],
    )

    quick_ic_screen.main()

    output = json.loads(capsys.readouterr().out)
    assert output[0]["formal_gate_eligible"] is True
    assert output[0]["split_receipt"]["split_id"] == "sector-h20-validation-v1"


def test_main_reports_missing_result_as_diagnostic(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    close_index = pd.MultiIndex.from_product(
        [pd.date_range("2024-01-02", periods=3, freq="B"), ["000001.SZ"]],
        names=["datetime", "instrument"],
    )
    _install_fake_qlib_reader(
        monkeypatch, pd.DataFrame({"close": [1.0, 1.1, 1.2]}, index=close_index)
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["quick_ic_screen.py", "--horizon", "20", str(tmp_path / "missing")],
    )

    quick_ic_screen.main()

    output = json.loads(capsys.readouterr().out)
    assert output[0]["verdict"] == "ERROR"
    assert output[0]["reason"] == "result.h5 not found"


def test_quick_ic_emits_h20_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    dates = pd.date_range("2024-07-01", periods=40, freq="B")
    instruments = [f"{index:06d}.SZ" for index in range(100)]
    index = pd.MultiIndex.from_product(
        [dates, instruments], names=["datetime", "instrument"]
    )

    base = np.tile(np.arange(100, dtype=float), len(dates))
    factor_frame = pd.DataFrame({"m_test_factor": base}, index=index)
    forward_returns = pd.DataFrame(
        np.vstack(
            [
                np.arange(100, dtype=float) + (day % 7) * np.linspace(0.0, 1.0, 100)
                for day in range(len(dates))
            ]
        ),
        index=dates,
        columns=instruments,
    )
    monkeypatch.setattr(quick_ic_screen.pd, "read_hdf", lambda _path: factor_frame)

    result = quick_ic_screen.quick_ic(
        Path("unused-result.h5"),
        forward_returns,
        forward_returns,
        horizon=20,
    )

    assert result["return_horizon_days"] == 20
    assert result["return_horizon_label"] == "T21T1"
    assert result["hac_lag"] == 19
    assert result["label_source_end"] == str(dates[-1].date())
    assert result["last_evaluable_signal_date"] == str(dates[-1].date())
    assert "icir_hac" in result
    assert "rank_icir_hac" in result
    assert "ic_std_ddof0" in result
    assert "icir_ddof0" in result
    assert "rank_ic_std_ddof0" in result
    assert "rank_icir_ddof0" in result
    assert result["verdict"] == "PASS"
