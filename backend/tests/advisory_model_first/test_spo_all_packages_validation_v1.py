"""Identity, original population and leakage boundaries for zero-fit applicability."""

import json

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first import spo_all_packages_validation_v1 as core


def reference(path):
    return dict(path=str(path), size_bytes=path.stat().st_size, sha256=core.file_sha(path))


def package():
    return dict(package_id="a", manifest_sha256="a" * 64, run_id="r", source_id="s")


def source(tmp_path, count=51):
    dates = [pd.Timestamp("2025-04-01").date()]
    calendar = [*dates, pd.Timestamp("2025-04-02").date()]
    frame = pd.DataFrame({"score": np.arange(count, dtype=float)},
                         index=pd.MultiIndex.from_product([pd.to_datetime(dates), [f"{i:06}.SZ" for i in range(count)]],
                                                          names=["datetime", "instrument"]))
    path = tmp_path / "scores.pkl"
    frame.to_pickle(path)
    return dict(**package(), file=reference(path)), dates, calendar


def models(tmp_path, cutoff="2025-03-31"):
    refs = {}
    for arm in ("candidate", "matched"):
        packet = dict(arm=arm, cutoff=cutoff, contract_sha256=core.spo.sha(core.spo.CONTRACT),
                      fields=list(core.spo.FEATURES), median=[0.] * 8, scale=[1.] * 8,
                      coef=list(range(16)), training_keys_sha256="b" * 64)
        packet["model_payload_sha256"] = core.spo.sha(packet)
        path = tmp_path / (arm + ".json")
        path.write_text(json.dumps(packet), encoding="utf-8")
        refs[arm] = reference(path)
    return refs


def test_original_score_projection_and_unknown_slots(tmp_path):
    s, dates, cal = source(tmp_path)
    rows = core.score_roster(s, package(), dates, cal)
    assert len(rows) == 50 and rows.iloc[0].instrument == "000050.SZ"
    assert rows.target_trade_date.eq(pd.Timestamp("2025-04-02")).all()
    for name in core.spo.FEATURES:
        rows[name] = 1.
    rows.loc[0, core.spo.FEATURES] = np.nan
    result = core.predict_fold(rows, package=package(), models=models(tmp_path), fold=1)
    for frame in result.values():
        assert frame.iloc[0].no_rank_advice and frame.iloc[0].advisory_rank == 1
        assert frame.selected.sum() == 10 and len(frame) == 50


@pytest.mark.parametrize("change", ["hash", "manifest", "missing_day", "small"])
def test_bad_source_fails_without_population_fallback(tmp_path, change):
    s, dates, cal = source(tmp_path, count=49 if change == "small" else 51)
    if change == "hash":
        s["file"]["sha256"] = "0" * 64
    if change == "manifest":
        s["manifest_sha256"] = "b" * 64
    if change == "missing_day":
        dates.append(cal[-1])
    with pytest.raises(ValueError):
        core.score_roster(s, package(), dates, cal)


def test_final_model_cannot_retest_training_period(tmp_path):
    s, dates, cal = source(tmp_path)
    rows = core.score_roster(s, package(), dates, cal)
    for name in core.spo.FEATURES:
        rows[name] = 1.
    with pytest.raises(ValueError, match="final model"):
        core.predict_fold(rows, package=package(), models=models(tmp_path, "2025-09-30"), fold=1)


def test_inventory_draft_dedup_and_missing_package_is_explicit():
    inv = dict(programs=[dict(status="ENABLED", package_ids=["a", "b"]),
                         dict(status="DRAFT", package_ids=["c"])],
               packages=[dict(package_id="a"), dict(package_id="b")], enabled_package_ids=["a", "b"])
    assert set(core.inventory_packages(inv)) == {"a", "b"}
    empty = pd.DataFrame(columns=["package_id"])
    report = core.all_package_report(inv, empty, {"a":"NO_SOURCE", "b":"NO_SOURCE"}, [])
    assert report["inventory_count"] == 2 and report["evaluated_count"] == 0
    assert report["full_inventory_mean_bps"] is None and not report["production_ready"]
    with pytest.raises(ValueError, match="all enabled"):
        core.all_package_report(inv, empty, {"a":"NO_SOURCE"}, [])


def test_missing_panel_date_is_not_dropped_from_denominator():
    panel = pd.DataFrame(dict(package_id=["a"], decision_date=pd.to_datetime(["2025-04-01"]),
                              fold=[1], original_candidates=[50]))
    with pytest.raises(ValueError, match="original axis"):
        core.package_summary(panel, pd.to_datetime(["2025-04-01", "2025-06-03", "2025-08-01"]))
