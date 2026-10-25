"""Direct synchrony, clock, common ranking and immutable six-fit contracts."""

import json
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first import intraday_peer_synchrony_top10_5td_v1 as core
from backend.services.advisory_model_first import intraday_peer_synchrony_top10_5td_pipeline_v1 as pipe
from backend.services.advisory_model_first import intraday_peer_synchrony_top10_5td_inputs_v1 as inp
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import POLICY
from backend.services.advisory_model_first.multiperiod_top10_5td_inputs_v1 import FINANCE
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError


def population():
    rows = []
    for d in pd.to_datetime(["2024-11-01", "2024-11-04", "2025-04-01"]):
        for pkg in ("a", "b"):
            for rank in range(1, 51):
                y = float(100 - rank)
                rows.append(
                    dict(
                        package_id=pkg,
                        manifest_sha256="a" * 64,
                        run_id="run_" + pkg,
                        source_id="source",
                        decision_as_of_trade_date=d,
                        target_trade_date=d + pd.offsets.BDay(1),
                        instrument=f"{rank - 1:06}.SZ",
                        selection_effective_rank=rank,
                        candidate_group_size=50,
                        parent_score=-rank,
                        **{n: np.sin(rank * (i + 1) / 50) for i, n in enumerate(core.FEATURES)},
                        label_information_end=d + pd.offsets.BDay(5),
                        valuation_status="AVAILABLE",
                        hypothetical_liquidation_net_bps=y,
                        observed_gap_bps=0.0,
                        valuation_gross_terminal_ratio=(1 + y / 10000)
                        * (1 + POLICY["buy_bps"] / 10000)
                        / (1 - POLICY["sell_bps"] / 10000),
                        exit_execution_status="NOMINAL_EXIT_ELIGIBLE_NOT_FILL_PROOF",
                        valuation_policy_sha256=core.base.VALUATION_POLICY_SHA256,
                    )
                )
    return pd.DataFrame(rows)


def test_training_maturity_common_support_json_slots_and_original_identity():
    rows = population()
    a = core.fit(rows, "2025-03-31", "candidate")
    bad = rows.copy()
    # Both future D and immature outcomes on a past D must be projected first.
    bad.loc[bad.decision_as_of_trade_date.gt("2025-03-31"), "hypothetical_liquidation_net_bps"] = np.inf
    assert core.fit(bad, "2025-03-31", "candidate") == a
    immature = bad.selection_effective_rank.eq(1) & bad.decision_as_of_trade_date.eq("2024-11-01")
    bad.loc[immature, "label_information_end"] = pd.Timestamp("2025-04-02")
    bad.loc[immature, "hypothetical_liquidation_net_bps"] = np.inf
    assert core.fit(bad, "2025-03-31", "candidate") == core.fit(rows.loc[~immature], "2025-03-31", "candidate")
    b = core.fit(rows, "2025-03-31", "matched")
    assert a["training_keys_sha256"] == b["training_keys_sha256"]
    rows.loc[rows.selection_effective_rank.eq(1), core.NEW[0]] = np.nan
    prediction = core.predict(rows, json.loads(json.dumps(a)))
    assert prediction.no_rank_advice.equals(core.predict(rows, b).no_rank_advice)
    assert prediction.loc[prediction.no_rank_advice, "advisory_rank"].eq(1).all()
    assert prediction.groupby(core.GROUP).selected.sum().eq(10).all()
    assert prediction[list(core.IDENTITY)].equals(rows[list(core.IDENTITY)])
    a["contract_sha256"] = "bad"
    with pytest.raises(ValueError, match="identity"):
        core.predict(rows, a)


def test_native_product_endpoint_is_not_a_matched_or_sign_gate():
    rows = []
    for i, d in enumerate(pd.bdate_range("2025-04-01", periods=60)):
        for pkg in ("a", "b"):
            rows.append(
                dict(
                    package_id=pkg,
                    decision_date=d,
                    fold=i // 20 + 1,
                    mature=True,
                    original_candidates=50,
                    candidate=30.0,
                    baseline=10.0,
                    candidate_full_model=50.0,
                    delta=20.0,
                    intervention_count=1,
                    matched_intervention_count=1,
                    candidate_unknown=0,
                    baseline_unknown=0,
                )
            )
    cohort = pd.DataFrame(rows)
    report, _ = core.summarize(cohort)
    assert (
        report["supports_future_confirmation_design"]
        and not report["proceed_outer"]
        and report["classification"] == "INNER_SUPPORTED"
    )
    assert report["versus_matched"]["mean_bps"] < 0
    cohort.candidate = 0.0
    assert core.summarize(cohort)[0]["classification"] == "INNER_NOT_SUPPORTED"
    cohort.candidate, cohort.intervention_count = 30.0, 0
    assert core.summarize(cohort)[0]["classification"] == "INNER_INCONCLUSIVE"


def test_claim_clock_and_interrupted_fit_are_not_silently_retried(tmp_path):
    study = object.__new__(pipe.Study)
    study.root, study.digest, study.budget = tmp_path, "a" * 64, lambda: None
    study.claim("fold1_matched")
    with pytest.raises(ValueError, match="unpublished fit"):
        pipe.fit_pair(
            study,
            None,
            None,
            None,
            prefix="fold1",
            cutoff="2025-03-31",
            first="2025-04-01",
            last="2025-05-30",
            progress=lambda x: None,
        )
    study.claim("fold1_matched", "b" * 64)
    with pytest.raises(ValueError, match="identity"):
        study.claim("fold1_matched", "c" * 64)
    with pytest.raises(ValueError, match="window"):
        study.finance("2026-08-31", "2026-09-01", cutoff="2026-09-01")


def test_both_predictions_precede_evaluation_and_retry_does_not_refit(tmp_path, monkeypatch):
    rows = population()
    study = object.__new__(pipe.Study)
    study.root, study.digest, study.budget = tmp_path, "a" * 64, lambda: None
    calls = []

    def finance(first, last, *, cutoff, outer=False):
        calls.append((first, last))
        if first == "2025-04-01":
            assert all((tmp_path / f"fold1_{a}/trained/manifest.json").exists() for a in ("matched", "candidate"))
        return rows.loc[
            rows.decision_as_of_trade_date.between(pd.Timestamp(first), pd.Timestamp(last)), [*core.KEY, *FINANCE]
        ]

    study.finance = finance
    kwargs = dict(prefix="fold1", cutoff="2025-03-31", first="2025-04-01", last="2025-05-30", progress=lambda e: None)
    inputs = rows.drop(columns=list(FINANCE))
    targets = inputs.loc[inputs.decision_as_of_trade_date.eq("2025-04-01")].reset_index(drop=True)
    cohort, parent = pipe.fit_pair(study, None, inputs, targets, **kwargs)
    assert len(cohort) == 2 and len(calls) == 2
    monkeypatch.setattr(core, "fit", lambda *a, **k: pytest.fail("exact retry attempted refit"))
    restored, digest = pipe.fit_pair(study, None, inputs, targets, evaluate=False, **kwargs)
    assert restored is None and digest == parent and len(calls) == 2


def test_exact_six_claims_and_no_outer_or_final(tmp_path):
    study = object.__new__(pipe.Study)
    study.root, study.digest, study.budget = tmp_path, "a" * 64, lambda: None
    for fold in range(1, 4):
        for arm in ("matched", "candidate"):
            study.claim(f"fold{fold}_{arm}")
    assert len(list((tmp_path / "fit_claims").glob("*started.json"))) == 6
    with pytest.raises(ValueError, match="budget"):
        study.claim("fold1_matched")
    with pytest.raises(ValueError, match="unregistered"):
        study.claim("final_candidate")
    for options in (
        dict(first="2025-10-01", last="2025-10-02", cutoff="2025-10-02"),
        dict(first="2025-04-01", last="2025-05-30", cutoff="2025-05-30", outer=True),
    ):
        with pytest.raises(ValueError, match="window"):
            study.finance(**options)
    with pytest.raises(ValueError, match="unregistered fold"):
        pipe.fit_pair(
            study,
            None,
            None,
            None,
            prefix="final",
            cutoff="2025-09-30",
            first="2025-10-01",
            last="2026-01-01",
            progress=lambda e: None,
        )


def test_finance_values_project_maturity_before_decode(tmp_path, monkeypatch):
    study = object.__new__(pipe.Study)
    study.source = SimpleNamespace(root=tmp_path)
    rows = population()
    calls = []

    def read(path, columns=None, filters=None):
        assert path == tmp_path / "prepared/train_finance.parquet"
        calls.append((columns, filters))
        selected = rows.loc[rows.decision_as_of_trade_date.le("2025-03-31")]
        if columns:
            assert columns == [*core.KEY, "label_information_end"]
            return selected[columns]
        assert filters[-1] == ("label_information_end", "<=", pd.Timestamp("2025-03-31"))
        return selected[[*core.KEY, *FINANCE]]

    monkeypatch.setattr(pd, "read_parquet", read)
    study.finance("2024-07-04", "2025-03-31", cutoff="2025-03-31")
    assert len(calls) == 2


def test_immutable_stage_rejects_tamper(tmp_path):
    study = object.__new__(pipe.Study)
    study.root, study.digest, study.budget = tmp_path, "a" * 64, lambda: None
    digest = study.publish("fold1_candidate/trained", None, {"model.json": b"{}"})
    assert study.read("fold1_candidate/trained", None) == digest
    (tmp_path / "fold1_candidate/trained/model.json").write_bytes(b'{"changed":true}')
    with pytest.raises(AdvisoryModelFirstError, match="hash mismatch"):
        study.read("fold1_candidate/trained", None)


def test_sparse_nonconstant_training_scale_preserves_unit_invariance():
    rows = population()
    rows[core.NEW[0]] = np.where(rows.selection_effective_rank.eq(50), 1000.0, 0.0)
    a = core.fit(rows, "2025-03-31", "candidate")
    idx = a["fields"].index(core.NEW[0])
    assert a["median"][idx] == 0 and a["scale"][idx] > 1
    scaled = rows.copy()
    scaled[core.NEW[0]] *= 10000
    b = core.fit(scaled, "2025-03-31", "candidate")
    assert b["scale"][idx] == pytest.approx(a["scale"][idx] * 10000)
    np.testing.assert_allclose(
        core.predict(rows, a).predicted_relative_net_bps,
        core.predict(scaled, b).predicted_relative_net_bps,
        rtol=1e-9,
        atol=1e-8,
    )
    matched = core.fit(rows, "2025-03-31", "matched")
    for key in ("fields", "median", "scale"):
        assert a[key][: len(matched[key])] == matched[key]
    constant = rows.copy()
    constant[core.NEW[0]] = 0.0
    c = core.fit(constant, "2025-03-31", "candidate")
    assert c["scale"][c["fields"].index(core.NEW[0])] == 1


def clock():
    return pd.date_range("2024-07-04 09:30", periods=121, freq="min").append(
        pd.date_range("2024-07-04 13:01", periods=120, freq="min")
    )


def prices():
    t = np.arange(len(clock()))[:, None]
    return 100 * np.exp(np.cumsum(0.001 * np.sin(t / 3 + np.arange(50)[None, :] / 9), axis=0))


def test_leave_one_out_is_hand_computed_and_permutation_scale_invariant():
    slots, a = clock(), prices()
    got = inp.synchrony(slots, a, day="2024-07-04")
    r = np.diff(np.log(a), axis=0)
    legal = np.diff(slots.asi8) == pd.Timedelta(minutes=1).value
    y = r[legal, 0]
    peer = r[legal, 1:].mean(axis=1)
    assert got.loc[0, "sync_correlation"] == pytest.approx(np.corrcoef(y, peer)[0, 1])
    assert got.loc[0, "sync_directional_participation"] == pytest.approx(np.corrcoef(y, peer)[0, 1] * y.sum())
    assert got.loc[0, "sync_peer_log_return_bps"] == pytest.approx(peer.sum() * 10000)
    perm = np.random.default_rng(9).permutation(50)
    permuted = inp.synchrony(slots, a[:, perm], day="2024-07-04")
    np.testing.assert_allclose(permuted[list(inp.NEW)].to_numpy(), got.iloc[perm][list(inp.NEW)].to_numpy(), atol=1e-12)
    scaled = inp.synchrony(slots, a * np.arange(1, 51), day="2024-07-04")
    np.testing.assert_allclose(scaled[list(inp.NEW)], got[list(inp.NEW)], atol=1e-12)
    # Changing only the target leaves its peer aggregates unchanged.
    changed = a.copy()
    changed[:, 0] = 100 * np.exp(np.cumsum(0.003 * np.cos(np.arange(len(a)))))
    other = inp.synchrony(slots, changed, day="2024-07-04")
    np.testing.assert_allclose(
        got.loc[0, ["sync_peer_log_return_bps", "sync_peer_rv_bps"]].to_numpy(float),
        other.loc[0, ["sync_peer_log_return_bps", "sync_peer_rv_bps"]].to_numpy(float),
        atol=1e-10,
    )


def test_peer_40_common_edges_lunch_missing_and_zero_variance_unknown():
    slots, a = clock(), prices()
    a[:, 41:] = np.nan
    got = inp.synchrony(slots, a, day="2024-07-04")
    assert got.loc[0, "sync_reason"] == "AVAILABLE" and got.loc[0, "sync_min_peer_fraction"] == pytest.approx(40 / 49)
    a[:, 40] = np.nan
    assert inp.synchrony(slots, a, day="2024-07-04").sync_reason.eq("UNKNOWN_INSUFFICIENT_JOINT_EDGES").all()
    a = prices()
    a[20:90, 0] = np.nan
    assert inp.synchrony(slots, a, day="2024-07-04").loc[0, "sync_reason"] == "UNKNOWN_INSUFFICIENT_JOINT_EDGES"
    constant = inp.synchrony(slots, np.ones_like(a), day="2024-07-04")
    assert constant.sync_reason.eq("UNKNOWN_ZERO_OWN_OR_PEER_VARIANCE").all()
    assert constant[list(inp.NEW)].isna().all().all()
    valid = inp.synchrony(slots, prices(), day="2024-07-04")
    assert valid.sync_possible_edges.eq(239).all()  # 240 differences minus lunch
    removed = np.delete(np.arange(len(slots)), 50)
    assert inp.synchrony(slots[removed], prices()[removed], day="2024-07-04").sync_possible_edges.eq(237).all()


def test_known_bad_prices_and_clocks_are_errors():
    slots, a = clock(), prices()
    for bad in (np.inf, 0.0, -1.0):
        b = a.copy()
        b[0, 0] = bad
        with pytest.raises(ValueError, match="peer close"):
            inp.synchrony(slots, b, day="2024-07-04")
    for badslots in (
        slots[::-1],
        slots.tz_localize("Asia/Shanghai"),
        slots + pd.Timedelta(seconds=1),
        slots + pd.Timedelta(days=1),
    ):
        with pytest.raises(ValueError, match="clock"):
            inp.synchrony(badslots, a, day="2024-07-04")


def test_joint_dependence_is_not_old_single_paths_or_marginals():
    slots = clock()
    a = np.full(len(slots), 101.0)
    b = a.copy()
    a[47:53] = [102, 101, 103, 102, 101, 101]
    b[51:57] = [102, 101, 103, 102, 101, 101]
    a[-1] = b[-1] = 102.0

    def old(c):
        arrays = {n: (np.ones(len(c)) if n == "volume" else c.copy()) for n in inp.FIELDS}
        return inp.minute_features(slots, arrays, day="2024-07-04")

    fa, fb = old(a), old(b)
    np.testing.assert_allclose([fa[n] for n in inp.OLD], [fb[n] for n in inp.OLD], equal_nan=True, atol=1e-8)
    qa = inp.synchrony(slots, np.column_stack([a] * 50), day="2024-07-04").iloc[0]
    qb = inp.synchrony(slots, np.column_stack([a] + [b] * 49), day="2024-07-04").iloc[0]
    np.testing.assert_allclose(qa[list(inp.CONTROL)].to_numpy(float), qb[list(inp.CONTROL)].to_numpy(float), atol=1e-8)
    assert abs(qa[inp.NEW[0]] - qb[inp.NEW[0]]) > 0.1
    assert len(core.MATCHED) == 57 and len(core.FEATURES) == 59


def test_screen_alignment_rejects_value_reason_and_population_drift(monkeypatch):
    frame = population().iloc[:100].copy()
    frame[core.base.KEY[0]] = pd.Timestamp("2024-07-04")
    frame["sync_reason"] = "AVAILABLE"
    frame["sync_observed_edges"] = 239
    frame["sync_possible_edges"] = 239
    screen = frame.copy()
    monkeypatch.setattr(inp, "screen_evidence", lambda: {"original_rows": 100})
    monkeypatch.setattr(pd, "read_parquet", lambda p: screen)
    assert inp.verify_screen_alignment(frame)["exact_equal_including_nan"]
    for col, value in ((inp.NEW[0], 99.0), ("sync_reason", "UNKNOWN"), ("instrument", "foreign")):
        bad = frame.copy()
        bad.loc[0, col] = value
        with pytest.raises(ValueError, match="differ"):
            inp.verify_screen_alignment(bad)


def test_screen_evidence_requires_all_pinned_files_and_zero_labels(tmp_path, monkeypatch):
    fields = {
        "input_spec.json": "spec_sha256",
        "screen.py": "code_sha256",
        "screen_rows.parquet": "output_sha256",
        "factor_audit.json": "factor_audit_sha256",
        "synthetic_receipt.json": "synthetic_sha256",
    }
    receipt = {"decision": "INPUT_SUPPORTED_FOR_F1", "fit_count": 0, "label_reads": 0}
    for name, key in fields.items():
        path = tmp_path / name
        path.write_bytes(b"evidence")
        receipt[key] = inp.file_sha(path)
    path = tmp_path / "screen_receipt.json"
    path.write_text(json.dumps(receipt))
    monkeypatch.setattr(inp, "SCREEN_ROOT", str(tmp_path))
    monkeypatch.setattr(inp, "SCREEN_SHA", inp.file_sha(path))
    assert inp.screen_evidence() == receipt
    (tmp_path / "screen.py").write_bytes(b"drift")
    with pytest.raises(ValueError, match="drift"):
        inp.screen_evidence()


def test_file_reader_projects_only_original_d_and_rejects_lease_drift(tmp_path, monkeypatch):
    slots = clock().append(clock() + pd.Timedelta(days=1))
    calendar = tmp_path / "calendar.txt"
    calendar.write_text("\n".join(str(t) for t in slots), encoding="utf-8")
    meta = tmp_path / "meta.json"
    meta.write_text("{}", encoding="utf-8")
    rows = population().iloc[:100].copy()
    rows[core.base.KEY[0]] = pd.Timestamp("2024-07-04")
    rows.target_trade_date = pd.Timestamp("2024-07-05")
    arrays = prices()
    lease = {}
    for j, symbol in enumerate(rows.instrument.unique()):
        folder = tmp_path / "features" / symbol.lower()
        folder.mkdir(parents=True)
        for field in inp.FIELDS:
            path = folder / (field + ".1min.bin")
            known = np.ones(len(clock())) if field == "volume" else arrays[:, j]
            np.r_[0., known, np.full(len(clock()), np.inf)].astype("<f4").tofile(path)
            lease[path.relative_to(tmp_path).as_posix()] = inp.stamp(path)

    class Dataset:
        def __init__(self, *args, **kwargs):
            self.root = tmp_path.resolve()
            self.calendar = pd.to_datetime(["2024-07-04", "2024-07-05"])
            self._bin_lease = lease

        def file(self, name):
            return {"minute_calendar": calendar, "minute_meta_export": meta}[name]

        def assert_unchanged(self):
            pass

        def close(self):
            pass

    monkeypatch.setattr(inp, "FrozenDataset", Dataset)
    monkeypatch.setattr(inp, "sources", lambda *a, **k: None)
    plan = {"binding": {"binding_hash": "frozen", "files": {"minute_calendar": {}, "minute_meta_export": {}},
        "profile": {"controller_paths": {"candidate_root": str(tmp_path)}}}}
    frame, receipt = inp.read_panel(plan, rows)
    assert frame[list(core.IDENTITY)].equals(rows[list(core.IDENTITY)])
    assert frame.sync_reason.eq("AVAILABLE").all()
    assert receipt["maximum_consumed_D"] == "2024-07-04" and receipt["label_reads"] == 0
    # Passing an already prepared frame must not duplicate its feature columns.
    again, repeated = inp.read_panel(plan, frame)
    pd.testing.assert_frame_equal(frame, again)
    assert repeated["consumed_slice_sha256"] == receipt["consumed_slice_sha256"]
    first = next(iter(lease))
    lease[first] = (lease[first][0], lease[first][1] + 1)
    with pytest.raises(ValueError, match="lease"):
        inp.read_panel(plan, rows)
