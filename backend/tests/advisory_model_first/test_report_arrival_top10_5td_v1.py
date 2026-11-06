"""Direct arrival presence, sparse-slot ranking, maturity and immutable study contracts."""

import json
import os
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first import report_arrival_top10_5td_v1 as core
from backend.services.advisory_model_first import report_arrival_top10_5td_pipeline_v1 as pipe
from backend.services.advisory_model_first import report_arrival_top10_5td_inputs_v1 as inp
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


def test_all_unknown_keeps_exact_original_slots_and_ties_are_native():
    rows = population()
    packet = core.fit(rows, "2025-03-31", "candidate")
    unknown = rows.copy()
    unknown[core.NEW[0]] = np.nan
    prediction = core.predict(unknown, packet)
    assert prediction.no_rank_advice.all()
    assert prediction.advisory_rank.equals(rows.selection_effective_rank)
    assert prediction.groupby(core.GROUP).selected.sum().eq(10).all()
    packet["coef"] = [0.0] * len(packet["coef"])
    tied = core.predict(rows, packet)
    assert tied.advisory_rank.equals(rows.selection_effective_rank)
    assert len(core.MATCHED) == 8 and len(core.FEATURES) == 9


def test_source_leases_and_full_content_detect_same_stat_tamper(tmp_path, monkeypatch):
    paths = {name: tmp_path / (name + ".data") for name in ("population", "panel", "manifest")}
    for path in paths.values():
        path.write_bytes(b"original")
    contract = dict(
        source_stamps={
            name: dict(path=str(paths[name]), size=8, mtime_ns=paths[name].stat().st_mtime_ns)
            for name in ("population", "panel")
        },
        manifest_path=str(paths["manifest"]),
        manifest_sha256=inp.file_sha(paths["manifest"]),
        panel_sha256=inp.file_sha(paths["panel"]),
    )
    (tmp_path / "inventory_contract.json").write_text(json.dumps(contract), encoding="utf-8")
    monkeypatch.setattr(inp, "SCREEN_ROOT", str(tmp_path))
    monkeypatch.setattr(inp, "screen_evidence", lambda: None)
    plan = {"binding": {"binding_hash": inp.BINDING}}
    _, _, frozen = inp.sources(plan)
    paths["panel"].write_bytes(b"tampered")
    with pytest.raises(ValueError, match="lease drift"):
        inp.sources(plan, frozen)
    os.utime(paths["panel"], ns=(frozen["panel"][1], frozen["panel"][1]))
    with pytest.raises(ValueError, match="content drift"):
        inp.sources(plan, frozen)


def test_missing_finance_identity_cannot_silently_shrink_training(tmp_path):
    study = object.__new__(pipe.Study)
    study.root, study.digest, study.budget = tmp_path, "a" * 64, lambda: None
    rows = population()
    study.finance = lambda *a, **k: rows.iloc[1:][[*core.KEY, *FINANCE]]
    inputs = rows.drop(columns=list(FINANCE))
    targets = inputs.loc[inputs.decision_as_of_trade_date.eq("2025-04-01")].reset_index(drop=True)
    with pytest.raises(ValueError, match="finance population differs"):
        pipe.fit_pair(
            study,
            None,
            inputs,
            targets,
            prefix="fold1",
            cutoff="2025-03-31",
            first="2025-04-01",
            last="2025-05-30",
            progress=lambda e: None,
        )
    assert not (tmp_path / "fit_claims").exists()


def row(date, *, org="literal_A", update=None, ordinal=0, **extra):
    return dict(
        ts_code="000001.SZ",
        org_key=org,
        report_date=pd.Timestamp(date),
        provider_create_time=pd.Timestamp(date if update is None else update),
        captured_at=pd.Timestamp("2026-10-23", tz="UTC"),
        capture_id="synthetic",
        row_ordinal=ordinal,
        source_row_sha256="a" * 64,
        vintage_status="NON_VINTAGE",
        **extra,
    )


def context(records, day="2024-07-10"):
    panel = pd.DataFrame(records, columns=[*inp.PANEL_COLUMNS, "quarter", "eps", "rating", "report_title"])
    events, invalid = inp.build_events(panel)
    return inp.stock_context(events, invalid, day)


def baseline():
    return [row("2024-06-01"), row("2024-07-01", ordinal=1)]


def test_org_day_caps_forecast_period_title_and_ignores_numeric_content():
    original = context(baseline())
    extra = row("2024-07-01", ordinal=2, quarter="2030Q4", eps=np.inf, rating="conflict", report_title="other")
    changed = context([*baseline(), extra])
    for key in (*inp.NEW, *inp.COUNTS, *inp.EXACT):
        assert original[key] == changed[key]
    assert changed["recent_snapshot_source_rows_diagnostic_only"] == 2
    assert changed[inp.NEW[0]] == pytest.approx(np.log(2) - np.log(4 / 3))


def test_literal_alias_not_collapsed_and_report_day_changes_information():
    one = context(baseline())
    alias = context([*baseline(), row("2024-07-01", org="literal_A_alias", ordinal=2)])
    another_day = context([*baseline(), row("2024-07-02", ordinal=2)])
    assert alias["recent_org_days"] == alias["recent_literal_org_count"] == 2
    assert another_day["recent_org_days"] == 2 and another_day["recent_literal_org_count"] == 1
    assert one[inp.NEW[0]] != another_day[inp.NEW[0]]


def test_future_update_and_invisible_appends_cannot_rewrite_previous_context():
    prior = context(baseline())
    later = context(
        [
            *baseline(),
            row("2024-07-01", update="2025-01-01", ordinal=2),
            row("2024-07-02", update="2025-01-01", ordinal=3),
        ]
    )
    for key in (*inp.NEW, *inp.COUNTS, *inp.EXACT):
        assert prior[key] == later[key]
    assert later["recent_snapshot_source_rows_diagnostic_only"] == 2
    late_old = context([row("2024-03-01", update="2024-07-01"), row("2024-07-01", ordinal=1)])
    assert late_old["baseline_org_days"] == 0  # Update date is not report date.


@pytest.mark.parametrize("field", ["org_key", "report_date", "provider_create_time"])
def test_unknown_identity_or_clock_taints_relevant_stock(field):
    records = baseline()
    records[1][field] = None
    result = context(records)
    assert result["input_status"] == "UNKNOWN_SOURCE_IDENTITY_OR_CLOCK"
    assert np.isnan(result[inp.NEW[0]])
    # Known stale report with missing clock is outside the relevant window.
    stale = row("2024-01-01", ordinal=2)
    stale["provider_create_time"] = None
    assert np.isfinite(context([*baseline(), stale])[inp.NEW[0]])


@pytest.mark.parametrize(
    "field,value",
    [("ts_code", None), ("ts_code", "bad"), ("capture_id", ""), ("row_ordinal", -1), ("source_row_sha256", "bad")],
)
def test_unassignable_stock_or_broken_occurrence_rejected(field, value):
    records = baseline()
    records[1][field] = value
    with pytest.raises(ValueError, match="identity|reference"):
        context(records)


def test_duplicate_occurrence_rejected_even_with_conflicting_identity():
    records = baseline()
    records[1]["row_ordinal"] = 0
    with pytest.raises(ValueError, match="duplicate"):
        context(records)


def test_exact_calendar_boundaries_disjoint_windows_and_next_day_availability():
    d = pd.Timestamp("2024-07-10")
    records = [row(d - pd.Timedelta(days=n), ordinal=i) for i, n in enumerate([0, 1, 29, 30, 119, 120])]
    value = context(records)
    assert value["recent_org_days"] == 2 and value["baseline_org_days"] == 2
    assert not set(json.loads(value["recent_event_ids_json"])) & set(json.loads(value["baseline_event_ids_json"]))
    records = baseline()
    records[1]["provider_create_time"] = d
    assert context(records)["input_status"] == "NO_RECENT_OBSERVED_ORG_DAY_UNKNOWN"
    records[1]["provider_create_time"] = d - pd.Timedelta(days=1)
    assert np.isfinite(context(records)[inp.NEW[0]])


def test_absence_is_unknown_not_numeric_zero():
    for records, reason in [([], "NO_RECENT"), ([baseline()[0]], "NO_RECENT"), ([baseline()[1]], "NO_BASELINE")]:
        value = context(records)
        assert value["input_status"].startswith(reason) and np.isnan(value[inp.NEW[0]])


def test_screen_alignment_rejects_signal_count_unknown_event_and_population_drift(monkeypatch):
    frame = population().iloc[:100].copy().reset_index(drop=True)
    for name in inp.COUNTS:
        frame[name] = 1
    frame["input_status"] = "AVAILABLE_NON_VINTAGE_ARRIVAL_PROXY"
    frame["recent_event_ids_json"], frame["baseline_event_ids_json"] = '["a"]', '["b"]'
    screen = frame.assign(inside_development=True)
    monkeypatch.setattr(inp, "screen_evidence", lambda: {"development_rows": 100})
    monkeypatch.setattr(pd, "read_parquet", lambda p, **k: screen)
    assert inp.verify_screen_alignment(frame)["maximum_absolute_error"] == 0
    for col, value in [
        (inp.NEW[0], 99.0),
        ("recent_org_days", 99),
        ("input_status", "UNKNOWN"),
        ("recent_event_ids_json", "changed"),
        ("instrument", "foreign"),
    ]:
        bad = frame.copy()
        bad.loc[0, col] = value
        with pytest.raises(ValueError, match="differ"):
            inp.verify_screen_alignment(bad)


def test_file_reader_projects_metadata_preserves_population_and_recovery(tmp_path, monkeypatch):
    frame = population().iloc[:100].copy()
    frame[core.base.KEY[0]], frame.target_trade_date = pd.Timestamp("2024-07-10"), pd.Timestamp("2024-07-11")
    panel = pd.DataFrame(baseline())
    source = tmp_path / "panel.parquet"
    panel.to_parquet(source, index=False)
    contract = dict(development_start="2024-07-04", development_end="2025-09-30", expected_panel_rows=2)
    (tmp_path / "inventory_contract.json").write_text(json.dumps(contract), encoding="utf-8")
    monkeypatch.setattr(inp, "SCREEN_ROOT", str(tmp_path))
    monkeypatch.setattr(
        inp, "sources", lambda *a, **k: ({"panel": source}, {"panel": "frozen"}, {"panel": inp.stat(source)})
    )
    real_read = pd.read_parquet

    def read(path, *, columns):
        assert columns == list(inp.PANEL_COLUMNS)
        return real_read(path, columns=columns)

    monkeypatch.setattr(pd, "read_parquet", read)
    measured, receipt = inp.read_panel({}, frame)
    known = measured.instrument.eq("000001.SZ")
    np.testing.assert_allclose(measured.loc[known, inp.NEW[0]], np.log(2) - np.log(4 / 3))
    assert measured.loc[~known, inp.NEW[0]].isna().all()
    assert measured[list(core.IDENTITY)].equals(frame[list(core.IDENTITY)])
    again, second = inp.read_panel({}, measured)
    pd.testing.assert_frame_equal(measured, again)
    assert receipt == second and receipt["label_reads"] == 0 and receipt["strict_capture_known_historical_rows"] == 0
    with pytest.raises(ValueError, match="outside"):
        inp.read_panel(
            {},
            frame.assign(
                decision_as_of_trade_date=pd.Timestamp("2026-01-01"), target_trade_date=pd.Timestamp("2026-01-02")
            ),
        )


def test_supported_evidence_and_dependent_content_are_pinned(tmp_path, monkeypatch):
    screen = dict(
        support={"decision": "SUPPORTS_ONE_FUTURE_F1_PROPOSAL_ONLY"},
        new_fits=0,
        outcome_labels_read=False,
        source_leases_unchanged=True,
        historical_pit_established=False,
    )
    for name, key in [
        ("inventory_rows.parquet", "rows_sha256"),
        ("event_ledger.parquet", "events_sha256"),
        ("inventory.py", "code_sha256"),
    ]:
        (tmp_path / name).write_bytes(b"fixed evidence")
        screen[key] = inp.file_sha(tmp_path / name)
    (tmp_path / "audit.py").write_bytes(b"audit")
    audit = dict(
        status="PASS", outcome_labels_read=False, new_fits=0, audit_code_sha256=inp.file_sha(tmp_path / "audit.py")
    )
    for name, value in [
        ("inventory_receipt.json", screen),
        ("audit_receipt.json", audit),
        ("input_validation_receipt.json", {"passed": 13, "tests": [{"passed": True}] * 13}),
        ("inventory_contract.json", {}),
        ("delivery_receipt.json", {}),
    ]:
        (tmp_path / name).write_text(json.dumps(value), encoding="utf-8")
    monkeypatch.setattr(inp, "SCREEN_ROOT", str(tmp_path))
    monkeypatch.setattr(inp, "EVIDENCE_PINS", {name: inp.file_sha(tmp_path / name) for name in inp.EVIDENCE_PINS})
    assert inp.screen_evidence() == screen
    (tmp_path / "inventory_rows.parquet").write_bytes(b"drift")
    with pytest.raises(ValueError, match="drift"):
        inp.screen_evidence()
