"""Shared-population clock/units/mass isolation, not fake model-profit acceptance."""
from datetime import date

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import FEATURES, KEY
from backend.services.advisory_model_first.generic_population_price_5td_contracts_v1 import FrozenPopulationSourceV1, PopulationMetadataRequestV1
from backend.services.advisory_model_first.generic_population_price_5td_population_v1 import FrozenPopulationMetadataV1, build_population_inputs_v1
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1


@pytest.fixture
def inputs(tmp_path):
    calendar = [d.date() for d in pd.bdate_range("2024-07-04", "2025-10-10")]
    decisions = [*calendar[:40], date(2025, 6, 3), date(2025, 6, 4), date(2025, 9, 30)]
    ref = EvidenceReferenceV1(role="unit_source", artifact_uri=str(tmp_path/"source"), sha256="a"*64, size_bytes=1)
    sources, records = [], []
    roles = ("MATCHED_ANCHOR", "TRANSFER_TRAIN", "HELD_PACKAGE_EVALUATION")
    for i, symbols in enumerate((["600000.SH", "600001.SH"], ["600000.SH", "600002.SH"], ["600003.SH"])):
        source = FrozenPopulationSourceV1(source_id=str(i), package_id="pkg"+str(i), manifest_sha256=str(i)*64,
            candidate_format="GP5_LEGACY_TOP20" if i == 0 else "FROZEN_ARM_TOP50", candidate_ref=ref,
            decision_dates=decisions, arm_id=None if i == 0 else str(i), lineage_ref=None if i == 0 else ref,
            bundle_manifest_ref=None if i == 0 else ref)
        sources.append(source)
        for d in decisions:
            p = calendar.index(d)
            for rank, symbol in enumerate(symbols, 1):
                records.append(dict(zip(KEY, (pd.Timestamp(d), pd.Timestamp(calendar[p+1]), symbol), strict=True)) |
                    dict(selection_effective_rank=rank, candidate_group_size=len(symbols), source_id=str(i),
                         population_role=roles[i], label_information_end=pd.Timestamp(calendar[p+5]),
                         maturity_at_cutoff="UNSETTLED_CUTOFF" if calendar[p+5] > date(2025, 9, 30) else "MATURE_BY_CALENDAR_ONLY"))
    roster = pd.DataFrame(records)
    clusters = []
    for key, group in roster.groupby(list(KEY), sort=True):
        clusters.append(dict(zip(KEY, key, strict=True)) | dict(source_ids=tuple(group.source_id), cluster_mass=1,
            supervision_ready=False, potential_anchor_train=group.source_id.eq("0").any(),
            potential_transfer_train=group.source_id.isin(["0", "1"]).any()))
    daily = pd.DataFrame([dict(trade_date=pd.Timestamp(d), instrument=symbol, raw_open_cny=100.5,
        raw_high_cny=103., raw_low_cny=96., raw_close_cny=100., adj_factor=1., volume_hand=10.,
        up_limit=110., down_limit=90., suspended=False, tradability_unknown=False, price_placeholder_fields="")
        for d in calendar if d <= date(2025, 9, 30) for symbol in ("600000.SH", "600001.SH", "600002.SH", "600003.SH")])
    index = pd.DataFrame(dict(trade_date=pd.to_datetime([d for d in calendar if d <= date(2025, 9, 30)]),
                              instrument="000300.SH", close=1000.))
    return dict(metadata=FrozenPopulationMetadataV1(roster, pd.DataFrame(), pd.DataFrame(clusters), {}),
        request=PopulationMetadataRequestV1(sources=sources, calendar_ref=ref), calendar=calendar, daily=daily,
        index_daily=index, source_context=dict(calendar_sha256="a"*64, prices_sha256="b"*64,
            references_sha256="b"*64, label_price_basis="D_REFERENCE_POLICY_RATIO", source_evidence="CURRENT_DATABASE_NON_VINTAGE"))


def test_original_rosters_one_cluster_mass_horizon_units_and_unknown_holes(inputs):
    halted = (inputs["daily"].trade_date.eq(pd.Timestamp(inputs["calendar"][31])) & inputs["daily"].instrument.eq("600001.SH"))
    inputs["daily"].loc[halted, "suspended"] = True
    rows, clusters, recipe = build_population_inputs_v1(**inputs)
    assert len(rows) == len(inputs["metadata"].rosters) and len(clusters) == len(inputs["metadata"].clusters)
    assert clusters.cluster_mass.eq(1).all() and rows.market_up_ratio.isna().all()
    assert rows.loc[rows[KEY[0]].eq(pd.Timestamp(inputs["calendar"][0])), list(FEATURES)].isna().all().all()
    boundary = rows[KEY[0]].eq(pd.Timestamp("2025-09-30"))
    assert rows.loc[boundary, "maturity_at_cutoff"].eq("UNSETTLED_CUTOFF").all()
    assert rows.loc[boundary, "label_status"].eq("IMMATURE").all()
    assert rows.loc[boundary, "label_reason"].eq("HORIZON_BEYOND_SOURCE_CUTOFF").all()
    assert not clusters.loc[clusters[KEY[0]].eq(pd.Timestamp("2025-09-30")), "supervision_ready"].any()
    assert "ENTRY_NOT_EXECUTABLE" in set(rows.label_status) and "UNKNOWN" in set(rows.label_status)
    valid = rows.loc[rows.label_status.eq("AVAILABLE")]
    assert np.allclose(valid.observed_gap_bps, 50.) and valid.gross_terminal_ratio.eq(1.).all()
    assert valid.path_min_ratio.eq(.96).all() and recipe["physical_fit_count"] == 0
    for name in ("matched_anchor", "candidate_transfer"):
        structure = clusters.loc[clusters[name+"_pool"].eq("STRUCTURE")]
        assert structure.label_information_end.lt(pd.Timestamp(recipe["estimation_first_D"])).all()
    held_only = clusters.source_ids.map(lambda ids: ids == ("2",))
    assert clusters.loc[held_only, "candidate_transfer_pool"].eq("NOT_TRAIN_SUPERVISION").all()
    assert recipe["eligible_new_training_clusters"] > 0 and len(recipe["matrix_order"]) == 19


def test_held_and_evaluation_price_poison_cannot_change_training_encoding(inputs):
    _, _, before = build_population_inputs_v1(**inputs)
    changed = inputs["daily"].instrument.eq("600003.SH") | inputs["daily"].trade_date.ge(pd.Timestamp("2025-06-03"))
    for name in ("raw_open_cny", "raw_high_cny", "raw_low_cny", "raw_close_cny", "up_limit", "down_limit"):
        inputs["daily"].loc[changed, name] *= 10
    inputs["daily"].loc[changed, "volume_hand"] *= 99
    _, _, after = build_population_inputs_v1(**inputs)
    assert before == after


def test_new_clusters_only_in_purged_days_do_not_fake_a_training_contrast(inputs):
    from backend.services.advisory_model_first.generic_price_5td_models_v1 import _support, feature_values
    from backend.services.advisory_model_first.generic_population_price_5td_contracts_v1 import MATRIX_ORDER
    from backend.services.advisory_model_first.generic_population_price_5td_population_v1 import _population_encoding_v1
    _, clusters, _ = build_population_inputs_v1(**inputs)
    purged_extra = (~clusters.potential_anchor_train & clusters.candidate_transfer_pool.eq("PURGED_LABEL_OVERLAP"))
    assert purged_extra.any()
    clusters["potential_transfer_train"] = clusters.potential_anchor_train | purged_extra
    recipe = _population_encoding_v1(clusters, inputs["request"], MATRIX_ORDER, _support, feature_values)
    assert recipe["eligible_new_training_clusters"] == 0
    assert recipe["eligible_new_training_clusters_before_purge"] > 0
    assert recipe["population_contrast_status"] == "NOT_TESTABLE_POPULATION_CONTRAST"


@pytest.mark.parametrize("fault", ["outside_cutoff", "wrong_horizon", "duplicate_quote"])
def test_clock_or_common_quote_contradictions_are_not_silently_repaired(inputs, fault):
    if fault == "outside_cutoff":
        row = inputs["daily"].iloc[[0]].copy()
        row["trade_date"] = pd.Timestamp("2025-10-09")
        inputs["daily"] = pd.concat([inputs["daily"], row], ignore_index=True)
    elif fault == "wrong_horizon":
        inputs["metadata"].rosters.loc[0, "label_information_end"] += pd.Timedelta(days=1)
    else:
        inputs["daily"] = pd.concat([inputs["daily"], inputs["daily"].iloc[[0]]], ignore_index=True)
    with pytest.raises(ValueError, match="cutoff|horizon|schema/keys"):
        build_population_inputs_v1(**inputs)


def test_empty_original_population_returns_precise_no_fit_not_a_fake_model(inputs):
    inputs["metadata"].rosters = inputs["metadata"].rosters.iloc[:0].copy()
    inputs["metadata"].clusters = inputs["metadata"].clusters.iloc[:0].copy()
    inputs["daily"] = inputs["daily"].iloc[:0].copy()
    rows, clusters, recipe = build_population_inputs_v1(**inputs)
    assert rows.empty and clusters.empty
    assert recipe["population_contrast_status"] == "NOT_TESTABLE_POPULATION_CONTRAST"
    assert recipe["physical_fit_count"] == 0 and not recipe["research_run_created"]


def test_snapshot_is_transactional_readonly_and_retains_suspend_and_zero_placeholders():
    from contextlib import contextmanager
    from backend.services.advisory_model_first.generic_population_price_5td_pipeline_v1 import _read_database
    day = date(2024, 7, 4)
    class Cursor:
        statements = []
        def execute(self, sql, params):
            self.statements.append(sql)
        def fetchall(self):
            sql = self.statements[-1]
            if "trading_calendar" in sql:
                return [(day,)]
            if "kline_daily_raw" in sql:
                return [(day, "600000.SH", 0, 0, 0, 0, 1., 0., 110., 90.)]
            if "suspend_d" in sql:
                return [(day, "600000.SH", "S", ""), (day, "600001.SH", "S", "")]
            return [(day, "000300.SH", 1000.)]
        def close(self):
            self.closed = True
    class Connection:
        rollbacks = 0
        def cursor(self):
            return cursor
        def set_session(self, **settings):
            assert settings == {"isolation_level": "REPEATABLE READ", "readonly": True, "autocommit": False}
        def rollback(self):
            self.rollbacks += 1
    cursor, connection = Cursor(), Connection()
    @contextmanager
    def factory():
        yield connection
    daily, _, receipt = _read_database(symbols=["600000.SH", "600001.SH"], calendar=[day], cutoff=day,
                                       connection_context_factory=factory)
    assert len(daily) == 2 and daily.suspended.all() and daily.raw_close_cny.isna().all()
    assert daily.loc[daily.instrument.eq("600000.SH"), "price_placeholder_fields"].iloc[0] == "open;high;low;close;"
    assert connection.rollbacks == 1 and cursor.closed
    assert all(s.lstrip().upper().startswith(("SELECT", "SET LOCAL")) for s in cursor.statements)
    assert not receipt["database_written"] and not receipt["native_capture"]


@pytest.fixture
def study(inputs, tmp_path):
    import json
    from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage
    from backend.services.advisory_model_first.generic_population_price_5td_contracts_v1 import PopulationInputPlanV1, PopulationStudyPlanV1
    from backend.services.advisory_model_first.generic_population_price_5td_pipeline_v1 import _parquet, study_implementation_sha256
    from backend.services.advisory_model_first.research_control import evidence_reference_for_file
    calendar = tmp_path/"calendar.json"
    calendar.write_bytes(_json_bytes([d.isoformat() for d in inputs["calendar"]]))
    calref = evidence_reference_for_file(calendar, role="original_calendar")
    request = inputs["request"].model_copy(update=dict(calendar_ref=calref))
    inputs["request"] = request
    rows, clusters, encoding = build_population_inputs_v1(**inputs)
    rows["package_id"] = rows.source_id.map({s.source_id: s.package_id for s in request.sources})
    rows["manifest_sha256"] = rows.source_id.map({s.source_id: s.manifest_sha256 for s in request.sources})
    days = rows.groupby(["source_id", KEY[0]], as_index=False).size().rename(columns={"size": "candidate_count"})
    days["roster_status"] = "PRESENT"
    ref = request.sources[0].candidate_ref
    parent = PopulationInputPlanV1(metadata_request=request, daily_ref=ref, index_ref=ref,
                                  source_receipt_ref=ref, preparation_implementation_sha256="a"*64)
    root = tmp_path/parent.component_id
    first = publish_stage(study_root=root, stage="preregistered", plan_sha256=parent.plan_sha256, parent_sha256=None,
                          artifacts={"plan.json": _json_bytes(parent.model_dump(mode="json"))})
    first_sha = json.loads((first/"manifest.json").read_bytes())["stage_sha256"]
    publish_stage(study_root=root, stage="prepared", plan_sha256=parent.plan_sha256, parent_sha256=first_sha,
        artifacts={"clusters.parquet": _parquet(clusters), "encoding.json": _json_bytes(encoding),
                   "rows.parquet": _parquet(rows), "days.parquet": _parquet(days),
                   "receipt.json": _json_bytes(dict(plan_sha256=parent.plan_sha256, original_rows=len(rows), unique_clusters=len(clusters)))})
    plan = PopulationStudyPlanV1(input_root_uri=str(root),
        input_plan_file_sha256=evidence_reference_for_file(first/"plan.json", role="plan").sha256,
        input_manifest_file_sha256=evidence_reference_for_file(root/"prepared"/"manifest.json", role="inputs").sha256,
        calendar_ref=calref, implementation_sha256=study_implementation_sha256(), source_commit="a"*40,
        node_python_uri="/existing/python", qe_api_base="http://127.0.0.1:8001/api/v1")
    return plan, tmp_path


def _idle():
    from datetime import datetime, timezone
    return dict(captured_at=datetime.now(timezone.utc).isoformat(), active_counts=dict(single=0, custom_evo=0, multi_alpha=0))


def test_registered_two_arm_fixture_fit_idempotence_and_journal(study, monkeypatch):
    import json
    from backend.services.advisory_model_first import generic_population_price_5td_pipeline_v1 as p
    plan, output = study
    p.preregister_population_study_v1(plan=plan, output_root=output)
    prepared = p.prepare_population_study_v1(plan=plan, output_root=output)
    monkeypatch.setattr(p, "_node_observation", lambda _: dict(execution_node="SYNTHETIC_UNIT_TEST"))
    calls = []
    def probe():
        calls.append(1)
        return _idle()
    trained = p.train_population_study_v1(plan=plan, output_root=output, qe_idle_probe=probe)
    assert len(calls) == 4 and json.loads((trained/"fit_receipt.json").read_bytes())["physical_fit_count"] == 2
    p.train_population_study_v1(plan=plan, output_root=output, qe_idle_probe=lambda: pytest.fail("cached stage must not fit"))
    journal = [json.loads(line) for line in (prepared.parent/"fit_journal.jsonl").read_text().splitlines()]
    assert [v["arm"] for v in journal if v["kind"] == "PHYSICAL_FIT_COMPLETED"] == ["matched_anchor", "candidate_transfer"]
    registry = [json.loads(line) for line in (output/"trial_registry.jsonl").read_text().splitlines()]
    assert max(r["generated_trial_count"] for r in registry) == 2
    assert {r["objective_contract"] for r in registry} == {"RISK_MANAGED_ADVISORY"}
    from backend.services.advisory_model_first import generic_population_price_5td_model_v1 as model
    original_query = model.query_population_price_nodes_v1
    def query(**kw):
        assert list(kw["features"].columns) == [*KEY, *FEATURES]
        return original_query(**kw)
    monkeypatch.setattr(model, "query_population_price_nodes_v1", query)
    evaluated = p.evaluate_population_study_v1(plan=plan, output_root=output)
    result = json.loads((evaluated/"evaluation.json").read_bytes())
    assert result["status"].startswith("NEGATIVE")
    assert result["source_summaries"]["0"]["paired"]["baseline"]["confidence_interval_95_bps"] is None
    p.evaluate_population_study_v1(plan=plan, output_root=output)
    records = [json.loads(line) for line in (output/"trial_registry.jsonl").read_text().splitlines()]
    assert [r["result_class"] for r in records if r["research_stage"] == "EVALUATED"] == ["NEGATIVE"]


def test_failed_started_attempt_preserved_without_implicit_refit(study, monkeypatch):
    from backend.services.advisory_model_first import generic_population_price_5td_pipeline_v1 as p
    from backend.services.advisory_model_first import generic_population_price_5td_model_v1 as model
    plan, output = study
    p.preregister_population_study_v1(plan=plan, output_root=output)
    prepared = p.prepare_population_study_v1(plan=plan, output_root=output)
    monkeypatch.setattr(p, "_node_observation", lambda _: {})
    def failure(**kw):
        kw["before_fit"](kw["arm"])
        raise RuntimeError("synthetic fit failure")
    monkeypatch.setattr(model, "train_population_price_5td_v1", failure)
    with pytest.raises(RuntimeError, match="synthetic fit failure"):
        p.train_population_study_v1(plan=plan, output_root=output, qe_idle_probe=_idle)
    assert (prepared.parent/"fit_failure.json").is_file()
    with pytest.raises(ValueError, match="never implicitly refit"):
        p.train_population_study_v1(plan=plan, output_root=output, qe_idle_probe=_idle)


def test_windows_boundary_busy_and_stale_qe_never_start(study, monkeypatch):
    from backend.services.advisory_model_first import generic_population_price_5td_pipeline_v1 as p
    plan, output = study
    p.preregister_population_study_v1(plan=plan, output_root=output)
    prepared = p.prepare_population_study_v1(plan=plan, output_root=output)
    with pytest.raises(ValueError, match="explicit existing WSL/worker Python"):
        p.train_population_study_v1(plan=plan, output_root=output, qe_idle_probe=_idle)
    monkeypatch.setattr(p, "_node_observation", lambda _: {})
    for observed in (_idle() | dict(captured_at="2020-01-01T00:00:00+00:00"),
                     _idle() | dict(active_counts=dict(single=0, custom_evo=1, multi_alpha=0))):
        with pytest.raises(ValueError, match="stale|idle QE"):
            p.train_population_study_v1(plan=plan, output_root=output, qe_idle_probe=lambda: observed)
    assert not (prepared.parent/"fit_attempt.json").exists()


def test_public_qe_probe_covers_six_gets_and_rejects_unknown():
    from backend.services.advisory_model_first.generic_population_price_5td_cli_v1 import public_qe_observation_v1
    calls = []
    def get(path, params):
        calls.append((path, params["status"]))
        if path.endswith("experiments"):
            return dict(ok=True, total=0, items=[], has_more=False)
        if path.endswith("tasks"):
            return dict(status="success", data=[])
        return dict(status="success", data=dict(count=0, runs=[]))
    assert not any(public_qe_observation_v1("http://127.0.0.1:8001/api/v1", get=get)["active_counts"].values())
    assert len(calls) == 6 and {s for _, s in calls} == {"running", "pending"}
    with pytest.raises(ValueError, match="incomplete"):
        public_qe_observation_v1("http://127.0.0.1:8001/api/v1", get=lambda *a, **kw: {})


def test_input_hash_change_and_qe_started_after_fit_preserve_boundaries(study, monkeypatch):
    import json
    from pathlib import Path
    from backend.services.advisory_model_first import generic_population_price_5td_pipeline_v1 as p
    plan, output = study
    p.preregister_population_study_v1(plan=plan, output_root=output)
    prepared = p.prepare_population_study_v1(plan=plan, output_root=output)
    monkeypatch.setattr(p, "_node_observation", lambda _: {})
    observations = iter([_idle(), _idle() | dict(active_counts=dict(single=1, custom_evo=0, multi_alpha=0))])
    with pytest.raises(ValueError, match="QE started during this fit"):
        p.train_population_study_v1(plan=plan, output_root=output, qe_idle_probe=lambda: next(observations))
    failure = json.loads((prepared.parent/"fit_failure.json").read_bytes())
    assert failure["completed_arms"] == ["matched_anchor"] and failure["started_fit_count"] == 1
    assert (prepared.parent/"fits"/"matched_anchor"/"trained"/"model.json").is_file()
    assert not (prepared.parent/"fits"/"candidate_transfer").exists()
    source_file = Path(plan.input_root_uri)/"prepared"/"encoding.json"
    source_file.write_bytes(b"{}")
    from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
    with pytest.raises(AdvisoryModelFirstError, match="artifact hash mismatch"):
        p.train_population_study_v1(plan=plan, output_root=output, qe_idle_probe=lambda: pytest.fail("changed source must not fit"))
