"""File-only explanation of frozen LSTM ranks; no fitting or live outcome source.

Contract: docs/architecture/advisory_lstm_rank_value_review_v1_f1_design_20261101.md.
The established predictor and immutable publisher remain the authority.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from backend.services.advisory_model_first import lstm_specialist_spo_v1 as core  # noqa: E402
from backend.services.advisory_model_first import lstm_spo_shadow_v1 as shadow  # noqa: E402
from backend.services.advisory_model_first.economic_entry_pipeline import publish_stage, read_stage  # noqa: E402

SCHEMA = "advisory_lstm_rank_value_review_v1"
ARCHIVE = Path("X:/AIstock_temp/ADVISORY/p19-lstm-specialist-20261027")
MODELS = Path("F:/Dev/AIstock_model_artifacts/advisory_model_first/lstm_spo_shadow_v1")
DAY, KEY, GROUP = core.DAY, list(core.KEY), list(core.spo.GROUP)
MARKET = tuple(core.spo.FEATURES[:6])
TERMINAL_SHA = "7edc47478d6c20dfe9ad1d0cf0010e6b3fd4d5da5dd1c88f7dd6761dab6c21ee"
CURRENT_SHA = "224d0203a0bb5c09db908f65b3e5fcc00845fbaff050f0865bf4bdce32ad8e14"
QUANTILES = (0., .05, .25, .5, .75, .95, 1.)


def source_spec():
    """Exact role/path pairs; an inventory cannot relabel current data as history."""
    study = ARCHIVE / "recovery-v2/study"
    spec = {
        "archive_protocol": (ARCHIVE / "recovery-v2/protocol.json", "ARCHIVE_PROVENANCE"),
        "archive_run": (ARCHIVE / "recovery-v2/run_receipt.json", "ARCHIVE_PROVENANCE"),
        "archive_audit": (ARCHIVE / "research_audit_receipt.json", "ARCHIVE_PROVENANCE"),
        "inputs": (study / "prepared/inputs.parquet", "DEVELOPMENT_FEATURES"),
        "current": (MODELS / "days/2026-10-30/prediction.json", "CURRENT_INPUT_NO_OUTCOMES"),
        "live_model": (MODELS / "model.json", "CURRENT_MODEL"),
        "release": (MODELS / "release.json", "CURRENT_RELEASE"),
    }
    for fold in (1, 2, 3):
        for prefix, filename, role in (
            ("model", "model.json", "FROZEN_FOLD_MODEL"),
            ("prediction", "predictions.parquet", "HELDOUT_PREDICTIONS"),
            ("shared", "shared_predictions.parquet", "SHARED_CONTROL"),
            ("train", "train_finance.parquet", "OLD_TRAINING_ELIGIBILITY"),
        ):
            spec[f"{prefix}{fold}"] = (study / f"fold{fold}/trained/{filename}", role)
    for name in ("panel", "daily", "finance", "report"):
        suffix = "json" if name == "report" else "parquet"
        spec[name] = (study / f"evaluated/{name}.{suffix}", "OLD_EVALUATED_DEVELOPMENT")
    return spec


def file_sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def json_bytes(value):
    def clean(x):
        if isinstance(x, dict):
            return {str(k): clean(v) for k, v in x.items()}
        if isinstance(x, (tuple, list)):
            return [clean(v) for v in x]
        if isinstance(x, (pd.Timestamp, Path)):
            return str(x)
        if isinstance(x, (float, np.floating)):
            if np.isinf(x):
                raise ValueError("infinite diagnostic result")
            return None if np.isnan(x) else float(x)
        if isinstance(x, (np.integer, np.bool_)):
            return x.item()
        return x
    return (json.dumps(clean(value), ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()


def parquet_bytes(frame):
    stream = io.BytesIO()
    frame.to_parquet(stream, index=False)
    return stream.getvalue()


def load_json(path):
    return json.loads(Path(path).read_bytes())


def checked_ref(protocol, name):
    path, role = source_spec()[name]
    ref = protocol["sources"][name]
    if ref["role"] != role or Path(ref["path"]).resolve() != path.resolve():
        raise ValueError(f"source role/path differs: {name}")
    if path.stat().st_size != ref["size_bytes"] or file_sha(path) != ref["sha256"]:
        raise ValueError(f"source bytes changed: {name}")
    return path


def verify_sources(protocol):
    if (protocol.get("schema") != SCHEMA or protocol.get("target") != core.LSTM
            or set(protocol.get("sources", {})) != set(source_spec())
            or protocol.get("max_new_fits") != 0
            or protocol.get("current_decision_date") != "2026-10-30"
            or protocol.get("script_sha256") != file_sha(__file__)):
        raise ValueError("diagnostic protocol identity differs")
    for name in source_spec():
        checked_ref(protocol, name)


def read_frame(protocol, name, *, columns=None, filters=None):
    # Only these named historical parquet roles are allowed; never arbitrary paths.
    if name not in {"inputs", "panel", "daily", "finance", *(
        f"{prefix}{fold}" for prefix in ("prediction", "shared", "train") for fold in (1, 2, 3)
    )}:
        raise ValueError("role has no historical parquet/finance access")
    return pd.read_parquet(checked_ref(protocol, name), columns=columns, filters=filters)


def output_root(path):
    path = Path(path).resolve()
    if (path.drive.upper() == "C:" or path == ROOT or ROOT in path.parents
            or path == ARCHIVE.resolve() or ARCHIVE.resolve() in path.parents
            or path == MODELS.resolve() or MODELS.resolve() in path.parents):
        raise ValueError("output must be separate from code, old studies and live models")
    return path


def freeze(inventory_path, destination):
    destination = output_root(destination)
    inventory = load_json(inventory_path)
    refs = {Path(ref["path"]).resolve(): ref for ref in inventory["sources"]}
    sources = {}
    for name, (path, role) in source_spec().items():
        ref = refs[path.resolve()]
        if ref["sha256"] != file_sha(path) or ref["size_bytes"] != path.stat().st_size:
            raise ValueError(f"planning source drift: {name}")
        sources[name] = dict(path=str(path), role=role, sha256=ref["sha256"], size_bytes=ref["size_bytes"])
    protocol = dict(schema=SCHEMA, target=core.LSTM, sources=sources, max_new_fits=0,
                    current_decision_date="2026-10-30", expected_input_rows=15250,
                    expected_prediction_rows=6300, expected_days=126, expected_common_days=102,
                    diagnostic_only=True, independent_confirmation=False,
                    script_sha256=file_sha(__file__), inventory_sha256=file_sha(inventory_path),
                    producer_commit=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip())
    verify_sources(protocol)
    publish_stage(study_root=destination / "study", stage="preregistered", plan_sha256=core.spo.sha(protocol),
                  parent_sha256=None, artifacts={"protocol.json": json_bytes(protocol)})
    return dict(status="PREREGISTERED", protocol_path=str(destination / "study/preregistered/protocol.json"),
                plan_sha256=core.spo.sha(protocol), new_fits=0)


def verify_archive(protocol):
    old_plan = load_json(checked_ref(protocol, "archive_protocol"))
    parent = None
    for stage in ("preregistered", "prepared", "fold1_trained", "fold2_trained", "fold3_trained", "evaluated", "completed"):
        path = core.stage_location(ARCHIVE / "recovery-v2/study", stage)
        parent = read_stage(path, stage=path.name, plan_sha256=core.spo.sha(old_plan), parent_sha256=parent)["stage_sha256"]
    receipt = load_json(checked_ref(protocol, "archive_run"))
    if parent != TERMINAL_SHA or receipt["completed_sha256"] != parent or receipt["cumulative_fits"] != 248:
        raise ValueError("completed P19 archive identity differs")


def ordered(frame):
    return frame.sort_values([*GROUP, "selection_effective_rank"], kind="stable").reset_index(drop=True)


def explain(inputs, packet, saved_predictions, *, current=False):
    """Original numerical encoding, decomposed without changing the policy."""
    inputs = ordered(inputs)
    predicted = (shadow.predict_current(inputs, packet) if current else core.predict_specialist(inputs, packet))
    saved = ordered(saved_predictions)
    if len(saved) != len(inputs) or saved.duplicated(KEY).any():
        raise ValueError("stored prediction population differs")
    pd.testing.assert_frame_equal(predicted, saved[predicted.columns], check_dtype=False, rtol=1e-11, atol=1e-10)
    m = packet["model"]
    fields = m["fields"]
    raw = inputs[fields].to_numpy(float)
    eligible = np.isfinite(raw).any(axis=1)
    median, scale, coef = (np.asarray(m[k], float) for k in ("median", "scale", "coef"))
    encoded = np.c_[(np.where(np.isnan(raw[eligible]), median, raw[eligible]) - median) / scale,
                    np.isnan(raw[eligible]).astype(float)]
    contribution = np.full((len(inputs), len(coef)), np.nan)
    contribution[eligible] = core.spo.group_center(encoded, inputs.loc[eligible]) * coef
    result = inputs.copy()
    result["advisory_rank"] = predicted.advisory_rank.to_numpy()
    result["decision_score"] = predicted.decision_score.to_numpy()
    result["no_rank_advice"] = predicted.no_rank_advice.to_numpy()
    result["fold"] = packet["fold"]
    result["sample_role"] = "CURRENT_NO_OUTCOMES" if current else "DEVELOPMENT_REUSE_ONLY"
    for index, field in enumerate(fields):
        result[f"value__{field}"] = contribution[:, index]
        result[f"missing__{field}"] = contribution[:, index + len(fields)]
    parent_indices = [i for i, field in enumerate(fields) if field.startswith("parent_")]
    market_indices = [i for i, field in enumerate(fields) if field in MARKET]
    def total(indices):
        return contribution[:, indices + [i + len(fields) for i in indices]].sum(axis=1)
    result["parent_contribution"], result["market_contribution"] = total(parent_indices), total(market_indices)
    np.testing.assert_allclose(contribution.sum(axis=1), result.decision_score, rtol=1e-11, atol=1e-10, equal_nan=True)
    native, model = result.selection_effective_rank.le(10), result.advisory_rank.le(10)
    result["selection_change"] = np.select([native & model, ~native & model, native & ~model],
                                           ["RETAINED", "ENTERED", "EXITED"], default="OUTSIDE_BOTH")
    return result


def boundaries(explained, model):
    """Same-group centering cancels in pair gaps; no profitability scan."""
    fields, coef = model["fields"], np.asarray(model["coef"], float)
    records = []
    for _, group in explained.groupby(GROUP, sort=False):
        inside, outside = group.loc[group.advisory_rank.le(10)], group.loc[group.advisory_rank.gt(10)]
        for _, a in inside.iterrows():
            for _, b in outside.iterrows():
                active = not a.no_rank_advice and not b.no_rank_advice
                gap = float(a.decision_score - b.decision_score) if active else None
                row = {**{k: a[k] for k in GROUP}, "target_trade_date": a.target_trade_date,
                       "inside": a.instrument, "outside": b.instrument, "inside_rank": int(a.advisory_rank),
                       "outside_rank": int(b.advisory_rank), "fold": int(a.fold), "sample_role": a.sample_role,
                       "score_gap": gap, "interpretation": "ALGEBRAIC_SENSITIVITY_ONLY"}
                distances = {}
                for field in MARKET:
                    index = fields.index(field)
                    valid = active and np.isfinite(a[field]) and np.isfinite(b[field]) and coef[index] != 0
                    distances[field] = max(gap, 0.) / abs(coef[index]) if valid else None
                    row[f"distance_scales__{field}"] = distances[field]
                    row[f"coordinate_status__{field}"] = (
                        "NO_RANK_ADVICE" if not active else "NONDIFFERENTIABLE_MISSING"
                        if not (np.isfinite(a[field]) and np.isfinite(b[field])) else "ZERO_SLOPE"
                        if coef[index] == 0 else "KNOWN_COORDINATE_ONLY")
                available = {k: v for k, v in distances.items() if v is not None}
                row["min_market_change_scales"] = min(available.values()) if available else None
                row["limiting_field"] = min(available, key=available.get) if available else None
                row["status"] = ("NO_RANK_ADVICE" if not active else "TIE_LIMIT_ZERO" if gap == 0
                                 else "KNOWN_BOTH_COORDINATE_ONLY" if available else "NO_KNOWN_CONTINUOUS_SLOPE")
                records.append(row)
    return pd.DataFrame(records)


def training_reference(protocol, inputs, fold, model):
    cutoff, _, _ = core.fold_window(fold)
    # Existing projection checks D/package then H before financial payload decoding.
    finance = core.finance_projection(checked_ref(protocol, f"train{fold}"), "2024-07-04", cutoff, cutoff)
    rows = inputs.loc[inputs[DAY].le(pd.Timestamp(cutoff))].merge(finance, on=KEY, validate="one_to_one")
    train, stats = core.spo.training(rows, cutoff)
    digest = core.spo.sha([[str(v) for v in row] for row in train[KEY].itertuples(index=False, name=None)])
    if digest != model["training_keys_sha256"] or len(train) != model["train_rows"]:
        raise ValueError("actual training population differs from frozen keys")
    return train, dict(status="TRAINING_KEYS_VERIFIED", training_keys_sha256=digest, **stats)


def profile(reference, heldout, current, model):
    fields = {}
    for i, field in enumerate(model["fields"]):
        train = reference[field].to_numpy(float)
        finite = train[np.isfinite(train)]
        row = {"training_missing_fraction": float(np.isnan(train).mean()),
               "training_quantiles": np.quantile(finite, QUANTILES).tolist() if len(finite) else None,
               "frozen_scale": model["scale"][i], "samples": {}}
        for name, frame in (("heldout", heldout), ("current", current)):
            values = frame[field].to_numpy(float)
            known = values[np.isfinite(values)]
            row["samples"][name] = dict(rows=len(values), missing_fraction=float(np.isnan(values).mean()),
                quantiles=np.quantile(known, QUANTILES).tolist() if len(known) else None,
                outside_training_range=int(((known < finite.min()) | (known > finite.max())).sum()) if len(finite) else None,
                known_count=len(known),
                empirical_training_percentiles=[float(np.mean(finite <= v)) for v in known] if len(finite) else None)
        fields[field] = row
    return dict(fields=fields, quantile_probabilities=QUANTILES, current_days=int(current[DAY].nunique()),
                drift_significance_tested=False, interpretation="DESCRIPTIVE_ONE_CURRENT_DAY_NOT_POPULATION_DRIFT")


def concentration(values):
    a = np.asarray(values, float)
    a = a[np.isfinite(a)]
    positive, negative = np.sort(a[a > 0])[::-1], np.sort(-a[a < 0])[::-1]
    return dict(known_count=len(a), net_sum_bps=float(a.sum()), positive_sum_bps=float(positive.sum()),
                negative_sum_bps=float(negative.sum()),
                largest_positive_share=float(positive[0] / positive.sum()) if len(positive) else None,
                top5_positive_share=float(positive[:5].sum() / positive.sum()) if len(positive) else None)


def historical_dependence(predictions, finance, panel, daily):
    if (len(panel) != 126 or panel.decision_date.duplicated().any() or len(predictions) != 6300
            or panel.original_candidates.ne(50).any() or set(panel.fold) != {1, 2, 3}):
        raise ValueError("original historical population differs")
    if finance.duplicated([*KEY, "fold"]).any():
        raise ValueError("duplicate historical outcome identity")
    rows = predictions.merge(finance, on=[*KEY, "fold"], validate="one_to_one", how="left", indicator=True)
    if len(rows) != 6300 or not rows._merge.eq("both").all():
        raise ValueError("historical outcomes changed the original keys")
    if not rows.valuation_policy_sha256.eq(core.spo.CONTRACT["valuation_policy_sha256"]).all():
        raise ValueError("historical valuation contract differs")
    panel = panel.sort_values("decision_date").reset_index(drop=True)
    common = panel.mature & panel[["candidate", "baseline", "candidate_full_model"]].notna().all(axis=1)
    if int(common.sum()) != 102:
        raise ValueError("original three-arm common denominator differs")
    mask = panel[["package_id", "decision_date", "fold"]].rename(columns={"decision_date": DAY})
    mask["in_primary_population"] = common.to_numpy()
    rows = rows.merge(mask, on=[*GROUP, "fold"], validate="many_to_one", how="left")
    if rows.in_primary_population.isna().any():
        raise ValueError("historical date coverage changed")
    change = rows.advisory_rank.le(10).astype(int) - rows.selection_effective_rank.le(10).astype(int)
    known = rows.valuation_status.isin(["AVAILABLE", "CASH_ENTRY_NOT_EXECUTABLE"])
    if not rows.loc[rows.valuation_status.eq("CASH_ENTRY_NOT_EXECUTABLE"), "hypothetical_liquidation_net_bps"].eq(0).all():
        raise ValueError("cash valuation must be known zero")
    if not np.isfinite(rows.loc[known, "hypothetical_liquidation_net_bps"].to_numpy(float)).all():
        raise ValueError("known historical return lost its value")
    # Zero weight difference cancels algebraically; it does not impute an unknown return.
    rows["stock_delta_bps"] = np.where(change.eq(0), 0.,
        np.where(known, change * rows.hypothetical_liquidation_net_bps / 10., np.nan))
    rows["slot_weight_change"] = change / 10.
    valid = rows.loc[rows.in_primary_population]
    sums = valid.groupby(["fold", DAY]).stock_delta_bps.sum()
    wanted = panel.loc[common].set_index(["fold", "decision_date"])
    np.testing.assert_allclose(sums.reindex(wanted.index), wanted.delta, rtol=1e-11, atol=1e-9)
    old_daily = daily.set_index(["fold", "decision_date"])
    np.testing.assert_allclose(old_daily.delta.reindex(wanted.index), wanted.delta, rtol=1e-11, atol=1e-9)
    by_stock = valid.groupby("instrument").stock_delta_bps.sum()
    net = float(wanted.delta.sum())
    effects = []
    for size in (1, 5):
        for begin in range(0, len(panel), size):
            block = panel.iloc[begin:begin + size]
            values = block.loc[common.iloc[begin:begin + size].to_numpy(), "delta"]
            remaining = len(wanted) - len(values)
            effects.append(dict(block_size=size, first=str(block.decision_date.iloc[0].date()),
                last=str(block.decision_date.iloc[-1].date()), primary_days=len(values),
                contribution_to_original_mean_bps=float(values.sum() / len(wanted)),
                mean_without_block_bps=float((net - values.sum()) / remaining) if remaining else None,
                use="INFLUENCE_DIAGNOSTIC_ORIGINAL_ESTIMATE_UNCHANGED"))
    by_fold = {str(int(f)): dict(days=len(g), mean_bps=float(g.delta.mean()), **concentration(g.delta))
               for f, g in wanted.reset_index().groupby("fold")}
    by_quarter = {str(q): dict(days=len(g), mean_bps=float(g.delta.mean()))
                  for q, g in wanted.reset_index().groupby(wanted.reset_index().decision_date.dt.to_period("Q"))}
    result = dict(schema=SCHEMA, use="DEVELOPMENT_REUSE_ONLY", independent_confirmation=False,
        original_days=126, mature_days=int(panel.mature.sum()), common_days=102,
        original_prediction_rows=6300, original_mean_delta_bps=float(wanted.delta.mean()),
        by_fold=by_fold, by_quarter=by_quarter, daily_concentration=concentration(wanted.delta),
        stock_concentration=concentration(by_stock), influence=effects,
        stock_events_are_independent=False, overlapping_5td=True,
        economic_components_bps={name: float(wanted[name].mean()) for name in
                                 ("avoided_loss", "added_loss", "added_profit", "missed_profit")},
        unknown_slots={name: int(panel[name].sum()) for name in ("candidate_unknown", "baseline_unknown", "shared_unknown")},
        primary_population_changed=False, new_policy_evaluations=0)
    keep = [*KEY, "fold", "selection_effective_rank", "advisory_rank", "selection_change", "valuation_status",
            "hypothetical_liquidation_net_bps", "in_primary_population", "slot_weight_change", "stock_delta_bps"]
    return result, rows[keep]


def rank_summary(explained, pairs):
    groups = []
    for (package, day), part in explained.groupby(GROUP, sort=False):
        selected_pairs = pairs.loc[pairs[DAY].eq(day)]
        group = {"package_id": package, "decision_date": str(day.date()), "sample_role": part.sample_role.iloc[0],
                 "fold": int(part.fold.iloc[0]), "entered": int(part.selection_change.eq("ENTERED").sum()),
                 "parent_contribution_std": float(part.parent_contribution.std(ddof=0)),
                 "market_contribution_std": float(part.market_contribution.std(ddof=0)),
                 "min_market_change_scales": selected_pairs.min_market_change_scales.min(),
                 "median_boundary_distance_scales": selected_pairs.min_market_change_scales.median()}
        groups.append(group)
    return groups


def run(protocol_path):
    started = time.monotonic()
    protocol = load_json(protocol_path)
    verify_sources(protocol)
    study = Path(protocol_path).resolve().parents[1]
    output_root(study.parent)
    plan_sha = core.spo.sha(protocol)
    first = read_stage(study / "preregistered", stage="preregistered", plan_sha256=plan_sha, parent_sha256=None)
    verify_archive(protocol)
    if (study / "evaluated").exists():
        prepared = read_stage(study / "prepared", stage="prepared", plan_sha256=plan_sha, parent_sha256=first["stage_sha256"])
        final = read_stage(study / "evaluated", stage="evaluated", plan_sha256=plan_sha, parent_sha256=prepared["stage_sha256"])
        return dict(status="DIAGNOSTICS_REUSED", stage_sha256=final["stage_sha256"], new_fits=0)
    inputs = ordered(read_frame(protocol, "inputs"))
    core.check_population(inputs)
    if len(inputs) != 15250 or inputs[DAY].nunique() != 305:
        raise ValueError("original development features changed")
    current_packet = shadow.read_packet(checked_ref(protocol, "current"))
    if (current_packet["sha256"] != CURRENT_SHA or current_packet["release_sha256"] !=
            shadow.read_packet(checked_ref(protocol, "release"))["sha256"]):
        raise ValueError("current immutable capture/release differs")
    current = shadow.frame(current_packet["inputs"])
    current_pred = shadow.frame(current_packet["predictions"])
    if not current[DAY].eq(pd.Timestamp("2026-10-30")).all() or not current.target_trade_date.eq(pd.Timestamp("2026-11-02")).all():
        raise ValueError("current capture clock differs")
    live_model = shadow.checked_model(checked_ref(protocol, "live_model").read_bytes())
    explained_current = explain(current, live_model, current_pred, current=True)
    current_pairs = boundaries(explained_current, live_model["model"])
    explanations, pair_tables, references, profiles = [], [], {}, {}
    for fold in (1, 2, 3):
        packet = load_json(checked_ref(protocol, f"model{fold}"))
        _, first_day, last_day = core.fold_window(fold)
        heldout = inputs.loc[inputs[DAY].between(pd.Timestamp(first_day), pd.Timestamp(last_day))]
        pred = read_frame(protocol, f"prediction{fold}")
        exp = explain(heldout, packet, pred)
        explanations.append(exp)
        pair_tables.append(boundaries(exp, packet["model"]))
        train, refs = training_reference(protocol, inputs, fold, packet["model"])
        references[str(fold)] = refs
        profiles[str(fold)] = profile(train, heldout, current, packet["model"])
    history = pd.concat(explanations, ignore_index=True)
    if len(history) != 6300:
        raise ValueError("heldout prediction count differs")
    profiles = dict(training_references=references, folds=profiles, industry_mapping="NOT_AVAILABLE",
                    interpretation="DESCRIPTIVE_NO_NEW_POLICY_OR_OVERALL_DRIFT_TEST")
    prepared_files = {"input_profile.json": json_bytes(profiles)}
    publish_stage(study_root=study, stage="prepared", plan_sha256=plan_sha,
                  parent_sha256=first["stage_sha256"], artifacts=prepared_files)
    prepared = read_stage(study / "prepared", stage="prepared", plan_sha256=plan_sha, parent_sha256=first["stage_sha256"])
    dependence, stock = historical_dependence(history, read_frame(protocol, "finance"),
                                             read_frame(protocol, "panel"), read_frame(protocol, "daily"))
    report = load_json(checked_ref(protocol, "report"))
    np.testing.assert_allclose(dependence["original_mean_delta_bps"], report["statistics"]["mean_bps"], atol=1e-9)
    all_exp = pd.concat([history, explained_current], ignore_index=True)
    all_pairs = pd.concat([*pair_tables, current_pairs], ignore_index=True)
    summary = rank_summary(all_exp, all_pairs)
    decision = dict(schema=SCHEMA, classification="KEEP_FROZEN_OBSERVATION", next_candidate_count=0,
                    reason="Diagnostic explanations do not establish a distinct superior policy; retain the existing frozen trial.",
                    physical_fit_count=0, cumulative_fits=248, prior_index=1,
                    independent_economic_confirmation=False, production_ranking_replaced=False)
    verify_sources(protocol)
    receipt = dict(schema=SCHEMA, status="PASS", producer_commit=protocol["producer_commit"],
        script_sha256=protocol["script_sha256"], plan_sha256=plan_sha, current_return_values_read=0,
        new_return_windows_read=0, old_evaluated_returns_reused=True, actual_training_keys_verified=references,
        original_input_rows=len(inputs), historical_prediction_rows=len(history), current_rows=len(current),
        boundary_pairs=len(all_pairs), feature_contribution_cells=len(all_exp) * 16,
        diagnostic_group_count=len(summary), influence_projection_count=len(dependence["influence"]),
        new_policy_evaluations=0, physical_fit_count=0, database_writes=0, service_control=False,
        source_bytes_unchanged=True, elapsed_seconds=time.monotonic()-started,
        independent_economic_confirmation=False, production_ranking_replaced=False)
    files = {"score_contributions.parquet": parquet_bytes(all_exp), "boundary_review.parquet": parquet_bytes(all_pairs),
             "historical_dependence.json": json_bytes(dependence), "stock_dependence.parquet": parquet_bytes(stock),
             "rank_summary.json": json_bytes(summary), "decision.json": json_bytes(decision),
             "receipt.json": json_bytes(receipt)}
    if sum(map(len, files.values())) > 200 * 1024**2 or receipt["elapsed_seconds"] > 1800:
        raise ValueError("diagnostic output/time budget exceeded")
    publish_stage(study_root=study, stage="evaluated", plan_sha256=plan_sha,
                  parent_sha256=prepared["stage_sha256"], artifacts=files)
    final = read_stage(study / "evaluated", stage="evaluated", plan_sha256=plan_sha, parent_sha256=prepared["stage_sha256"])
    return {**receipt, "status": "DIAGNOSTICS_COMPLETE", "stage_sha256": final["stage_sha256"],
            "historical_mean_delta_bps": dependence["original_mean_delta_bps"], "decision": decision["classification"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    register = commands.add_parser("freeze")
    register.add_argument("--inventory", required=True, type=Path)
    register.add_argument("--output", required=True, type=Path)
    execute = commands.add_parser("run")
    execute.add_argument("--protocol", required=True, type=Path)
    args = parser.parse_args()
    result = freeze(args.inventory, args.output) if args.command == "freeze" else run(args.protocol)
    print(json_bytes(result).decode())


if __name__ == "__main__":
    main()
