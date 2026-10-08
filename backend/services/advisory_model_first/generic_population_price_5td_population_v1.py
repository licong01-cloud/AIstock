"""Read existing frozen keys, never scores, outcomes, Selection or sealed finance."""
from dataclasses import dataclass
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.dataset as ds

from backend.services.advisory_model_first.economic_entry_pipeline import file_sha256
from backend.services.advisory_model_first.generic_daily_price_input_v1 import _day
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import KEY, POLICY_SHA256, ROSTER
from backend.services.advisory_model_first.generic_price_5td_labels_v1 import validate_roster
from backend.services.advisory_model_first.generic_population_price_5td_contracts_v1 import PopulationMetadataRequestV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


@dataclass
class FrozenPopulationMetadataV1:
    rosters: pd.DataFrame
    days: pd.DataFrame
    clusters: pd.DataFrame
    receipt: dict


def _verify(ref):
    path = Path(ref.artifact_uri)
    if (not path.is_absolute() or not path.is_file() or path.stat().st_size != ref.size_bytes
            or file_sha256(path) != ref.sha256):
        raise ValueError(f"population frozen reference differs: {ref.role}")
    return path


def _arm_identity(source):
    request_path, manifest_path = _verify(source.lineage_ref), _verify(source.bundle_manifest_ref)
    request = json.loads(request_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for ref in (source.candidate_ref, source.lineage_ref):
        descriptor = manifest.get("files", {}).get(Path(ref.artifact_uri).name, {})
        if descriptor.get("sha256") != ref.sha256 or descriptor.get("size_bytes") != ref.size_bytes:
            raise ValueError(f"population bundle descriptor differs: {ref.role}")
    matches = [p for p in request.get("packages", []) if p.get("arm_id") == source.arm_id]
    if (len(matches) != 1 or matches[0].get("package_id") != source.package_id
            or matches[0].get("manifest_sha256") != source.manifest_sha256):
        raise ValueError(f"population original arm/package identity differs: {source.source_id}")


def _read_source(source, request):
    path = _verify(source.candidate_ref)
    if source.candidate_format == "FROZEN_ARM_TOP50":
        _arm_identity(source)
    dataset = ds.dataset(path, format="parquet")
    columns = [*KEY, "selection_effective_rank"]
    columns += (["is_candidate_decision", "package_id", "manifest_sha256"]
                if source.candidate_format == "GP5_LEGACY_TOP20" else ["arm_id"])
    if not set(columns).issubset(dataset.schema.names):
        raise ValueError(f"population frozen key schema differs: {source.source_id}")
    predicate = ((ds.field(KEY[0]) >= pd.Timestamp(request.train_start))
                 & (ds.field(KEY[0]) <= pd.Timestamp(request.evaluation_end)))
    if source.arm_id is not None:
        predicate &= ds.field("arm_id") == source.arm_id
    # Enforce the resource bound before decoding even the projected column batch.
    if dataset.count_rows(filter=predicate) > 200000:
        raise ValueError("population source exceeds study roster budget")
    frame = dataset.to_table(columns=columns, filter=predicate).to_pandas()
    if source.candidate_format == "GP5_LEGACY_TOP20":
        if not frame.is_candidate_decision.map(lambda v: type(v) is bool).all():
            raise ValueError("population original candidate flags are not explicit")
        frame = frame.loc[frame.is_candidate_decision].copy()
    def integer(v):
        return isinstance(v, (int, np.integer)) and not isinstance(v, (bool, np.bool_))
    if not frame.selection_effective_rank.map(integer).all():
        raise ValueError("population original ranks are not integers")
    if source.candidate_format == "GP5_LEGACY_TOP20":
        if (not frame.package_id.eq(source.package_id).all()
                or not frame.manifest_sha256.eq(source.manifest_sha256).all()):
            raise ValueError("population original anchor package/manifest differs")
        # Precisely the existing GP5 prepare projection, not original file's Top40/50.
        frame = frame.loc[frame.selection_effective_rank.le(20)].copy()
    for name in KEY[:2]:
        frame[name] = frame[name].map(_day).map(pd.Timestamp)
    frame = frame.sort_values([KEY[0], "selection_effective_rank"], kind="stable").reset_index(drop=True)
    frame["candidate_group_size"] = frame.groupby(KEY[0]).instrument.transform("size")
    return frame.loc[:, ROSTER].copy()


def prepare_population_metadata_v1(*, request):
    """Metadata slice only; time maturity is not feature/label/training readiness."""
    if not isinstance(request, PopulationMetadataRequestV1):
        raise ValueError("population request must bind the approved source contract")
    refs = [request.calendar_ref]
    for source in request.sources:
        refs.extend(r for r in (source.candidate_ref, source.lineage_ref, source.bundle_manifest_ref) if r is not None)
    calendar_path = _verify(request.calendar_ref)
    calendar = [_day(d) for d in json.loads(calendar_path.read_text(encoding="utf-8"))]
    if not calendar or len(calendar) > 5000 or calendar != sorted(set(calendar)):
        raise ValueError("population original calendar differs")
    positions = {d: i for i, d in enumerate(calendar)}
    ordered = [request.sources[0], *sorted(request.sources[1:], key=lambda s: (s.package_id, s.manifest_sha256))]
    held = ordered[-1].source_id if len(ordered) >= 3 else None
    rosters, day_rows, summaries, total = [], [], [], 0
    for number, source in enumerate(ordered):
        role = "MATCHED_ANCHOR" if number == 0 else (
            "HELD_PACKAGE_EVALUATION" if source.source_id == held else "TRANSFER_TRAIN")
        decisions = [d for d in source.decision_dates if request.train_start <= d <= request.evaluation_end]
        if not decisions:
            raise ValueError(f"population source has no declared development days: {source.source_id}")
        frame = _read_source(source, request)
        total += len(frame)
        if total > 200000:
            raise ValueError("population union exceeds study roster budget")
        # Reuse the original single-source key/rank contract in complete-day chunks.
        for start in range(0, len(decisions), 100):
            block_days = decisions[start:start+100]
            block = frame.loc[frame[KEY[0]].isin(pd.to_datetime(block_days))]
            validate_roster(block, block_days, calendar)
        if not frame[KEY[0]].isin(pd.to_datetime(decisions)).all():
            raise ValueError("population frozen row is outside its original decision schedule")
        counts = frame.groupby(KEY[0]).size()
        if any(pd.Timestamp(d) in counts.index for d in source.empty_decision_dates):
            raise ValueError("population explicit empty day contains frozen candidates")
        for d in decisions:
            count = int(counts.get(pd.Timestamp(d), 0))
            day_rows.append(dict(source_id=source.source_id, decision_as_of_trade_date=pd.Timestamp(d),
                candidate_count=count if count or d in source.empty_decision_dates else None,
                roster_status="PRESENT" if count else (
                    "EXPLICIT_EMPTY" if d in source.empty_decision_dates else "UNKNOWN_ABSENT_FROZEN_DAY")))
        ends = frame[KEY[0]].map(lambda d: calendar[positions[d.date()]+5]
                               if positions[d.date()]+5 < len(calendar) else None)
        frame["label_information_end"] = pd.to_datetime(ends)
        frame["maturity_at_cutoff"] = np.where(frame.label_information_end.isna(), "UNSETTLED_CALENDAR",
            np.where(frame.label_information_end.dt.date > request.evaluation_end,
                     "UNSETTLED_CUTOFF", "MATURE_BY_CALENDAR_ONLY"))
        frame["potential_train_by_calendar"] = (
            frame[KEY[0]].dt.date.le(request.train_end)
            & frame.label_information_end.dt.date.le(request.train_end))
        frame["source_id"], frame["population_role"] = source.source_id, role
        frame["package_id"], frame["manifest_sha256"] = source.package_id, source.manifest_sha256
        for name in ("run_id", "list_version_id", "source_policy_hash", "source_dataset_identity", "source_evidence"):
            frame[name] = getattr(source, name)
        frame["universe_identity"] = [source.universe_identity for _ in range(len(frame))]
        frame["shadow_policy_sha256"] = POLICY_SHA256
        rosters.append(frame)
        summaries.append(dict(source_id=source.source_id, population_role=role, package_id=source.package_id,
            manifest_sha256=source.manifest_sha256, source_evidence=source.source_evidence,
            rows=len(frame), declared_days=len(decisions), observed_days=len(counts),
            potential_train_by_calendar=int(frame.potential_train_by_calendar.sum()),
            unsettled_cutoff_rows=int(frame.maturity_at_cutoff.eq("UNSETTLED_CUTOFF").sum())))
    roster = pd.concat(rosters, ignore_index=True)
    # Training mass is deduplicated; per-source rows, ranks and UNKNOWN identities remain untouched.
    cluster_rows = []
    for key, group in roster.groupby(list(KEY), sort=True):
        potential = group.loc[group.potential_train_by_calendar]
        cluster_rows.append(dict(zip(KEY, key, strict=True)) | dict(shadow_policy_sha256=POLICY_SHA256,
            source_ids=tuple(group.source_id), cluster_mass=1, supervision_ready=False,
            potential_anchor_train=bool(potential.population_role.eq("MATCHED_ANCHOR").any()),
            potential_transfer_train=bool(potential.population_role.isin(["MATCHED_ANCHOR", "TRANSFER_TRAIN"]).any())))
    clusters = pd.DataFrame(cluster_rows, columns=[*KEY, "shadow_policy_sha256", "source_ids", "cluster_mass",
        "supervision_ready", "potential_anchor_train", "potential_transfer_train"])
    if int(clusters.potential_transfer_train.sum()) > 100000:
        raise ValueError("population exceeds unique training cluster budget")
    anchor = int(clusters.potential_anchor_train.sum())
    union = int(clusters.potential_transfer_train.sum())
    for ref in refs:
        _verify(ref)  # Detect source changes during the read, never overwrite a moving input.
    held_distinct = held is not None and all(s.package_id != ordered[-1].package_id for s in ordered[:-1])
    receipt = dict(schema_version="generic_population_price_5td_metadata_v1", status="METADATA_PREPARED_ONLY",
        request_sha256=sha(request.model_dump(mode="json")), shadow_policy_sha256=POLICY_SHA256,
        train_window=[request.train_start.isoformat(), request.train_end.isoformat()],
        evaluation_window=[request.evaluation_start.isoformat(), request.evaluation_end.isoformat()],
        sources=summaries, input_refs=[r.model_dump(mode="json") for r in refs],
        potential_anchor_train_clusters=anchor, potential_transfer_train_clusters=union,
        new_potential_training_clusters=union-anchor, held_source_id=held,
        held_package_is_distinct=held_distinct,
        unseen_package_transfer_status="HELD_PACKAGE_DECLARED_NOT_EVALUATED" if held_distinct else
            "UNSEEN_PACKAGE_TRANSFER_NOT_TESTED",
        population_contrast_status="POTENTIAL_NEW_CLUSTERS_LABELS_UNVERIFIED" if union > anchor else
            "NOT_TESTABLE_POPULATION_CONTRAST", labels_ready=False, features_ready=False,
        financial_columns_read=False, physical_fit_count=0, native_receipt_created=False, deployable=False)
    return FrozenPopulationMetadataV1(roster, pd.DataFrame(day_rows), clusters, receipt)


def build_population_inputs_v1(*, metadata, request, calendar, daily, index_daily, source_context, progress=None):
    """One normalized source for all rosters; no estimator or future feature access."""
    from backend.services.advisory_model_first.generic_daily_price_input_v1 import FEATURES, _number, build_generic_daily_price_input_v1
    from backend.services.advisory_model_first.generic_price_5td_labels_v1 import PRICE_FIELDS, REFERENCE_FIELDS, build_generic_price_5td_labels_v1
    from backend.services.advisory_model_first.generic_price_5td_models_v1 import _support, feature_values
    from backend.services.advisory_model_first.generic_population_price_5td_contracts_v1 import DAILY_FIELDS, INDEX_FIELDS, MATRIX_ORDER

    days = [_day(v) for v in calendar]
    if days != sorted(set(days)) or not days:
        raise ValueError("population input calendar differs")
    positions = {d: i for i, d in enumerate(days)}
    wanted_symbols = set(metadata.rosters.instrument)
    daily, index_daily = daily.copy(deep=True), index_daily.copy(deep=True)
    for frame, columns, symbols in ((daily, DAILY_FIELDS, wanted_symbols), (index_daily, INDEX_FIELDS, {"000300.SH"})):
        if (not isinstance(frame, pd.DataFrame) or not frame.columns.is_unique or set(frame.columns) != set(columns)
                or len(frame) > len(symbols)*len(days) or frame.duplicated(["trade_date", "instrument"]).any()
                or not set(frame.instrument).issubset(symbols)):
            raise ValueError("population common source schema/keys/budget differs")
        frame["trade_date"] = frame.trade_date.map(_day).map(pd.Timestamp)
        if (not frame.trade_date.isin(pd.to_datetime(days)).all()
                or frame.trade_date.gt(pd.Timestamp(request.evaluation_end)).any()):
            raise ValueError("population common source contains outside-cutoff finance")
    numeric = [name for name in DAILY_FIELDS if name.startswith("raw_") or name in ("adj_factor", "volume_hand", "up_limit", "down_limit")]
    for name in numeric:
        daily[name] = daily[name].map(lambda v, n=name: _number(v, positive=n != "volume_hand", nonnegative=n == "volume_hand")).astype(float)
    for name in ("suspended", "tradability_unknown"):
        if not daily[name].map(lambda v: type(v) is bool).all():
            raise ValueError("population common source trading states must be explicit")
    if any(daily.raw_high_cny.lt(daily[name]).any() or daily.raw_low_cny.gt(daily[name]).any()
           for name in ("raw_open_cny", "raw_close_cny")) or daily.raw_high_cny.lt(daily.raw_low_cny).any():
        raise ValueError("population common source OHLC units contradict")
    if metadata.rosters.empty:
        from backend.services.advisory_model_first.generic_price_5td_labels_v1 import LABEL_FIELDS
        rows, clusters = metadata.rosters.copy(), metadata.clusters.copy()
        for frame in (rows, clusters):
            for name in (*FEATURES, *LABEL_FIELDS):
                if name not in frame:
                    frame[name] = pd.Series(dtype="datetime64[ns]" if name == "label_information_end" else "object")
        rows["feature_unknown_reasons"] = pd.Series(dtype="object")
        return rows, clusters, _population_encoding_v1(clusters, request, MATRIX_ORDER, _support, feature_values)
    quotes = daily.set_index(["trade_date", "instrument"]).sort_index()
    features, prices, refs = [], [], []
    source_specs = {s.source_id: s for s in request.sources}
    for ordinal, (d, full) in enumerate(metadata.rosters.groupby(KEY[0], sort=True), 1):
        pos = positions[d.date()]
        symbols = sorted(set(full.instrument))
        anchor = quotes.reindex(pd.MultiIndex.from_product(([d], symbols)))
        factors = dict(zip(symbols, anchor.adj_factor, strict=True))
        close = dict(zip(symbols, anchor.raw_close_cny, strict=True))
        original = full.loc[:, KEY].drop_duplicates()
        reference = original.copy()
        reference["reference_cny"] = reference.instrument.map(close)
        reference["reference_visible_through"] = d.date()
        refs.append(reference)
        history = None
        if pos >= 19:
            history_days = pd.to_datetime(days[pos-19:pos+1])
            history = quotes.reindex(pd.MultiIndex.from_product((history_days, symbols))).dropna(how="all").reset_index()
            history.columns = ["trade_date", "instrument", *quotes.columns]
            scale = history.adj_factor / history.instrument.map(factors)
            for name in ("open", "high", "low", "close"):
                history[name] = history["raw_"+name+"_cny"]*scale
            history["volume"] = history.volume_hand*100.
        horizon = pd.to_datetime([day for day in days[pos+1:pos+6] if day <= request.evaluation_end])
        path = quotes.reindex(pd.MultiIndex.from_product((horizon, symbols))).dropna(how="all").reset_index()
        path.columns = ["trade_date", "instrument", *quotes.columns]
        path[KEY[0]] = d
        path["d_anchor_factor"] = path.adj_factor / path.instrument.map(factors)
        for name in ("open", "high", "low", "close"):
            path[name] = path["raw_"+name+"_cny"]
        prices.append(path.loc[:, PRICE_FIELDS])
        for source_id, group in full.groupby("source_id", sort=False):
            group = group.sort_values("selection_effective_rank").reset_index(drop=True)
            spec = source_specs[source_id]
            if history is None:
                block = group.loc[:, ROSTER].copy()
                for name in FEATURES:
                    block[name] = np.nan
                reasons = [{name: "INSUFFICIENT_ORIGINAL_HISTORY" for name in FEATURES} for _ in range(len(block))]
            else:
                panel = history.loc[history.instrument.isin(group.instrument), ["trade_date", "instrument", "open", "high", "low", "close", "volume"]]
                benchmark = index_daily.loc[index_daily.trade_date.isin(history_days)].copy()
                block, evidence = build_generic_daily_price_input_v1(candidates=group.loc[:, ROSTER],
                    calendar=days[pos-19:pos+2], panel=panel, benchmark_daily=benchmark,
                    market_state=dict(trade_date=d.date(), market_up_ratio=None, market_definition_id=None, visible_through=None),
                    source_context=dict(package_id=spec.package_id, run_id=spec.run_id, list_version_id=spec.list_version_id,
                        universe_identity=spec.universe_identity, source_evidence=source_context["source_evidence"],
                        price_basis="D_ADJUSTED_CNY", volume_basis="RAW_SHARES", source_visible_through=d.date(),
                        benchmark_visible_through=d.date()))
                reasons = [row["fields"] for row in evidence["unknown_fields"]]
            block["source_id"] = source_id
            block["feature_unknown_reasons"] = [json.dumps(r, sort_keys=True) for r in reasons]
            features.append(block)
        if progress is not None and ordinal % 20 == 0:
            progress({"phase": "features", "decision_days_completed": ordinal})
    price = pd.concat(prices, ignore_index=True)
    reference = pd.concat(refs, ignore_index=True).loc[:, REFERENCE_FIELDS]
    labels = []
    for spec in request.sources:
        original = metadata.rosters.loc[metadata.rosters.source_id.eq(spec.source_id), ROSTER]
        decision_days = [d for d in spec.decision_dates if request.train_start <= d <= request.evaluation_end]
        for start in range(0, len(decision_days), 100):
            block = original.loc[original[KEY[0]].isin(pd.to_datetime(decision_days[start:start+100]))].copy()
            if block.empty:
                continue
            block_prices = price.merge(block.loc[:, [KEY[0], "instrument"]], on=[KEY[0], "instrument"], validate="many_to_one")
            block_refs = reference.merge(block.loc[:, KEY], on=list(KEY), validate="one_to_one")
            result, _ = build_generic_price_5td_labels_v1(candidates=block, decision_dates=decision_days[start:start+100],
                calendar=days, prices=block_prices, references=block_refs, source_context=source_context)
            outside_cutoff = result.label_information_end.gt(pd.Timestamp(request.evaluation_end))
            result.loc[outside_cutoff, "label_status"] = "IMMATURE"
            result.loc[outside_cutoff, "label_reason"] = "HORIZON_BEYOND_SOURCE_CUTOFF"
            result["source_id"] = spec.source_id
            labels.append(result)
    feature_rows = pd.concat(features, ignore_index=True).loc[:, ["source_id", *KEY, *FEATURES, "feature_unknown_reasons"]]
    label_rows = pd.concat(labels, ignore_index=True).drop(columns=["selection_effective_rank", "candidate_group_size"])
    rows = metadata.rosters.merge(feature_rows, on=["source_id", *KEY], validate="one_to_one", sort=False)
    rows = rows.merge(label_rows.rename(columns={"label_information_end": "computed_label_information_end"}),
                      on=["source_id", *KEY], validate="one_to_one", sort=False)
    expected, actual = pd.to_datetime(rows.label_information_end), pd.to_datetime(rows.computed_label_information_end)
    if not (expected.eq(actual) | expected.isna() & actual.isna()).all():
        raise ValueError("population label horizon contradicts original frozen H")
    rows = rows.drop(columns=["computed_label_information_end"])
    if len(rows) != len(metadata.rosters):
        raise ValueError("population input preparation changed original roster count")
    value_fields = [*FEATURES, "observed_gap_bps", "gross_terminal_ratio", "path_min_ratio", "label_status", "label_reason",
                    "label_information_end", "policy_sha256", "label_contract"]
    unique = []
    for key, group in rows.groupby(list(KEY), sort=True):
        values = {}
        for name in value_fields:
            known = group[name].dropna().unique()
            if len(known) > 1:
                raise ValueError(f"population normalized cluster conflict: {key}/{name}/{list(group.source_id)}")
            values[name] = known[0] if len(known) else None
        unique.append(dict(zip(KEY, key, strict=True)) | values)
    clusters = metadata.clusters.merge(pd.DataFrame(unique), on=list(KEY), validate="one_to_one", sort=False)
    return rows, clusters, _population_encoding_v1(clusters, request, MATRIX_ORDER, _support, feature_values)


def _population_encoding_v1(clusters, request, matrix_order, support_builder, feature_reader):
    _, stock_known = feature_reader(clusters)
    d, h = clusters[KEY[0]], pd.to_datetime(clusters.label_information_end)
    train_time = d.between(pd.Timestamp(request.train_start), pd.Timestamp(request.train_end)) & h.le(pd.Timestamp(request.train_end))
    anchor = clusters.loc[clusters.potential_anchor_train & train_time & stock_known].copy()
    values, _ = feature_reader(anchor)
    medians = [float(np.median(v[~np.isnan(v)])) if (~np.isnan(v)).any() else 0. for v in values.T]
    support = support_builder(anchor)
    supported = clusters.observed_gap_bps.map(lambda v: False if pd.isna(v) else support.contains(float(v)))
    eligible = train_time & stock_known & supported & clusters.label_status.eq("AVAILABLE") & h.lt(pd.Timestamp(request.evaluation_start))
    training_dates = sorted(clusters.loc[eligible & clusters.potential_anchor_train, KEY[0]].unique())
    first = pd.Timestamp(training_dates[len(training_dates)//2]) if training_dates else None
    for name, member in (("matched_anchor", clusters.potential_anchor_train), ("candidate_transfer", clusters.potential_transfer_train)):
        field = name+"_pool"
        clusters[field] = "NOT_TRAIN_SUPERVISION"
        if first is not None:
            clusters.loc[eligible & member & d.ge(first), field] = "ESTIMATION"
            clusters.loc[eligible & member & d.lt(first) & h.lt(first), field] = "STRUCTURE"
            clusters.loc[eligible & member & d.lt(first) & h.ge(first), field] = "PURGED_LABEL_OVERLAP"
    clusters["supervision_ready"] = clusters.label_status.eq("AVAILABLE") & h.le(pd.Timestamp(request.evaluation_end))
    new_before_purge = int((eligible & clusters.potential_transfer_train & ~clusters.potential_anchor_train).sum())
    new = int((~clusters.potential_anchor_train & clusters.candidate_transfer_pool.isin(["STRUCTURE", "ESTIMATION"])).sum())
    pools = {name: {str(k): int(v) for k, v in clusters[name+"_pool"].value_counts().items()}
             for name in ("matched_anchor", "candidate_transfer")}
    identifiable = new > 0 and all(pools[n].get(p, 0) > 0 for n in pools for p in ("STRUCTURE", "ESTIMATION"))
    return dict(matrix_order=list(matrix_order), medians=medians, intervals_bps=support.intervals_bps,
        estimation_first_D=None if first is None else first.date().isoformat(), encoding_anchor_rows=len(anchor),
        eligible_new_training_clusters=new, eligible_new_training_clusters_before_purge=new_before_purge,
        pools=pools, physical_fit_count=0, research_run_created=False,
        population_contrast_status="PREPARED_IDENTIFIABLE_NO_FIT" if identifiable else "NOT_TESTABLE_POPULATION_CONTRAST",
        features_ready=True, labels_ready=True, unknown_features_preserved=True, deployable=False)
