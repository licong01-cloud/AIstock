"""Complete frozen lists first, then train-only finance; no SQL or outcome-selected context."""
from fractions import Fraction
import json
from math import isfinite
import re

import numpy as np
import pandas as pd
import pyarrow.dataset as ds

from backend.services.advisory_model_first.cross_package_consensus_price_5td_contracts_v1 import (
    BASE_ORDER, CONTEXT_COLUMNS, CONTEXT_SCHEMA, FEATURES, KEY, MARK_POLICY_SHA256,
    RANK_COLUMNS, SOURCE_COLUMNS, node_path,
)
from backend.services.advisory_model_first.economic_entry_pipeline import file_sha256, read_stage
from backend.services.advisory_model_first.generic_daily_price_input_v1 import _number
from backend.services.advisory_model_first.generic_price_5td_models_v1 import feature_values
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


def _date(value):
    value = pd.Timestamp(value)
    if pd.isna(value) or value.tz is not None or value != value.normalize():
        raise ValueError("original clock must be a known naive market date")
    return value


def _integer(value):
    if not isinstance(value, (int, np.integer)) or isinstance(value, (bool, np.bool_)):
        raise ValueError("original rank/N must be true integers")
    return int(value)


def build_cross_package_rank_context_v1(*, roster_metadata, day_metadata, calendar, package_pair):
    """O(N) complete-list indexing. Absent-from-known-list is not a negative prediction."""
    sources = sorted([dict(s) for s in package_pair], key=lambda s: s["package_id"])
    if (len(sources) != 2 or len({s["package_id"] for s in sources}) != 2
            or any(re.fullmatch(r"[a-f0-9]{64}", s.get("artifact_sha256", "")) is None
                   or re.fullmatch(r"[a-f0-9]{64}", s.get("original_source_stage_sha256", "")) is None for s in sources)
            or len({s["original_source_stage_sha256"] for s in sources}) != 1
            or len(roster_metadata) > 30500 or len(day_metadata) > 610
            or roster_metadata.duplicated(["package_id", *KEY]).any()
            or day_metadata.duplicated(["package_id", "decision_date"]).any()):
        raise ValueError("explicit pair, original metadata keys or bounded lists differ")
    sessions = [_date(v) for v in calendar]
    if sessions != sorted(set(sessions)):
        raise ValueError("original calendar must have unique sorted market sessions")
    positions = {v: i for i, v in enumerate(sessions)}
    roster = roster_metadata.loc[:, SOURCE_COLUMNS].copy()
    roster[KEY[0]], roster[KEY[1]] = roster[KEY[0]].map(_date), roster[KEY[1]].map(_date)
    days = day_metadata.copy()
    days["decision_date"], days["target_date"] = days.decision_date.map(_date), days.target_date.map(_date)
    expected_packages = {s["package_id"] for s in sources}
    if not set(days.package_id).issubset(expected_packages) or not set(roster.package_id).issubset(expected_packages):
        raise ValueError("original source package differs")
    if set(days.package_id) != expected_packages:
        raise ValueError("an expected original package/D is missing")
    dates = sorted(set(days.decision_date))
    if set(zip(days.package_id, days.decision_date, strict=True)) != {(p, d) for p in expected_packages for d in dates}:
        raise ValueError("an expected original package/D is missing")
    if not set(zip(roster.package_id, roster[KEY[0]], strict=True)).issubset(set(zip(days.package_id, days.decision_date, strict=True))):
        raise ValueError("roster has no original package/D metadata")
    groups = {k: v for k, v in roster.groupby(["package_id", KEY[0]], sort=False)}
    lists = {}
    for source in sources:
        rows = roster.loc[roster.package_id.eq(source["package_id"])]
        if any(not rows[n].eq(source[n]).all() for n in ("manifest_sha256", "run_id", "source_id")):
            raise ValueError("known original manifest/run/source differs")
        for packet in days.loc[days.package_id.eq(source["package_id"])].to_dict("records"):
            d, t = packet["decision_date"], packet["target_date"]
            pos = positions.get(d)
            if pos is None or pos+1 >= len(sessions) or sessions[pos+1] != t:
                raise ValueError("original D/T are not adjacent market sessions")
            block = groups.get((source["package_id"], d), roster.iloc[:0])
            ranks = [_integer(v) for v in block.selection_effective_rank]
            n = packet["original_candidates"]
            n_known = not pd.isna(n)
            n = _integer(n) if n_known else None
            status = packet["source_status"]
            if (status not in {"KNOWN_ROSTER", "KNOWN_EMPTY", "UNKNOWN"} or len(block) > 50
                    or not block[KEY[1]].eq(t).all() or block.instrument.isna().any()
                    or block.instrument.duplicated().any() or any(not isinstance(v, str) or not v for v in block.instrument)
                    or any(not 1 <= r <= 50 for r in ranks)):
                raise ValueError("original status, unique members or true ranks contradict metadata")
            if n_known and (not 0 <= n <= 50 or len(block) != n or sorted(ranks) != list(range(1, n+1))
                    or any(_integer(v) != n for v in block.candidate_group_size)):
                raise ValueError("declared original N/list completeness contradicts members or ranks")
            if status == "KNOWN_EMPTY" and (n != 0 or len(block)) or status == "KNOWN_ROSTER" and n == 0:
                raise ValueError("known empty/roster status contradicts original N")
            visible = packet.get("semantic_visible_through")
            if visible is None and source.get("semantic_clock") == "D_FROZEN_BACKTEST":
                visible = d
            visible = _date(visible) if visible is not None else None
            if visible is not None and visible > d:
                raise ValueError("known original list is not D-visible")
            known = status != "UNKNOWN" and n_known and visible is not None
            ordered = sorted(zip(block.instrument, ranks, strict=True), key=lambda v: v[1])
            digest = sha(dict(package_id=source["package_id"], D=d.date().isoformat(), T=t.date().isoformat(),
                N=n, status=status, roster=ordered, semantic_visible_through=visible.date().isoformat() if visible is not None else None))
            if packet.get("roster_sha256") is not None and packet["roster_sha256"] != digest:
                raise ValueError("known frozen original list hash differs")
            lists[source["package_id"], d] = dict(known=known, members=dict(ordered), N=n, digest=digest,
                capture=packet.get("capture_timestamp") if packet.get("capture_proven") is True else None)
    pair_refs = [{k: s[k] for k in ("package_id", "manifest_sha256", "run_id", "source_id", "artifact_sha256")}
                 | {"pool_status": s.get("pool_status", "UNKNOWN"), "pool_identity": s.get("universe_identity")}
                 for s in sources]
    pair_hash = sha(pair_refs)
    output = []
    for d in dates:
        pair = [lists[s["package_id"], d] for s in sources]
        # Full list digests are computed once per package/D, not copied into each stock/tick.
        day_hash = sha(dict(pair_sha256=pair_hash, lists=[v["digest"] for v in pair]))
        t = sessions[positions[d]+1]
        symbols = sorted(set().union(*(set(v["members"]) for v in pair)))
        for symbol in symbols:
            membership, values = [], []
            for source, item in zip(sources, pair, strict=True):
                rank = item["members"].get(symbol)
                value = Fraction(item["N"]-rank+1, item["N"]) if item["known"] and rank is not None else Fraction(0)
                values.append(value)
                membership.append(dict(package_id=source["package_id"], membership="UNKNOWN_SOURCE" if not item["known"] else
                    "KNOWN_MEMBER" if rank is not None else "KNOWN_ABSENT_FROM_COMPLETE_LIST", rank=rank, N=item["N"]))
            known = all(v["known"] for v in pair)
            strongest, consensus = max(values), min(values)
            packet = dict(schema=CONTEXT_SCHEMA, D=d.date().isoformat(), T=t.date().isoformat(), instrument=symbol,
                sorted_source_pair_refs=pair_refs, original_source_stage_sha256=sources[0]["original_source_stage_sha256"],
                availability="AVAILABLE" if known else "UNKNOWN_CONTEXT", source_evidence="QE_FROZEN_BACKTEST_SCORE",
                semantic_visible_through=d.date().isoformat() if known else None,
                capture_timestamp_if_originally_proven=[v["capture"] for v in pair],
                strongest_rank_strength=float(strongest) if known else None,
                cross_package_consensus_strength=float(consensus) if known else None,
                per_source_membership=membership, unknown_reasons=[] if known else ["UNKNOWN_ORIGINAL_LIST_N_OR_D_SEMANTICS"],
                context_pair_sha256=pair_hash, day_list_sha256=day_hash,
                strongest_rank_fraction=str(strongest) if known else None, consensus_rank_fraction=str(consensus) if known else None)
            packet["context_sha256"] = sha(packet)
            output.append({**packet, KEY[0]: d, KEY[1]: t, "context_availability": packet["availability"]})
    columns = [*KEY, *CONTEXT_COLUMNS]
    return pd.DataFrame(output) if output else pd.DataFrame(columns=columns)


def validate_context_rows_v1(rows, *, pair_sha256=None):
    """Known values may never be silently filled; typed unknowns keep their source rows."""
    if not set(CONTEXT_COLUMNS).issubset(rows) or len(rows) > 30500:
        raise ValueError("context schema or original row budget differs")
    if not rows.context_availability.isin(["AVAILABLE", "UNKNOWN_CONTEXT"]).all():
        raise ValueError("unknown context status differs")
    known = rows.context_availability.eq("AVAILABLE")
    values = rows.loc[known, RANK_COLUMNS].map(_number)
    if (values.isna().any().any() or not values.map(lambda v: isfinite(v) and 0 <= v <= 1).all().all()
            or values.iloc[:, 1].gt(values.iloc[:, 0]).any()
            or rows.loc[~known, RANK_COLUMNS].notna().any().any()
            or pair_sha256 is not None and not rows.context_pair_sha256.eq(pair_sha256).all()):
        raise ValueError("known/unknown rank coordinates or frozen pair differ")
    for row in rows.loc[known, [*RANK_COLUMNS, "strongest_rank_fraction", "consensus_rank_fraction"]].itertuples(index=False, name=None):
        if any(float(Fraction(f)) != value for value, f in zip(row[:2], row[2:], strict=True)):
            raise ValueError("original rational ranks differ from model coordinates")
    if rows.context_sha256.isna().any() or rows.context_pair_sha256.nunique() > 1:
        raise ValueError("frozen context identities are missing or mixed")


def build_consensus_training_encoding_v1(*, rows, parent_encoding, plan):
    """Same unique supervision for both arms; keep only the original train-only 18D medians."""
    dates = {n: getattr(plan, n).isoformat() for n in ("train_start", "train_end", "estimation_first_D", "evaluation_start", "evaluation_end")}
    if (parent_encoding.get("matrix_order") != list(BASE_ORDER)
            or any(parent_encoding.get(k) != v for k, v in dates.items())
            or len(parent_encoding.get("medians", ())) != 9
            or any(_number(v) is None for v in parent_encoding["medians"])):
        raise ValueError("original train-only median provenance differs")
    validate_context_rows_v1(rows)
    d, h = rows[KEY[0]], pd.to_datetime(rows.label_information_end)
    temporal = d.between(pd.Timestamp(plan.train_start), pd.Timestamp(plan.train_end)) & h.le(pd.Timestamp(plan.train_end)) & h.lt(pd.Timestamp(plan.evaluation_start))
    projected = rows.loc[temporal].copy()
    fields = [*FEATURES, "reference_cny", "reference_visible_through", "terminal_mark_ratio", "path_mark_min_ratio",
        "mark_label_status", "mark_policy_sha256", "label_information_end", *CONTEXT_COLUMNS]
    if projected.groupby("label_cluster")[fields].nunique(dropna=False).gt(1).any().any():
        raise ValueError("same economic cluster has contradictory D inputs, context or paired target")
    _, basic = feature_values(projected)
    available = projected.mark_label_status.eq("AVAILABLE")
    target = projected.loc[available, ["terminal_mark_ratio", "path_mark_min_ratio"]].map(lambda v: _number(v, positive=True))
    reference = projected.reference_cny.map(lambda v: _number(v, positive=True))
    if (target.isna().any().any() or target.iloc[:, 1].gt(target.iloc[:, 0]).any()
            or not projected.loc[available, "mark_policy_sha256"].eq(MARK_POLICY_SHA256).all()
            or (available & reference.isna()).any()
            or not pd.to_datetime(projected.loc[reference.notna(), "reference_visible_through"]).le(projected.loc[reference.notna(), KEY[0]]).all()):
        raise ValueError("known mark/reference clock or paired supervision differs")
    selected = projected.loc[available.to_numpy() & basic & projected.context_availability.eq("AVAILABLE").to_numpy()]
    rows["consensus_training_role"] = "NOT_TRAIN_SUPERVISION"
    rows.loc[selected.index, "consensus_training_role"] = "PURGED_LABEL_OVERLAP"
    first = pd.Timestamp(plan.estimation_first_D)
    rows.loc[selected.index[selected[KEY[0]].ge(first)], "consensus_training_role"] = "ESTIMATION"
    rows.loc[selected.index[selected[KEY[0]].lt(first) & pd.to_datetime(selected.label_information_end).lt(first)], "consensus_training_role"] = "STRUCTURE"
    roles = {role: rows.loc[rows.consensus_training_role.eq(role)].drop_duplicates("label_cluster").sort_values(list(KEY))
             for role in ("STRUCTURE", "ESTIMATION")}
    if sum(len(v) for v in roles.values()) > 21400:
        raise ValueError("unique training cluster budget exceeds the preregistered limit")
    varying = roles["STRUCTURE"].groupby("strongest_rank_fraction").consensus_rank_fraction.nunique().gt(1).any()
    identifiable = all(len(v) for v in roles.values()) and varying
    role_clusters = {role: block.label_cluster.tolist() for role, block in roles.items()}
    return dict(**dates, matrix_order=list(BASE_ORDER), medians=list(parent_encoding["medians"]),
        parent_median_sha256=sha(dict(medians=parent_encoding["medians"], dates=dates, order=list(BASE_ORDER))),
        median_population=parent_encoding.get("median_population"), role_clusters=role_clusters,
        role_sha256=sha(role_clusters), pools={k: len(v) for k, v in roles.items()},
        context_pair_sha256=rows.context_pair_sha256.iloc[0] if len(rows) else None,
        context_population_sha256=sha(sorted(set(rows.context_sha256))), physical_fit_count=0,
        contrast_status="PREPARED_IDENTIFIABLE_NO_FIT" if identifiable else "NOT_IDENTIFIABLE_CONSENSUS_CONTRAST",
        estimation_consensus_support=dict(known=len(roles["ESTIMATION"]),
            positive=int(roles["ESTIMATION"].cross_package_consensus_strength.gt(0).sum())))


def verified_consensus_parent_v1(plan):
    """Verify one immutable prepared stage/chain once, before projected financial decoding."""
    refs = (plan.parent_manifest_ref, plan.parent_rows_ref, plan.parent_days_ref, plan.calendar_ref, plan.encoding_ref, plan.daily_ref)
    root = node_path(plan.parent_manifest_ref.artifact_uri).parent
    initial = read_stage(root.parent/"preregistered", stage="preregistered", plan_sha256=plan.parent_plan_sha256, parent_sha256=None)
    if initial["stage_sha256"] != plan.parent_preregistered_stage_sha256:
        raise ValueError("original parent preregistration differs")
    header = read_stage(root, stage="prepared", plan_sha256=plan.parent_plan_sha256,
                        parent_sha256=plan.parent_preregistered_stage_sha256)
    if header["stage_sha256"] != plan.parent_stage_sha256:
        raise ValueError("original prepared stage differs")
    # read_stage already checks each bound file once. Compare explicit refs to those
    # verified descriptors, rather than hashing the entire snapshot a second time.
    for ref in refs:
        path = node_path(ref.artifact_uri)
        actual = dict(sha256=file_sha256(path), size_bytes=path.stat().st_size) if path.name == "manifest.json" else header["files"].get(path.name)
        if actual != dict(sha256=ref.sha256, size_bytes=ref.size_bytes):
            raise ValueError("explicit prepared input reference hash/size differs")
    original = json.loads((root.parent/"preregistered"/"plan.json").read_bytes())
    for source in plan.sources:
        peer = [s for s in original["sources"] if s["package_id"] == source.package_id]
        if len(peer) != 1 or any(peer[0][n] != getattr(source, n) for n in ("manifest_sha256", "run_id", "source_id")):
            raise ValueError("original prepared source chain identity differs")
    roster = ds.dataset(root/"rows.parquet", format="parquet").to_table(columns=list(SOURCE_COLUMNS)).to_pandas()
    days = ds.dataset(root/"days.parquet", format="parquet").to_table().to_pandas()
    calendar = json.loads((root/"calendar.json").read_bytes())
    expected = [pd.Timestamp(v) for v in calendar if plan.train_start <= pd.Timestamp(v).date() <= plan.train_end
                or plan.evaluation_start <= pd.Timestamp(v).date() <= plan.evaluation_end]
    for source in plan.sources:
        if sorted(days.loc[days.package_id.eq(source.package_id), "decision_date"].tolist()) != expected:
            raise ValueError("an original source decision day is missing or foreign")
    pair = [s.model_dump(mode="json") | dict(artifact_sha256=plan.parent_rows_ref.sha256,
            original_source_stage_sha256=plan.parent_stage_sha256, semantic_clock="D_FROZEN_BACKTEST", pool_status="UNKNOWN") for s in plan.sources]
    context = build_cross_package_rank_context_v1(roster_metadata=roster, day_metadata=days, calendar=calendar, package_pair=pair)
    return root, header, roster, days, calendar, context


def prepare_consensus_inputs_v1(*, plan):
    root, header, roster, days, calendar, context = verified_consensus_parent_v1(plan)
    # Metadata and full lists are complete before any train target is decoded.
    public_columns = [*SOURCE_COLUMNS, *FEATURES, "reference_cny", "reference_visible_through", "label_information_end"]
    public = ds.dataset(root/"rows.parquet", format="parquet").to_table(columns=public_columns).to_pandas()
    if not public.loc[:, SOURCE_COLUMNS].equals(roster):
        raise ValueError("projection changed original row order or source keys")
    public = public.merge(context.loc[:, [*KEY, *CONTEXT_COLUMNS]], on=list(KEY), how="left", validate="many_to_one", sort=False)
    sessions = [pd.Timestamp(v) for v in calendar]
    positions = {d: i for i, d in enumerate(sessions)}
    expected_h = public[KEY[1]].map(lambda t: sessions[positions[t]+4] if positions[t]+4 < len(sessions) else pd.NaT)
    if not (public.label_information_end.eq(expected_h) | public.label_information_end.isna() & expected_h.isna()).all():
        raise ValueError("original label horizon contradicts the original five-session calendar")
    for day in days.itertuples(index=False):
        pos = positions[day.decision_date]
        h = sessions[pos+5] if pos+5 < len(sessions) else pd.NaT
        if not (day.horizon_end == h or pd.isna(day.horizon_end) and pd.isna(h)):
            raise ValueError("original day horizon contradicts the frozen calendar")
    predicate = ((ds.field(KEY[0]) >= pd.Timestamp(plan.train_start)) & (ds.field(KEY[0]) <= pd.Timestamp(plan.train_end))
                 & (ds.field("label_information_end") <= pd.Timestamp(plan.train_end))
                 & (ds.field("label_information_end") < pd.Timestamp(plan.evaluation_start)))
    labels = ds.dataset(root/"rows.parquet", format="parquet").to_table(filter=predicate,
        columns=[*SOURCE_COLUMNS, "label_information_end", "terminal_mark_ratio", "path_mark_min_ratio", "mark_label_status", "mark_policy_sha256"]).to_pandas()
    joined = public.merge(labels.drop(columns=["label_information_end"]), on=list(SOURCE_COLUMNS), how="left", validate="one_to_one", sort=False)
    joined["mark_policy_sha256"] = joined.mark_policy_sha256.fillna(MARK_POLICY_SHA256)
    joined["mark_label_status"] = joined.mark_label_status.fillna("IMMATURE")
    evaluation = joined[KEY[0]].between(pd.Timestamp(plan.evaluation_start), pd.Timestamp(plan.evaluation_end))
    joined.loc[evaluation, "mark_label_status"] = "NOT_EVALUATED_D_INPUT_ONLY"
    joined["label_cluster"] = [sha([d.date().isoformat(), t.date().isoformat(), s, MARK_POLICY_SHA256])
        for d, t, s in joined.loc[:, KEY].itertuples(index=False, name=None)]
    if joined.groupby(list(KEY))[[*FEATURES, "reference_cny", "reference_visible_through", *CONTEXT_COLUMNS]].nunique(dropna=False).gt(1).any().any():
        raise ValueError("original shared D input/context/reference conflicts between packages")
    encoding = build_consensus_training_encoding_v1(rows=joined, parent_encoding=json.loads((root/"encoding.json").read_bytes()), plan=plan)
    return joined, days, calendar, encoding, context, header
