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
