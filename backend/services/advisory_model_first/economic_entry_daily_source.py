"""Consume exact frozen research rows and explicit PIT prices without reselecting."""

from __future__ import annotations

import json
import io
import hashlib
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from contextlib import contextmanager
from pathlib import Path

import pandas as pd
import numpy as np

from backend.services.advisory_model_first.economic_entry_daily_contracts import EconomicEntryDailyInputV1, EconomicEntryCandidateProjectionV1
from backend.services.advisory_model_first.economic_entry_daily_inference import daily_decision_feature_values_v1
from backend.services.advisory_model_first.economic_entry_contracts import EconomicEntryInputIdentityV1
from backend.services.advisory_model_first.economic_entry_labels import KEY, _day, _fail
from backend.services.advisory_model_first.economic_risk_alignment_inference import price_context_sha256
from backend.services.advisory_model_first.economic_entry_pipeline import _parquet_bytes, file_sha256
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


def _economic_native_archive_v1(*, version, selection, program_id, binding_version_id, projection, read_at):
    from backend.services.selection_center.advisory_input_archive import validate_archive_reference
    from backend.services.advisory_model_first.economic_entry_pipeline import _verify_reference
    from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1
    from backend.services.advisory_model_first.model_inference import _resolve_decision_date
    summary = version.get("summary_json") or {}
    reference = summary.get("advisory_frozen_input_archive")
    if not isinstance(reference, dict) or reference != selection.runtime_config.get("advisory_frozen_input_archive"):
        _fail("economic native run/list lacks one identical original archive reference")
    original = EvidenceReferenceV1.model_validate(reference["reference"])
    if original.role != "SELECTION_EXECUTION_INPUTS":
        _fail("economic native archive reference has a different evidence role")
    path = Path(original.artifact_uri)
    if not path.is_absolute() or path.resolve() != path.absolute() or path.drive.upper() == "C:":
        _fail("economic native archive must be its explicit non-C nonredirected original path")
    _verify_reference(original)
    payload = validate_archive_reference(reference, run=selection, program_id=program_id,
        binding_version_id=binding_version_id, review_policy_sha256=projection.review_policy_sha256)
    decision = _resolve_decision_date(list_version=version, selection_run=selection)
    members = payload.get("full_source_universe_members")
    if (payload.get("schema_version") != "advisory_selection_input_archive_v1"
            or payload.get("historical_capture_backfilled") is not False
            or payload.get("decision_as_of_trade_date") != decision.isoformat()
            or not isinstance(members, list) or not members
            or any(not isinstance(value, str) or re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", value) is None for value in members)
            or members != sorted(set(members))
            or canonical_json_sha256(members) != payload.get("source_universe_members_sha256")):
        _fail("economic native archive lacks exact original D and complete source membership")
    observed = datetime.fromisoformat(str(payload.get("observed_at") or ""))
    published = datetime.fromisoformat(str(version.get("created_at") or ""))
    if observed.utcoffset() is None or published.utcoffset() is None or observed > published or published > read_at:
        _fail("economic native archive or list publication time is unproven or in the future")
    inputs = payload.get("package_inputs") or {}
    if set(inputs) != {projection.scope.package_id} or set(reference.get("package_artifact_hashes") or {}) != set(inputs):
        _fail("economic native archive package partition differs")
    for package_id, item in inputs.items():
        artifact = item.get("artifact") or {}
        content_hash = canonical_json_sha256(artifact)
        context = (artifact.get("metadata") or {}).get("artifact_input_context") or {}
        if (content_hash != item.get("artifact_content_sha256") or content_hash != reference["package_artifact_hashes"][package_id]
                or artifact.get("package_id") != package_id or artifact.get("manifest_sha256") != projection.scope.manifest_sha256
                or artifact.get("trade_date") != selection.trade_date.isoformat() or artifact.get("data_source") != selection.data_source
                or artifact.get("status") != "SUCCEEDED" or artifact.get("universe_count") != len(members)
                or context.get("cutoff_date") != decision.isoformat() or context.get("score_trade_date") != decision.isoformat()
                or context.get("requested_trade_date") != selection.trade_date.isoformat()
                or context.get("universe_input_hash") != payload["source_universe_members_sha256"]
                or canonical_json_sha256(context) != artifact.get("artifact_input_context_hash")):
            _fail("economic native package artifact, D-1, source members or hash differs")
    for candidate in selection.aggregate_results:
        if candidate.symbol not in members:
            _fail("economic frozen candidate is outside its original complete source members")
        for name in ("selection_entry_price_time", "current_price_time"):
            value = getattr(candidate, name, None)
            if value and _day(str(value)[:10]).date() > decision:
                _fail("economic native candidate includes a price clock after D")
    return decision, payload, original


class EconomicEntryReadonlyCandidateSourceV1:
    """Read native frozen inputs only; never ensure a binding, reselect or capture old archives."""

    def __init__(self, *, read_session, pit_universe_key, index_membership_reader=None, now=None):
        if not isinstance(pit_universe_key, str) or not pit_universe_key.strip():
            _fail("economic native input needs its explicit profile-owned PIT key")
        self._session, self._key = read_session, pit_universe_key
        self._index_members = index_membership_reader or read_economic_index_members_v1
        self._now = now or (lambda: datetime.now(timezone.utc))

    def load_day(self, *, program_id, binding_version_id, target_date, projection, frozen_list_id=None):
        from backend.services.advisory_program import AdvisoryProgramPGRepository, list_version_to_dict, list_item_to_dict
        from backend.services.selection_center.repository import SelectionCenterRepository
        from backend.services.selection_center.models import SelectionRunStatus
        from backend.services.selection_center.canonical_pit_runtime import require_canonical_pit_runtime_binding, require_canonical_pit_generation_current
        from backend.services.canonical_equity_pit import CanonicalPitAuthorityResolver, PitAuthorityStatus
        from backend.services.advisory_model_first.model_inference import _validate_review_policy_identity
        projection = EconomicEntryCandidateProjectionV1.model_validate(projection.model_dump())
        target, observed = _day(target_date).date(), self._now()
        if (any(not isinstance(value, str) or not value.strip() for value in (program_id, binding_version_id))
                or (frozen_list_id is not None and (not isinstance(frozen_list_id, str) or not frozen_list_id.strip()))
                or not isinstance(observed, datetime) or observed.utcoffset() is None):
            _fail("economic native input requires original program/binding and a real aware read clock")
        with self._session.connection() as connection:
            @contextmanager
            def pinned_connection():
                self._session.budget.check()
                yield connection
                self._session.budget.check()
            programs = AdvisoryProgramPGRepository(conn_factory=pinned_connection)
            program = programs.get_program(program_id)
            binding = programs.get_active_binding_version(program_id)
            universe = projection.universe_selection.model_dump(mode="json")
            if (binding is None or binding.binding_version_id != binding_version_id or binding.program_id != program_id
                    or binding.activation_status != "ACTIVE" or program.program_id != program_id or binding.effective_from_trade_date is None
                    or target < binding.effective_from_trade_date or (binding.effective_to_trade_date is not None and target >= binding.effective_to_trade_date)
                    or list(binding.package_ids) != [projection.scope.package_id] or list(program.package_ids) != [projection.scope.package_id]
                    or (binding.runtime_config_json or {}).get("universe_selection") != universe
                    or program.review_policy_sha256 != projection.review_policy_sha256 or program.target_count != 20):
                _fail("economic native program/binding/package/universe/review scope differs")
            version_object = programs.get_list_version(frozen_list_id) if frozen_list_id else programs.list_version_for_date(program_id, target, status="PUBLISHED")
            if version_object is None:
                _fail("economic native target lacks an already published frozen list", "ADVISORY_ECONOMIC_FROZEN_LIST_NOT_READY")
            version = list_version_to_dict(version_object)
            if (version.get("program_id") != program_id or version.get("binding_version_id") != binding_version_id
                    or version.get("target_trade_date") != target.isoformat() or version.get("version_status") != "PUBLISHED"):
                _fail("economic native list program/binding/target/publication differs")
            items = [list_item_to_dict(value) for value in programs.list_version_items(version["list_version_id"])]
            with connection.cursor() as cursor:
                cursor.execute("""SELECT review_run_id, program_id, binding_version_id, trade_date, selection_run_id, selection_run_ids
                                    FROM app.advisory_review_run WHERE review_run_id = %s""", (version["review_run_id"],))
                review = cursor.fetchone()
            if (review is None or tuple(review[:4]) != (version["review_run_id"], program_id, binding_version_id, target)
                    or not review[4] or tuple(review[5] or ()) != (review[4],)):
                _fail("economic native persisted review identity differs from its original list")
            selection = SelectionCenterRepository(conn_factory=pinned_connection).get_run(review[4])
            if (selection.run_id != review[4] or selection.trade_date != target or selection.package_ids != [projection.scope.package_id]
                    or selection.manifest_sha256_by_package != {projection.scope.package_id: projection.scope.manifest_sha256}
                    or selection.status not in (SelectionRunStatus.SUCCEEDED, SelectionRunStatus.VALID_NO_CANDIDATE)
                    or (selection.status == SelectionRunStatus.SUCCEEDED and selection.valid_no_candidate is not False)
                    or (selection.status == SelectionRunStatus.VALID_NO_CANDIDATE
                        and (selection.valid_no_candidate is not True or selection.aggregate_results or not selection.no_candidate_reason))):
                _fail("economic native Selection package/manifest/status differs")
            decision, archive, archive_ref = _economic_native_archive_v1(version=version, selection=selection,
                program_id=program_id, binding_version_id=binding_version_id, projection=projection, read_at=observed)
            from backend.services.advisory_model_first.realtime_feature_source import PostgresRealtimeFeatureSource
            with connection.cursor() as cursor:
                calendar = PostgresRealtimeFeatureSource._trading_calendar(cursor, start_date=decision, end_date=target)
            if tuple(calendar.date) != (decision, target):
                _fail("economic native target is not the authoritative next trading day")
            lease = require_canonical_pit_runtime_binding(selection.runtime_config, trade_date=decision)
            if lease.universe_key != self._key or lease.authority_status != PitAuthorityStatus.ACTIVE_CANONICAL:
                _fail("economic native Selection PIT key differs from explicit consumer data identity")
            resolver = CanonicalPitAuthorityResolver(connection_factory=pinned_connection)
            # This verifies the lease against the same repeatable-read snapshot.
            # Repeating it here cannot prove freshness outside that snapshot.
            require_canonical_pit_generation_current(selection.runtime_config, authority_resolver=resolver)
            rows, membership, limitations = _economic_native_pool_rows_v1(selection=selection, items=items, version=version,
                archive=archive, universe=universe, decision=decision, target=target, index_reader=self._index_members,
                pinned_connection=pinned_connection, pit_universe_key=self._key, pit_rule_version=lease.rule_version)
            candidates = project_economic_frozen_candidate_roster_v1(rows=rows, decision_date=decision, target_date=target,
                component_roles=projection.component_roles.model_dump(), terminal_weights=projection.terminal_weights,
                candidate_group_size=len(rows))
            _validate_review_policy_identity(list_items=items, expected_symbols=candidates.instrument.tolist(),
                review_policy_sha256=projection.review_policy_sha256)
            # The injected repositories and quote source share this same snapshot.
            class PinnedSession:
                budget = self._session.budget
                connection = staticmethod(pinned_connection)
            source = EconomicEntryReadonlyDSourceV1(read_session=PinnedSession(), pit_universe_key=self._key).load_day(
                candidates=candidates, decision_date=decision, target_date=target, component_roles=projection.component_roles.model_dump())
        completed_at = self._now()
        if not isinstance(completed_at, datetime) or completed_at.utcoffset() is None or completed_at < observed:
            _fail("economic native source completion clock moved backwards or lost its timezone")
        return {**source, "candidates": candidates, "program_id": program_id, "binding_version_id": binding_version_id,
            "trading_calendar": source["trading_calendar"] or (decision, target),
            "selection_run_id": selection.run_id, "list_version_id": version["list_version_id"], "review_run_id": version["review_run_id"],
            "decision_date": decision, "target_date": target, "captured_at": completed_at, "source_members": membership,
            "candidate_receipt": {"kind": "ORIGINAL_NATIVE_ARCHIVE_READ", "original_archive_reference": archive_ref.model_dump(mode="json"),
                "original_observed_at": archive["observed_at"], "projection_sha256": projection.projection_sha256,
                "read_started_at": observed.isoformat(), "read_completed_at": completed_at.isoformat(),
                "pit_generation_checked_at": "READONLY_SNAPSHOT_NOT_FRESH_POST_READ",
                "pit_universe_key": lease.universe_key, "pit_rule_version": lease.rule_version,
                "source_universe_members_sha256": archive["source_universe_members_sha256"],
                "admitted_universe_members_sha256": canonical_json_sha256(membership),
                "historical_vintage_proven": archive.get("historical_vintage_proven") is True,
                "source_evidence_limitations": limitations, "model_scope_qualified": False,
                "native_receipt_created": False, "new_selection_runs": 0, "outcomes_read": False}}


def _economic_native_pool_rows_v1(*, selection, items, version, archive, universe, decision, target,
                                 index_reader, pinned_connection, pit_universe_key, pit_rule_version=None):
    from backend.services.advisory_model_first.model_inference import _candidate_rows_for_recommendation_list
    receipt = (version.get("summary_json") or {}).get("advisory_universe_receipt") or {}
    if (receipt.get("universe_selection") != universe or receipt.get("trade_date") != target.isoformat()
            or receipt.get("universe_as_of_trade_date") != decision.isoformat()
            or receipt.get("schema_version") != "advisory_universe_admission_receipt_v1"
            or receipt.get("admission_stage") != "AFTER_SELECTION_BEFORE_ADVISORY_RANKING"
            or any(type(receipt.get(name)) is not int for name in ("input_candidate_count", "output_candidate_count", "excluded_candidate_count"))
            or receipt["input_candidate_count"] != len(selection.aggregate_results)
            or not 0 <= receipt["output_candidate_count"] <= receipt["input_candidate_count"]
            or receipt["excluded_candidate_count"] != receipt["input_candidate_count"] - receipt["output_candidate_count"]
            or (selection.runtime_config.get("advisory_universe_receipt") is not None
                and selection.runtime_config["advisory_universe_receipt"] != receipt)):
        _fail("economic native list universe/D identity differs")
    expected_count = min(20, receipt["output_candidate_count"])
    active = [item for item in items if item.get("action") != "EXIT"]
    keys = [(item.get("symbol"), item.get("rank")) for item in active]
    if any(not isinstance(symbol, str) or type(rank) is not int or rank < 1 for symbol, rank in keys):
        _fail("economic native list candidates contain malformed or duplicate symbol/rank identities")
    current_keys = [(symbol, rank) for symbol, rank in keys if rank <= expected_count]
    if (len({symbol for symbol, _ in current_keys}) != len(current_keys)
            or len({rank for _, rank in current_keys}) != len(current_keys)):
        _fail("economic native Top20 list candidates duplicate symbol/rank identities")
    if universe["mode"] == "stock_universe":
        if receipt["output_candidate_count"] != receipt["input_candidate_count"]:
            _fail("economic stock universe receipt cannot silently filter original candidates")
        members, rows, limitations = archive["full_source_universe_members"], selection.aggregate_results, ()
    else:
        if index_reader is None:
            _fail("economic native index pool requires explicit readonly full membership readback; no legacy fallback")
        if not isinstance(pit_rule_version, str) or not pit_rule_version.strip():
            _fail("economic native index members need their explicit PIT rule version")
        snapshot = index_reader(universe_selection=universe, decision_date=decision,
            connection_factory=pinned_connection, pit_universe_key=pit_universe_key, pit_rule_version=pit_rule_version)
        members = snapshot.get("members")
        if (snapshot.get("universe_selection") != universe or snapshot.get("decision_date") != decision.isoformat()
                or snapshot.get("pit_universe_key") != pit_universe_key
                or snapshot.get("pit_rule_version") != pit_rule_version or snapshot.get("complete") is not True
                or not isinstance(members, list)
                or any(not isinstance(value, str) or re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", value) is None for value in members)
                or members != sorted(set(members))
                or canonical_json_sha256(members) != receipt.get("symbol_set_sha256")):
            _fail("economic native index members differ from the original admission hash")
        rows = _candidate_rows_for_recommendation_list(selection.aggregate_results,
            [item for item in active if item["rank"] <= expected_count])
        limitations = ("INDEX_MEMBERS_READ_BACK_NOW_NOT_ORIGINAL_FULL_INDEX_MEMBER_CAPTURE",)
    if any(row.symbol not in members for row in rows):
        _fail("economic native frozen candidate is outside the exact admitted universe")
    selected = [row for row in rows if row.rank <= expected_count]
    if (len(selected) != expected_count or len(current_keys) != expected_count
            or sorted((row.symbol, row.rank) for row in selected) != sorted(current_keys)):
        _fail("economic native roster does not preserve the complete admitted Top20")
    return selected, members, limitations


def read_economic_index_members_v1(*, universe_selection, decision_date, connection_factory,
                                  pit_universe_key, pit_rule_version):
    """Public core-index resolution with the explicit native lease, never fallback."""
    from backend.services.core_index_membership import (
        CoreIndexMembershipRepository, CanonicalEquityInterval, UniverseSelection, resolve_universe,
    )
    from backend.services.advisory_model_first.entry_price_contracts import EntryUniverseSelection
    selection = EntryUniverseSelection.model_validate(universe_selection).model_dump(mode="json")
    if (selection["mode"] == "stock_universe" or not isinstance(pit_universe_key, str) or not pit_universe_key.strip()
            or not isinstance(pit_rule_version, str) or not pit_rule_version.strip()):
        _fail("economic index read needs its explicit native key/rule and nonempty core-index definition")
    class ExplicitLeaseRepository(CoreIndexMembershipRepository):
        def fetch_canonical_intervals(self, start_date, end_date):
            with self._connection_factory() as connection:
                with connection.cursor() as cursor:
                    cursor.execute("""SELECT ts_code, eligible_start, eligible_end FROM market.stock_universe_pit_spans
                                       WHERE universe_key = %s AND rule_version = %s AND eligible_start <= %s AND eligible_end >= %s
                                       ORDER BY ts_code, eligible_start, eligible_end""",
                                   (pit_universe_key, pit_rule_version, end_date, start_date))
                    rows = cursor.fetchall()
            return tuple(CanonicalEquityInterval(ts_code=row[0], eligible_start=row[1], eligible_end=row[2]) for row in rows)
    result = resolve_universe(UniverseSelection.from_mapping(selection), decision_date, decision_date,
        repository=ExplicitLeaseRepository(connection_factory))
    return {"universe_selection": selection, "decision_date": decision_date.isoformat(),
        "members": sorted({row.ts_code for row in result.intervals if row.eligible_start <= decision_date <= row.eligible_end}),
        "pit_universe_key": pit_universe_key, "pit_rule_version": pit_rule_version,
        "membership_revision": result.membership_revision, "complete": True}


def _validate_candidate_projection(candidates, decision, target, component_roles):
    """Reject contradictory input before any DB access, including bool ranks."""
    required = {*KEY, "combined_score", "candidate_group_size", "selection_effective_rank"}
    if (not isinstance(component_roles, dict) or set(component_roles) != {"lstm", "fund"}
            or any(not isinstance(value, str) or not value.strip() for value in component_roles.values())
            or len(set(component_roles.values())) != 2):
        _fail("economic feature adapter requires the real frozen two-leg role mapping")
    required.update(f"norm__{value}" for value in component_roles.values())
    if (not isinstance(candidates, pd.DataFrame) or len(candidates) > 20
            or not required.issubset(candidates.columns) or candidates.duplicated(KEY).any()
            or candidates.instrument.duplicated().any()
            or not candidates.instrument.map(lambda value: isinstance(value, str) and re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", value) is not None).all()
            or not pd.to_datetime(candidates[KEY[0]], errors="coerce").eq(decision).all()
            or not pd.to_datetime(candidates[KEY[1]], errors="coerce").eq(target).all()
            or decision >= target):
        _fail("economic D reader requires an exact bounded frozen candidate roster")
    rank = pd.to_numeric(candidates.selection_effective_rank, errors="coerce")
    groups = pd.to_numeric(candidates.candidate_group_size, errors="coerce")
    if any(candidates[name].map(lambda value: isinstance(value, (bool, np.bool_))).any()
            for name in ("combined_score", *(f"norm__{value}" for value in component_roles.values()))):
        _fail("economic frozen scores cannot be boolean placeholders")
    if (candidates.selection_effective_rank.map(lambda value: isinstance(value, (bool, np.bool_))).any()
            or candidates.candidate_group_size.map(lambda value: isinstance(value, (bool, np.bool_))).any()
            or rank.isna().any() or not rank.between(1, 20).all() or not rank.mod(1).eq(0).all()
            or rank.duplicated().any() or (groups.notna() & (~np.isfinite(groups) | groups.lt(rank) | ~groups.mod(1).eq(0))).any()):
        _fail("economic frozen rank/group projection is contradictory")
    return rank, groups


def project_economic_frozen_candidate_roster_v1(*, rows, decision_date, target_date,
                                              component_roles, terminal_weights, candidate_group_size):
    """Project real frozen scores without a fabricated ranking/M4 parent bundle.

    Caller must first verify the original run/list/archive and the trained
    projection contract. This pure adapter creates neither receipts nor model
    qualification; it performs no filtering, reranking or score imputation.
    """
    if (not isinstance(component_roles, dict) or set(component_roles) != {"lstm", "fund"}
            or not all(isinstance(value, str) and value.strip() for value in component_roles.values()) or len(set(component_roles.values())) != 2
            or not isinstance(terminal_weights, dict) or set(terminal_weights) != set(component_roles.values())
            or any(isinstance(value, (bool, np.bool_)) or not isinstance(value, (float, int, np.integer, np.floating))
                   or not np.isfinite(value) or value <= 0 for value in terminal_weights.values())
            or not np.isclose(sum(terminal_weights.values()), 1., rtol=0., atol=1e-10)
            or type(candidate_group_size) is not int or not 0 <= candidate_group_size <= 20
            or len(rows) != candidate_group_size):
        _fail("economic frozen projection needs its actual two-leg weights and original Top20 group size")
    decision, target = _day(decision_date), _day(target_date)
    columns = [*KEY, "selection_effective_rank", "candidate_group_size", "combined_score",
               *(f"norm__{value}" for value in component_roles.values())]
    payload = []
    for row in rows:
        scores = row.component_scores
        if not isinstance(scores, dict):
            _fail("economic frozen candidate lacks original component scores")
        normalized = {}
        for component_id in component_roles.values():
            leg = scores.get(component_id)
            if not isinstance(leg, dict) or not {"normalized_score", "weight"}.issubset(leg):
                _fail("economic frozen candidate lacks a real frozen component leg")
            value, weight = leg["normalized_score"], leg["weight"]
            if (any(isinstance(number, (bool, np.bool_)) or not isinstance(number, (float, int, np.integer, np.floating))
                    or not np.isfinite(number) for number in (value, weight))
                    or not np.isclose(weight, terminal_weights[component_id], rtol=0., atol=1e-10)):
                _fail("economic frozen candidate leg weight or score differs from trained projection")
            normalized[component_id] = value
        combined = sum(normalized[key] * terminal_weights[key] for key in normalized)
        if (isinstance(row.score, (bool, np.bool_)) or not isinstance(row.score, (float, int, np.integer, np.floating))
                or not np.isfinite(row.score) or not np.isclose(row.score, combined, rtol=0., atol=1e-8)):
            _fail("economic frozen combined score differs from the original weighted legs")
        payload.append({KEY[0]: decision, KEY[1]: target, KEY[2]: row.symbol,
            "selection_effective_rank": row.rank, "candidate_group_size": candidate_group_size,
            "combined_score": row.score, **{f"norm__{key}": value for key, value in normalized.items()}})
    frame = pd.DataFrame(payload, columns=columns)
    rank, _ = _validate_candidate_projection(frame, decision, target, component_roles)
    if list(rank) != list(range(1, candidate_group_size + 1)):
        _fail("economic frozen projection must preserve the complete original Top20 order")
    return frame


def build_economic_D_features_v1(*, candidates, candidate_daily, market_daily, benchmark_daily,
                               suspend_rows, trading_calendar, decision_date, component_roles):
    """Eight-field consumer adapter; no full ranking/HMM/M4 feature-coverage gate.

    Reuses the trained Advisory bar/technical/breadth/benchmark calculations.
    Source/projection and model applicability must be verified before production
    use; this pure result never manufactures a native receipt or model identity.
    """
    from backend.services.advisory_model_first.shared_feature_builder import (
        _build_benchmark_features, _build_instrument_features, _build_market_features,
    )
    from backend.services.advisory_model_first.suspension_aware_bar_policy import build_suspension_aware_bar_panel
    from backend.services.advisory_model_first.economic_entry_contracts import ECONOMIC_FEATURE_NAMES
    decision = _day(decision_date)
    calendar = pd.DatetimeIndex([_day(value) for value in trading_calendar])
    targets = pd.to_datetime(candidates[KEY[1]], errors="coerce") if KEY[1] in candidates else pd.Series(dtype="datetime64[ns]")
    if len(candidates) and targets.nunique() != 1:
        _fail("economic frozen candidates do not share one target date")
    target = targets.iloc[0] if len(targets) else decision + pd.Timedelta(days=1)
    rank, groups = _validate_candidate_projection(candidates, decision, target, component_roles)
    if (calendar.empty or len(calendar) > 80 or not calendar.is_unique or not calendar.is_monotonic_increasing
            or calendar[-1] != decision):
        _fail("economic feature adapter needs a bounded D-only market calendar")
    for name, frame, maximum in (("candidate", candidate_daily, 1600), ("market", market_daily, 800000),
                                 ("benchmark", benchmark_daily, 800)):
        if len(frame) > maximum or not isinstance(frame.index, pd.MultiIndex) or frame.index.names != ["datetime", "instrument"]:
            _fail(f"economic {name} frame exceeds its bounded datetime/instrument contract")
        dates = pd.DatetimeIndex(frame.index.get_level_values("datetime"))
        if frame.index.has_duplicates or (dates < calendar[0]).any() or (dates > decision).any():
            _fail(f"economic {name} source has duplicate or foreign-time rows")
    if set(candidate_daily.index.get_level_values("instrument")) - set(candidates.instrument):
        _fail("economic daily bar source contains foreign candidates")
    if (not {"trade_date", "instrument", "suspend_type"}.issubset(suspend_rows.columns)
            or not set(suspend_rows.instrument).issubset(set(candidates.instrument))
            or (pd.to_datetime(suspend_rows.trade_date) > decision).any()):
        _fail("economic suspension source has foreign candidates or future dates")
    # Same parent normalization as shared_feature_builder, with the original
    # group size, never the count of a filtered Top20 or accepted price nodes.
    base = candidates[KEY].copy()
    base["parent_combined_score"] = pd.to_numeric(candidates.combined_score, errors="coerce")
    base["parent_rank_pct"] = 1. - (rank - 1) / (groups - 1).clip(lower=1)
    base["leg_norm_score_gap"] = (pd.to_numeric(candidates[f"norm__{component_roles['lstm']}"], errors="coerce")
                                  - pd.to_numeric(candidates[f"norm__{component_roles['fund']}"], errors="coerce"))
    for field in ECONOMIC_FEATURE_NAMES[3:-1]:
        base[field] = np.nan
    missing = []
    for row_index, symbol in zip(base.index, base.instrument, strict=True):
        raw = candidate_daily.loc[candidate_daily.index.get_level_values("instrument") == symbol]
        suspends = suspend_rows.loc[suspend_rows.instrument.eq(symbol)]
        if raw.empty:
            missing.append({"symbol": symbol, "reason_code": "NO_EXISTING_CANDIDATE_BAR_HISTORY"})
            continue
        try:
            normalized = build_suspension_aware_bar_panel(daily=raw, suspend_rows=suspends, trading_calendar=calendar)
        except Exception as exc:
            reason = getattr(exc, "reason_code", "")
            if reason not in {"ADVISORY_SUSPENSION_UNEXPLAINED_MISSING", "ADVISORY_SUSPENSION_LAST_CLOSE_UNAVAILABLE"}:
                raise
            missing.append({"symbol": symbol, "reason_code": reason})
            continue
        panel = normalized.panel.copy()
        # Unconsumed static attributes remain unknown. No DB requests are made
        # for them, and they do not become manufactured model inputs.
        absent_static = ("db_turnover_rate", "db_volume_ratio", "mf_lg_buy_amt", "mf_elg_buy_amt", "mf_lg_sell_amt", "mf_elg_sell_amt",
            "db_pe_ttm", "db_pb", "db_circ_mv", "bb_rev_yoy", "bb_profit_yoy", "bb_gpr", "bb_npr", "cp_winner_rate",
            "cp_cost_95pct", "cp_cost_5pct", "cp_cost_50pct", "md_rzye", "l2_code_id")
        for field in absent_static:
            if field not in panel:
                panel[field] = np.nan
        calculated = _build_instrument_features(panel)
        if (decision, symbol) in calculated.index:
            for field in ("ret_1", "ret_5", "atr14_close"):
                base.loc[row_index, field] = calculated.loc[(decision, symbol), field]
    if not benchmark_daily.empty:
        benchmark = _build_benchmark_features(benchmark_daily)
        if decision in benchmark.index:
            base["csi300_ret_5"] = benchmark.loc[decision, "csi300_ret_5"]
    if not market_daily.empty:
        breadth = _build_market_features(market_daily)
        if decision in breadth.index:
            base["market_up_ratio"] = breadth.loc[decision, "market_up_ratio"]
    return base, {"schema_version": "economic_eight_D_feature_adapter_v1", "candidate_count_preserved": len(base),
        "missing_candidate_bar_inputs": missing, "hmm_loaded": False, "m4_loaded": False,
        "feature_visible_through": decision.date().isoformat(), "native_evidence_created": False}


@dataclass(frozen=True)
class EconomicEntryPreparedDayV1:
    inputs: tuple[EconomicEntryDailyInputV1, ...]
    decision_features: tuple[dict, ...]
    price_contexts: tuple[object | None, ...]
    trading_calendar: tuple
    source_receipt: dict


def prepare_native_economic_research_day_v1(*, loaded, observed, projection):
    """Package verified native references as *limited research*, not qualification.

    The service authorizes the consumed target before invoking the DB reader.
    Native archives do not establish the original feature vintage or the
    applicability of the currently unconfirmed weights.
    """
    return _prepare_native_economic_day_v1(loaded=loaded, observed=observed, projection=projection, formal_role=None)


def prepare_native_economic_formal_day_v1(*, loaded, observed, role):
    """Prospective native consumer only; cannot repair historical or research scope."""
    from backend.services.advisory_model_first.economic_entry_daily_contracts import EconomicEntryValueRoleV1
    from backend.services.advisory_model_first.economic_entry_serving_bundle import LoadedEconomicEntryConfirmedBundleV1
    if not isinstance(loaded, LoadedEconomicEntryConfirmedBundleV1):
        _fail("formal economic input requires the verified confirmed bundle")
    role = EconomicEntryValueRoleV1.model_validate(role.model_dump())
    if (role.scope != loaded.manifest.scope or role.bundle_id != loaded.manifest.bundle_id
            or role.confirmation_request_sha256 != loaded.confirmation_request.request_sha256
            or loaded.confirmation_result.get("status") != "ECONOMIC_GATES_PASSED"):
        _fail("formal economic input role differs from its actual qualified bundle")
    proof = loaded.native_training_scope
    receipt, source = observed["candidate_receipt"], observed["source_receipt"]
    empty = observed["candidates"].empty
    if (receipt.get("pit_universe_key") != proof.pit_universe_key or receipt.get("pit_rule_version") != proof.pit_rule_version
            or (empty and (source.get("status") != "NO_CANDIDATES" or type(source.get("query_count")) is not int or source["query_count"] != 0))
            or (not empty and (source.get("pit_universe_key") != proof.pit_universe_key or source.get("shared_builder_sha256") != proof.shared_builder_sha256
            or source.get("bar_policy_sha256") != proof.bar_policy_sha256
            or {name: source.get("source_window_contract", {}).get(name) for name in
                ("candidate_sessions", "benchmark_sessions", "breadth_sessions")}
                != {"candidate_sessions": 20, "benchmark_sessions": 20, "breadth_sessions": 2}))
            or observed["program_id"] != role.program_id or observed["binding_version_id"] != role.binding_version_id):
        _fail("formal economic input PIT, feature recipe or program differs from native trained scope")
    return _prepare_native_economic_day_v1(loaded=loaded, observed=observed, projection=proof.projection, formal_role=role)


def _prepare_native_economic_day_v1(*, loaded, observed, projection, formal_role):
    from backend.services.advisory_model_first.economic_entry_contracts import ECONOMIC_FEATURE_NAMES
    projection = EconomicEntryCandidateProjectionV1.model_validate(projection.model_dump())
    identity = EconomicEntryInputIdentityV1.model_validate_json(loaded.original_input_files["identity.json"])
    request = loaded.source_plan.training_request.source_request
    decision, target = _day(observed["decision_date"]), _day(observed["target_date"])
    formal = formal_role is not None
    calendar = pd.DatetimeIndex(observed["trading_calendar"] if formal else json.loads(loaded.original_input_files["calendar.json"]))
    original_identity_sha = loaded.native_training_scope.input_identity_sha256 if formal else loaded.manifest.original_input_identity_sha256
    if (projection.scope != loaded.manifest.scope or identity.identity_sha256 != original_identity_sha
            or (not formal and (observed["program_id"] != identity.program_id or observed["binding_version_id"] != identity.binding_version_id))
            or (not formal and not request.test_start <= decision.date() <= request.test_end)
            or (formal and (identity.source_evidence != "NATIVE_COMPLETE" or decision.date() <= max(request.label_cutoff,
                loaded.native_training_scope.latest_upstream_training_date) or target.date() < formal_role.effective_from_target_date))
            or calendar.empty or calendar.tz is not None or not calendar.normalize().equals(calendar)
            or not calendar.is_unique or not calendar.is_monotonic_increasing or decision not in calendar
            or calendar.get_loc(decision) + 1 >= len(calendar) or calendar[calendar.get_loc(decision) + 1] != target):
        _fail("economic native research identity or consumed window differs")
    receipt = observed["candidate_receipt"]
    source = observed["source_receipt"]
    captured = observed.get("captured_at")
    if (receipt.get("projection_sha256") != projection.projection_sha256 or receipt.get("model_scope_qualified") is not False
            or receipt.get("native_receipt_created") is not False or receipt.get("outcomes_read") is not False
            or source.get("outcomes_read") is not False
            or not isinstance(captured, datetime) or captured.utcoffset() is None
            or not observed.get("selection_run_id") or not observed.get("list_version_id")):
        _fail("economic native research cannot promote evidence or read outcomes")
    if formal:
        from datetime import time
        from zoneinfo import ZoneInfo
        zone = ZoneInfo("Asia/Shanghai")
        if not max(formal_role.created_at, datetime.combine(decision.date(), time(15), zone)) <= captured < datetime.combine(target.date(), time(9, 30), zone):
            _fail("formal economic input was not actually frozen in its prospective D/T window")
    candidates = observed["candidates"]
    _validate_candidate_projection(candidates, decision, target, projection.component_roles.model_dump())
    features = observed["features"]
    names = list(ECONOMIC_FEATURE_NAMES[:-1])
    if (not {*KEY, *names}.issubset(features.columns) or features.duplicated(KEY).any()
            or len(features) != len(candidates)
            or set(map(tuple, features[KEY].itertuples(index=False, name=None)))
                != set(map(tuple, candidates[KEY].itertuples(index=False, name=None)))):
        _fail("economic native D features must preserve every original candidate key")
    contexts, unavailable = observed["price_contexts"], observed["price_context_unavailable"]
    symbols = set(candidates.instrument)
    missing = [item.get("symbol") for item in unavailable]
    if (len(set(missing)) != len(missing) or set(contexts) & set(missing) or set(contexts) | set(missing) != symbols):
        _fail("economic native research contexts must partition the exact original roster")
    records = [{"instrument": row["instrument"], "selection_rank": int(row["selection_effective_rank"])}
               for row in candidates.to_dict("records")]
    roster_sha = canonical_json_sha256(records)
    members = observed["source_members"]
    if (not isinstance(members, list) or not members
            or any(not isinstance(value, str) or re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", value) is None for value in members)
            or members != sorted(set(members))
            or symbols - set(members)):
        _fail("economic native research requires exact complete source membership")
    membership_sha = canonical_json_sha256(members)
    if membership_sha != receipt.get("admitted_universe_members_sha256"):
        _fail("economic native research membership differs from verified source")
    feature_source_sha = canonical_json_sha256({"D_source": source, "candidate_source": receipt})
    feature_map = features.set_index(KEY).to_dict("index")
    inputs, values, ordered_contexts = [], [], []
    limitations = tuple(receipt.get("source_evidence_limitations", ())) if formal else tuple(dict.fromkeys((*loaded.manifest.evidence_limitations,
        *receipt.get("source_evidence_limitations", ()), "NATIVE_ARCHIVE_IS_NOT_MODEL_QUALIFICATION",
        "DATABASE_READ_NOW_NOT_ORIGINAL_FEATURE_VINTAGE", "TRAINING_TEMPORAL_PARITY_UNPROVEN")))
    for row in candidates.to_dict("records"):
        features = daily_decision_feature_values_v1(feature_map[tuple(row[key] for key in KEY)], request.feature_names)
        context = contexts.get(row["instrument"])
        inputs.append(EconomicEntryDailyInputV1(scope=loaded.manifest.scope, program_id=observed["program_id"] if formal else identity.program_id,
            binding_version_id=observed["binding_version_id"] if formal else identity.binding_version_id,
            run_id=observed["selection_run_id"], list_id=observed["list_version_id"],
            decision_date=decision.date(), target_date=target.date(), feature_visible_through=decision.date(),
            price_visible_through=decision.date(), captured_at=observed["captured_at"], instrument=row["instrument"],
            selection_rank=int(row["selection_effective_rank"]), candidate_roster_sha256=roster_sha,
            feature_source_sha256=feature_source_sha, feature_values_sha256=canonical_json_sha256(features),
            price_context_sha256=price_context_sha256(context) if context is not None else None,
            universe_membership_sha256=membership_sha, source_evidence="NATIVE_COMPLETE" if formal else "RECOVERED_LIMITED",
            evidence_level="PROSPECTIVE_INPUT" if formal else "HISTORICAL_REPLAY",
            evidence_limitations=limitations))
        values.append(features)
        ordered_contexts.append(context)
    return EconomicEntryPreparedDayV1(tuple(inputs), tuple(values), tuple(ordered_contexts), tuple(calendar.date),
        {"source_evidence": "NATIVE_COMPLETE" if formal else "RECOVERED_LIMITED", "candidate_source": receipt, "D_source": source,
         "roster_sha256": roster_sha, "price_context_unavailable": list(unavailable), "candidate_rows_preserved": len(inputs),
         "native_receipts_restored": 0, "new_selection_runs": 0, "outcomes_read": False, "sealed_accessed": False,
         "capture_as_of": observed["captured_at"].isoformat(), "evidence_limitations": limitations,
         "candidate_roster": records, "admitted_universe_members": list(members),
         "model_scope_qualification": "VERIFIED_BY_CONFIRMED_BUNDLE" if formal else "UNPROVEN"})


class EconomicEntryReadonlyPriceContextSourceV1:
    def __init__(self, *, read_session, pit_universe_key):
        if not isinstance(pit_universe_key, str) or not pit_universe_key.strip():
            _fail("economic price input requires its explicit canonical PIT component key; no legacy fallback")
        self._session, self._key = read_session, pit_universe_key
        self.last_receipt = None

    def close(self):
        self._session.close()

    def load(self, *, symbols, decision_date, target_date):
        self.last_receipt = None
        from backend.services.advisory_model_first.realtime_feature_source import PostgresRealtimeFeatureSource
        from backend.services.canonical_equity_pit import (
            require_canonical_rolling_universe_key, CANONICAL_PIT_RULE_VERSION, CanonicalPitAuthorityResolver,
        )
        require_canonical_rolling_universe_key(self._key)
        if len(symbols) > 20 or len(set(symbols)) != len(symbols) or decision_date >= target_date:
            _fail("economic price context batch exceeds candidate/clock contract")
        if not symbols:
            self.last_receipt = {"status": "NO_CANDIDATES", "query_count": 0, "outcomes_read": False}
            return {}, ()
        started = datetime.now(timezone.utc)
        with self._session.connection() as connection:
            @contextmanager
            def pinned_connection():
                yield connection
            live = CanonicalPitAuthorityResolver(pinned_connection).resolve_live_binding()
            with connection.cursor() as cursor:
                cursor.execute("""SELECT rule_version, scope, status, dirty, start_date, end_date, source_fingerprint_sha256
                                    FROM market.stock_universe_pit_state WHERE universe_key = %s""", (self._key,))
                state = cursor.fetchone()
                if (state is None or state[0] != CANONICAL_PIT_RULE_VERSION or state[1] != "canonical_all_listed"
                        or state[2] != "ready" or state[3] is not False or state[4] is None or state[5] is None
                        or not state[4] <= decision_date <= state[5]
                        or not isinstance(state[6], str) or re.fullmatch(r"[0-9a-f]{64}", state[6]) is None):
                    _fail("economic historical price component is not an exact ready canonical PIT materialization")
                contexts, unavailable = PostgresRealtimeFeatureSource._price_range_contexts(cursor, symbols=symbols,
                    decision_as_of_trade_date=decision_date, target_trade_date=target_date, pit_universe_key=self._key)
        self.last_receipt = {"source": "READONLY_DATABASE_D_ONLY", "decision_date": decision_date.isoformat(),
            "target_date": target_date.isoformat(), "pit_universe_key": self._key, "pit_rule_version": state[0],
            "component_state": {"scope": state[1], "status": state[2], "dirty": state[3],
                "start_date": state[4].isoformat(), "end_date": state[5].isoformat(), "source_fingerprint_sha256": state[6]},
            "live_authority_status": str(live.authority_status), "live_universe_key": live.universe_key,
            "live_activation_generation": live.activation_generation,
            "component_is_live": live.universe_key == self._key and str(live.authority_status) == "ACTIVE_CANONICAL",
            "read_started_at": started.isoformat(), "read_completed_at": datetime.now(timezone.utc).isoformat(),
            "source_evidence": "RECOVERED_LIMITED", "read_purpose": "CONSUMED_HISTORICAL_FUNCTIONAL_NAVIGATION_ONLY",
            "offline_qe_profile_consumed": False, "data_activation": False, "legacy_fallback": False,
            "outcomes_read": False, "candidate_count": len(symbols)}
        self._session.budget.check()
        return contexts, unavailable


class EconomicEntryReadonlyDSourceV1(EconomicEntryReadonlyPriceContextSourceV1):
    """D-only 20-session inputs and D-visible price attributes in one bounded snapshot."""

    def load_day(self, *, candidates, decision_date, target_date, component_roles):
        from backend.services.advisory_model_first.realtime_feature_source import (
            PostgresRealtimeFeatureSource, _market_frame, _read_frame, _indexed_numeric_frame,
        )
        from backend.services.advisory_model_first.economic_entry_contracts import ECONOMIC_FEATURE_NAMES
        from backend.services.advisory_model_first import shared_feature_builder, suspension_aware_bar_policy
        decision, target = _day(decision_date).date(), _day(target_date).date()
        _validate_candidate_projection(candidates, pd.Timestamp(decision), pd.Timestamp(target), component_roles)
        if candidates.empty:
            return {"features": pd.DataFrame(columns=[*KEY, *ECONOMIC_FEATURE_NAMES[:-1]]), "price_contexts": {},
                    "price_context_unavailable": (), "trading_calendar": (), "source_receipt": {
                        "status": "NO_CANDIDATES", "query_count": 0, "outcomes_read": False}}
        symbols = candidates.instrument.tolist()
        with self._session.connection() as connection:
            with connection.cursor() as cursor:
                calendar = PostgresRealtimeFeatureSource._recent_trading_dates(cursor, end_date=decision, limit=20)
                next_days = PostgresRealtimeFeatureSource._trading_calendar(cursor, start_date=decision, end_date=target)
                if len(calendar) == 0 or calendar[-1].date() != decision or tuple(next_days.date) != (decision, target):
                    _fail("economic D reader needs the authoritative D and immediate next T calendar")
                parameters = {"symbols": symbols, "start_date": calendar[0].date(), "end_date": decision}
                raw = _read_frame(cursor, """
                    WITH base_adj AS (
                        SELECT DISTINCT ON (ts_code) ts_code, adj_factor AS base_adj_factor
                          FROM market.adj_factor
                         WHERE ts_code = ANY(%(symbols)s) AND trade_date <= %(end_date)s
                         ORDER BY ts_code, trade_date DESC
                    )
                    SELECT price.trade_date, price.ts_code, price.open_li, price.high_li, price.low_li, price.close_li,
                           price.volume_hand, price.amount_li, adj.adj_factor, base.base_adj_factor,
                           limits.pre_close, limits.up_limit, limits.down_limit
                      FROM market.kline_daily_raw price
                      LEFT JOIN market.adj_factor adj ON adj.ts_code = price.ts_code AND adj.trade_date = price.trade_date
                      LEFT JOIN base_adj base ON base.ts_code = price.ts_code
                      LEFT JOIN market.stk_limit limits ON limits.ts_code = price.ts_code AND limits.trade_date = price.trade_date
                     WHERE price.ts_code = ANY(%(symbols)s) AND price.trade_date BETWEEN %(start_date)s AND %(end_date)s
                     ORDER BY price.trade_date, price.ts_code
                """, parameters)
                if raw.empty:
                    candidate_daily = pd.DataFrame(columns=["open", "high", "low", "close", "volume", "amount", "factor",
                        "up_limit_price", "down_limit_price", "limit_up", "limit_down"],
                        index=pd.MultiIndex.from_tuples([], names=["datetime", "instrument"]))
                else:
                    candidate_daily = _market_frame(raw, context="economic_candidate_daily")
                suspends = PostgresRealtimeFeatureSource._suspend_rows(cursor, **parameters)
                # Matches the existing daily source's two-session availability
                # window. Reusing its formula alone is NOT proof of parity with
                # an older training panel's longer window (e.g. suspended stocks).
                breadth_raw = _read_frame(cursor, """
                    SELECT price.trade_date, price.ts_code, price.close_li, adj.adj_factor
                      FROM market.kline_daily_raw price
                      JOIN market.stock_basic stock ON stock.ts_code = price.ts_code
                      JOIN market.sector_data eligible ON eligible.trade_date = price.trade_date AND eligible.ts_code = price.ts_code
                      LEFT JOIN market.adj_factor adj ON adj.ts_code = price.ts_code AND adj.trade_date = price.trade_date
                     WHERE price.trade_date BETWEEN %(start_date)s AND %(end_date)s
                       AND stock.list_date <= price.trade_date AND (stock.delist_date IS NULL OR stock.delist_date > price.trade_date)
                       AND (price.ts_code LIKE '%%.SH' OR price.ts_code LIKE '%%.SZ')
                     ORDER BY price.trade_date, price.ts_code
                """, {"start_date": calendar[max(0, len(calendar) - 2)].date(), "end_date": decision})
                market_daily = pd.DataFrame({"datetime": pd.to_datetime(breadth_raw.trade_date), "instrument": breadth_raw.ts_code,
                    "close": pd.to_numeric(breadth_raw.close_li, errors="coerce") / 1000. * pd.to_numeric(breadth_raw.adj_factor, errors="coerce"),
                    "limit_up": np.nan}).set_index(["datetime", "instrument"]).sort_index()
                benchmark_raw = _read_frame(cursor, """
                    SELECT trade_date, ts_code, close FROM market.index_daily
                     WHERE ts_code = '000300.SH' AND trade_date BETWEEN %s AND %s ORDER BY trade_date
                """, (calendar[0].date(), decision))
                benchmark = _indexed_numeric_frame(benchmark_raw, context="economic_benchmark_daily") if not benchmark_raw.empty else pd.DataFrame(
                    columns=["close"], index=pd.MultiIndex.from_tuples([], names=["datetime", "instrument"]))
                contexts, unavailable = PostgresRealtimeFeatureSource._price_range_contexts(cursor, symbols=symbols,
                    decision_as_of_trade_date=decision, target_trade_date=target, pit_universe_key=self._key)
                unavailable_symbols = [value.get("symbol") for value in unavailable]
                if (set(contexts) & set(unavailable_symbols) or len(set(unavailable_symbols)) != len(unavailable_symbols)
                        or set(contexts) | set(unavailable_symbols) != set(symbols)):
                    _fail("economic D price attributes do not partition the exact frozen candidate roster")
                unavailable = tuple({**value, "last_existing_price_trade_date": (
                    candidate_daily.loc[(candidate_daily.index.get_level_values("instrument") == value["symbol"])
                        & candidate_daily.close.notna().to_numpy()].index.get_level_values("datetime").max().date().isoformat()
                    if ((candidate_daily.index.get_level_values("instrument") == value["symbol"]) & candidate_daily.close.notna().to_numpy()).any() else None)}
                    for value in unavailable)
                self._session.budget.check()
        features, calculation = build_economic_D_features_v1(candidates=candidates, candidate_daily=candidate_daily,
            market_daily=market_daily, benchmark_daily=benchmark, suspend_rows=suspends,
            trading_calendar=calendar, decision_date=decision, component_roles=component_roles)
        hashes = {name: hashlib.sha256(_parquet_bytes(frame)).hexdigest() for name, frame in (
            ("candidate_daily", candidate_daily), ("market_daily", market_daily), ("benchmark_daily", benchmark), ("suspend_rows", suspends))}
        receipt = {**calculation, "source": "READONLY_DATABASE_D_ONLY", "pit_universe_key": self._key,
            "raw_price_unit_divisor": 1000., "decision_date": decision.isoformat(), "target_date": target.isoformat(),
            "raw_inputs_sha256": hashes, "source_sha256": canonical_json_sha256(hashes),
            "shared_builder_sha256": file_sha256(shared_feature_builder.__file__),
            "bar_policy_sha256": suspension_aware_bar_policy.BAR_POLICY_HASH,
            "source_window_contract": {"candidate_sessions": 20, "breadth_sessions": 2,
                "benchmark_sessions": 20, "training_temporal_parity": "UNPROVEN"},
            "feature_semantics_qualification": "DAILY_FORMULA_REUSED_TRAINING_WINDOW_UNPROVEN",
            "new_native_receipt": False, "hmm_queries": 0, "outcomes_read": False}
        self._session.budget.check()
        return {"features": features, "price_contexts": contexts, "price_context_unavailable": unavailable,
                "trading_calendar": (*calendar.date, target), "source_receipt": receipt}


def prepare_frozen_economic_research_day_v1(*, loaded, decision_date, context_source, captured_at=None):
    """No outcomes are read. These are restored historical inputs, never native capture."""
    source = loaded.source_plan.training_request.source_request
    decision = _day(decision_date)
    if not source.test_start <= decision.date() <= source.test_end:
        _fail("economic daily functional read lies outside the registered consumed test decisions")
    frozen = loaded.original_input_files
    identity = EconomicEntryInputIdentityV1.model_validate_json(frozen["identity.json"])
    if identity.identity_sha256 != loaded.manifest.original_input_identity_sha256:
        _fail("economic original program identity differs from its verified serving view")
    calendar = pd.DatetimeIndex(json.loads(frozen["calendar.json"]))
    if (calendar.empty or calendar.tz is not None or not calendar.normalize().equals(calendar)
            or not calendar.is_unique or not calendar.is_monotonic_increasing or decision not in calendar):
        _fail("economic frozen daily calendar is invalid")
    position = calendar.get_loc(decision)
    if position + 1 >= len(calendar):
        _fail("economic frozen daily calendar lacks the next target")
    target = calendar[position + 1]
    rankings = pd.read_parquet(io.BytesIO(frozen["frozen_rankings.parquet"]))
    if not {*KEY, "is_candidate_decision", "selection_effective_rank"}.issubset(rankings.columns):
        _fail("economic frozen rankings lack original candidate fields")
    day_rows = rankings.loc[pd.to_datetime(rankings[KEY[0]], errors="coerce").eq(decision)]
    if not day_rows.is_candidate_decision.map(lambda value: isinstance(value, (bool, np.bool_))).all():
        _fail("economic frozen candidate flags are not original boolean decisions")
    original_candidates = day_rows.loc[day_rows.is_candidate_decision]
    ranks = pd.to_numeric(original_candidates.selection_effective_rank, errors="coerce")
    if (ranks.isna().any() or not np.isfinite(ranks).all() or not ranks.gt(0).all() or not ranks.mod(1).eq(0).all()
            or original_candidates.selection_effective_rank.map(lambda value: isinstance(value, (bool, np.bool_))).any()
            or ranks.duplicated().any() or original_candidates.duplicated(KEY).any()
            or original_candidates.instrument.duplicated().any()
            or not pd.to_datetime(original_candidates[KEY[1]], errors="coerce").eq(target).all()):
        _fail("economic frozen candidate identities cannot be silently dropped by Top20 filtering")
    candidates = original_candidates.loc[ranks.le(20), KEY + ["selection_effective_rank"]].copy()
    candidates = candidates.sort_values("selection_effective_rank")
    if (len(candidates) > 20 or candidates.duplicated(KEY).any() or candidates.selection_effective_rank.duplicated().any()
            or not candidates.selection_effective_rank.between(1, 20).all()
            or not candidates.selection_effective_rank.mod(1).eq(0).all()
            or not pd.to_datetime(candidates[KEY[1]]).eq(target).all()):
        _fail("economic frozen candidate roster, rank or next-day identity differs")
    names = [name for name in source.feature_names if name != "query_gap_bps"]
    feature_columns = KEY + names + ["feature_visible_through", "feature_source_sha256"]
    features = pd.read_parquet(io.BytesIO(frozen["features.parquet"]), columns=feature_columns)
    features = features.loc[pd.to_datetime(features[KEY[0]]).eq(decision)]
    if features.duplicated(KEY).any() or not features.feature_source_sha256.eq(source.feature_source_sha256).all():
        _fail("economic frozen D feature keys/source differ")
    feature_clock = pd.to_datetime(features.feature_visible_through)
    if feature_clock.isna().any() or (feature_clock > decision).any():
        _fail("economic frozen feature cutoff exceeds D")
    feature_map = features.set_index(KEY).to_dict("index")
    roster_sha = canonical_json_sha256([{name: value.isoformat() if isinstance(value, pd.Timestamp) else value
                                       for name, value in row.items()} for row in candidates.to_dict("records")])
    symbols = candidates.instrument.tolist()
    contexts, unavailable = context_source.load(symbols=symbols, decision_date=decision.date(), target_date=target.date())
    unavailable_symbols = [item.get("symbol") for item in unavailable]
    if (set(contexts) - set(symbols) or any(symbol not in symbols for symbol in unavailable_symbols)
            or len(set(unavailable_symbols)) != len(unavailable_symbols) or set(contexts) & set(unavailable_symbols)
            or set(contexts) | set(unavailable_symbols) != set(symbols)):
        _fail("economic price source must partition the exact candidate roster into contexts and explicit unavailable items")
    cohort_sha = canonical_json_sha256({"aligned_study": loaded.source_plan.experiment_id,
        "original_input_identity_sha256": identity.identity_sha256, "decision_date": decision.date().isoformat(),
        "candidate_roster_sha256": roster_sha})
    now = captured_at or datetime.now(timezone.utc)
    inputs, values, ordered_contexts = [], [], []
    for candidate in candidates.to_dict("records"):
        key = tuple(candidate[name] for name in KEY)
        feature = feature_map.get(key)
        raw = {name: feature[name] if feature is not None else None for name in names}
        numeric = daily_decision_feature_values_v1(raw, source.feature_names)
        context = contexts.get(candidate["instrument"])
        values.append(numeric)
        ordered_contexts.append(context)
        inputs.append(EconomicEntryDailyInputV1(scope=loaded.manifest.scope,
            program_id=identity.program_id, binding_version_id=identity.binding_version_id,
            restored_cohort_sha256=cohort_sha,
            decision_date=decision.date(), target_date=target.date(),
            feature_visible_through=_day(feature["feature_visible_through"]).date() if feature is not None else decision.date(),
            price_visible_through=decision.date(),
            captured_at=now, instrument=candidate["instrument"], selection_rank=int(candidate["selection_effective_rank"]),
            candidate_roster_sha256=roster_sha, feature_source_sha256=source.feature_source_sha256,
            feature_values_sha256=canonical_json_sha256(numeric), price_context_sha256=price_context_sha256(context) if context is not None else None,
            source_evidence="RECOVERED_LIMITED", evidence_level="HISTORICAL_REPLAY",
            evidence_limitations=(*loaded.manifest.evidence_limitations, "FROZEN_DATASET_NOT_NATIVE_RUN_LIST_CAPTURE", "PIT_PRICE_CONTEXT_READ_BACK_NOW")))
    return EconomicEntryPreparedDayV1(tuple(inputs), tuple(values), tuple(ordered_contexts), tuple(calendar.date),
        {"source_evidence": "RECOVERED_LIMITED", "candidate_rows_preserved": len(candidates), "roster_sha256": roster_sha,
         "price_context_unavailable": list(unavailable), "new_selection_runs": 0, "native_receipts_restored": 0,
         "outcomes_read": False, "sealed_accessed": False, "capture_as_of": now.isoformat(),
         "price_component_receipt": getattr(context_source, "last_receipt", None)})
