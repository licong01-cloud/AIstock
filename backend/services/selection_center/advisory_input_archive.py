"""Atomic execution-time inputs for future Advisory runs, never historical repair."""
from __future__ import annotations

from datetime import date, datetime, timezone
import json
from pathlib import Path
import re

from .prospective_evidence import canonical_evidence_json_sha256
from backend.services.trading_core.errors import RuntimeConfigInvalidError


def _invalid(message):
    raise RuntimeConfigInvalidError(message, context={"reason_code": "ADVISORY_INPUT_ARCHIVE_INCOMPLETE"})


def _sha(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        _invalid("Advisory input archive requires an actual source hash")
    return value


class AdvisorySelectionInputArchive:
    def __init__(self, *, members_reader=None, calendar_reader=None, now=None):
        self._members = members_reader or self._read_current_execution_members
        self._calendar = calendar_reader
        self._now = now or (lambda: datetime.now(timezone.utc))

    @staticmethod
    def validate_context(context, *, target_date):
        root = str(context.get("artifact_root") or "").strip()
        if not root or not Path(root).is_absolute():
            _invalid("future Advisory archive requires a configured absolute artifact root")
        try:
            decision = date.fromisoformat(context["decision_as_of_trade_date"])
        except (ValueError, TypeError, KeyError):
            _invalid("future Advisory archive lacks a valid explicit D-1")
        if decision >= target_date or not context.get("program_id") or not context.get("binding_version_id"):
            _invalid("future Advisory archive requires exact Program/binding and D-1")
        _sha(context.get("review_policy_sha256"))
        return decision

    @staticmethod
    def _read_current_execution_members(*, decision_date, runtime_config):
        from backend.services.canonical_equity_pit import CanonicalPitAuthorityResolver
        from backend.services.stock_universe_pit_service import StockUniversePitService
        from .canonical_pit_runtime import require_canonical_pit_generation_current
        lease = require_canonical_pit_generation_current(runtime_config)
        binding = CanonicalPitAuthorityResolver().resolve_live_binding()
        members = StockUniversePitService().get_eligible_codes(
            trade_date=decision_date, universe_key=lease.universe_key, ensure=False,
            authority_binding=binding, consumer="selection")
        require_canonical_pit_generation_current(runtime_config)
        return members

    def capture(self, *, run, selection, context):
        from backend.services.advisory_model_first.entry_price_confirmation import _immutable_json
        from backend.services.advisory_model_first.research_control import _exclusive_file_lock, evidence_reference_for_file
        root = str(context.get("artifact_root") or "").strip()
        if not root or not Path(root).is_absolute() or not re.fullmatch(r"sel_[0-9a-f]{32}", run.run_id):
            _invalid("future Advisory archive needs a configured root and allocated Selection run")
        decision = self.validate_context(context, target_date=run.trade_date)
        if self._calendar is None:
            from backend.services.simulation_data.trading_calendar import TradeCalendarProvider
            calendar = TradeCalendarProvider().list_trading_days(decision, run.trade_date)
        else:
            calendar = self._calendar(decision, run.trade_date)
        if list(calendar) != [decision, run.trade_date]:
            _invalid("future archive decision must be the actual previous trading session")
        policy = _sha(context["review_policy_sha256"])
        artifacts = selection.score_artifacts_by_package
        if set(artifacts) != set(run.package_ids):
            _invalid("future Advisory archive lacks the exact consumed package artifacts")
        if set(selection.evidence_by_package) != set(run.package_ids):
            _invalid("future Advisory archive lacks complete package DSE evidence")
        members = sorted(self._members(decision_date=decision, runtime_config=selection.runtime_config))
        if not members or len(members) != len(set(members)):
            _invalid("future Advisory archive full source members are empty or duplicated")
        members_sha = canonical_evidence_json_sha256(members)
        package_inputs = {}
        for package_id, artifact in artifacts.items():
            source = artifact.model_dump(mode="json")
            clock = (source.get("metadata") or {}).get("artifact_input_context") or {}
            if (source["package_id"] != package_id or source["manifest_sha256"] != run.manifest_sha256_by_package[package_id]
                    or source["trade_date"] != run.trade_date.isoformat() or source["data_source"] != run.data_source
                    or source["status"] != "SUCCEEDED"
                    or clock.get("cutoff_date") != decision.isoformat()
                    or clock.get("score_trade_date") != decision.isoformat()
                    or clock.get("requested_trade_date") != run.trade_date.isoformat()
                    or clock.get("universe_input_hash") != members_sha
                    or canonical_evidence_json_sha256(clock) != source.get("artifact_input_context_hash")
                    or source.get("universe_count") != len(members)):
                _invalid("future archive members/package/artifact/D-1 differ from actual execution")
            package_inputs[package_id] = dict(artifact=source, artifact_content_sha256=
                                              canonical_evidence_json_sha256(source))
        observed = self._now()
        if observed.utcoffset() is None:
            _invalid("future archive observation time must be timezone aware")
        payload = dict(schema_version="advisory_selection_input_archive_v1", selection_run_id=run.run_id,
                       observed_at=observed.isoformat(), historical_capture_backfilled=False,
                       historical_vintage_proven=False, program_id=context["program_id"],
                       binding_version_id=context["binding_version_id"], review_policy_sha256=policy,
                       decision_as_of_trade_date=decision.isoformat(), target_trade_date=run.trade_date.isoformat(),
                       full_source_universe_members=members, source_universe_members_sha256=members_sha,
                       runtime_config=selection.runtime_config, package_inputs=package_inputs,
                       frozen_candidates=[row.model_dump(mode="json") for row in run.aggregate_results],
                       package_candidates={key: [row.model_dump(mode="json") for row in rows]
                                           for key, rows in run.package_results.items()},
                       manifest_sha256_by_package=run.manifest_sha256_by_package,
                       daily_selection_evidence={key: value.model_dump(mode="json")
                                                 for key, value in selection.evidence_by_package.items()})
        target = Path(root) / "selection_input_archives" / run.run_id
        target.mkdir(parents=True, exist_ok=True)
        path = target / "inputs.json"
        with _exclusive_file_lock(target / "archive.lock"):
            if path.exists():
                existing = json.loads(path.read_text(encoding="utf-8"))
                # Retry keeps the first real observation time, never backdates.
                if {k: v for k, v in existing.items() if k != "observed_at"} != {
                        k: v for k, v in payload.items() if k != "observed_at"}:
                    _invalid("future archive cannot overwrite different frozen execution inputs")
                payload = existing
            _immutable_json(path, payload)
        ref = evidence_reference_for_file(path, role="SELECTION_EXECUTION_INPUTS").model_dump(mode="json")
        return dict(schema_version="advisory_selection_input_archive_ref_v1", selection_run_id=run.run_id,
                    reference=ref, package_artifact_hashes={key: value["artifact_content_sha256"]
                                                          for key, value in package_inputs.items()})


def validate_archive_reference(ref, *, run, program_id, binding_version_id, review_policy_sha256):
    """Read-only publication guard. An old run is never captured retrospectively."""
    from backend.services.advisory_model_first.research_control import evidence_reference_for_file
    reference = ref["reference"]
    actual = evidence_reference_for_file(reference["artifact_uri"], role=reference["role"])
    if (actual.sha256 != reference["sha256"] or actual.size_bytes != reference["size_bytes"]
            or ref.get("selection_run_id") != run.run_id):
        _invalid("Advisory publication input archive file/run hash differs")
    payload = json.loads(Path(reference["artifact_uri"]).read_text(encoding="utf-8"))
    expected = dict(selection_run_id=run.run_id, program_id=program_id, binding_version_id=binding_version_id,
                    review_policy_sha256=review_policy_sha256, target_trade_date=run.trade_date.isoformat(),
                    manifest_sha256_by_package=run.manifest_sha256_by_package,
                    frozen_candidates=[row.model_dump(mode="json") for row in run.aggregate_results])
    if any(payload.get(key) != value for key, value in expected.items()):
        _invalid("Advisory publication Program/policy/package/frozen roster differs from Selection archive")
    config = {k: v for k, v in run.runtime_config.items() if k != "advisory_frozen_input_archive"}
    if payload.get("runtime_config") != config:
        _invalid("Advisory publication runtime/PIT differs from Selection archive")
    return payload
