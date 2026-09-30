from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
from datetime import date
import json
from pathlib import Path
from typing import Sequence

import numpy as np
import pandas as pd

from .entry_price_confirmation_contracts import (
    AdvisoryEntryPriceConfirmationRequestV1, EntryPriceConfirmationPredictionDay,
    EntryPriceConfirmationSettlementDay, build_entry_price_confirmation_request,
)
from .errors import AdvisoryModelFirstError
from .price_range_contracts import canonical_json_sha256
from .research_control import (
    AdvisoryResearchTrialRegistryV1, _exclusive_file_lock, _write_atomic_text,
    authorize_research_window_access, evidence_reference_for_file, research_policy_identity,
)
from .research_control_contracts import (
    SealedHoldoutConsumptionReceiptV1, build_holdout_consumption_receipt,
    build_trial_record, build_window_access_request,
)


class AdvisoryEntryPriceConfirmationService:
    """Artifact-only stages. No result source is reachable until the complete prediction is durable."""

    def __init__(self, *, day_service=None, outcome_source=None, input_verifier=None, execution_guard=None):
        self._day_service = day_service
        self._outcomes = outcome_source
        self._verify = input_verifier or verify_confirmation_inputs
        self._guard = execution_guard

    def _check_execution(self, request, slot, output_root):
        if self._guard is not None:
            self._guard(request, slot)
        else:
            require_entry_replay_execution(request, slot, output_root=output_root)

    def prepare(self, *, spec: dict, model_root, output_root) -> Path:
        request = build_entry_price_confirmation_request(**spec)
        self._verify(request, model_root=model_root)
        target = _root(output_root, request)
        target.mkdir(parents=True, exist_ok=True)
        with _exclusive_file_lock(target / "stage.lock"):
            _immutable_json(target / "request.json", request.model_dump(mode="json"))
            self._register(request, target, stage="PREPARED", result_class="CONTROL_READY", generated=0, evaluated=0)
        return target / "request.json"

    def predict(self, *, request_path, model_root, output_root, exclusive_slot=None) -> dict:
        request, root = _read_request(request_path, output_root)
        self._check_execution(request, exclusive_slot, root)
        self._verify(request, model_root=model_root)
        if self._day_service is None:
            from .entry_price_replay_runtime import validate_replay_dependencies
            validate_replay_dependencies()  # Dependency failure must precede window consumption.
        from .entry_price_replay_runtime import replay_thread_budget
        with _exclusive_file_lock(root / "stage.lock"), replay_thread_budget():
            access = _window_access(request)
            authorization = _authorize_or_resume(request, access)
            _immutable_json(root / "consuming.json", {
                "request_sha256": request.request_sha256, "stage": "CONSUMING", "authorization": authorization,
            })
            if (root / "prediction" / "all.json").exists():
                return _read_stage(root, "prediction/all.json", request.request_sha256)
            service = self._day_service or _cached_day_service()
            predictions = []
            for day in request.days:
                self._check_execution(request, exclusive_slot, root)
                result = service.evaluate_day(
                    model_root=model_root, program_id=request.program_id, binding_version_id=request.binding_version_id,
                    target_trade_date=day.target_trade_date, scope=request.scope,
                    role_binding_sha256=request.request_sha256,
                    frozen_input_ids={key: getattr(day, key) for key in ("list_version_id", "review_run_id", "selection_run_id")},
                )
                predictions.append(_prediction_day(request, result))
                self._check_execution(request, exclusive_slot, root)
            validate_prediction_plan(request, predictions)
            payload = _stage_payload(request, "PREDICTED", days=[row.model_dump(mode="json") for row in predictions], execution_slot=_slot_payload(exclusive_slot))
            _immutable_json(root / "prediction" / "all.json", payload)
            return payload

    def settle(self, *, request_path, model_root, output_root, exclusive_slot=None) -> dict:
        del model_root  # Outcomes may not reload/recompute model predictions.
        request, root = _read_request(request_path, output_root)
        self._check_execution(request, exclusive_slot, root)
        with _exclusive_file_lock(root / "stage.lock"):
            frozen = _read_stage(root, "prediction/all.json", request.request_sha256)
            predictions = tuple(EntryPriceConfirmationPredictionDay.model_validate(day) for day in frozen["days"])
            validate_prediction_plan(request, predictions)
            _verify_existing_consumption(request, root)
            if (root / "settlement" / "all.json").exists():
                return _read_stage(root, "settlement/all.json", request.request_sha256, parent_sha256=frozen["stage_sha256"])
            source = self._outcomes or PostgresEntryPriceConfirmationOutcomeSource()
            days = []
            for day in request.days:
                self._check_execution(request, exclusive_slot, root)
                days.append(source.load(symbols=day.candidate_symbols, target_trade_date=day.target_trade_date))
                self._check_execution(request, exclusive_slot, root)
            payload = _stage_payload(
                request, "SETTLED", days=[row.model_dump(mode="json") for row in days],
                parent_sha256=frozen["stage_sha256"],
                execution_slot=_slot_payload(exclusive_slot),
            )
            # Validate the *whole* roster, including unknown/suspended rows, before publishing.
            if any(tuple(row.symbol for row in actual.outcomes) != plan.candidate_symbols or actual.target_trade_date != plan.target_trade_date
                   for actual, plan in zip(days, request.days)):
                _invalid("outcome reader returned foreign/missing candidate identities")
            _immutable_json(root / "settlement" / "all.json", payload)
            return payload

    def evaluate(self, *, request_path, model_root, output_root, exclusive_slot=None) -> dict:
        del model_root
        request, root = _read_request(request_path, output_root)
        self._check_execution(request, exclusive_slot, root)
        with _exclusive_file_lock(root / "stage.lock"):
            _verify_existing_consumption(request, root)
            prediction = _read_stage(root, "prediction/all.json", request.request_sha256)
            settlement = _read_stage(root, "settlement/all.json", request.request_sha256, parent_sha256=prediction["stage_sha256"])
            result = evaluate_entry_price_confirmation(
                request,
                [EntryPriceConfirmationPredictionDay.model_validate(row) for row in prediction["days"]],
                [EntryPriceConfirmationSettlementDay.model_validate(row) for row in settlement["days"]],
            )
            self._check_execution(request, exclusive_slot, root)
            payload = _stage_payload(request, "CONSUMED", evaluation=result, parent_sha256=settlement["stage_sha256"], execution_slot=_slot_payload(exclusive_slot))
            if (root / "evaluation.json").exists():
                payload = _read_stage(root, "evaluation.json", request.request_sha256, parent_sha256=settlement["stage_sha256"])
                if payload["evaluation"] != result:
                    _invalid("exact retry evaluation differs from the original fixed statistic")
            _immutable_json(root / "evaluation.json", payload)
            status = result["status"]
            classification = (
                "CONFIRMED" if status == "CONFIRMED_PRICE_DISTRIBUTION" else
                "NEGATIVE" if status == "NOT_CONFIRMED" else "INCOMPLETE_NEGATIVE"
            ) if request.study_type == "CONFIRMATION" else "EXPLORATORY"
            self._register(request, root, stage="EVALUATED", result_class=classification, generated=1, evaluated=1,
                           evidence_path=root / "evaluation.json", decision_use=result["decision_use"])
            manifest = {
                "request_sha256": request.request_sha256,
                "files": {name: evidence_reference_for_file(root / name, role=name).model_dump(mode="json")
                          for name in ("request.json", "consuming.json", "prediction/all.json", "settlement/all.json", "evaluation.json")},
            }
            _immutable_json(root / "manifest.json", manifest)
            return payload

    @staticmethod
    def _register(request, root, *, stage, result_class, generated, evaluated, evidence_path=None, decision_use=None):
        record = build_trial_record(
            experiment_id=request.request_id, attempt_id="exact-v1", research_stage=stage,
            study_type=request.study_type, hypothesis_family_id=request.hypothesis_family_id,
            parent_lineage=request.parent_lineage, unique_variable="frozen_entry_price_vs_validation_empirical_control",
            objective_contract=request.objective_contract, dataset_identity=request.data_identity.dataset_identity,
            schema_identity=request.scope.feature_schema_sha256, policy_identity=_policy(request),
            planned_trial_count=1, generated_trial_count=generated, evaluated_trial_count=evaluated,
            selected_trial_count=int(result_class == "CONFIRMED"),
            consumed_windows=() if stage == "PREPARED" else ({
                "window_id": request.window_contract.contract_id, "dataset_identity": request.data_identity.dataset_identity,
                "start_date": request.target_calendar[0], "end_date": request.target_calendar[-1],
            },),
            result_class=result_class, decision_use=decision_use or request.decision_use,
            evidence_refs=(evidence_reference_for_file(evidence_path or root / "request.json", role=stage),),
        )
        AdvisoryResearchTrialRegistryV1(request.registry_path).append_batch((record,))


def require_entry_replay_execution(request, slot=None, *, now=None, capacity_probe=None, output_root=None):
    from datetime import datetime, timezone
    from .entry_price_confirmation_contracts import EntryPriceExclusiveSlot
    from .entry_price_replay_runtime import probe_replay_capacity
    # Existing explicit coordination files remain validated, but are optional.
    # This path runs frozen historical inference, never QE training or dispatch.
    if slot is not None:
        slot = EntryPriceExclusiveSlot.model_validate(slot)
        clock = now or datetime.now(timezone.utc)
        if slot.request_sha256 != request.request_sha256 or not slot.starts_at <= clock < slot.expires_at:
            raise AdvisoryModelFirstError("supplied replay slot is expired or belongs to another request", reason_code="ADVISORY_ENTRY_RESOURCE_WAITING")
    state = (capacity_probe or probe_replay_capacity)(output_root if output_root is not None else Path(request.registry_path).parent)
    if state["status"] != "REPLAY_CAPACITY_AVAILABLE":
        raise AdvisoryModelFirstError("local replay capacity unavailable", reason_code="ADVISORY_ENTRY_RESOURCE_WAITING", context=state)


def _slot_payload(slot):
    from .entry_price_confirmation_contracts import EntryPriceExclusiveSlot
    return EntryPriceExclusiveSlot.model_validate(slot).model_dump(mode="json") if slot is not None else None


def _prediction_day(request, result):
    from .price_range_inference import project_entry_price

    rows = []
    for scored in result.scored:
        control_range = None
        if scored.candidate.entry_price.status == "AVAILABLE":
            projection = project_entry_price(
                symbol=scored.candidate.symbol, context=result.contexts[scored.candidate.symbol],
                entry_gaps=(request.control.q10, request.control.q50, request.control.q90),
                calibrated_entry_gaps=None, calibration_spec=None, target_trade_date=result.envelope.target_trade_date,
            )
            control_range = projection["entry_price_range"]
        rows.append(dict(symbol=scored.candidate.symbol, raw_gaps=scored.raw_gaps,
                         calibrated_gaps=scored.calibrated_gaps, control_range=control_range))
    return EntryPriceConfirmationPredictionDay(
        envelope=result.envelope, rows=rows, candidate_source_sha256=result.candidate_source_sha256,
        feature_values_sha256=result.feature_values_sha256,
    )


def _cached_day_service(*, metadata_only=False):
    from functools import lru_cache, partial
    from .entry_price_service import AdvisoryEntryPriceService
    from .model_bundle import load_frozen_research_bundle
    from .price_range_runtime_bundle import load_frozen_price_range_bundle
    from .entry_price_daily_service import EntryReadOnlyCalendar
    from .entry_price_replay_runtime import load_replay_booster
    from .realtime_feature_source import PostgresRealtimeFeatureSource
    from backend.services.advisory_program import AdvisoryProgramService

    # Preparing frozen input metadata still verifies bundle assets, but must not
    # construct predictors. Prediction callers retain the real loader default.
    loader_options = {"booster_factory": (lambda _path: None) if metadata_only else load_replay_booster}
    return AdvisoryEntryPriceService(
        program_service=AdvisoryProgramService(calendar_provider=EntryReadOnlyCalendar(_historical_calendar_connection)),
        feature_source=PostgresRealtimeFeatureSource(statement_timeout_ms=30_000),
        parent_loader=lru_cache(maxsize=1)(partial(load_frozen_research_bundle, **loader_options)),
        price_loader=lru_cache(maxsize=1)(partial(load_frozen_price_range_bundle, **loader_options)),
    )


@contextmanager
def _historical_calendar_connection():
    from backend.db.pg_pool import get_conn
    with get_conn(autocommit=False, manage_transaction=False) as conn:
        try:
            conn.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
            with conn.cursor() as cursor:
                cursor.execute("SET LOCAL statement_timeout = %s", (30_000,))
            yield conn
        finally:
            conn.rollback()


def verify_confirmation_inputs(request, *, model_root):
    from .model_bundle import load_frozen_research_bundle
    from .price_range_runtime_bundle import load_frozen_price_range_bundle

    for reference in (request.data_identity.vintage_evidence, request.data_identity.candidate_provenance,
                      request.data_identity.consumption_review, request.control.validation_labels):
        actual = evidence_reference_for_file(reference.artifact_uri, role=reference.role)
        if (actual.sha256, actual.size_bytes) != (reference.sha256, reference.size_bytes):
            _invalid("confirmation evidence changed after input review")
    scope = request.scope
    # Validate model assets without importing/constructing booster predictors at prepare time.
    bundle = load_frozen_price_range_bundle(
        model_root=model_root, price_range_bundle_id=scope.price_range_bundle_id,
        price_range_bundle_manifest_sha256=scope.price_range_bundle_manifest_sha256,
        expected_package_id=scope.package_id, expected_manifest_sha256=scope.package_manifest_sha256,
        expected_style_profile_hash=scope.style_profile_hash, expected_parent_bundle_id=scope.parent_bundle_id,
        expected_outcome_bundle_id=scope.outcome_bundle_id, booster_factory=lambda _path: None,
    )
    split = _read_json(bundle.bundle_path / "split.json")
    source_request_id = bundle.manifest["parent_price_range_request_id"]
    if not isinstance(source_request_id, str) or Path(source_request_id).name != source_request_id:
        _invalid("v4 parent training request path is invalid")
    label_source = Path(model_root).resolve() / "price_range_runs" / source_request_id / "daily_price_envelope_labels.parquet"
    if Path(request.control.validation_labels.artifact_uri).resolve() != label_source.resolve():
        _invalid("control source is not the original v4 training-run labels")
    parent = load_frozen_research_bundle(
        model_root=model_root, bundle_id=scope.parent_bundle_id, expected_package_id=scope.package_id,
        expected_manifest_sha256=scope.package_manifest_sha256,
        expected_selection_runtime_semantics_hash=scope.selection_runtime_semantics_hash,
        booster_factory=lambda _path: None,
    )
    if parent.manifest_file_sha256 != scope.parent_bundle_manifest_sha256:
        _invalid("parent provenance differs from the request")
    validation_dates = tuple(sorted(date.fromisoformat(day) for day in split["validation"]))
    if validation_dates != request.control.validation_dates:
        _invalid("control dates differ from the frozen v4 validation split")
    columns = ["decision_as_of_trade_date", "target_trade_date", "instrument", "entry_gap_return", "entry_gap_label_status", "gap_modelable"]
    labels = pd.read_parquet(request.control.validation_labels.artifact_uri, columns=columns, filters=[("split", "==", "validation")])
    labels["decision_as_of_trade_date"] = pd.to_datetime(labels["decision_as_of_trade_date"]).dt.date
    labels["target_trade_date"] = pd.to_datetime(labels["target_trade_date"]).dt.date
    if (
        tuple(sorted(labels["decision_as_of_trade_date"].unique())) != validation_dates
        or labels.duplicated(["decision_as_of_trade_date", "instrument"]).any()
        or (request.study_type == "CONFIRMATION" and labels["target_trade_date"].max() >= request.days[0].decision_as_of_trade_date)
    ):
        _invalid("validation control input contains foreign dates, duplicate stocks or future labels")
    usable = labels.loc[labels["gap_modelable"].eq(True) & labels["entry_gap_label_status"].eq("AVAILABLE"), "entry_gap_return"]
    if usable.empty or not np.isfinite(usable).all() or (usable <= -1).any():
        _invalid("validation empirical control lacks finite usable labels")
    quantiles = np.quantile(usable.to_numpy(dtype=float), [0.1, 0.5, 0.9], method="linear")
    if not np.allclose(quantiles, (request.control.q10, request.control.q50, request.control.q90), atol=1e-14, rtol=0):
        _invalid("control quantiles were not frozen from the exact validation source")
    if request.study_type == "CONFIRMATION":
        identity = request.data_identity
        actual_validation_target = labels["target_trade_date"].max()
        # v3 uses validation for early stopping; its fitted model identity therefore
        # consumes these labels too, not only the rows explicitly named "train".
        if (identity.latest_model_training_date < actual_validation_target
                or identity.latest_calibration_date < actual_validation_target
                or identity.latest_transform_fit_date < date.fromisoformat(parent.manifest["continuation_cutoff"])):
            _invalid("declared fit boundaries precede actual validation/HMM training evidence")
        _verify_qualification_review(request, validation_rows=len(usable))
    from .entry_price_service import _frame_sha256
    reader = _cached_day_service(metadata_only=True)
    for day in request.days:
        prepared = reader.prepare_day(
            model_root=model_root, program_id=request.program_id, binding_version_id=request.binding_version_id,
            target_trade_date=day.target_trade_date, scope=scope, role_binding_sha256=request.request_sha256,
            frozen_input_ids={key: getattr(day, key) for key in ("list_version_id", "review_run_id", "selection_run_id")},
        )
        if (prepared.decision_date != day.decision_as_of_trade_date
                or tuple(prepared.candidates["instrument"]) != day.candidate_symbols
                or _frame_sha256(prepared.candidates) != day.candidate_source_sha256):
            _invalid("prepared candidate provenance differs from frozen source metadata")


def _verify_qualification_review(request, *, validation_rows=None):
    from .entry_price_confirmation_contracts import EntryCoordinateReview
    identity = request.data_identity
    vintage = _read_json(Path(identity.vintage_evidence.artifact_uri))
    expected = identity.model_dump(mode="json", include={
        "profile_generation", "release_id", "data_root_uri", "complete", "dataset_identity",
        "latest_model_training_date", "latest_transform_fit_date", "latest_calibration_date", "latest_upstream_training_date",
    })
    expected.update(scope_sha256=canonical_json_sha256(request.scope.model_dump(mode="json")), pit_visibility_verified=True)
    if any(vintage.get(key) != value for key, value in expected.items()):
        _invalid("vintage audit does not attest the exact input identities and fit clocks")
    try:
        coordinate = EntryCoordinateReview.model_validate(vintage.get("entry_coordinate_review"))
    except ValueError:
        _invalid("validation entry coordinate parity is missing or failed")
    if (coordinate.validation_labels_sha256 != request.control.validation_labels.sha256
            or coordinate.scope_sha256 != canonical_json_sha256(request.scope.model_dump(mode="json"))
            or coordinate.projection_producer_version != request.projection_producer_version
            or (validation_rows is not None and coordinate.checked_validation_rows != validation_rows)):
        _invalid("validation entry coordinate review differs from the frozen source or scope")
    provenance = _read_json(Path(identity.candidate_provenance.artifact_uri))
    if (provenance.get("days_sha256") != canonical_json_sha256([day.model_dump(mode="json") for day in request.days])
            or provenance.get("pit_candidate_generation_verified") is not True):
        _invalid("candidate audit does not attest the exact frozen source plan")
    consumption = _read_json(Path(identity.consumption_review.artifact_uri))
    if (consumption.get("complete") is not True
            or tuple(consumption.get("reviewed_lineage", ())) != request.parent_lineage
            or consumption.get("hypothesis_family_id") != request.hypothesis_family_id
            or not isinstance(consumption.get("consumed_windows"), list)):
        _invalid("research consumption review does not cover the complete relevant lineage")
    for window in consumption["consumed_windows"]:
        start, end = date.fromisoformat(window["start_date"]), date.fromisoformat(window["end_date"])
        if end < start or (start <= request.target_calendar[-1] and end >= request.days[0].decision_as_of_trade_date):
            _invalid("confirmation window overlaps reviewed prior research consumption")


class PostgresEntryPriceConfirmationOutcomeSource:
    def __init__(self, *, connection_context_factory=None):
        from backend.db.pg_pool import get_conn
        self._connection = connection_context_factory or (lambda: get_conn(autocommit=False, manage_transaction=False))

    def load(self, *, symbols, target_trade_date):
        from .prospective_price_evaluation import PostgresAdvisoryPriceOutcomeSource
        with self._connection() as conn:
            cursor = conn.cursor()
            try:
                conn.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
                cursor.execute("SET LOCAL statement_timeout = %s", (30_000,))
                audits = PostgresAdvisoryPriceOutcomeSource._read_audits(cursor, target_trade_date=target_trade_date)
                cursor.execute("SELECT ts_code, open_li FROM market.kline_daily_raw WHERE trade_date=%s AND ts_code=ANY(%s) ORDER BY ts_code", (target_trade_date, list(symbols)))
                open_rows = cursor.fetchall()
                cursor.execute("SELECT ts_code, suspend_type FROM market.suspend_d WHERE trade_date=%s AND ts_code=ANY(%s) ORDER BY ts_code, suspend_type", (target_trade_date, list(symbols)))
                suspended_rows = cursor.fetchall()
            finally:
                conn.rollback()
                cursor.close()
        opens, suspend_types = {}, {}
        for symbol, value in open_rows:
            if symbol in opens or symbol not in symbols:
                _invalid("outcome has duplicate or foreign stock")
            try:
                opens[symbol] = float(value) / 1000.0
            except (ValueError, TypeError, OverflowError):
                _invalid("outcome price is not numeric")
            if not np.isfinite(opens[symbol]) or opens[symbol] <= 0:
                _invalid("outcome price is nonpositive or nonfinite")
        for symbol, kind in suspended_rows:
            if symbol not in symbols or kind not in {"S", "R"}:
                _invalid("suspension evidence has foreign or unknown identity")
            suspend_types.setdefault(symbol, set()).add(kind)
        if any(len(kinds) != 1 for kinds in suspend_types.values()):
            _invalid("conflicting suspension evidence")
        suspended = {symbol for symbol, kinds in suspend_types.items() if kinds == {"S"}}
        if suspended & opens.keys():
            _invalid("outcome is both traded and suspended")
        rows = [dict(
            symbol=symbol, market_status="NOT_APPLICABLE" if symbol in suspended else "AVAILABLE" if symbol in opens else "UNAVAILABLE",
            raw_open=opens.get(symbol), reason_code="AUTHORITATIVE_SUSPENSION" if symbol in suspended else None if symbol in opens else "TARGET_MARKET_ROW_MISSING_UNEXPLAINED",
        ) for symbol in symbols]
        return EntryPriceConfirmationSettlementDay(
            target_trade_date=target_trade_date, outcomes=rows,
            source_sha256=canonical_json_sha256({"rows": rows, "audits": [audit.model_dump(mode="json") for audit in audits]}),
        )


def _root(output_root, request):
    root = Path(output_root).resolve()
    target = root / "entry_price_confirmations" / request.request_id
    if not target.resolve().is_relative_to(root) or target.resolve() != target.absolute():
        _invalid("confirmation output path escapes artifact root")
    return target


def _read_request(path, output_root):
    request = AdvisoryEntryPriceConfirmationRequestV1.model_validate(_read_json(Path(path)))
    root = _root(output_root, request)
    if Path(path).resolve() != (root / "request.json").resolve():
        _invalid("request path is not the prepared artifact identity")
    return request, root


def inspect_entry_price_confirmation(*, request_path, output_root):
    request, root = _read_request(request_path, output_root)
    result = {"request_id": request.request_id, "request_sha256": request.request_sha256,
              "stage": "PREPARED", "evidence_level": request.evidence_level,
              "database_written": False, "binding_activated": False}
    parent = None
    for name in ("prediction/all.json", "settlement/all.json", "evaluation.json"):
        if not (root / name).exists():
            break
        payload = _read_stage(root, name, request.request_sha256, parent_sha256=parent)
        parent = payload["stage_sha256"]
        result["stage"] = payload["stage"]
        if "evaluation" in payload:
            result["evaluation"] = payload["evaluation"]
    return result


def read_confirmed_entry_price_artifact(evaluation_path, *, recompute=False):
    path = Path(evaluation_path).resolve()
    root = path.parent
    if path.name != "evaluation.json":
        _invalid("confirmation evidence must reference its canonical evaluation file")
    request, _ = _read_request(root / "request.json", root.parent.parent)
    _verify_existing_consumption(request, root)
    manifest = _read_json(root / "manifest.json")
    if manifest.get("request_sha256") != request.request_sha256:
        _invalid("confirmation manifest targets another request")
    expected_files = {"request.json", "consuming.json", "prediction/all.json", "settlement/all.json", "evaluation.json"}
    if set(manifest.get("files", {})) != expected_files:
        _invalid("confirmation manifest is incomplete")
    for name, reference in manifest["files"].items():
        actual = evidence_reference_for_file(root / name, role=name).model_dump(mode="json")
        if actual != reference:
            _invalid("confirmation manifest member changed after evaluation")
    prediction = _read_stage(root, "prediction/all.json", request.request_sha256)
    settlement = _read_stage(root, "settlement/all.json", request.request_sha256, parent_sha256=prediction["stage_sha256"])
    evaluation = _read_stage(root, "evaluation.json", request.request_sha256, parent_sha256=settlement["stage_sha256"])
    result = evaluation["evaluation"]
    if (request.study_type != "CONFIRMATION" or request.evidence_level != "LOCKED_HISTORICAL_OOT"
            or result.get("status") != "CONFIRMED_PRICE_DISTRIBUTION"
            or result.get("request_sha256") != request.request_sha256):
        _invalid("artifact does not confirm the selected entry price scope")
    if recompute:
        verified = evaluate_entry_price_confirmation(
            request, [EntryPriceConfirmationPredictionDay.model_validate(row) for row in prediction["days"]],
            [EntryPriceConfirmationSettlementDay.model_validate(row) for row in settlement["days"]],
        )
        if verified != result:
            _invalid("confirmation evaluation differs from the frozen observations")
    return request, result


def _immutable_json(path, payload):
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != encoded:
            _invalid("immutable confirmation artifact differs; only exact retry is allowed")
        return
    _write_atomic_text(path, encoded, replace_existing=False)


def _read_json(path):
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        _invalid("confirmation artifact must be a JSON object")
    return payload


def _stage_payload(request, stage, **values):
    payload = {"request_sha256": request.request_sha256, "stage": stage, **values}
    return {**payload, "stage_sha256": canonical_json_sha256(payload)}


def _read_stage(root, name, request_sha256, *, parent_sha256=None):
    value = _read_json(root / name)
    payload = {key: item for key, item in value.items() if key != "stage_sha256"}
    if (value.get("request_sha256") != request_sha256 or value.get("stage_sha256") != canonical_json_sha256(payload)
            or (parent_sha256 is not None and value.get("parent_sha256") != parent_sha256)):
        _invalid("confirmation stage hash or parent identity changed")
    return value


def _policy(request):
    contract = request.window_contract
    return research_policy_identity(baseline_policy_sha256=contract.baseline_policy_sha256,
                                    shadow_policy_sha256=contract.shadow_policy_sha256, cost_policy_sha256=contract.cost_policy_sha256)


def _window_access(request):
    return build_window_access_request(
        contract_sha256=request.window_contract.contract_sha256, study_type=request.study_type,
        objective_contract=request.objective_contract, decision_use=request.decision_use,
        dataset_identity=request.data_identity.dataset_identity, policy_identity=_policy(request),
        start_date=request.target_calendar[0], end_date=request.target_calendar[-1], frontier_id=request.frontier_id,
        candidate_id=f"{request.candidate_id}:{request.request_sha256}",
    )


def _verify_existing_consumption(request, root):
    consuming = _read_json(root / "consuming.json")
    if consuming.get("request_sha256") != request.request_sha256 or consuming.get("stage") != "CONSUMING":
        _invalid("missing or conflicting window consumption before settlement")
    access = _window_access(request)
    if request.study_type == "CONFIRMATION":
        prior = SealedHoldoutConsumptionReceiptV1.model_validate(_read_json(Path(request.window_contract.sealed_consumption_receipt_uri)))
        sealed = next(window for window in request.window_contract.windows if window.state == "SEALED_UNCONSUMED")
        expected = build_holdout_consumption_receipt(contract=request.window_contract, request=access, window_id=sealed.window_id)
        if (prior.functional_payload() != expected.functional_payload()
                or consuming.get("authorization", {}).get("consumption_receipt") != prior.model_dump(mode="json")):
            _invalid("global sealed consumption receipt differs from frozen prediction request")
    elif consuming.get("authorization", {}).get("request_id") != access.request_id:
        _invalid("development access authorization differs from frozen request")


def _authorize_or_resume(request, access):
    contract = request.window_contract
    path = Path(contract.sealed_consumption_receipt_uri)
    if request.study_type == "CONFIRMATION" and path.exists():
        prior = SealedHoldoutConsumptionReceiptV1.model_validate(_read_json(path))
        sealed = next(window for window in contract.windows if window.state == "SEALED_UNCONSUMED")
        expected = build_holdout_consumption_receipt(contract=contract, request=access, window_id=sealed.window_id)
        if prior.functional_payload() != expected.functional_payload():
            _invalid("sealed window belongs to another request; only exact resume is allowed")
        return {
            "status": "AUTHORIZED_SEALED_HOLDOUT_ONCE", "request_id": access.request_id,
            "sealed_holdout_accessed": True, "consumption_receipt": prior.model_dump(mode="json"),
        }
    return authorize_research_window_access(contract=contract, request=access, consume_receipt_path=path)


def evaluate_entry_price_confirmation(
    request: AdvisoryEntryPriceConfirmationRequestV1,
    predictions: Sequence[EntryPriceConfirmationPredictionDay],
    settlements: Sequence[EntryPriceConfirmationSettlementDay],
) -> dict:
    """Fixed, paired, date-weighted price-distribution test, never a profitability test."""
    validate_prediction_plan(request, predictions)
    if tuple(day.target_trade_date for day in settlements) != request.target_calendar:
        _invalid("settlement dates differ from the full prediction window")
    counts = Counter(total_candidates=0, market_available=0, market_unknown=0, suspended=0, model_available=0, prediction_crossings=0)
    records, daily, paired_calendar, valid_per_date = [], [], [], []
    for planned, predicted, settled in zip(request.days, predictions, settlements):
        if tuple(row.symbol for row in settled.outcomes) != planned.candidate_symbols:
            _invalid("settlement roster differs from frozen candidates")
        day_records = []
        for candidate, continuous, outcome in zip(predicted.envelope.candidates, predicted.rows, settled.outcomes):
            counts["total_candidates"] += 1
            counts["prediction_crossings"] += int(any(
                values is not None and (values[0] > values[1] or values[1] > values[2])
                for values in (continuous.raw_gaps, continuous.calibrated_gaps)
            ))
            if outcome.market_status == "NOT_APPLICABLE":
                counts["suspended"] += 1
                continue
            if outcome.market_status == "UNAVAILABLE":
                counts["market_unknown"] += 1
                continue
            counts["market_available"] += 1
            if candidate.entry_price.status != "AVAILABLE":
                continue
            counts["model_available"] += 1
            day_records.append(entry_price_observation_metrics(
                candidate, continuous, outcome, (request.control.q10, request.control.q50, request.control.q90),
            ))
        valid_per_date.append(len(day_records))
        if day_records:
            records.extend(day_records)
            daily.append(_means(day_records))
            paired_calendar.append(daily[-1]["interval_score_difference"])
        else:
            paired_calendar.append(float("nan"))
    row_weighted, date_weighted = _means(records), _means(daily)
    criteria = request.criteria
    sufficient_days = sum(n >= criteria.minimum_rows_per_date for n in valid_per_date)
    model_availability = counts["model_available"] / counts["market_available"] if counts["market_available"] else 0.0
    support = (
        len(daily) >= criteria.minimum_dates and len(records) >= criteria.minimum_rows
        and sufficient_days / len(request.days) >= criteria.minimum_date_fraction
    )
    ci = moving_block_interval(
        paired_calendar, request_hash=request.request_sha256,
        block_days=criteria.block_days, samples=criteria.bootstrap_samples,
    ) if len(daily) >= criteria.block_days else None
    width_ratio = (
        date_weighted["tick_width"] / date_weighted["control_tick_width"]
        if date_weighted and date_weighted["control_tick_width"] > 0 else None
    )
    reasons = []
    if counts["market_unknown"] or any(row["zero_control_width"] for row in records):
        status = "INPUT_INCOMPLETE"
        reasons.append("UNKNOWN_MARKET_INPUT_OR_ZERO_CONTROL_WIDTH")
    elif not support or ci is None:
        status = "INCONCLUSIVE"
        reasons.append("INSUFFICIENT_INDEPENDENT_DATE_SUPPORT")
    elif (
        model_availability < criteria.minimum_model_availability
        or counts["prediction_crossings"] > 0
        or not criteria.minimum_coverage <= date_weighted["continuous_coverage"] <= criteria.maximum_coverage
        or date_weighted["tick_coverage"] < criteria.minimum_coverage
        or width_ratio is None or width_ratio > criteria.maximum_width_ratio
        or ci[0] > 0
    ):
        status = "NOT_CONFIRMED"
        reasons.append("FROZEN_PRICE_DISTRIBUTION_CRITERIA_NOT_MET")
    elif ci[1] > 0:
        status = "INCONCLUSIVE"
        reasons.append("PAIRED_INTERVAL_SCORE_COMPARISON_STRADDLES_ZERO")
    else:
        status = "CONFIRMED_PRICE_DISTRIBUTION"
    # Old windows may exercise every criterion but cannot create new confirmation evidence.
    criteria_status = status
    if request.study_type != "CONFIRMATION" and status == "CONFIRMED_PRICE_DISTRIBUTION":
        status = "INCONCLUSIVE"
        reasons.append("DEVELOPMENT_REPLAY_IS_NOT_CONFIRMATION_EVIDENCE")
    return {
        "schema_version": "advisory_entry_price_confirmation_evaluation_v1",
        "request_sha256": request.request_sha256, "status": status, "criteria_status": criteria_status,
        "reason_codes": reasons, "objective_contract": request.objective_contract,
        "evidence_level": request.evidence_level,
        "decision_use": request.decision_use if status in {"CONFIRMED_PRICE_DISTRIBUTION", "NOT_CONFIRMED"} else "NAVIGATION_ONLY",
        "counts": dict(counts), "valid_rows": len(records), "valid_dates": len(daily),
        "planned_dates": len(request.days), "valid_rows_per_date": valid_per_date,
        "model_availability_in_evaluable_population": model_availability,
        "row_weighted": row_weighted, "date_weighted": date_weighted,
        "business_width_ratio": width_ratio, "interval_score_difference_ci95": ci,
        "database_written": False, "binding_activated": False, "profitability_confirmed": False,
    }


def entry_price_observation_metrics(candidate, continuous, outcome, control) -> dict[str, float]:
    """One frozen prediction/outcome pair, identical in historical and natural-forward paths."""
    anchor = candidate.decision_reference_price * candidate.target_raw_price_multiplier
    actual_gap = outcome.raw_open / anchor - 1
    raw, gaps = continuous.raw_gaps, continuous.calibrated_gaps
    band, control_band = candidate.entry_price.calibrated_range, continuous.control_range
    continuous_metrics = _interval_metrics(gaps, actual_gap)
    raw_metrics = _interval_metrics(raw, actual_gap)
    business_gaps = tuple(value / anchor - 1 for value in (band.low, band.mid, band.high))
    tick_metrics = _interval_metrics(business_gaps, actual_gap)
    control_metrics = _interval_metrics(control, actual_gap)
    control_width = (control_band.high - control_band.low) / anchor
    return {
        **{f"continuous_{k}": v for k, v in continuous_metrics.items()},
        **{f"raw_{k}": v for k, v in raw_metrics.items()},
        **{f"tick_{k}": v for k, v in tick_metrics.items()},
        "control_tick_width": control_width,
        "control_interval_score": control_metrics["interval_score"],
        "interval_score_difference": continuous_metrics["interval_score"] - control_metrics["interval_score"],
        "rounding_rescue": float(not continuous_metrics["coverage"] and tick_metrics["coverage"]),
        "rounding_harm": float(continuous_metrics["coverage"] and not tick_metrics["coverage"]),
        "crossing": float(raw[0] > raw[1] or raw[1] > raw[2] or gaps[0] > gaps[1] or gaps[1] > gaps[2]),
        "zero_control_width": float(control_width <= 0),
    }


def validate_prediction_plan(request, predictions) -> None:
    from .entry_price_confirmation_contracts import validate_entry_projection
    if tuple(row.envelope.target_trade_date for row in predictions) != request.target_calendar:
        _invalid("prediction dates differ from the entire frozen calendar")
    scope = request.scope
    for day, prediction in zip(request.days, predictions):
        env = prediction.envelope
        expected = {
            "program_id": request.program_id, "binding_version_id": request.binding_version_id,
            "package_id": scope.package_id, "package_manifest_sha256": scope.package_manifest_sha256,
            "style_profile_hash": scope.style_profile_hash, "review_policy_sha256": scope.review_policy_sha256,
            "universe_identity_sha256": scope.universe_identity_sha256,
            "candidate_projection_sha256": scope.candidate_projection_sha256,
            "feature_schema_sha256": scope.feature_schema_sha256,
            "price_range_bundle_id": scope.price_range_bundle_id,
            "price_range_bundle_manifest_sha256": scope.price_range_bundle_manifest_sha256,
            "decision_as_of_trade_date": day.decision_as_of_trade_date,
            "target_trade_date": day.target_trade_date, "role_binding_sha256": request.request_sha256,
            "evidence_state": "EXPERIMENTAL", "nominal_coverage": request.criteria.nominal_coverage,
            "projection_producer_version": request.projection_producer_version,
        }
        if (
            any(getattr(env, key) != value for key, value in expected.items())
            or env.training_lineage is None or env.training_lineage.parent_bundle_id != scope.parent_bundle_id
            or env.training_lineage.outcome_bundle_id != scope.outcome_bundle_id
            or tuple(row.symbol for row in prediction.rows) != day.candidate_symbols
            or prediction.candidate_source_sha256 != day.candidate_source_sha256
        ):
            _invalid("prediction scope, lineage or candidate roster differs from request")
        for candidate, row in zip(env.candidates, prediction.rows):
            if candidate.entry_price.status == "AVAILABLE":
                validate_entry_projection(candidate, (request.control.q10, request.control.q50, request.control.q90), row.control_range)


def moving_block_interval(values, *, request_hash: str, block_days: int, samples: int) -> list[float] | None:
    data = np.asarray(values, dtype=float)
    if len(data) < block_days or np.isinf(data).any() or np.isfinite(data).sum() < block_days:
        _invalid("paired date statistic lacks finite block support")
    rng = np.random.default_rng(int(request_hash[:16], 16))
    block_count = (len(data) + block_days - 1) // block_days
    means = np.empty(samples)
    offsets = np.arange(block_days)
    # Bounded memory: no samples x candidates tensor; only one sample's date blocks at a time.
    for index in range(samples):
        starts = rng.integers(0, len(data) - block_days + 1, size=block_count)
        draw = (starts[:, None] + offsets).ravel()[:len(data)]
        draw_values = data[draw]
        if not np.isfinite(draw_values).any():
            return None
        means[index] = np.nanmean(draw_values)
    return [float(value) for value in np.quantile(means, [0.025, 0.975], method="linear")]


def _interval_metrics(gaps, actual) -> dict[str, float]:
    low, mid, high = gaps
    losses = []
    for quantile, prediction in zip((0.1, 0.5, 0.9), gaps):
        error = actual - prediction
        losses.append(max(quantile * error, (quantile - 1) * error))
    return {
        "coverage": float(low <= actual <= high), "lower_miss": float(actual < low),
        "upper_miss": float(actual > high), "width": high - low,
        "midpoint_absolute_error": abs(actual - mid),
        "pinball_q10": losses[0], "pinball_q50": losses[1], "pinball_q90": losses[2],
        "interval_score": high - low + 10 * max(low - actual, 0) + 10 * max(actual - high, 0),
    }


def _means(rows) -> dict[str, float]:
    return {key: float(np.mean([row[key] for row in rows])) for key in rows[0]} if rows else {}


def _invalid(message: str):
    raise AdvisoryModelFirstError(message, reason_code="ADVISORY_ENTRY_CONFIRMATION_IDENTITY_MISMATCH")
