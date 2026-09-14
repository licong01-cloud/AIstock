from __future__ import annotations

import hashlib
import json
import os
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from uuid import uuid4

from pydantic import ValidationError

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import FEATURE_SCHEMA_HASH
from backend.services.advisory_model_first.model_binding_resolution import (
    AdvisoryModelBindingResolutionV1,
)
from backend.services.advisory_model_first.model_bundle import load_frozen_research_bundle
from backend.services.advisory_model_first.model_inference import (
    AdvisoryModelShadowService,
    _resolve_decision_date,
)
from backend.services.advisory_model_first.outcome_bundle import (
    read_outcome_bundle_manifest,
)
from backend.services.advisory_model_first.outcome_runtime_bundle import (
    load_frozen_outcome_bundle,
)
from backend.services.advisory_model_first.price_range_calibration_bundle import (
    validate_calibrated_daily_price_envelope_bundle,
)
from backend.services.advisory_model_first.price_range_contracts import (
    canonical_json_sha256,
)
from backend.services.advisory_model_first.prospective_price_contracts import (
    AdvisoryPriceProspectivePredictionReceiptV1,
    FrozenAdvisoryPriceProspectiveRequestV1,
    build_advisory_price_prospective_prediction_receipt,
    build_frozen_price_prospective_request,
    target_open_utc,
)
from backend.services.advisory_model_first.price_range_runtime_bundle import (
    load_frozen_price_range_bundle,
)
from backend.services.advisory_model_first.realtime_feature_source import (
    PostgresAdvisoryReviewSource,
)
from backend.services.advisory_program import AdvisoryProgramService
from backend.services.selection_center.models import SelectionRunStatus
from backend.services.selection_center.service import SelectionCenterService


PROSPECTIVE_ROOT_NAME = "price_range_prospective_predictions"


class _FrozenProspectiveResolver:
    def __init__(self, request: FrozenAdvisoryPriceProspectiveRequestV1) -> None:
        self._request = request

    def is_configured(self, **_kwargs: Any) -> bool:
        return True

    def resolve(
        self,
        *,
        model_root: str | Path,
        program: Any,
        active_binding: Mapping[str, Any],
        selection_run: Any,
    ) -> AdvisoryModelBindingResolutionV1:
        del model_root
        request = self._request
        package_ids = tuple(str(value) for value in program.package_ids)
        manifest_sha256 = str(selection_run.manifest_sha256_by_package.get(request.package_id) or "")
        actual = {
            "program_id": str(program.program_id),
            "binding_version_id": str(active_binding.get("binding_version_id") or ""),
            "package_ids": package_ids,
            "selection_run_id": str(selection_run.run_id),
            "manifest_sha256": manifest_sha256,
        }
        expected = {
            "program_id": request.program_id,
            "binding_version_id": request.binding_version_id,
            "package_ids": (request.package_id,),
            "selection_run_id": request.selection_run_id,
            "manifest_sha256": request.manifest_sha256,
        }
        if actual != expected:
            raise AdvisoryModelFirstError(
                "prospective runtime inputs differ from the frozen request",
                reason_code="ADVISORY_PRICE_PROSPECTIVE_INPUT_IDENTITY_MISMATCH",
                context={"actual": actual},
            )
        return AdvisoryModelBindingResolutionV1(
            program_id=request.program_id,
            binding_version_id=request.binding_version_id,
            package_id=request.package_id,
            manifest_sha256=request.manifest_sha256,
            style_profile_id=request.style_profile_id,
            style_profile_hash=request.style_profile_hash,
            selection_runtime_semantics_hash=request.selection_runtime_semantics_hash,
            feature_schema_version=request.feature_schema_version,
            feature_schema_hash=request.feature_schema_hash,
            bundle_id=request.parent_bundle_id,
            bundle_manifest_sha256=request.parent_bundle_manifest_sha256,
            component_roles=dict(request.component_roles),
            descriptor_sha256=request.request_sha256,
            terminal_weights=dict(request.terminal_weights),
        )


class AdvisoryPriceProspectivePredictionService:
    def __init__(
        self,
        *,
        program_service: AdvisoryProgramService | Any | None = None,
        review_source: PostgresAdvisoryReviewSource | Any | None = None,
        selection_service: SelectionCenterService | Any | None = None,
        shadow_service_factory: Callable[[FrozenAdvisoryPriceProspectiveRequestV1], Any] | None = None,
        now_provider: Callable[[], datetime] | None = None,
    ) -> None:
        self._program_service = program_service or AdvisoryProgramService()
        self._review_source = review_source or PostgresAdvisoryReviewSource()
        self._selection_service = selection_service or SelectionCenterService()
        self._shadow_service_factory = shadow_service_factory
        self._now_provider = now_provider or (lambda: datetime.now(timezone.utc))

    def prepare_request(
        self,
        *,
        program_id: str,
        target_trade_date: date,
        model_root: str | Path,
        parent_bundle_id: str,
        outcome_bundle_id: str,
        price_range_bundle_id: str,
        list_version_id: str | None = None,
    ) -> FrozenAdvisoryPriceProspectiveRequestV1:
        now = _aware_utc(self._now_provider(), field="now")
        target_open = target_open_utc(target_trade_date)
        if now >= target_open:
            raise _prospective_error(
                "prospective request cannot be prepared at or after target open",
                "ADVISORY_PRICE_PROSPECTIVE_CLOCK_INVALID",
            )
        root = Path(model_root).resolve()
        price_path = root / "price_range_bundles" / price_range_bundle_id
        price_manifest = validate_calibrated_daily_price_envelope_bundle(
            price_path,
            expected_bundle_id=price_range_bundle_id,
        )
        if price_manifest.get("schema_version") != "advisory_price_range_bundle_v4":
            raise _prospective_error(
                "prospective price prediction requires the frozen v4 bundle",
                "ADVISORY_PRICE_PROSPECTIVE_BUNDLE_INVALID",
            )
        if (
            str(price_manifest.get("parent_bundle_id")) != parent_bundle_id
            or str(price_manifest.get("outcome_bundle_id")) != outcome_bundle_id
        ):
            raise _prospective_error(
                "prospective bundle lineage differs from the requested M1/M3 identity",
                "ADVISORY_PRICE_PROSPECTIVE_BUNDLE_INVALID",
            )
        package_id = str(price_manifest["package_id"])
        package_manifest_sha256 = str(price_manifest["manifest_sha256"])
        style_profile_hash = str(price_manifest["style_profile_hash"])
        parent_path = root / "bundles" / parent_bundle_id
        parent_manifest_path = parent_path / "manifest.json"
        parent_manifest = _read_json(parent_manifest_path)
        parent_manifest_sha256 = _sha256_file(parent_manifest_path)
        parent_bundle = load_frozen_research_bundle(
            model_root=root,
            bundle_id=parent_bundle_id,
            expected_package_id=package_id,
            expected_manifest_sha256=package_manifest_sha256,
            expected_selection_runtime_semantics_hash=str(
                parent_manifest.get("selection_runtime_semantics_hash") or ""
            ),
            booster_factory=lambda path: path,
        )
        outcome_path = root / "outcome_bundles" / outcome_bundle_id
        outcome_manifest_path = outcome_path / "manifest.json"
        outcome_manifest_sha256 = _sha256_file(outcome_manifest_path)
        read_outcome_bundle_manifest(outcome_path, expected_bundle_id=outcome_bundle_id)
        load_frozen_outcome_bundle(
            model_root=root,
            outcome_bundle_id=outcome_bundle_id,
            outcome_bundle_manifest_sha256=outcome_manifest_sha256,
            expected_package_id=package_id,
            expected_manifest_sha256=package_manifest_sha256,
            expected_style_profile_hash=style_profile_hash,
            expected_parent_bundle_id=parent_bundle_id,
            booster_factory=lambda path: path,
        )
        price_manifest_sha256 = _sha256_file(price_path / "manifest.json")
        load_frozen_price_range_bundle(
            model_root=root,
            price_range_bundle_id=price_range_bundle_id,
            price_range_bundle_manifest_sha256=price_manifest_sha256,
            expected_package_id=package_id,
            expected_manifest_sha256=package_manifest_sha256,
            expected_style_profile_hash=style_profile_hash,
            expected_parent_bundle_id=parent_bundle_id,
            expected_outcome_bundle_id=outcome_bundle_id,
            booster_factory=lambda path: path,
        )
        calibration_request = _read_json(price_path / "calibration_request.json")
        model_frozen_at = _parse_datetime(calibration_request.get("created_at"), field="model_frozen_at")
        if now < model_frozen_at:
            raise _prospective_error(
                "prospective request predates the frozen v4 model",
                "ADVISORY_PRICE_PROSPECTIVE_CLOCK_INVALID",
            )
        program = self._program_service.get_program(program_id)
        binding = self._program_service.active_binding(program_id)
        if tuple(program.package_ids) != (package_id,) or tuple(binding.get("package_ids") or ()) != (package_id,):
            raise _prospective_error(
                "prospective Program package differs from the frozen v4 bundle",
                "ADVISORY_PRICE_PROSPECTIVE_INPUT_IDENTITY_MISMATCH",
            )
        list_version, list_items = self._list_context(
            program_id=program_id,
            target_trade_date=target_trade_date,
            list_version_id=list_version_id,
        )
        if (
            list_version.get("version_status") != "PUBLISHED"
            or list_version.get("binding_version_id") != binding["binding_version_id"]
        ):
            raise _prospective_error(
                "prospective recommendation list is not the published active binding output",
                "ADVISORY_PRICE_PROSPECTIVE_INPUT_IDENTITY_MISMATCH",
            )
        review_run_id = str(list_version.get("review_run_id") or "")
        review = self._review_source.get(review_run_id)
        selection_run_id = str(review.selection_run_id or "")
        selection_ids = tuple(str(value) for value in review.selection_run_ids)
        selection_run = self._selection_service.get_run(selection_run_id)
        if (
            review.program_id != program_id
            or review.binding_version_id != binding["binding_version_id"]
            or review.trade_date != target_trade_date
            or selection_ids != (selection_run_id,)
            or selection_run.status != SelectionRunStatus.SUCCEEDED
            or selection_run.trade_date != target_trade_date
            or tuple(selection_run.package_ids) != (package_id,)
            or selection_run.manifest_sha256_by_package.get(package_id) != package_manifest_sha256
        ):
            raise _prospective_error(
                "prospective persisted input lineage is inconsistent",
                "ADVISORY_PRICE_PROSPECTIVE_INPUT_IDENTITY_MISMATCH",
            )
        decision_date = _resolve_decision_date(
            list_version=list_version,
            selection_run=selection_run,
        )
        _require_next_trading_day(
            self._program_service.calendar_provider,
            decision_date=decision_date,
            target_trade_date=target_trade_date,
        )
        symbols = _prospective_candidate_symbols(
            selection_rows=selection_run.aggregate_results,
            list_items=list_items,
            list_version=list_version,
            target_count=int(program.target_count),
        )
        terminal_weights = {
            str(key): float(value) for key, value in dict(parent_bundle.manifest["terminal_weights"]).items()
        }
        component_roles = _component_roles(terminal_weights)
        return build_frozen_price_prospective_request(
            created_at=now,
            model_frozen_at=model_frozen_at,
            target_open_at=target_open,
            program_id=program_id,
            binding_version_id=str(binding["binding_version_id"]),
            list_version_id=str(list_version["list_version_id"]),
            review_run_id=review_run_id,
            selection_run_id=selection_run_id,
            candidate_count=len(symbols),
            candidate_symbols_sha256=canonical_json_sha256(symbols),
            decision_as_of_trade_date=decision_date,
            target_trade_date=target_trade_date,
            package_id=package_id,
            manifest_sha256=package_manifest_sha256,
            style_profile_id=str(price_manifest["style_profile_id"]),
            style_profile_hash=style_profile_hash,
            selection_runtime_semantics_hash=str(parent_bundle.manifest["selection_runtime_semantics_hash"]),
            parent_bundle_id=parent_bundle_id,
            parent_bundle_manifest_sha256=parent_manifest_sha256,
            outcome_bundle_id=outcome_bundle_id,
            outcome_bundle_manifest_sha256=outcome_manifest_sha256,
            price_range_bundle_id=price_range_bundle_id,
            price_range_bundle_manifest_sha256=price_manifest_sha256,
            feature_schema_hash=FEATURE_SCHEMA_HASH,
            review_policy_sha256=str(program.review_policy_sha256),
            component_roles=component_roles,
            terminal_weights=terminal_weights,
        )

    def capture(
        self,
        *,
        request: FrozenAdvisoryPriceProspectiveRequestV1,
        model_root: str | Path,
    ) -> AdvisoryPriceProspectivePredictionReceiptV1:
        started = time.monotonic()
        root = Path(model_root).resolve()
        artifact_root = root / PROSPECTIVE_ROOT_NAME
        target = artifact_root / request.request_id
        if target.exists():
            return self._read_existing(target, request=request, status="ALREADY_MATERIALIZED")
        now = _aware_utc(self._now_provider(), field="now")
        if now >= request.target_open_at.astimezone(timezone.utc):
            raise _prospective_error(
                "prospective prediction cannot be computed at or after target open",
                "ADVISORY_PRICE_PROSPECTIVE_CLOCK_INVALID",
            )
        if now < request.created_at.astimezone(timezone.utc):
            raise _prospective_error(
                "prospective capture time predates its frozen request",
                "ADVISORY_PRICE_PROSPECTIVE_CLOCK_INVALID",
            )
        program = self._program_service.get_program(request.program_id)
        service = (
            self._shadow_service_factory(request)
            if self._shadow_service_factory is not None
            else self._build_shadow_service(request=request, model_root=root)
        )
        result = service.model_shadow_for_forward(
            program=program,
            binding_version_id=request.binding_version_id,
            target_trade_date=request.target_trade_date,
            list_version_id=request.list_version_id,
            review_run_id=request.review_run_id,
            selection_run_id=request.selection_run_id,
        )
        price = result.get("price_range") if isinstance(result, Mapping) else None
        if (
            result.get("status") != "EXPERIMENTAL_SHADOW"
            or not isinstance(price, Mapping)
            or price.get("status") != "EXPERIMENTAL_SHADOW"
            or price.get("price_range_bundle_id") != request.price_range_bundle_id
            or result.get("bundle_id") != request.parent_bundle_id
        ):
            raise _prospective_error(
                "prospective model output is unavailable or differs from the frozen request",
                "ADVISORY_PRICE_PROSPECTIVE_BUNDLE_INVALID",
                context={
                    "model_status": result.get("status"),
                    "model_reason_code": result.get("reason_code"),
                    "price_status": price.get("status") if isinstance(price, Mapping) else None,
                    "price_reason_code": price.get("reason_code") if isinstance(price, Mapping) else None,
                },
            )
        candidates = list(price.get("candidates") or [])
        symbols = _candidate_symbols(candidates)
        if (
            len(candidates) != request.candidate_count
            or canonical_json_sha256(symbols) != request.candidate_symbols_sha256
        ):
            raise _prospective_error(
                "prospective prediction candidate identity differs from the frozen list",
                "ADVISORY_PRICE_PROSPECTIVE_INPUT_IDENTITY_MISMATCH",
            )
        prediction = {
            "schema_version": "advisory_price_prospective_prediction_v1",
            "request_id": request.request_id,
            "request_sha256": request.request_sha256,
            "generated_at": now.isoformat(),
            "program_id": request.program_id,
            "binding_version_id": request.binding_version_id,
            "list_version_id": request.list_version_id,
            "review_run_id": request.review_run_id,
            "selection_run_id": request.selection_run_id,
            "decision_as_of_trade_date": request.decision_as_of_trade_date.isoformat(),
            "target_trade_date": request.target_trade_date.isoformat(),
            "parent_bundle_id": request.parent_bundle_id,
            "outcome_bundle_id": request.outcome_bundle_id,
            "price_range_bundle_id": request.price_range_bundle_id,
            "evidence_level": "PROSPECTIVE_OOS",
            "realized_outcome_accessed": False,
            "price_envelope": dict(price),
        }
        _reject_result_fields(prediction)
        prediction_sha256 = canonical_json_sha256(prediction)
        prediction_bundle_id = canonical_json_sha256(
            {
                "request_sha256": request.request_sha256,
                "prediction_sha256": prediction_sha256,
            }
        )
        available_count = sum(item.get("availability_status") == "AVAILABLE" for item in candidates)
        receipt_seed = {
            "status": "PUBLISHED",
            "request_id": request.request_id,
            "request_sha256": request.request_sha256,
            "prediction_bundle_id": prediction_bundle_id,
            "prediction_sha256": prediction_sha256,
            "decision_as_of_trade_date": request.decision_as_of_trade_date,
            "target_trade_date": request.target_trade_date,
            "published_at": now,
            "candidate_count": len(candidates),
            "available_count": available_count,
            "unavailable_count": len(candidates) - available_count,
            "elapsed_seconds": round(time.monotonic() - started, 6),
            "parent_bundle_id": request.parent_bundle_id,
            "outcome_bundle_id": request.outcome_bundle_id,
            "price_range_bundle_id": request.price_range_bundle_id,
        }
        return self._publish(
            target=target,
            request=request,
            prediction=prediction,
            receipt_seed=receipt_seed,
        )

    def _build_shadow_service(
        self,
        *,
        request: FrozenAdvisoryPriceProspectiveRequestV1,
        model_root: Path,
    ) -> AdvisoryModelShadowService:
        return AdvisoryModelShadowService(
            program_service=self._program_service,
            review_source=self._review_source,
            selection_service=self._selection_service,
            model_root_provider=lambda: str(model_root),
            binding_resolver=_FrozenProspectiveResolver(request),
            bundle_loader=lambda **_kwargs: load_frozen_research_bundle(
                model_root=model_root,
                bundle_id=request.parent_bundle_id,
                expected_package_id=request.package_id,
                expected_manifest_sha256=request.manifest_sha256,
                expected_selection_runtime_semantics_hash=request.selection_runtime_semantics_hash,
            ),
            outcome_bundle_loader=lambda **_kwargs: load_frozen_outcome_bundle(
                model_root=model_root,
                outcome_bundle_id=request.outcome_bundle_id,
                outcome_bundle_manifest_sha256=request.outcome_bundle_manifest_sha256,
                expected_package_id=request.package_id,
                expected_manifest_sha256=request.manifest_sha256,
                expected_style_profile_hash=request.style_profile_hash,
                expected_parent_bundle_id=request.parent_bundle_id,
            ),
            price_range_bundle_loader=lambda **_kwargs: load_frozen_price_range_bundle(
                model_root=model_root,
                price_range_bundle_id=request.price_range_bundle_id,
                price_range_bundle_manifest_sha256=request.price_range_bundle_manifest_sha256,
                expected_package_id=request.package_id,
                expected_manifest_sha256=request.manifest_sha256,
                expected_style_profile_hash=request.style_profile_hash,
                expected_parent_bundle_id=request.parent_bundle_id,
                expected_outcome_bundle_id=request.outcome_bundle_id,
            ),
        )

    def _list_context(
        self,
        *,
        program_id: str,
        target_trade_date: date,
        list_version_id: str | None,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        if list_version_id:
            detail = self._program_service.recommendation_list_version_detail(list_version_id)
        else:
            versions = self._program_service.recommendation_list_versions(
                program_id,
                limit=500,
                offset=0,
            )
            matches = [
                item
                for item in versions
                if date.fromisoformat(str(item.get("target_trade_date") or item.get("trade_date"))[:10])
                == target_trade_date
            ]
            if not matches:
                raise _prospective_error(
                    "target date has no persisted Advisory recommendation list",
                    "ADVISORY_PRICE_PROSPECTIVE_INPUT_IDENTITY_MISMATCH",
                )
            detail = self._program_service.recommendation_list_version_detail(str(matches[0]["list_version_id"]))
        version = dict(detail.get("list_version") or {})
        items = list(detail.get("items") or [])
        raw_target = version.get("target_trade_date") or version.get("trade_date")
        if (
            version.get("program_id") != program_id
            or not raw_target
            or date.fromisoformat(str(raw_target)[:10]) != target_trade_date
            or not items
        ):
            raise _prospective_error(
                "prospective recommendation list identity is invalid",
                "ADVISORY_PRICE_PROSPECTIVE_INPUT_IDENTITY_MISMATCH",
            )
        return version, items

    def _publish(
        self,
        *,
        target: Path,
        request: FrozenAdvisoryPriceProspectiveRequestV1,
        prediction: dict[str, Any],
        receipt_seed: dict[str, Any],
    ) -> AdvisoryPriceProspectivePredictionReceiptV1:
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.parent / f".{target.name}.{uuid4().hex}.tmp"
        temporary.mkdir()
        try:
            request_payload = request.model_dump(mode="json")
            _write_json(temporary / "request.json", request_payload)
            _write_json(temporary / "prediction.json", prediction)
            manifest = {
                "schema_version": "advisory_price_prospective_prediction_manifest_v1",
                "request_id": request.request_id,
                "request_sha256": request.request_sha256,
                "prediction_bundle_id": receipt_seed["prediction_bundle_id"],
                "files": {
                    "request.json": _file_identity(temporary / "request.json"),
                    "prediction.json": _file_identity(temporary / "prediction.json"),
                },
            }
            _write_json(temporary / "manifest.json", manifest)
            manifest_sha256 = _sha256_file(temporary / "manifest.json")
            receipt = build_advisory_price_prospective_prediction_receipt(
                **receipt_seed,
                manifest_sha256=manifest_sha256,
            )
            _write_json(temporary / "receipt.json", receipt.model_dump(mode="json"))
            for name in ("request.json", "prediction.json", "manifest.json", "receipt.json"):
                if not (temporary / name).is_file():
                    raise _prospective_error(
                        "prospective artifact publication is incomplete",
                        "ADVISORY_PRICE_PROSPECTIVE_ARTIFACT_CONFLICT",
                    )
            try:
                os.replace(temporary, target)
            except OSError:
                if not target.exists():
                    raise
                return self._read_existing(
                    target,
                    request=request,
                    status="ALREADY_MATERIALIZED",
                )
            return self._read_existing(target, request=request, status="PUBLISHED")
        finally:
            _remove_fixed_temporary_directory(temporary)

    def _read_existing(
        self,
        target: Path,
        *,
        request: FrozenAdvisoryPriceProspectiveRequestV1,
        status: str,
    ) -> AdvisoryPriceProspectivePredictionReceiptV1:
        try:
            stored_request = FrozenAdvisoryPriceProspectiveRequestV1.model_validate(_read_json(target / "request.json"))
            prediction = _read_json(target / "prediction.json")
            manifest = _read_json(target / "manifest.json")
            receipt_payload = _read_json(target / "receipt.json")
        except (OSError, ValueError, ValidationError) as exc:
            raise _prospective_error(
                "existing prospective artifact cannot be read",
                "ADVISORY_PRICE_PROSPECTIVE_ARTIFACT_CONFLICT",
                context={"error_type": type(exc).__name__},
            ) from exc
        if stored_request != request:
            raise _prospective_error(
                "existing prospective request differs from the exact retry",
                "ADVISORY_PRICE_PROSPECTIVE_ARTIFACT_CONFLICT",
            )
        files = manifest.get("files") if isinstance(manifest, Mapping) else None
        expected_files = {name: _file_identity(target / name) for name in ("request.json", "prediction.json")}
        prediction_sha256 = canonical_json_sha256(prediction)
        prediction_bundle_id = canonical_json_sha256(
            {
                "request_sha256": request.request_sha256,
                "prediction_sha256": prediction_sha256,
            }
        )
        price_envelope = prediction.get("price_envelope")
        candidates = list(price_envelope.get("candidates") or []) if isinstance(price_envelope, Mapping) else []
        available_count = sum(
            item.get("availability_status") == "AVAILABLE" for item in candidates if isinstance(item, Mapping)
        )
        receipt_expected = {
            "request_id": request.request_id,
            "request_sha256": request.request_sha256,
            "prediction_bundle_id": prediction_bundle_id,
            "prediction_sha256": prediction_sha256,
            "manifest_sha256": _sha256_file(target / "manifest.json"),
            "decision_as_of_trade_date": request.decision_as_of_trade_date.isoformat(),
            "target_trade_date": request.target_trade_date.isoformat(),
            "candidate_count": request.candidate_count,
            "available_count": available_count,
            "unavailable_count": request.candidate_count - available_count,
            "parent_bundle_id": request.parent_bundle_id,
            "outcome_bundle_id": request.outcome_bundle_id,
            "price_range_bundle_id": request.price_range_bundle_id,
            "evidence_level": "PROSPECTIVE_OOS",
            "realized_outcome_accessed": False,
            "binding_activated": False,
            "database_written": False,
            "sealed_holdout_consumed": False,
        }
        receipt_actual = {key: receipt_payload.get(key) for key in receipt_expected}
        generated_at = prediction.get("generated_at")
        if (
            manifest.get("schema_version") != "advisory_price_prospective_prediction_manifest_v1"
            or manifest.get("request_id") != request.request_id
            or manifest.get("request_sha256") != request.request_sha256
            or manifest.get("prediction_bundle_id") != prediction_bundle_id
            or files != expected_files
            or receipt_actual != receipt_expected
            or receipt_payload.get("status") != "PUBLISHED"
            or _parse_datetime(receipt_payload.get("published_at"), field="published_at")
            != _parse_datetime(generated_at, field="generated_at")
            or prediction.get("request_id") != request.request_id
            or prediction.get("request_sha256") != request.request_sha256
            or prediction.get("target_trade_date") != request.target_trade_date.isoformat()
            or len(candidates) != request.candidate_count
            or canonical_json_sha256(_candidate_symbols(candidates)) != request.candidate_symbols_sha256
        ):
            raise _prospective_error(
                "existing prospective artifact hashes are inconsistent",
                "ADVISORY_PRICE_PROSPECTIVE_ARTIFACT_CONFLICT",
            )
        receipt = AdvisoryPriceProspectivePredictionReceiptV1.model_validate({**receipt_payload, "status": status})
        return receipt


def write_prospective_request(
    request: FrozenAdvisoryPriceProspectiveRequestV1,
    path: str | Path,
) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = request.model_dump(mode="json")
    if target.exists():
        if _read_json(target) != payload:
            raise _prospective_error(
                "prospective request path already contains different content",
                "ADVISORY_PRICE_PROSPECTIVE_ARTIFACT_CONFLICT",
            )
        return target
    temporary = target.with_name(f".{target.name}.{uuid4().hex}.tmp")
    try:
        _write_json(temporary, payload)
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    return target


def read_prospective_request(path: str | Path) -> FrozenAdvisoryPriceProspectiveRequestV1:
    try:
        return FrozenAdvisoryPriceProspectiveRequestV1.model_validate(_read_json(Path(path)))
    except (OSError, ValueError, ValidationError) as exc:
        raise _prospective_error(
            "prospective request is invalid",
            "ADVISORY_PRICE_PROSPECTIVE_INPUT_IDENTITY_MISMATCH",
            context={"error_type": type(exc).__name__},
        ) from exc


def _candidate_symbols(rows: Sequence[Any]) -> tuple[str, ...]:
    if any(not isinstance(item, Mapping) for item in rows):
        raise _prospective_error(
            "prospective candidates must be JSON objects",
            "ADVISORY_PRICE_PROSPECTIVE_INPUT_IDENTITY_MISMATCH",
        )
    symbols = tuple(
        sorted(
            {
                str(item.get("symbol") or item.get("instrument") or "").strip().upper()
                for item in rows
                if str(item.get("symbol") or item.get("instrument") or "").strip()
            }
        )
    )
    if len(symbols) != len(rows):
        raise _prospective_error(
            "prospective candidates contain missing or duplicate symbols",
            "ADVISORY_PRICE_PROSPECTIVE_INPUT_IDENTITY_MISMATCH",
        )
    return symbols


def _prospective_candidate_symbols(
    *,
    selection_rows: Sequence[Any],
    list_items: Sequence[Mapping[str, Any]],
    list_version: Mapping[str, Any],
    target_count: int,
) -> tuple[str, ...]:
    if target_count < 1 or target_count > 20:
        raise _prospective_error(
            "prospective candidate target count is outside the model contract",
            "ADVISORY_PRICE_PROSPECTIVE_INPUT_IDENTITY_MISMATCH",
        )
    summary = list_version.get("summary_json")
    universe_receipt = summary.get("advisory_universe_receipt") if isinstance(summary, Mapping) else None
    universe_selection = universe_receipt.get("universe_selection") if isinstance(universe_receipt, Mapping) else None
    index_scoped = isinstance(universe_selection, Mapping) and universe_selection.get("mode") in {
        "single_index",
        "index_union",
    }
    rank_by_symbol: dict[str, int] = {}
    if index_scoped:
        for item in list_items:
            if str(item.get("action") or "").upper() == "EXIT":
                continue
            symbol = str(item.get("symbol") or "").strip().upper()
            rank = item.get("rank")
            if symbol and isinstance(rank, int) and not isinstance(rank, bool) and rank > 0:
                rank_by_symbol[symbol] = rank
    selected: list[tuple[int, str]] = []
    for row in selection_rows:
        symbol = str(getattr(row, "symbol", "")).strip().upper()
        rank = rank_by_symbol.get(symbol) if index_scoped else getattr(row, "rank", None)
        if symbol and isinstance(rank, int) and not isinstance(rank, bool) and rank <= target_count:
            selected.append((rank, symbol))
    selected.sort()
    ranks = [rank for rank, _symbol in selected]
    symbols = [symbol for _rank, symbol in selected]
    if (
        not selected
        or len(selected) > target_count
        or ranks != list(range(1, len(selected) + 1))
        or len(set(symbols)) != len(symbols)
    ):
        raise _prospective_error(
            "prospective Selection candidate group is incomplete or non-contiguous",
            "ADVISORY_PRICE_PROSPECTIVE_INPUT_IDENTITY_MISMATCH",
            context={"candidate_count": len(selected), "ranks": ranks},
        )
    return tuple(sorted(symbols))


def _component_roles(terminal_weights: Mapping[str, float]) -> dict[str, str]:
    roles: dict[str, str] = {}
    for role, marker in (("lstm", "lstm"), ("fund", "fund")):
        matches = [name for name in terminal_weights if marker in name.lower()]
        if len(matches) != 1:
            raise _prospective_error(
                "frozen parent bundle component roles are ambiguous",
                "ADVISORY_PRICE_PROSPECTIVE_BUNDLE_INVALID",
                context={"role": role, "matches": sorted(matches)},
            )
        roles[role] = matches[0]
    if set(roles.values()) != set(terminal_weights):
        raise _prospective_error(
            "frozen parent bundle contains unsupported component roles",
            "ADVISORY_PRICE_PROSPECTIVE_BUNDLE_INVALID",
            context={"terminal_weight_keys": sorted(terminal_weights)},
        )
    return roles


def _require_next_trading_day(
    calendar_provider: Any,
    *,
    decision_date: date,
    target_trade_date: date,
) -> None:
    calendar = list(calendar_provider.list_trading_days(decision_date, target_trade_date))
    if calendar != [decision_date, target_trade_date]:
        raise _prospective_error(
            "prospective target is not the next trading day after its decision cutoff",
            "ADVISORY_PRICE_PROSPECTIVE_CLOCK_INVALID",
            context={"trading_days": [item.isoformat() for item in calendar]},
        )


def _reject_result_fields(payload: Mapping[str, Any]) -> None:
    forbidden = {
        "realized_return",
        "realized_excess_return",
        "label",
        "hit_rate",
        "coverage",
        "winner",
    }

    def walk(value: Any) -> None:
        if isinstance(value, Mapping):
            overlap = forbidden.intersection(str(key) for key in value)
            if overlap:
                raise _prospective_error(
                    "prospective prediction contains realized-result fields",
                    "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_ACCESS_FORBIDDEN",
                    context={"fields": sorted(overlap)},
                )
            for item in value.values():
                walk(item)
        elif isinstance(value, (list, tuple)):
            for item in value:
                walk(item)

    walk(payload)


def _file_identity(path: Path) -> dict[str, Any]:
    return {"sha256": _sha256_file(path), "size_bytes": path.stat().st_size}


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON payload is not an object: {path}")
    return payload


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _remove_fixed_temporary_directory(path: Path) -> None:
    if not path.exists():
        return
    for name in ("request.json", "prediction.json", "manifest.json", "receipt.json"):
        (path / name).unlink(missing_ok=True)
    try:
        path.rmdir()
    except OSError:
        pass


def _parse_datetime(value: Any, *, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise _prospective_error(
            f"{field} is invalid",
            "ADVISORY_PRICE_PROSPECTIVE_CLOCK_INVALID",
        ) from exc
    return _aware_utc(parsed, field=field)


def _aware_utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise _prospective_error(
            f"{field} must be timezone-aware",
            "ADVISORY_PRICE_PROSPECTIVE_CLOCK_INVALID",
        )
    return value.astimezone(timezone.utc)


def _prospective_error(
    message: str,
    reason_code: str,
    *,
    context: Mapping[str, Any] | None = None,
) -> AdvisoryModelFirstError:
    return AdvisoryModelFirstError(message, reason_code=reason_code, context=context)
