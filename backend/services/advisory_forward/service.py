from __future__ import annotations

import logging
import hashlib
import os
from dataclasses import replace
from datetime import date, datetime
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from backend.services.advisory_forward.models import (
    AdvisoryForwardModelObservationV1,
    AdvisoryForwardRunV1,
    utcnow,
)
from backend.services.advisory_forward.repository import (
    RETRYABLE_MODEL_OBSERVATION_REASON_CODES,
    AdvisoryForwardPGRepository,
    is_retryable_model_observation,
)
from backend.services.advisory_model_first.model_binding_resolution import AdvisoryModelBindingResolver
from backend.services.advisory_model_first.model_inference import AdvisoryModelShadowService
from backend.services.advisory_program import (
    ACTION_HOLD,
    ACTION_WAITING,
    LIST_VERSION_STATUS_PUBLISHED,
    PROGRAM_STATUS_ENABLED,
    REVIEW_RUN_STATUS_WAITING_DATA,
    REVIEW_RUN_TYPE_RUN,
    AdvisoryCandidate,
    AdvisoryProgram,
    AdvisoryProgramService,
    AdvisoryRecommendationListItem,
    AdvisoryRecommendationListVersion,
    AdvisoryReviewRun,
    candidates_from_selection_run,
    compute_program_metrics,
    decision_to_dict,
    episode_to_dict,
    list_item_to_dict,
    program_to_dict,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256
from backend.services.trading_calendar_status import TradingCalendarStatusService
from backend.services.trading_core.errors import DataUnavailableError


LOGGER = logging.getLogger(__name__)
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
ACTION_WATCH = "WATCH"


class AdvisoryForwardService:
    def __init__(
        self,
        *,
        repository: AdvisoryForwardPGRepository | Any | None = None,
        program_service: AdvisoryProgramService | Any | None = None,
        model_service: AdvisoryModelShadowService | Any | None = None,
        model_resolver: AdvisoryModelBindingResolver | Any | None = None,
        calendar: TradingCalendarStatusService | Any | None = None,
        now_provider: Any | None = None,
        after_close_hour: int | None = None,
        after_close_minute: int | None = None,
    ) -> None:
        self.repository = repository or AdvisoryForwardPGRepository()
        self.program_service = program_service or AdvisoryProgramService()
        self.model_service = model_service or AdvisoryModelShadowService()
        self.model_resolver = model_resolver or AdvisoryModelBindingResolver()
        self.calendar = calendar or TradingCalendarStatusService()
        self.now_provider = now_provider or (lambda: datetime.now(SHANGHAI_TZ))
        if after_close_hour is None and after_close_minute is None:
            self.after_close_hour, self.after_close_minute = _after_close_time()
        elif after_close_hour is None or after_close_minute is None:
            raise ValueError("after_close_hour and after_close_minute must be supplied together")
        else:
            self.after_close_hour, self.after_close_minute = _validate_after_close_parts(
                after_close_hour,
                after_close_minute,
            )

    def status(self) -> dict[str, Any]:
        now = self.now_provider().astimezone(SHANGHAI_TZ)
        return {
            "schema_version": "advisory_forward_status_v1",
            "now": now.isoformat(),
            "is_trading_day": self.calendar.is_trading_day(now.date()),
            "after_close_time": f"{self.after_close_hour:02d}:{self.after_close_minute:02d}:00",
            "run_count": len(self.repository.list_runs(limit=100)),
        }

    def run_once(self) -> dict[str, Any]:
        now = self.now_provider().astimezone(SHANGHAI_TZ)
        results: list[dict[str, Any]] = []
        blocked_program_ids: set[str] = set()
        for pending in self.repository.pending_settlements(on_or_before=now.date()):
            pending_program_id = str(pending["program_id"])
            if pending_program_id in blocked_program_ids:
                results.append(
                    {
                        "program_id": pending_program_id,
                        "forward_run_id": pending.get("forward_run_id"),
                        "status": "SKIPPED_PREVIOUS_SETTLEMENT_PENDING",
                        "stage": "TARGET_OPEN_SETTLE",
                        "reason_code": "ADVISORY_FORWARD_PREVIOUS_SETTLEMENT_PENDING",
                        "target_trade_date": pending["target_trade_date"].isoformat(),
                    }
                )
                continue
            try:
                settlement = self._settle(pending)
                results.append(settlement)
                if settlement.get("status") in {"WAITING_DATA", "FAILED"}:
                    blocked_program_ids.add(pending_program_id)
            except Exception as exc:
                results.append(self._visible_failure(pending, stage="TARGET_OPEN_SETTLE", exc=exc))
                blocked_program_ids.add(pending_program_id)
        if not self._publication_due(now):
            self._retry_one_model_observation(results)
            return {
                "schema_version": "advisory_forward_run_once_v1",
                "decision_as_of_trade_date": None,
                "publication_due": False,
                "results": results,
            }
        decision_date = now.date()
        target_date = self.calendar.next_trading_day(decision_date, inclusive=False)
        programs = self.program_service.list_programs(include_archived=False)
        for program in programs:
            if not _scheduled(program):
                continue
            if program.program_id in blocked_program_ids:
                results.append(
                    {
                        "program_id": program.program_id,
                        "status": "WAITING_DATA",
                        "stage": "AFTER_CLOSE_PUBLISH",
                        "reason_code": "ADVISORY_FORWARD_PREVIOUS_SETTLEMENT_PENDING",
                        "target_trade_date": target_date.isoformat(),
                    }
                )
                continue
            try:
                results.append(self._publish(program.program_id, decision_date=decision_date, target_date=target_date))
            except Exception as exc:
                placeholder = {
                    "program_id": program.program_id,
                    "decision_as_of_trade_date": decision_date,
                    "target_trade_date": target_date,
                }
                results.append(self._visible_failure(placeholder, stage="AFTER_CLOSE_PUBLISH", exc=exc))
        self._retry_one_model_observation(results)
        return {
            "schema_version": "advisory_forward_run_once_v1",
            "decision_as_of_trade_date": decision_date.isoformat(),
            "target_trade_date": target_date.isoformat(),
            "publication_due": True,
            "results": results,
        }

    def detail(self, forward_run_id: str) -> dict[str, Any]:
        return self.repository.get(forward_run_id)

    def list_runs(self, *, program_id: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return self.repository.list_runs(program_id=program_id, limit=limit)

    def _publication_due(self, now: datetime) -> bool:
        return self.calendar.is_trading_day(now.date()) and (
            now.hour,
            now.minute,
        ) >= (self.after_close_hour, self.after_close_minute)

    def _retry_one_model_observation(self, results: list[dict[str, Any]]) -> None:
        for persisted in self.repository.retryable_model_observations(limit=1):
            try:
                retry_result = self._resume_published_observation(persisted)
            except Exception as exc:
                LOGGER.exception(
                    "advisory forward bounded model observation retry failed forward_run_id=%s",
                    persisted.get("forward_run_id"),
                )
                results.append(
                    {
                        "program_id": persisted.get("program_id"),
                        "forward_run_id": persisted.get("forward_run_id"),
                        "status": "FAILED",
                        "stage": "MODEL_OBSERVATION_RETRY",
                        "reason_code": str(
                            getattr(exc, "reason_code", None)
                            or getattr(exc, "error_code", None)
                            or "ADVISORY_FORWARD_MODEL_OBSERVATION_RETRY_FAILED"
                        ),
                    }
                )
                continue
            results.append({**retry_result, "stage": "MODEL_OBSERVATION_RETRY"})

    def _publish(self, program_id: str, *, decision_date: date, target_date: date) -> dict[str, Any]:
        initial_program = self.program_service.get_program(program_id)
        initial_binding = self.program_service.active_binding(program_id)
        run = AdvisoryForwardRunV1(
            program_id=initial_program.program_id,
            program_version=initial_program.version,
            binding_version_id=str(initial_binding["binding_version_id"]),
            decision_as_of_trade_date=decision_date,
            target_trade_date=target_date,
        )
        persisted = self.repository.begin_attempt(run)
        stable_identity = {
            "program_id": run.program_id,
            "decision_as_of_trade_date": run.decision_as_of_trade_date,
            "target_trade_date": run.target_trade_date,
        }
        if {key: persisted.get(key) for key in stable_identity} != stable_identity:
            raise RuntimeError(
                "advisory forward attempt identity differs from the current Program/date context"
            )
        if persisted["publication_status"] == "PUBLISHED":
            return self._resume_published_observation(persisted)
        expected_attempt_identity = {
            **stable_identity,
            "program_version": run.program_version,
            "binding_version_id": run.binding_version_id,
        }
        actual_attempt_identity = {key: persisted.get(key) for key in expected_attempt_identity}
        if actual_attempt_identity != expected_attempt_identity:
            raise RuntimeError(
                "unpublished advisory forward attempt identity differs from the current Program binding"
            )
        publication_payload: dict[str, Any] | None = None
        try:
            program, binding, selection_run, runtime_config = self.program_service.prepare_forward_selection(
                program_id,
                decision_as_of_trade_date=decision_date,
                target_trade_date=target_date,
            )
            if (
                program.version != initial_program.version
                or binding.binding_version_id != initial_binding["binding_version_id"]
            ):
                raise DataUnavailableError(
                    "Advisory Program binding changed before forward Selection completed",
                    context={"program_id": program_id, "target_trade_date": target_date.isoformat()},
                )
            active_episodes = self.program_service.active_episode_objects(program_id)
            previous_list = next(
                (
                    item
                    for item in self.program_service.repository.list_versions(program_id, limit=100, offset=0)
                    if item.version_status == LIST_VERSION_STATUS_PUBLISHED and item.trade_date < target_date
                ),
                None,
            )
            previous_items = self.program_service.repository.list_version_items(previous_list.list_version_id) if previous_list else []
            candidates = candidates_from_selection_run(selection_run)
            review_run_id = _stable_id(
                "advrun",
                program.program_id,
                binding.binding_version_id,
                target_date.isoformat(),
                selection_run.run_id,
            )
            list_version, items = _build_publication_list(
                program=program,
                binding_version_id=binding.binding_version_id,
                review_run_id=review_run_id,
                target_trade_date=target_date,
                decision_as_of_trade_date=decision_date,
                selection_run_id=selection_run.run_id,
                candidates=candidates,
                active_episodes=active_episodes,
                previous_list=previous_list,
                previous_items=previous_items,
            )
            review_run = AdvisoryReviewRun(
                review_run_id=review_run_id,
                program_id=program.program_id,
                binding_version_id=binding.binding_version_id,
                trade_date=target_date,
                run_type=REVIEW_RUN_TYPE_RUN,
                status=REVIEW_RUN_STATUS_WAITING_DATA,
                data_source=selection_run.data_source,
                selection_run_id=selection_run.run_id,
                selection_run_ids=[selection_run.run_id],
                runtime_config_json=runtime_config,
                finished_at=None,
                created_by="advisory_forward",
            )
            try:
                model_resolution = self._freeze_model_resolution(
                    program=program,
                    active_binding={
                        "binding_version_id": binding.binding_version_id,
                        "package_ids": list(binding.package_ids),
                    },
                    selection_run=selection_run,
                )
            except Exception as exc:
                LOGGER.exception(
                    "advisory forward model resolution failed program_id=%s target=%s",
                    program.program_id,
                    target_date.isoformat(),
                )
                model_resolution = {
                    "status": "FAILED",
                    "reason_code": str(
                        getattr(exc, "reason_code", None)
                        or getattr(exc, "error_code", None)
                        or "ADVISORY_MODEL_PROGRAM_DESCRIPTOR_INVALID"
                    ),
                    "message": str(exc),
                    "error_type": type(exc).__name__,
                }
            publication_payload = {
                "schema_version": "advisory_forward_publication_v1",
                "program_id": program.program_id,
                "program_version": program.version,
                "binding_version_id": binding.binding_version_id,
                "decision_as_of_trade_date": decision_date.isoformat(),
                "target_trade_date": target_date.isoformat(),
                "selection_run_id": selection_run.run_id,
                "list_version_id": list_version.list_version_id,
                "active_episode_state_hash": _active_episode_state_hash(active_episodes),
                "program_snapshot": program_to_dict(program),
                "items": [list_item_to_dict(item) for item in items],
            }
            committed = self.repository.commit_publication(
                forward_run_id=str(persisted["forward_run_id"]),
                expected_program_version=program.version,
                expected_binding_version_id=binding.binding_version_id,
                review_run=review_run,
                list_version=list_version,
                items=items,
                model_resolution=model_resolution,
                publication_payload=publication_payload,
            )
        except Exception as exc:
            failed = self.repository.mark_failure(
                forward_run_id=str(persisted["forward_run_id"]),
                stage="AFTER_CLOSE_PUBLISH",
                reason_code=str(
                    getattr(exc, "error_code", None)
                    or getattr(exc, "reason_code", None)
                    or "ADVISORY_FORWARD_PUBLICATION_FAILED"
                ),
                error={"message": str(exc), "context": dict(getattr(exc, "context", {}) or {})},
                waiting_data=isinstance(exc, DataUnavailableError),
            )
            if _published_publication_matches(failed, publication_payload):
                return self._resume_published_observation(failed)
            raise
        try:
            observation = self._model_observation(
                forward_run_id=str(committed["forward_run_id"]),
                program=program,
                binding_version_id=binding.binding_version_id,
                decision_date=decision_date,
                target_date=target_date,
                frozen_resolution=model_resolution,
                selection_run_id=str(committed["selection_run_id"]),
                review_run_id=str(committed["review_run_id"]),
                list_version_id=str(committed["list_version_id"]),
            )
        except Exception as exc:
            LOGGER.exception(
                "advisory forward model observation failed program_id=%s target=%s",
                program.program_id,
                target_date.isoformat(),
            )
            observation = AdvisoryForwardModelObservationV1(
                observation_id=_stable_id("advobs", str(committed["forward_run_id"])),
                forward_run_id=str(committed["forward_run_id"]),
                program_id=program.program_id,
                binding_version_id=binding.binding_version_id,
                decision_as_of_trade_date=decision_date,
                target_trade_date=target_date,
                status="FAILED",
                reason_code="ADVISORY_FORWARD_MODEL_OBSERVATION_FAILED",
                message=f"unexpected model observation failure: {type(exc).__name__}",
                package_id=model_resolution.get("package_id"),
                manifest_sha256=model_resolution.get("manifest_sha256"),
                style_profile_id=model_resolution.get("style_profile_id"),
                style_profile_hash=model_resolution.get("style_profile_hash"),
                model_descriptor_sha256=model_resolution.get("descriptor_sha256"),
                bundle_id=model_resolution.get("bundle_id"),
            )
        self._save_observation_visible(observation)
        return {
            "program_id": program.program_id,
            "forward_run_id": committed["forward_run_id"],
            "status": "PUBLISHED",
            "target_trade_date": target_date.isoformat(),
            "model_status": observation.status,
            "model_reason_code": observation.reason_code,
        }

    def _resume_published_observation(self, persisted: Mapping[str, Any]) -> dict[str, Any]:
        detail = self.repository.get(str(persisted["forward_run_id"]))
        existing = detail.get("model_observation")
        if existing is not None and not is_retryable_model_observation(existing):
            return {
                "program_id": persisted["program_id"],
                "forward_run_id": persisted["forward_run_id"],
                "status": "IDEMPOTENT_REPLAY",
                "target_trade_date": persisted["target_trade_date"].isoformat(),
                "model_status": existing["status"],
                "model_reason_code": existing.get("reason_code"),
            }
        current_program = self.program_service.get_program(str(persisted["program_id"]))
        run_payload = dict(persisted.get("run_payload_json") or {})
        program = _frozen_program(current_program, run_payload.get("program_snapshot"))
        try:
            observation = self._model_observation(
                forward_run_id=str(persisted["forward_run_id"]),
                program=program,
                binding_version_id=str(persisted["binding_version_id"]),
                decision_date=persisted["decision_as_of_trade_date"],
                target_date=persisted["target_trade_date"],
                frozen_resolution=dict(persisted.get("model_resolution_json") or {}),
                selection_run_id=str(persisted["selection_run_id"]),
                review_run_id=str(persisted["review_run_id"]),
                list_version_id=str(persisted["list_version_id"]),
            )
        except Exception as exc:
            LOGGER.exception(
                "advisory forward model observation retry failed program_id=%s target=%s",
                program.program_id,
                persisted["target_trade_date"].isoformat(),
            )
            observation = AdvisoryForwardModelObservationV1(
                observation_id=_stable_id("advobs", str(persisted["forward_run_id"])),
                forward_run_id=str(persisted["forward_run_id"]),
                program_id=program.program_id,
                binding_version_id=str(persisted["binding_version_id"]),
                decision_as_of_trade_date=persisted["decision_as_of_trade_date"],
                target_trade_date=persisted["target_trade_date"],
                status="FAILED",
                reason_code="ADVISORY_FORWARD_MODEL_OBSERVATION_FAILED",
                message=f"unexpected model observation failure: {type(exc).__name__}",
                package_id=(persisted.get("model_resolution_json") or {}).get("package_id"),
                manifest_sha256=(persisted.get("model_resolution_json") or {}).get("manifest_sha256"),
                model_descriptor_sha256=(persisted.get("model_resolution_json") or {}).get("descriptor_sha256"),
                bundle_id=(persisted.get("model_resolution_json") or {}).get("bundle_id"),
            )
        self._save_observation_visible(observation)
        return {
            "program_id": persisted["program_id"],
            "forward_run_id": persisted["forward_run_id"],
            "status": "IDEMPOTENT_REPLAY",
            "target_trade_date": persisted["target_trade_date"].isoformat(),
            "model_status": observation.status,
            "model_reason_code": observation.reason_code,
        }

    def _save_observation_visible(self, observation: AdvisoryForwardModelObservationV1) -> None:
        try:
            self.repository.save_observation(observation)
        except Exception as exc:
            LOGGER.exception(
                "advisory forward model observation persistence failed forward_run_id=%s",
                observation.forward_run_id,
            )
            reason_code = "ADVISORY_FORWARD_MODEL_OBSERVATION_PERSIST_FAILED"
            self.repository.mark_observation_failure(
                forward_run_id=observation.forward_run_id,
                reason_code=reason_code,
                error={"message": str(exc), "error_type": type(exc).__name__},
            )
            raise

    def _freeze_model_resolution(
        self,
        *,
        program: AdvisoryProgram,
        active_binding: Mapping[str, Any],
        selection_run: Any,
    ) -> dict[str, Any]:
        model_root = self.model_service.model_root()
        binding_version_id = str(active_binding["binding_version_id"])
        if not model_root or not self.model_resolver.is_configured(
            model_root=model_root,
            program_id=program.program_id,
            binding_version_id=binding_version_id,
        ):
            return {
                "status": "UNAVAILABLE",
                "reason_code": "ADVISORY_MODEL_BUNDLE_NOT_AVAILABLE_FOR_PACKAGE",
            }
        resolution = self.model_resolver.resolve(
            model_root=model_root,
            program=program,
            active_binding=active_binding,
            selection_run=selection_run,
        )
        return {
            "status": "CONFIGURED",
            "program_id": resolution.program_id,
            "binding_version_id": resolution.binding_version_id,
            "package_id": resolution.package_id,
            "manifest_sha256": resolution.manifest_sha256,
            "style_profile_id": resolution.style_profile_id,
            "style_profile_hash": resolution.style_profile_hash,
            "bundle_id": resolution.bundle_id,
            "bundle_manifest_sha256": resolution.bundle_manifest_sha256,
            "model_role": resolution.model_role,
            "shadow_policy_sha256": resolution.shadow_policy_sha256,
            "selection_runtime_semantics_hash": resolution.selection_runtime_semantics_hash,
            "feature_schema_version": resolution.feature_schema_version,
            "feature_schema_hash": resolution.feature_schema_hash,
            "component_roles": dict(resolution.component_roles),
            "terminal_weights": dict(resolution.terminal_weights),
            "descriptor_sha256": resolution.descriptor_sha256,
        }

    def _model_observation(
        self,
        *,
        forward_run_id: str,
        program: AdvisoryProgram,
        binding_version_id: str,
        decision_date: date,
        target_date: date,
        frozen_resolution: Mapping[str, Any],
        selection_run_id: str,
        review_run_id: str,
        list_version_id: str,
    ) -> AdvisoryForwardModelObservationV1:
        if frozen_resolution.get("status") != "CONFIGURED":
            resolution_status = str(frozen_resolution.get("status") or "UNAVAILABLE")
            return AdvisoryForwardModelObservationV1(
                observation_id=_stable_id("advobs", forward_run_id),
                forward_run_id=forward_run_id,
                program_id=program.program_id,
                binding_version_id=binding_version_id,
                decision_as_of_trade_date=decision_date,
                target_trade_date=target_date,
                status="FAILED" if resolution_status == "FAILED" else "UNAVAILABLE",
                reason_code=str(
                    frozen_resolution.get("reason_code")
                    or "ADVISORY_MODEL_BUNDLE_NOT_AVAILABLE_FOR_PACKAGE"
                ),
                message=str(
                    frozen_resolution.get("message")
                    or (
                        "model descriptor resolution failed at publication time"
                        if resolution_status == "FAILED"
                        else "no exact model descriptor was configured at publication time"
                    )
                ),
            )
        prediction = self.model_service.model_shadow_for_forward(
            program=program,
            binding_version_id=binding_version_id,
            target_trade_date=target_date,
            list_version_id=list_version_id,
            review_run_id=review_run_id,
            selection_run_id=selection_run_id,
        )
        effective_descriptor_sha256 = (
            prediction.get("model_descriptor_sha256") or frozen_resolution.get("descriptor_sha256")
        )
        effective_bundle_id = prediction.get("bundle_id") or frozen_resolution.get("bundle_id")
        if frozen_resolution.get("status") == "CONFIGURED" and (
            effective_descriptor_sha256 != frozen_resolution.get("descriptor_sha256")
            or effective_bundle_id != frozen_resolution.get("bundle_id")
        ):
            raise RuntimeError("model inference identity differs from the publication-frozen descriptor")
        outcome = prediction.get("outcome") if isinstance(prediction.get("outcome"), Mapping) else {}
        price_range = prediction.get("price_range") if isinstance(prediction.get("price_range"), Mapping) else {}
        reason_code = (
            prediction.get("reason_code")
            or outcome.get("reason_code")
            or price_range.get("reason_code")
        )
        status = str(prediction.get("status") or "FAILED")
        if status == "EXPERIMENTAL_SHADOW":
            observation_status = "EXPERIMENTAL_SHADOW"
        elif str(reason_code or "") in RETRYABLE_MODEL_OBSERVATION_REASON_CODES:
            observation_status = "FAILED"
        else:
            observation_status = "UNAVAILABLE"
        maturity_horizons = list(outcome.get("horizons") or [])
        for candidate in outcome.get("candidates") or []:
            if not isinstance(candidate, Mapping):
                raise RuntimeError("model outcome candidate is not an object")
            holding = candidate.get("holding_period")
            if not isinstance(holding, Mapping) or "range_high_days" not in holding:
                raise RuntimeError("model outcome candidate is missing holding-period maturity")
            maturity_horizons.append(holding["range_high_days"])
        maturity = _maturity_date(
            target_date,
            horizons=maturity_horizons,
            calendar=self.calendar,
        )
        return AdvisoryForwardModelObservationV1(
            observation_id=_stable_id("advobs", forward_run_id),
            forward_run_id=forward_run_id,
            program_id=program.program_id,
            binding_version_id=binding_version_id,
            decision_as_of_trade_date=decision_date,
            target_trade_date=target_date,
            status=observation_status,
            reason_code=reason_code,
            message=(
                prediction.get("message")
                or outcome.get("message")
                or price_range.get("message")
            ),
            package_id=prediction.get("package_id") or frozen_resolution.get("package_id"),
            manifest_sha256=prediction.get("manifest_sha256") or frozen_resolution.get("manifest_sha256"),
            style_profile_id=prediction.get("style_profile_id") or frozen_resolution.get("style_profile_id"),
            style_profile_hash=prediction.get("style_profile_hash") or frozen_resolution.get("style_profile_hash"),
            model_descriptor_sha256=effective_descriptor_sha256,
            bundle_id=effective_bundle_id,
            outcome_bundle_id=outcome.get("outcome_bundle_id"),
            price_range_bundle_id=price_range.get("price_range_bundle_id"),
            feature_schema_version=prediction.get("feature_schema_version"),
            candidate_count=int(prediction.get("candidate_count") or 0),
            shortlist_count=int(prediction.get("shortlist_count") or 0),
            maturity_trade_date=maturity,
            prediction_payload_json={
                **prediction,
                "child_status": {
                    "outcome": outcome.get("status") or "OUTCOME_UNAVAILABLE",
                    "price_range": price_range.get("status") or "PRICE_RANGE_UNAVAILABLE",
                },
            },
        )

    def _settle(self, persisted: Mapping[str, Any]) -> dict[str, Any]:
        program_id = str(persisted["program_id"])
        terminal = self._terminal_settlement(str(persisted["forward_run_id"]))
        if terminal is not None:
            return terminal
        current_program = self.program_service.get_program(program_id)
        publication_payload = dict(persisted.get("run_payload_json") or {})
        program = _frozen_program(current_program, publication_payload.get("program_snapshot"))
        selection_run = self.program_service.selection_service.get_run(str(persisted["selection_run_id"]))
        candidates = [
            replace(candidate, next_open_executable=None, next_close=None)
            for candidate in candidates_from_selection_run(selection_run)
        ]
        active_episodes = self.program_service.active_episode_objects(program_id)
        active_hash = _active_episode_state_hash(active_episodes)
        terminal = self._terminal_settlement(str(persisted["forward_run_id"]))
        if terminal is not None:
            return terminal
        symbols = sorted({candidate.symbol for candidate in candidates} | {episode.symbol for episode in active_episodes})
        marks = self.program_service.load_forward_market_marks(
            symbols=symbols,
            target_trade_date=persisted["target_trade_date"],
        )
        if not marks:
            raise DataUnavailableError(
                "target-open market rows are unavailable",
                context={"target_trade_date": persisted["target_trade_date"].isoformat()},
            )
        result = self.program_service.evaluate_forward_settlement(
            program=program,
            target_trade_date=persisted["target_trade_date"],
            candidates=candidates,
            market_by_symbol=marks,
            active_episodes=active_episodes,
        )
        if result.review_status == "WAITING_DATA":
            waiting_symbols = sorted(
                decision.symbol for decision in result.decisions if decision.action == "WAITING"
            )
            waiting = self.repository.mark_failure(
                forward_run_id=str(persisted["forward_run_id"]),
                stage="TARGET_OPEN_SETTLE",
                reason_code="ADVISORY_FORWARD_TARGET_OPEN_WAITING_DATA",
                error={
                    "message": "target-open settlement is waiting for authoritative price data",
                    "context": {
                        "target_trade_date": persisted["target_trade_date"].isoformat(),
                        "symbols": waiting_symbols,
                    },
                },
                waiting_data=True,
            )
            return {
                "program_id": program_id,
                "forward_run_id": waiting["forward_run_id"],
                "status": "WAITING_DATA",
                "target_trade_date": persisted["target_trade_date"].isoformat(),
                "reason_code": "ADVISORY_FORWARD_TARGET_OPEN_WAITING_DATA",
                "waiting_symbols": waiting_symbols,
            }
        enriched_decisions = [
            replace(
                decision,
                binding_version_id=str(persisted["binding_version_id"]),
                review_run_id=str(persisted["review_run_id"]),
                list_version_id=str(persisted["list_version_id"]),
            )
            for decision in result.decisions
        ]
        updated_program = replace(
            current_program,
            status=current_program.status,
            last_review_status=result.review_status,
            latest_review_trade_date=persisted["target_trade_date"],
            updated_at=utcnow(),
        )
        metrics = compute_program_metrics(updated_program, result.active_pool)
        result = replace(result, program=updated_program, decisions=enriched_decisions, metrics=metrics)
        settlement_payload = _settlement_payload(
            program_id=program_id,
            target_trade_date=persisted["target_trade_date"],
            review_status=result.review_status,
            decisions=enriched_decisions,
            active_pool=result.active_pool,
        )
        committed = self.repository.commit_settlement(
            forward_run_id=str(persisted["forward_run_id"]),
            expected_active_episode_state_hash=active_hash,
            expected_program_version=current_program.version,
            expected_program_status=current_program.status,
            result=result,
            decisions=enriched_decisions,
            program=updated_program,
            settlement_payload=settlement_payload,
        )
        return {
            "program_id": program_id,
            "forward_run_id": committed["forward_run_id"],
            "status": committed["settlement_status"],
            "target_trade_date": persisted["target_trade_date"].isoformat(),
        }

    def _terminal_settlement(self, forward_run_id: str) -> dict[str, Any] | None:
        detail = self.repository.get(forward_run_id)
        forward = dict(detail["forward_run"])
        status = str(forward.get("settlement_status") or "")
        if status not in {"SETTLED", "NOT_ENTERED"}:
            return None
        return {
            "program_id": str(forward["program_id"]),
            "forward_run_id": str(forward["forward_run_id"]),
            "status": status,
            "target_trade_date": forward["target_trade_date"].isoformat(),
            "idempotent_replay": True,
        }

    def _visible_failure(self, payload: Mapping[str, Any], *, stage: str, exc: Exception) -> dict[str, Any]:
        reason_code = getattr(exc, "error_code", None) or getattr(exc, "reason_code", None) or f"ADVISORY_FORWARD_{stage}_FAILED"
        context = dict(getattr(exc, "context", {}) or {})
        LOGGER.exception(
            "advisory forward stage failed program_id=%s decision=%s target=%s stage=%s reason_code=%s context=%s",
            payload.get("program_id"),
            payload.get("decision_as_of_trade_date"),
            payload.get("target_trade_date"),
            stage,
            reason_code,
            context,
        )
        forward_run_id = payload.get("forward_run_id")
        if forward_run_id:
            self.repository.mark_failure(
                forward_run_id=str(forward_run_id),
                stage=stage,
                reason_code=str(reason_code),
                error={"message": str(exc), "context": context},
                waiting_data=isinstance(exc, DataUnavailableError),
            )
        return {
            "program_id": payload.get("program_id"),
            "forward_run_id": forward_run_id,
            "status": "WAITING_DATA" if isinstance(exc, DataUnavailableError) else "FAILED",
            "stage": stage,
            "reason_code": reason_code,
            "message": str(exc),
            "context": context,
        }


def _scheduled(program: AdvisoryProgram) -> bool:
    schedule = dict(program.review_schedule or {})
    return program.status == PROGRAM_STATUS_ENABLED and schedule.get("frequency") == "daily_after_close"


def _after_close_time() -> tuple[int, int]:
    raw = (os.getenv("AISTOCK_ADVISORY_FORWARD_AFTER_CLOSE_TIME") or "16:30:00").strip()
    parts = raw.split(":")
    if len(parts) not in {2, 3} or any(not part.isdigit() for part in parts):
        raise ValueError("AISTOCK_ADVISORY_FORWARD_AFTER_CLOSE_TIME must be HH:MM or HH:MM:SS")
    hour, minute = int(parts[0]), int(parts[1])
    second = int(parts[2]) if len(parts) == 3 else 0
    if second != 0:
        raise ValueError("AISTOCK_ADVISORY_FORWARD_AFTER_CLOSE_TIME seconds must be 00")
    return _validate_after_close_parts(hour, minute)


def _validate_after_close_parts(hour: int, minute: int) -> tuple[int, int]:
    if not 0 <= int(hour) <= 23 or not 0 <= int(minute) <= 59:
        raise ValueError("Advisory forward after-close time is outside the valid clock range")
    return int(hour), int(minute)


def _active_episode_state_hash(episodes: list[Any]) -> str:
    return canonical_json_sha256(sorted((episode_to_dict(item) for item in episodes), key=lambda row: row["episode_id"]))


def _build_publication_list(
    *,
    program: AdvisoryProgram,
    binding_version_id: str,
    review_run_id: str,
    target_trade_date: date,
    decision_as_of_trade_date: date,
    selection_run_id: str,
    candidates: list[AdvisoryCandidate],
    active_episodes: list[Any],
    previous_list: AdvisoryRecommendationListVersion | None,
    previous_items: list[AdvisoryRecommendationListItem],
) -> tuple[AdvisoryRecommendationListVersion, list[AdvisoryRecommendationListItem]]:
    previous_by_symbol = {item.symbol: item for item in previous_items}
    active_by_symbol = {item.symbol: item for item in active_episodes}
    list_version_id = _stable_id(
        "advlv",
        program.program_id,
        binding_version_id,
        target_trade_date.isoformat(),
        selection_run_id,
    )
    items: list[AdvisoryRecommendationListItem] = []
    for candidate in sorted(candidates, key=lambda row: (row.rank, row.symbol)):
        previous = previous_by_symbol.get(candidate.symbol)
        episode = active_by_symbol.get(candidate.symbol)
        if episode is not None:
            action = ACTION_HOLD
            state = "ACTIVE"
            reason = "PENDING_TARGET_OPEN_REVIEW"
        else:
            action = ACTION_WATCH
            state = "WATCH"
            reason = "PENDING_TARGET_OPEN_ENTRY" if candidate.rank <= int(program.review_policy["rank_enter_threshold"]) else "OUTSIDE_ENTRY_THRESHOLD"
        evidence = {
            "schema_version": "advisory_forward_publication_item_v1",
            "source_run_id": selection_run_id,
            "decision_as_of_trade_date": decision_as_of_trade_date.isoformat(),
            "target_trade_date": target_trade_date.isoformat(),
            "review_policy_sha256": program.review_policy_sha256,
            "component_scores": candidate.component_scores,
        }
        items.append(
            AdvisoryRecommendationListItem(
                list_item_id=_stable_id("advli", list_version_id, candidate.symbol),
                list_version_id=list_version_id,
                program_id=program.program_id,
                binding_version_id=binding_version_id,
                symbol=candidate.symbol,
                item_state=state,
                action=action,
                reason_code=reason,
                stock_name=candidate.stock_name,
                episode_id=episode.episode_id if episode else None,
                previous_action=previous.action if previous else None,
                rank=candidate.rank,
                score=candidate.score,
                previous_rank=previous.rank if previous else None,
                previous_score=previous.score if previous else None,
                price_basis=program.entry_price_basis,
                effective_trade_date=target_trade_date,
                component_scores_json=candidate.component_scores,
                evidence_json=evidence,
                operation_advice_json={
                    "action": action,
                    "reason_code": reason,
                    "effective_trade_date": target_trade_date.isoformat(),
                    "price_basis": program.entry_price_basis,
                },
            )
        )
    candidate_symbols = {candidate.symbol for candidate in candidates}
    for episode in sorted(active_episodes, key=lambda row: (row.symbol, row.episode_id)):
        if episode.symbol in candidate_symbols:
            continue
        previous = previous_by_symbol.get(episode.symbol)
        reason = "PENDING_TARGET_OPEN_REVIEW"
        evidence = {
            "schema_version": "advisory_forward_publication_item_v1",
            "source_run_id": selection_run_id,
            "decision_as_of_trade_date": decision_as_of_trade_date.isoformat(),
            "target_trade_date": target_trade_date.isoformat(),
            "review_policy_sha256": program.review_policy_sha256,
            "component_scores": {},
        }
        items.append(
            AdvisoryRecommendationListItem(
                list_item_id=_stable_id("advli", list_version_id, episode.symbol),
                list_version_id=list_version_id,
                program_id=program.program_id,
                binding_version_id=binding_version_id,
                symbol=episode.symbol,
                item_state="WAITING",
                action=ACTION_WAITING,
                reason_code=reason,
                stock_name=episode.stock_name,
                episode_id=episode.episode_id,
                previous_action=previous.action if previous else None,
                previous_rank=previous.rank if previous else episode.current_rank,
                previous_score=previous.score if previous else episode.current_score,
                price_basis=program.exit_price_basis,
                effective_trade_date=target_trade_date,
                evidence_json=evidence,
                operation_advice_json={
                    "action": ACTION_WAITING,
                    "reason_code": reason,
                    "effective_trade_date": target_trade_date.isoformat(),
                    "price_basis": program.exit_price_basis,
                },
            )
        )
    active_count = sum(item.item_state == "ACTIVE" for item in items)
    summary = {
        "schema_version": "advisory_forward_publication_summary_v1",
        "advisory_date_context": {
            "decision_as_of_trade_date": decision_as_of_trade_date.isoformat(),
            "selection_as_of_trade_date": decision_as_of_trade_date.isoformat(),
            "target_trade_date": target_trade_date.isoformat(),
        },
        "selection_run_id": selection_run_id,
        "publication_stage": "AFTER_CLOSE_PUBLISH",
        "active_count": active_count,
        "watch_count": sum(item.action == ACTION_WATCH for item in items),
        "manual_gate": False,
    }
    return (
        AdvisoryRecommendationListVersion(
            list_version_id=list_version_id,
            program_id=program.program_id,
            binding_version_id=binding_version_id,
            review_run_id=review_run_id,
            trade_date=target_trade_date,
            previous_list_version_id=previous_list.list_version_id if previous_list else None,
            version_status=LIST_VERSION_STATUS_PUBLISHED,
            target_count=program.target_count,
            active_count=active_count,
            entered_count=0,
            held_count=sum(item.action == ACTION_HOLD for item in items),
            exited_count=0,
            waiting_count=sum(item.action == ACTION_WAITING for item in items),
            changed_count=sum(item.action != item.previous_action for item in items),
            turnover_rate=0.0,
            overlap_rate=None,
            summary_json=summary,
        ),
        items,
    )


def _maturity_date(target_date: date, *, horizons: list[Any], calendar: Any) -> date | None:
    parsed = [int(value) for value in horizons if int(value) > 0]
    if not parsed:
        return None
    current = target_date
    for _ in range(max(parsed)):
        current = calendar.next_trading_day(current, inclusive=False)
    return current


def _stable_id(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()[:32]
    return f"{prefix}_{digest}"


def _settlement_decision_identity(decision: Any) -> dict[str, Any]:
    payload = decision_to_dict(decision)
    payload.pop("created_at", None)
    return payload


def _published_publication_matches(
    persisted: Mapping[str, Any],
    publication_payload: Mapping[str, Any] | None,
) -> bool:
    if persisted.get("publication_status") != "PUBLISHED" or publication_payload is None:
        return False
    return str(persisted.get("publication_payload_sha256") or "") == canonical_json_sha256(
        publication_payload
    )


def _settlement_payload(
    *,
    program_id: str,
    target_trade_date: date,
    review_status: str,
    decisions: list[Any],
    active_pool: list[Any],
) -> dict[str, Any]:
    ordered_decisions = sorted(decisions, key=lambda row: (row.symbol, row.action))
    ordered_episodes = sorted(active_pool, key=lambda row: row.episode_id)
    return {
        "schema_version": "advisory_forward_settlement_v1",
        "program_id": program_id,
        "target_trade_date": target_trade_date.isoformat(),
        "review_status": review_status,
        "entered_count": sum(item.action == "ENTER" for item in ordered_decisions),
        "held_count": sum(item.action == "HOLD" for item in ordered_decisions),
        "exited_count": sum(item.action == "EXIT" for item in ordered_decisions),
        "waiting_count": sum(item.action == "WAITING" for item in ordered_decisions),
        "episode_ids": [episode.episode_id for episode in ordered_episodes],
        "decisions": [_settlement_decision_identity(item) for item in ordered_decisions],
        "active_pool": [_settlement_episode_identity(item) for item in ordered_episodes],
    }


def _settlement_episode_identity(episode: Any) -> dict[str, Any]:
    payload = episode_to_dict(episode)
    payload.pop("created_at", None)
    payload.pop("updated_at", None)
    return payload


def _frozen_program(current: AdvisoryProgram, payload: Any) -> AdvisoryProgram:
    if not isinstance(payload, Mapping):
        raise RuntimeError("forward publication is missing its frozen Program snapshot")
    required = {
        "program_id",
        "version",
        "target_count",
        "package_mode",
        "package_ids",
        "package_weights",
        "package_set_hash",
        "review_policy",
        "review_policy_sha256",
        "entry_price_basis",
        "exit_price_basis",
        "review_schedule",
    }
    if not required.issubset(payload) or payload["program_id"] != current.program_id:
        raise RuntimeError("forward publication Program snapshot is incomplete or mismatched")
    return replace(
        current,
        version=int(payload["version"]),
        target_count=int(payload["target_count"]),
        package_mode=str(payload["package_mode"]),
        package_ids=list(payload["package_ids"]),
        package_weights={str(key): float(value) for key, value in dict(payload["package_weights"]).items()},
        fusion_method=payload.get("fusion_method"),
        package_set_hash=str(payload["package_set_hash"]),
        fusion_policy_sha256=payload.get("fusion_policy_sha256"),
        review_policy=dict(payload["review_policy"]),
        review_policy_sha256=str(payload["review_policy_sha256"]),
        entry_price_basis=str(payload["entry_price_basis"]),
        exit_price_basis=str(payload["exit_price_basis"]),
        review_schedule=dict(payload["review_schedule"]),
    )
