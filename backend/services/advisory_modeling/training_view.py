from __future__ import annotations

from bisect import bisect_left
from datetime import UTC, date, datetime, time
from typing import Any, Literal
from zoneinfo import ZoneInfo

from pydantic import Field, field_validator, model_validator

from backend.services.advisory_modeling.identity import (
    FrozenModel,
    set_computed_hash,
    strict_identifier,
    utc_datetime,
    validated_hash,
)


DATASET_BUILD_REQUEST_SCHEMA_VERSION = "advisory_reranker_dataset_build_request_v1"
TRAINING_VIEW_SCHEMA_VERSION = "advisory_reranker_training_view_v1"
SPLIT_FOLD_SCHEMA_VERSION = "advisory_reranker_split_fold_v1"
SPLIT_PLAN_SCHEMA_VERSION = "advisory_reranker_split_plan_v1"
FOLD_EVIDENCE_SCHEMA_VERSION = "advisory_reranker_fold_evidence_closure_v1"
REQUESTED_WINDOWS_YEARS = (2, 3, 5)


class DatasetBuildRequestV1(FrozenModel):
    request_schema_version: Literal[DATASET_BUILD_REQUEST_SCHEMA_VERSION] = (
        DATASET_BUILD_REQUEST_SCHEMA_VERSION
    )
    style_profile_id: str = Field(min_length=1, max_length=160)
    style_profile_hash: str = Field(min_length=64, max_length=64)
    package_id: str = Field(min_length=1, max_length=160)
    package_manifest_sha256: str = Field(min_length=64, max_length=64)
    package_asset_closure_hash: str = Field(min_length=64, max_length=64)
    selection_runtime_semantics_hash: str = Field(min_length=64, max_length=64)
    multi_alpha_parent_contract_version: str = Field(min_length=1, max_length=160)
    multi_alpha_component_identity_set_hash: str = Field(min_length=64, max_length=64)
    decision_date_start: date
    decision_date_end: date
    requested_windows_years: tuple[int, ...] = REQUESTED_WINDOWS_YEARS
    evaluation_tail_trade_days: Literal[420] = 420
    candidate_observation_top_k: Literal[20] = 20
    feature_schema_id: str = Field(min_length=1, max_length=160)
    feature_schema_hash: str = Field(min_length=64, max_length=64)
    feature_formula_registry_hash: str = Field(min_length=64, max_length=64)
    feature_query_registry_hash: str = Field(min_length=64, max_length=64)
    market_regime_policy_template_id: str = Field(min_length=1, max_length=160)
    market_regime_policy_template_hash: str = Field(min_length=64, max_length=64)
    label_policy_id: str = Field(min_length=1, max_length=160)
    label_policy_hash: str = Field(min_length=64, max_length=64)
    source_revision_set_id: str = Field(min_length=1, max_length=160)
    source_revision_set_hash: str = Field(min_length=64, max_length=64)
    universe_policy_set_id: str = Field(min_length=1, max_length=160)
    universe_policy_set_hash: str = Field(min_length=64, max_length=64)
    calendar_version: str = Field(min_length=1, max_length=160)
    calendar_hash: str = Field(min_length=64, max_length=64)
    evidence_scope: Literal["RETROSPECTIVE_RESEARCH_ONLY"] = "RETROSPECTIVE_RESEARCH_ONLY"
    repository_commit: str = Field(min_length=7, max_length=64)
    final_fit_as_of: datetime
    request_semantic_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "style_profile_id",
        "package_id",
        "multi_alpha_parent_contract_version",
        "feature_schema_id",
        "market_regime_policy_template_id",
        "label_policy_id",
        "source_revision_set_id",
        "universe_policy_set_id",
        "calendar_version",
        "repository_commit",
    )
    @classmethod
    def _identifiers(cls, value: str, info: Any) -> str:
        return strict_identifier(value, field_name=info.field_name)

    @field_validator(
        "style_profile_hash",
        "package_manifest_sha256",
        "package_asset_closure_hash",
        "selection_runtime_semantics_hash",
        "multi_alpha_component_identity_set_hash",
        "feature_schema_hash",
        "feature_formula_registry_hash",
        "feature_query_registry_hash",
        "market_regime_policy_template_hash",
        "label_policy_hash",
        "source_revision_set_hash",
        "universe_policy_set_hash",
        "calendar_hash",
        "request_semantic_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validated_hash(value, field_name=info.field_name)

    @field_validator("final_fit_as_of")
    @classmethod
    def _as_of(cls, value: datetime) -> datetime:
        return utc_datetime(value, field_name="final_fit_as_of")

    @model_validator(mode="after")
    def _identity(self) -> "DatasetBuildRequestV1":
        if self.decision_date_start > self.decision_date_end:
            raise ValueError("decision_date_start must not exceed decision_date_end")
        if self.requested_windows_years != REQUESTED_WINDOWS_YEARS:
            raise ValueError(f"requested_windows_years must equal {REQUESTED_WINDOWS_YEARS}")
        set_computed_hash(
            self,
            field_name="request_semantic_hash",
            exclude={"request_semantic_hash"},
        )
        return self


class TrainingWindowV1(FrozenModel):
    window_years: Literal[2, 3, 5]
    fit_dates: tuple[date, ...]
    fit_start_calendar_position: int = Field(ge=0)
    fit_end_calendar_position: int = Field(ge=0)
    target_fit_start_date: date
    coverage_status: Literal["COMPLETE", "INSUFFICIENT_CALENDAR_HISTORY"]
    fit_date_set_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @model_validator(mode="after")
    def _identity(self) -> "TrainingWindowV1":
        if not self.fit_dates or tuple(sorted(set(self.fit_dates))) != self.fit_dates:
            raise ValueError("fit_dates must be non-empty, unique and ascending")
        if self.fit_start_calendar_position > self.fit_end_calendar_position:
            raise ValueError("fit calendar positions are reversed")
        if self.fit_end_calendar_position - self.fit_start_calendar_position + 1 != len(
            self.fit_dates
        ):
            raise ValueError("fit calendar positions differ from fit_dates")
        set_computed_hash(self, field_name="fit_date_set_hash", exclude={"fit_date_set_hash"})
        return self


class FoldEvidenceClosureV1(FrozenModel):
    schema_version: Literal[FOLD_EVIDENCE_SCHEMA_VERSION] = FOLD_EVIDENCE_SCHEMA_VERSION
    fold_index: int = Field(ge=0, le=4)
    available_observation_set_hash: str = Field(min_length=64, max_length=64)
    available_label_set_hash: str = Field(min_length=64, max_length=64)
    available_member_set_hash: str = Field(min_length=64, max_length=64)
    exclusion_reasons: tuple[str, ...] = ()

    @field_validator(
        "available_observation_set_hash",
        "available_label_set_hash",
        "available_member_set_hash",
    )
    @classmethod
    def _hashes(cls, value: str, info: Any) -> str:
        return str(validated_hash(value, field_name=info.field_name))


class SplitFoldV1(FrozenModel):
    schema_version: Literal[SPLIT_FOLD_SCHEMA_VERSION] = SPLIT_FOLD_SCHEMA_VERSION
    fold_index: int = Field(ge=0, le=4)
    test_dates: tuple[date, ...]
    pre_test_embargo_dates: tuple[date, ...]
    validation_dates: tuple[date, ...]
    fit_validation_gap_dates: tuple[date, ...]
    training_windows: tuple[TrainingWindowV1, ...]
    fold_training_as_of: datetime
    label_availability_as_of: datetime
    test_calendar_positions: tuple[int, ...]
    pre_test_embargo_calendar_positions: tuple[int, ...]
    validation_calendar_positions: tuple[int, ...]
    fit_validation_gap_calendar_positions: tuple[int, ...]
    test_start_calendar_position: int = Field(ge=0)
    test_end_calendar_position: int = Field(ge=0)
    pre_test_embargo_start_calendar_position: int = Field(ge=0)
    pre_test_embargo_end_calendar_position: int = Field(ge=0)
    validation_start_calendar_position: int = Field(ge=0)
    validation_end_calendar_position: int = Field(ge=0)
    fit_validation_gap_start_calendar_position: int = Field(ge=0)
    fit_validation_gap_end_calendar_position: int = Field(ge=0)
    available_observation_set_hash: str = Field(min_length=64, max_length=64)
    available_label_set_hash: str = Field(min_length=64, max_length=64)
    available_member_set_hash: str = Field(min_length=64, max_length=64)
    exclusion_reasons: tuple[str, ...] = ()
    fold_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("fold_training_as_of", "label_availability_as_of")
    @classmethod
    def _as_of(cls, value: datetime) -> datetime:
        return utc_datetime(value, field_name="fold_training_as_of")

    @field_validator(
        "available_observation_set_hash",
        "available_label_set_hash",
        "available_member_set_hash",
        "fold_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validated_hash(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _identity(self) -> "SplitFoldV1":
        if len(self.test_dates) != 60:
            raise ValueError("each outer test block must contain exactly 60 eligible dates")
        if len(self.pre_test_embargo_dates) != 20:
            raise ValueError("pre-test embargo must contain exactly 20 trading dates")
        if len(self.validation_dates) != 60:
            raise ValueError("validation must contain exactly 60 trading dates")
        if len(self.fit_validation_gap_dates) != 20:
            raise ValueError("fit-validation gap must contain exactly 20 trading dates")
        if tuple(window.window_years for window in self.training_windows) != REQUESTED_WINDOWS_YEARS:
            raise ValueError("training windows must be 2, 3 and 5 years in canonical order")
        if self.label_availability_as_of != self.fold_training_as_of:
            raise ValueError("label availability cutoff must equal fold_training_as_of")
        position_lengths = (
            (
                self.pre_test_embargo_start_calendar_position,
                self.pre_test_embargo_end_calendar_position,
                20,
            ),
            (self.validation_start_calendar_position, self.validation_end_calendar_position, 60),
            (
                self.fit_validation_gap_start_calendar_position,
                self.fit_validation_gap_end_calendar_position,
                20,
            ),
        )
        if any(end - start + 1 != length for start, end, length in position_lengths):
            raise ValueError("fold calendar positions differ from frozen split lengths")
        position_sets = (
            (self.test_calendar_positions, 60),
            (self.pre_test_embargo_calendar_positions, 20),
            (self.validation_calendar_positions, 60),
            (self.fit_validation_gap_calendar_positions, 20),
        )
        if any(
            len(positions) != length or tuple(sorted(set(positions))) != positions
            for positions, length in position_sets
        ):
            raise ValueError("fold calendar position sets must be unique, ascending and complete")
        if (
            self.test_calendar_positions[0] != self.test_start_calendar_position
            or self.test_calendar_positions[-1] != self.test_end_calendar_position
        ):
            raise ValueError("test boundary positions differ from the exact test position set")
        date_sets = (
            set(self.test_dates),
            set(self.pre_test_embargo_dates),
            set(self.validation_dates),
            set(self.fit_validation_gap_dates),
        )
        if any(left & right for index, left in enumerate(date_sets) for right in date_sets[index + 1 :]):
            raise ValueError("test, embargo, validation and gap dates must not overlap")
        set_computed_hash(self, field_name="fold_hash", exclude={"fold_hash"})
        return self


class SplitPlanV1(FrozenModel):
    schema_version: Literal[SPLIT_PLAN_SCHEMA_VERSION] = SPLIT_PLAN_SCHEMA_VERSION
    request_semantic_hash: str = Field(min_length=64, max_length=64)
    calendar_hash: str = Field(min_length=64, max_length=64)
    eligible_decision_dates: tuple[date, ...]
    coverage_status: Literal[
        "COMPLETE",
        "INSUFFICIENT_ELIGIBLE_DATES",
        "INSUFFICIENT_SPLIT_HISTORY",
        "INSUFFICIENT_CALENDAR_HISTORY",
    ]
    folds: tuple[SplitFoldV1, ...]
    split_plan_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("request_semantic_hash", "calendar_hash", "split_plan_hash")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validated_hash(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _identity(self) -> "SplitPlanV1":
        if tuple(sorted(set(self.eligible_decision_dates))) != self.eligible_decision_dates:
            raise ValueError("eligible_decision_dates must be unique and ascending")
        if self.coverage_status in {"COMPLETE", "INSUFFICIENT_CALENDAR_HISTORY"} and len(
            self.folds
        ) != 5:
            raise ValueError("outer split coverage requires five complete fold descriptions")
        if self.coverage_status in {
            "INSUFFICIENT_ELIGIBLE_DATES",
            "INSUFFICIENT_SPLIT_HISTORY",
        } and self.folds:
            raise ValueError("insufficient outer split coverage cannot publish partial folds")
        test_dates = tuple(day for fold in self.folds for day in fold.test_dates)
        if self.folds and test_dates != self.eligible_decision_dates[-300:]:
            raise ValueError("fold tests must equal the last 300 eligible dates")
        has_short_window = any(
            window.coverage_status == "INSUFFICIENT_CALENDAR_HISTORY"
            for fold in self.folds
            for window in fold.training_windows
        )
        if has_short_window != (self.coverage_status == "INSUFFICIENT_CALENDAR_HISTORY"):
            raise ValueError("split coverage status differs from training-window coverage")
        set_computed_hash(self, field_name="split_plan_hash", exclude={"split_plan_hash"})
        return self


def _calendar_year_floor(day: date, *, years: int) -> date:
    try:
        return day.replace(year=day.year - years)
    except ValueError:
        return day.replace(year=day.year - years, day=28)


def build_split_plan(
    *,
    request_semantic_hash: str,
    calendar_hash: str,
    trading_dates: tuple[date, ...],
    eligible_decision_dates: tuple[date, ...],
    fold_evidence_closures: tuple[FoldEvidenceClosureV1, ...] = (),
) -> SplitPlanV1:
    if tuple(sorted(set(trading_dates))) != trading_dates:
        raise ValueError("trading_dates must be unique and ascending")
    if tuple(sorted(set(eligible_decision_dates))) != eligible_decision_dates:
        raise ValueError("eligible_decision_dates must be unique and ascending")
    if not set(eligible_decision_dates).issubset(trading_dates):
        raise ValueError("eligible dates must belong to the frozen trading calendar")
    if len(eligible_decision_dates) < 300:
        return SplitPlanV1(
            request_semantic_hash=request_semantic_hash,
            calendar_hash=calendar_hash,
            eligible_decision_dates=eligible_decision_dates,
            coverage_status="INSUFFICIENT_ELIGIBLE_DATES",
            folds=(),
        )

    tests = eligible_decision_dates[-300:]
    positions = {day: index for index, day in enumerate(trading_dates)}
    if positions[tests[0]] < 101:
        return SplitPlanV1(
            request_semantic_hash=request_semantic_hash,
            calendar_hash=calendar_hash,
            eligible_decision_dates=eligible_decision_dates,
            coverage_status="INSUFFICIENT_SPLIT_HISTORY",
            folds=(),
        )
    if tuple(item.fold_index for item in fold_evidence_closures) != (0, 1, 2, 3, 4):
        raise ValueError("complete outer split requires evidence closures for folds 0 through 4")
    evidence_by_fold = {item.fold_index: item for item in fold_evidence_closures}
    folds: list[SplitFoldV1] = []
    has_short_window = False
    for fold_index in range(5):
        evidence = evidence_by_fold[fold_index]
        test_dates = tests[fold_index * 60 : (fold_index + 1) * 60]
        test_start_position = positions[test_dates[0]]
        embargo = trading_dates[test_start_position - 20 : test_start_position]
        validation = trading_dates[test_start_position - 80 : test_start_position - 20]
        gap = trading_dates[test_start_position - 100 : test_start_position - 80]
        fit_end_position = test_start_position - 101
        fit_end = trading_dates[fit_end_position]
        windows: list[TrainingWindowV1] = []
        for years in REQUESTED_WINDOWS_YEARS:
            target_start = _calendar_year_floor(fit_end, years=years)
            start_position = bisect_left(trading_dates, target_start, 0, fit_end_position + 1)
            if start_position > fit_end_position:
                raise ValueError(f"calendar has no fit dates for {years}-year window")
            window_complete = trading_dates[0] <= target_start
            has_short_window = has_short_window or not window_complete
            windows.append(
                TrainingWindowV1(
                    window_years=years,
                    fit_dates=trading_dates[start_position : fit_end_position + 1],
                    fit_start_calendar_position=start_position,
                    fit_end_calendar_position=fit_end_position,
                    target_fit_start_date=target_start,
                    coverage_status=(
                        "COMPLETE" if window_complete else "INSUFFICIENT_CALENDAR_HISTORY"
                    ),
                )
            )
        training_as_of = datetime.combine(
            embargo[-1],
            time(15, 0),
            tzinfo=ZoneInfo("Asia/Shanghai"),
        ).astimezone(UTC)
        folds.append(
            SplitFoldV1(
                fold_index=fold_index,
                test_dates=test_dates,
                pre_test_embargo_dates=embargo,
                validation_dates=validation,
                fit_validation_gap_dates=gap,
                training_windows=tuple(windows),
                fold_training_as_of=training_as_of,
                label_availability_as_of=training_as_of,
                test_calendar_positions=tuple(positions[day] for day in test_dates),
                pre_test_embargo_calendar_positions=tuple(
                    range(test_start_position - 20, test_start_position)
                ),
                validation_calendar_positions=tuple(
                    range(test_start_position - 80, test_start_position - 20)
                ),
                fit_validation_gap_calendar_positions=tuple(
                    range(test_start_position - 100, test_start_position - 80)
                ),
                test_start_calendar_position=test_start_position,
                test_end_calendar_position=positions[test_dates[-1]],
                pre_test_embargo_start_calendar_position=test_start_position - 20,
                pre_test_embargo_end_calendar_position=test_start_position - 1,
                validation_start_calendar_position=test_start_position - 80,
                validation_end_calendar_position=test_start_position - 21,
                fit_validation_gap_start_calendar_position=test_start_position - 100,
                fit_validation_gap_end_calendar_position=test_start_position - 81,
                available_observation_set_hash=evidence.available_observation_set_hash,
                available_label_set_hash=evidence.available_label_set_hash,
                available_member_set_hash=evidence.available_member_set_hash,
                exclusion_reasons=evidence.exclusion_reasons,
            )
        )
    return SplitPlanV1(
        request_semantic_hash=request_semantic_hash,
        calendar_hash=calendar_hash,
        eligible_decision_dates=eligible_decision_dates,
        coverage_status=("INSUFFICIENT_CALENDAR_HISTORY" if has_short_window else "COMPLETE"),
        folds=tuple(folds),
    )
