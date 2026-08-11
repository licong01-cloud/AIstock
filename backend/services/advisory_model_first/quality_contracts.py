from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.services.strategy_package.runtime_variant import canonical_json_sha256


QUALITY_SEEDS = (20260808, 20260817, 20260829, 20260843, 20260871)
QUALITY_WINDOWS = ("EXPANDING_ALL", "ROLLING_160", "ROLLING_120")
QUALITY_FAMILIES = (
    "LAMBDARANK_NDCG5",
    "RANK_XENDCG5",
    "REGRESSION_L1_UTILITY5",
)
QUALITY_MODEL_WEIGHTS = (0.25, 0.5, 0.75, 1.0)
ENSEMBLE_SCORE_POLICY = "PERCENTILE_RANK_MEAN_V1"
SELECTION_PRIOR_POLICY = "SELECTION_EFFECTIVE_RANK_PERCENTILE_V1"
TEST_ONCE_POLICY = "WINNER_FROZEN_BEFORE_TEST_READ_V1"
M5A_PARENT_BUNDLE_ID = "9cf14e80cf13fad5473684d825935978aa40f3ff2f429fd98cbac0c7b7f87629"


class QualityProjectionDescriptor(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    path: str
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    row_count: int = Field(gt=0)
    date_start: str
    date_end: str
    split_names: tuple[str, ...]


class ParentArtifactDescriptor(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    path: str
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class QualityTrialMatrix(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    windows: tuple[str, ...] = QUALITY_WINDOWS
    families: tuple[str, ...] = QUALITY_FAMILIES
    seeds: tuple[int, ...] = QUALITY_SEEDS
    model_weights: tuple[float, ...] = QUALITY_MODEL_WEIGHTS

    @model_validator(mode="after")
    def validate_frozen_matrix(self) -> "QualityTrialMatrix":
        if (
            self.windows != QUALITY_WINDOWS
            or self.families != QUALITY_FAMILIES
            or self.seeds != QUALITY_SEEDS
            or self.model_weights != QUALITY_MODEL_WEIGHTS
        ):
            raise ValueError("M5A trial matrix differs from the frozen design")
        return self


class AdvisoryRerankerQualityTrainRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["advisory_reranker_quality_train_request_v1"] = "advisory_reranker_quality_train_request_v1"
    request_id: str
    request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    created_at: str
    output_root: str
    parent_bundle_id: Literal["9cf14e80cf13fad5473684d825935978aa40f3ff2f429fd98cbac0c7b7f87629"] = M5A_PARENT_BUNDLE_ID
    parent_request_id: str
    parent_artifacts: dict[str, ParentArtifactDescriptor]
    parent_split_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    train_validation_projection: QualityProjectionDescriptor
    package_id: str
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    style_profile_id: str
    style_profile_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    selection_runtime_semantics_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    repository_root: str
    repository_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    conda_environment: Literal["rdagent-gpu"] = "rdagent-gpu"
    lightgbm_version: str
    trial_matrix: QualityTrialMatrix = QualityTrialMatrix()
    ensemble_score_policy: Literal["PERCENTILE_RANK_MEAN_V1"] = ENSEMBLE_SCORE_POLICY
    selection_prior_policy: Literal["SELECTION_EFFECTIVE_RANK_PERCENTILE_V1"] = SELECTION_PRIOR_POLICY
    test_once_policy: Literal["WINNER_FROZEN_BEFORE_TEST_READ_V1"] = TEST_ONCE_POLICY
    resource_max_rss_bytes: int = 8 * 1024**3

    @model_validator(mode="after")
    def validate_identity_and_scope(self) -> "AdvisoryRerankerQualityTrainRequestV1":
        if self.train_validation_projection.split_names != ("train", "validation"):
            raise ValueError("Stage A projection must contain only train and validation")
        expected = canonical_json_sha256(self.functional_payload())
        if self.request_sha256 != expected or self.request_id != f"advm5train_{expected[:24]}":
            raise ValueError("M5A train request identity mismatch")
        required_parent = {
            "training_request.json",
            "feature_schema.json",
            "label_policy.json",
            "split.json",
        }
        if set(self.parent_artifacts) != required_parent:
            raise ValueError("M5A parent artifact set is incomplete")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(
            mode="json",
            exclude={"request_id", "request_sha256", "created_at", "output_root"},
        )

    def write_json(self, path: str | Path) -> None:
        _write_model(self, path)


class QualityWinnerCandidate(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    candidate_id: str
    window_id: str
    family_id: str
    model_weight: float = Field(ge=0.0, le=1.0)
    seeds: tuple[int, ...]
    member_model_paths: tuple[str, ...] = ()
    member_model_sha256: tuple[str, ...] = ()
    categorical_vocabulary_path: str | None = None
    categorical_vocabulary_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    validation_metrics: dict[str, Any]

    @model_validator(mode="after")
    def validate_candidate(self) -> "QualityWinnerCandidate":
        if self.model_weight == 0.0:
            if (
                self.candidate_id != "SELECTION_PRIOR_ONLY"
                or self.member_model_paths
                or self.member_model_sha256
                or self.categorical_vocabulary_path is not None
                or self.categorical_vocabulary_sha256 is not None
            ):
                raise ValueError("selection-prior winner cannot carry model members")
        elif (
            self.window_id not in QUALITY_WINDOWS
            or self.family_id not in QUALITY_FAMILIES
            or self.seeds != QUALITY_SEEDS
            or len(self.member_model_paths) != len(QUALITY_SEEDS)
            or len(self.member_model_sha256) != len(QUALITY_SEEDS)
            or not self.categorical_vocabulary_path
            or not self.categorical_vocabulary_sha256
        ):
            raise ValueError("model winner is not a complete frozen five-seed candidate")
        return self


class QualityWinnerReceiptV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["advisory_reranker_quality_winner_receipt_v1"] = (
        "advisory_reranker_quality_winner_receipt_v1"
    )
    receipt_id: str
    receipt_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    created_at: str
    train_request_id: str
    train_request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    status: Literal["MODEL_WINNER_SELECTED", "NO_VALIDATION_MODEL_LIFT_OBSERVED"]
    winner: QualityWinnerCandidate
    tournament_report_path: str
    tournament_report_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_identity(self) -> "QualityWinnerReceiptV1":
        expected = canonical_json_sha256(self.functional_payload())
        if self.receipt_sha256 != expected or self.receipt_id != f"advm5winner_{expected[:24]}":
            raise ValueError("M5A winner receipt identity mismatch")
        expected_status = (
            "NO_VALIDATION_MODEL_LIFT_OBSERVED" if self.winner.model_weight == 0.0 else "MODEL_WINNER_SELECTED"
        )
        if self.status != expected_status:
            raise ValueError("M5A winner status differs from the selected candidate")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"receipt_id", "receipt_sha256", "created_at"})

    def write_json(self, path: str | Path) -> None:
        _write_model(self, path)


class AdvisoryRerankerQualityTestRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["advisory_reranker_quality_test_request_v1"] = "advisory_reranker_quality_test_request_v1"
    evaluation_id: str
    request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    created_at: str
    output_root: str
    train_request_id: str
    train_request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    parent_bundle_id: Literal["9cf14e80cf13fad5473684d825935978aa40f3ff2f429fd98cbac0c7b7f87629"] = M5A_PARENT_BUNDLE_ID
    parent_split_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    winner_receipt_path: str
    winner_receipt_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    winner_receipt_id: str
    test_projection: QualityProjectionDescriptor
    parent_test_predictions: ParentArtifactDescriptor
    test_once_policy: Literal["WINNER_FROZEN_BEFORE_TEST_READ_V1"] = TEST_ONCE_POLICY

    @model_validator(mode="after")
    def validate_identity_and_scope(self) -> "AdvisoryRerankerQualityTestRequestV1":
        if self.test_projection.split_names != ("test",):
            raise ValueError("Stage B projection must contain only test")
        expected = canonical_json_sha256(self.functional_payload())
        if self.request_sha256 != expected or self.evaluation_id != f"advm5test_{expected[:24]}":
            raise ValueError("M5A test request identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(
            mode="json",
            exclude={"evaluation_id", "request_sha256", "created_at", "output_root"},
        )

    def write_json(self, path: str | Path) -> None:
        _write_model(self, path)


def build_quality_train_request(**values: Any) -> AdvisoryRerankerQualityTrainRequestV1:
    created_at = str(values.pop("created_at", datetime.now(timezone.utc).isoformat()))
    values["parent_artifacts"] = {
        key: ParentArtifactDescriptor.model_validate(value) for key, value in dict(values["parent_artifacts"]).items()
    }
    values["train_validation_projection"] = QualityProjectionDescriptor.model_validate(
        values["train_validation_projection"]
    )
    if "trial_matrix" in values:
        values["trial_matrix"] = QualityTrialMatrix.model_validate(values["trial_matrix"])
    seed = AdvisoryRerankerQualityTrainRequestV1.model_construct(
        schema_version="advisory_reranker_quality_train_request_v1",
        request_id="pending",
        request_sha256="0" * 64,
        created_at=created_at,
        **values,
    )
    digest = canonical_json_sha256(seed.functional_payload())
    return AdvisoryRerankerQualityTrainRequestV1(
        schema_version="advisory_reranker_quality_train_request_v1",
        request_id=f"advm5train_{digest[:24]}",
        request_sha256=digest,
        created_at=created_at,
        **values,
    )


def build_winner_receipt(**values: Any) -> QualityWinnerReceiptV1:
    created_at = str(values.pop("created_at", datetime.now(timezone.utc).isoformat()))
    seed = QualityWinnerReceiptV1.model_construct(
        schema_version="advisory_reranker_quality_winner_receipt_v1",
        receipt_id="pending",
        receipt_sha256="0" * 64,
        created_at=created_at,
        **values,
    )
    digest = canonical_json_sha256(seed.functional_payload())
    return QualityWinnerReceiptV1(
        schema_version="advisory_reranker_quality_winner_receipt_v1",
        receipt_id=f"advm5winner_{digest[:24]}",
        receipt_sha256=digest,
        created_at=created_at,
        **values,
    )


def build_quality_test_request(**values: Any) -> AdvisoryRerankerQualityTestRequestV1:
    created_at = str(values.pop("created_at", datetime.now(timezone.utc).isoformat()))
    seed = AdvisoryRerankerQualityTestRequestV1.model_construct(
        schema_version="advisory_reranker_quality_test_request_v1",
        evaluation_id="pending",
        request_sha256="0" * 64,
        created_at=created_at,
        **values,
    )
    digest = canonical_json_sha256(seed.functional_payload())
    return AdvisoryRerankerQualityTestRequestV1(
        schema_version="advisory_reranker_quality_test_request_v1",
        evaluation_id=f"advm5test_{digest[:24]}",
        request_sha256=digest,
        created_at=created_at,
        **values,
    )


def _write_model(model: BaseModel, path: str | Path) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.write_text(model.model_dump_json(indent=2), encoding="utf-8")
    temporary.replace(target)
