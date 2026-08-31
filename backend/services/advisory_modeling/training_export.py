from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Literal, Mapping

from pydantic import Field, field_validator, model_validator

from backend.services.advisory_historical_range.canonical import (
    canonical_json_sha256,
    canonical_json_text,
)

from .bundle_store import (
    ArtifactCompletionReceiptV1,
    BundleFileDescriptorV1,
    ImmutableArtifactStore,
    build_file_descriptors,
)
from .dataset_spool import RerankerDatasetSpool
from .errors import (
    AdvisoryModelingError,
    REASON_LABEL_CLOSURE_INCOMPLETE,
    REASON_SPLIT_PLAN_MISMATCH,
)
from .feature_builder import BuiltFeatureRowV1
from .feature_schema import FeatureSchemaV1
from .feature_snapshot import RerankerFeatureSnapshotV1, _feature_arrow_schema, _pyarrow
from .identity import FrozenModel, validated_hash
from .label_policy import (
    RankingGroupIdentityV1,
    RankingGroupLabelResultV1,
    RankingLabelInputV1,
    RankingLabelPolicyV1,
    build_ranking_labels,
)
from .training_view import (
    DatasetBuildRequestV1,
    FoldEvidenceClosureV1,
    SplitPlanV1,
    build_split_plan,
)


TRAINING_EXPORT_SCHEMA_VERSION = "advisory_reranker_training_export_v1"
TRAINING_VIEW_SCHEMA_VERSION_V1 = "advisory_reranker_training_view_manifest_v1"


class TrainingViewFoldV1(FrozenModel):
    fold_index: int = Field(ge=0, le=4)
    fit_start_date: date
    fit_end_date: date
    validation_start_date: date
    validation_end_date: date
    test_start_date: date
    test_end_date: date
    fold_training_as_of: datetime
    eligible_fit_date_count: int = Field(ge=0)
    modelable_fit_date_count: int = Field(ge=0)
    fit_date_set_hash: str = Field(min_length=64, max_length=64)
    fold_hash: str = Field(min_length=64, max_length=64)

    @field_validator("fit_date_set_hash", "fold_hash")
    @classmethod
    def _hashes(cls, value: str, info: Any) -> str:
        return str(validated_hash(value, field_name=info.field_name))


class TrainingViewManifestV1(FrozenModel):
    schema_version: Literal[TRAINING_VIEW_SCHEMA_VERSION_V1] = TRAINING_VIEW_SCHEMA_VERSION_V1
    window_years: Literal[2, 3, 5]
    coverage_status: Literal[
        "COMPLETE",
        "INSUFFICIENT_CALENDAR_HISTORY",
        "INSUFFICIENT_SPLIT_HISTORY",
        "INSUFFICIENT_EFFECTIVE_DATES",
    ]
    trainable: bool
    base_snapshot_id: str
    base_snapshot_content_hash: str = Field(min_length=64, max_length=64)
    feature_snapshot_id: str
    feature_snapshot_hash: str = Field(min_length=64, max_length=64)
    request_semantic_hash: str = Field(min_length=64, max_length=64)
    split_plan_hash: str = Field(min_length=64, max_length=64)
    training_rows_relative_path: Literal["training_rows.parquet"] = "training_rows.parquet"
    eligible_decision_date_count: int = Field(ge=0)
    modelable_decision_date_count: int = Field(ge=0)
    folds: tuple[TrainingViewFoldV1, ...]
    view_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "base_snapshot_content_hash",
        "feature_snapshot_hash",
        "request_semantic_hash",
        "split_plan_hash",
        "view_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validated_hash(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _identity(self) -> "TrainingViewManifestV1":
        if self.trainable != (self.coverage_status == "COMPLETE" and len(self.folds) == 5):
            raise ValueError("training view trainable state differs from complete coverage")
        digest = canonical_json_sha256(self.model_dump(mode="python", exclude={"view_hash"}))
        if self.view_hash is not None and self.view_hash != digest:
            raise ValueError("training view hash differs from canonical payload")
        object.__setattr__(self, "view_hash", digest)
        return self


class TrainingExportManifestV1(FrozenModel):
    schema_version: Literal[TRAINING_EXPORT_SCHEMA_VERSION] = TRAINING_EXPORT_SCHEMA_VERSION
    request_semantic_hash: str = Field(min_length=64, max_length=64)
    base_snapshot_id: str
    base_snapshot_content_hash: str = Field(min_length=64, max_length=64)
    feature_snapshot_id: str
    feature_snapshot_hash: str = Field(min_length=64, max_length=64)
    label_policy_hash: str = Field(min_length=64, max_length=64)
    split_plan: SplitPlanV1
    views: tuple[TrainingViewManifestV1, ...]
    row_count: int = Field(ge=0)
    files: tuple[BundleFileDescriptorV1, ...]
    export_hash: str | None = Field(default=None, min_length=64, max_length=64)
    export_id: str | None = Field(default=None, min_length=20, max_length=80)

    @field_validator(
        "request_semantic_hash",
        "base_snapshot_content_hash",
        "feature_snapshot_hash",
        "label_policy_hash",
        "export_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return validated_hash(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _identity(self) -> "TrainingExportManifestV1":
        if tuple(item.window_years for item in self.views) != (2, 3, 5):
            raise ValueError("training export requires 2/3/5-year views in canonical order")
        payload = self.model_dump(mode="python", exclude={"export_hash", "export_id"})
        digest = canonical_json_sha256(payload)
        if self.export_hash is not None and self.export_hash != digest:
            raise ValueError("training export hash differs from canonical payload")
        expected_id = f"advrtx_{digest[:24]}"
        if self.export_id is not None and self.export_id != expected_id:
            raise ValueError("training export id differs from canonical payload")
        object.__setattr__(self, "export_hash", digest)
        object.__setattr__(self, "export_id", expected_id)
        return self


class TrainingExportStore:
    def __init__(self, *, artifact_root: Path, repository_root: Path) -> None:
        self.store = ImmutableArtifactStore(
            artifact_root=artifact_root,
            repository_root=repository_root,
            namespace="training_exports",
        )

    def publish(
        self,
        *,
        manifest: TrainingExportManifestV1,
        payload_files: Mapping[str, bytes],
    ) -> ArtifactCompletionReceiptV1:
        if build_file_descriptors(payload_files) != manifest.files:
            raise AdvisoryModelingError(
                REASON_SPLIT_PLAN_MISMATCH,
                "training export payload differs from manifest descriptors",
            )
        files = dict(payload_files)
        files["training_export_manifest.json"] = (
            canonical_json_text(manifest.model_dump(mode="python")) + "\n"
        ).encode("utf-8")
        return self.store.publish(
            artifact_id=str(manifest.export_id),
            semantic_hash=str(manifest.export_hash),
            files=files,
        )


def _label_groups(
    *,
    base_snapshot_id: str,
    spool: RerankerDatasetSpool,
    feature_rows: tuple[BuiltFeatureRowV1, ...],
    label_policy: RankingLabelPolicyV1,
) -> tuple[dict[str, Any], ...]:
    labels = tuple(
        spool.iter_rows(
            source_kind="BASE_SNAPSHOT",
            source_identity=base_snapshot_id,
            logical_role="outcome_labels",
        )
    )
    selected = tuple(
        spool.iter_rows(
            source_kind="BASE_SNAPSHOT",
            source_identity=base_snapshot_id,
            logical_role="selected_labels",
        )
    )
    source_evidence = tuple(
        spool.iter_rows(
            source_kind="BASE_SNAPSHOT",
            source_identity=base_snapshot_id,
            logical_role="outcome_source_evidence",
        )
    )
    evidence_by_version = {
        str(item["label_version_id"]): item for item in source_evidence
    }
    if len(evidence_by_version) != len(source_evidence):
        raise AdvisoryModelingError(
            REASON_LABEL_CLOSURE_INCOMPLETE,
            "outcome source evidence contains duplicate label versions",
        )
    selected_versions = {str(item["terminal_label_version_id"]) for item in selected}
    by_key: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in labels:
        if (
            row["label_version_id"] in selected_versions
            and row["owner_type"] == "CANDIDATE"
            and int(row["horizon_trading_days"]) == 5
            and row["projection"] in {"RETURN_NET_EXCESS", "EXECUTABLE_MFE", "EXECUTABLE_MAE"}
        ):
            key = (str(row["canonical_signal_id"]), str(row["symbol"]), str(row["projection"]))
            by_key.setdefault(key, []).append(row)
    grouped_features: dict[tuple[str, str, str], list[BuiltFeatureRowV1]] = {}
    for row in feature_rows:
        key = (
            row.decision_trade_date.isoformat(),
            row.target_trade_date.isoformat(),
            row.stable_signal_semantics_hash,
        )
        grouped_features.setdefault(key, []).append(row)
    output: list[dict[str, Any]] = []
    for group_key in sorted(grouped_features):
        rows = sorted(grouped_features[group_key], key=lambda item: item.identity.symbol)
        label_inputs: list[RankingLabelInputV1] = []
        label_by_symbol: dict[str, dict[str, dict[str, Any]]] = {}
        for feature in rows:
            projections: dict[str, dict[str, Any]] = {}
            for projection in ("RETURN_NET_EXCESS", "EXECUTABLE_MFE", "EXECUTABLE_MAE"):
                matches = by_key.get(
                    (feature.identity.canonical_signal_id, feature.identity.symbol, projection),
                    [],
                )
                if len(matches) != 1:
                    continue
                label = matches[0]
                evidence = evidence_by_version.get(str(label["label_version_id"]))
                if evidence is None or any(
                        evidence.get(field) != label.get(field)
                        for field in (
                            "owner_type",
                            "canonical_signal_id",
                            "symbol",
                            "horizon_trading_days",
                            "projection",
                            "calculation_evidence_sha256",
                        )
                    ):
                    raise AdvisoryModelingError(
                        REASON_LABEL_CLOSURE_INCOMPLETE,
                        "selected label differs from its outcome source evidence",
                        context={"label_version_id": label["label_version_id"]},
                    )
                if (
                    label["maturity_status"] != "MATURED"
                    or label.get("projection_value_decimal") is None
                ):
                    continue
                projections[projection] = label
            if len(projections) != 3:
                continue
            label_by_symbol[feature.identity.symbol] = projections
            label_inputs.append(
                RankingLabelInputV1(
                    symbol=feature.identity.symbol,
                    return_5=projections["RETURN_NET_EXCESS"]["projection_value_decimal"],
                    executable_mfe_5=projections["EXECUTABLE_MFE"]["projection_value_decimal"],
                    executable_mae_5=projections["EXECUTABLE_MAE"]["projection_value_decimal"],
                    label_source_closure_hash=canonical_json_sha256(
                        tuple(
                            canonical_json_sha256(
                                {
                                    "label_content_hash": projections[name]["label_content_hash"],
                                    "calculation_evidence_sha256": projections[name][
                                        "calculation_evidence_sha256"
                                    ],
                                }
                            )
                            for name in ("RETURN_NET_EXCESS", "EXECUTABLE_MFE", "EXECUTABLE_MAE")
                        )
                    ),
                )
            )
        group_identity = RankingGroupIdentityV1(
            decision_as_of_trade_date=date.fromisoformat(group_key[0]),
            target_trade_date=date.fromisoformat(group_key[1]),
            stable_signal_semantics_hash=group_key[2],
            label_policy_hash=str(label_policy.label_policy_hash),
        )
        result: RankingGroupLabelResultV1 = build_ranking_labels(
            tuple(label_inputs),
            group_identity=group_identity,
            policy=label_policy,
        )
        relevance = {item.symbol: item for item in result.labels}
        for feature in rows:
            ranked = relevance.get(feature.identity.symbol)
            if ranked is None:
                continue
            projections = label_by_symbol[feature.identity.symbol]
            event_statuses = {
                str(item.get("outcome_event_status") or "")
                for item in projections.values()
            }
            if len(event_statuses) != 1 or not event_statuses.issubset(
                {"NONE", "BARRIER", "TERMINAL"}
            ):
                raise AdvisoryModelingError(
                    REASON_LABEL_CLOSURE_INCOMPLETE,
                    "selected label projections disagree on outcome event status",
                    context={"symbol": feature.identity.symbol},
                )
            computed_at = max(
                datetime.fromisoformat(str(item["computed_at"]).replace("Z", "+00:00")).astimezone(UTC)
                for item in projections.values()
            )
            output.append(
                {
                    "feature": feature,
                    "group_identity_hash": result.group_identity.group_identity_hash,
                    "group_status": result.status.value,
                    "relevance": ranked.relevance,
                    "raw_utility_5": float(ranked.raw_utility_5),
                    "return_5": float(projections["RETURN_NET_EXCESS"]["projection_value_decimal"]),
                    "executable_mfe_5": float(projections["EXECUTABLE_MFE"]["projection_value_decimal"]),
                    "executable_mae_5": float(projections["EXECUTABLE_MAE"]["projection_value_decimal"]),
                    "label_available_at": computed_at,
                    "outcome_event_status": next(iter(event_statuses)),
                    "label_source_closure_hash": ranked.label_source_closure_hash,
                }
            )
    return tuple(output)


def _fold_evidence(
    *,
    trading_dates: tuple[date, ...],
    eligible_dates: tuple[date, ...],
    labeled_rows: tuple[dict[str, Any], ...],
) -> tuple[FoldEvidenceClosureV1, ...]:
    if len(eligible_dates) < 300:
        return ()
    positions = {day: index for index, day in enumerate(trading_dates)}
    tests = eligible_dates[-300:]
    if positions[tests[0]] < 101:
        return ()
    result: list[FoldEvidenceClosureV1] = []
    for fold_index in range(5):
        test_start = tests[fold_index * 60]
        latest_training_date = trading_dates[positions[test_start] - 1]
        training_as_of = datetime.combine(
            latest_training_date,
            datetime.min.time(),
            tzinfo=UTC,
        ).replace(hour=7)
        available = tuple(
            item
            for item in labeled_rows
            if item["label_available_at"] <= training_as_of
            and item["feature"].decision_trade_date <= latest_training_date
        )
        result.append(
            FoldEvidenceClosureV1(
                fold_index=fold_index,
                available_observation_set_hash=canonical_json_sha256(
                    tuple(sorted(item["feature"].identity.observation_version_id for item in available))
                ),
                available_label_set_hash=canonical_json_sha256(
                    tuple(sorted(item["label_source_closure_hash"] for item in available))
                ),
                available_member_set_hash=canonical_json_sha256(
                    tuple(sorted(str(item["feature"].identity.row_identity_hash) for item in available))
                ),
                exclusion_reasons=(),
            )
        )
    return tuple(result)


def materialize_training_export(
    *,
    request: DatasetBuildRequestV1,
    base_snapshot_id: str,
    base_snapshot_content_hash: str,
    feature_snapshot: RerankerFeatureSnapshotV1,
    feature_schema: FeatureSchemaV1,
    feature_rows: tuple[BuiltFeatureRowV1, ...],
    base_spool: RerankerDatasetSpool,
    trading_dates: tuple[date, ...],
    label_policy: RankingLabelPolicyV1,
) -> tuple[TrainingExportManifestV1, dict[str, bytes]]:
    labeled = _label_groups(
        base_snapshot_id=base_snapshot_id,
        spool=base_spool,
        feature_rows=feature_rows,
        label_policy=label_policy,
    )
    eligible_dates = tuple(
        sorted({item["feature"].decision_trade_date for item in labeled})
    )
    fold_evidence = _fold_evidence(
        trading_dates=trading_dates,
        eligible_dates=eligible_dates,
        labeled_rows=labeled,
    )
    split_plan = build_split_plan(
        request_semantic_hash=str(request.request_semantic_hash),
        calendar_hash=request.calendar_hash,
        trading_dates=trading_dates,
        eligible_decision_dates=eligible_dates,
        fold_evidence_closures=fold_evidence,
    )
    pa, pq = _pyarrow()
    base_schema = _feature_arrow_schema(feature_schema)
    extra_fields = [
        pa.field("group_identity_hash", pa.string(), nullable=False),
        pa.field("group_status", pa.string(), nullable=False),
        pa.field("relevance", pa.int64(), nullable=False),
        pa.field("raw_utility_5", pa.float64(), nullable=False),
        pa.field("label_return_5", pa.float64(), nullable=False),
        pa.field("label_executable_mfe_5", pa.float64(), nullable=False),
        pa.field("label_executable_mae_5", pa.float64(), nullable=False),
        pa.field("label_available_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("label_outcome_event_status", pa.string(), nullable=False),
        pa.field("label_source_closure_hash", pa.string(), nullable=False),
    ]
    schema = pa.schema([*base_schema, *extra_fields]).with_metadata(
        {
            b"aistock_training_export_schema_version": TRAINING_EXPORT_SCHEMA_VERSION.encode("ascii"),
            b"aistock_feature_schema_hash": str(feature_schema.feature_schema_hash).encode("ascii"),
        }
    )
    flat_rows = []
    from .feature_snapshot import _feature_flat_row

    for item in sorted(
        labeled,
        key=lambda value: (
            value["feature"].decision_trade_date,
            value["feature"].identity.symbol,
        ),
    ):
        flat_rows.append(
            {
                **_feature_flat_row(item["feature"]),
                "group_identity_hash": item["group_identity_hash"],
                "group_status": item["group_status"],
                "relevance": item["relevance"],
                "raw_utility_5": item["raw_utility_5"],
                "label_return_5": item["return_5"],
                "label_executable_mfe_5": item["executable_mfe_5"],
                "label_executable_mae_5": item["executable_mae_5"],
                "label_available_at": item["label_available_at"],
                "label_outcome_event_status": item["outcome_event_status"],
                "label_source_closure_hash": item["label_source_closure_hash"],
            }
        )
    table = pa.Table.from_pylist(flat_rows, schema=schema)
    output = pa.BufferOutputStream()
    pq.write_table(
        table,
        output,
        compression="zstd",
        compression_level=9,
        use_dictionary=False,
        version="2.6",
    )
    training_payload = output.getvalue().to_pybytes()
    readback = pq.read_table(pa.BufferReader(training_payload))
    if (
        readback.schema != schema
        or readback.num_rows != len(flat_rows)
        or tuple(readback.column("row_identity_hash").to_pylist())
        != tuple(item["row_identity_hash"] for item in flat_rows)
        or tuple(readback.column("label_outcome_event_status").to_pylist())
        != tuple(item["label_outcome_event_status"] for item in flat_rows)
    ):
        raise AdvisoryModelingError(
            REASON_LABEL_CLOSURE_INCOMPLETE,
            "training rows differ on exact Parquet readback",
        )
    views: list[TrainingViewManifestV1] = []
    minimum_effective_dates = {2: 480, 3: 720, 5: 1200}
    for years in (2, 3, 5):
        folds: list[TrainingViewFoldV1] = []
        coverage = "INSUFFICIENT_SPLIT_HISTORY"
        if split_plan.folds:
            window_incomplete = False
            effective_dates_incomplete = False
            for fold in split_plan.folds:
                window = next(item for item in fold.training_windows if item.window_years == years)
                window_incomplete = window_incomplete or window.coverage_status != "COMPLETE"
                fit_date_set = set(window.fit_dates)
                available_fit_rows = tuple(
                    item
                    for item in labeled
                    if item["feature"].decision_trade_date in fit_date_set
                    and item["label_available_at"] <= fold.fold_training_as_of
                )
                eligible_fit_dates = {
                    item["feature"].decision_trade_date for item in available_fit_rows
                }
                modelable_fit_dates = {
                    item["feature"].decision_trade_date
                    for item in available_fit_rows
                    if item["group_status"] == "MODELABLE"
                }
                effective_dates_incomplete = (
                    effective_dates_incomplete
                    or len(modelable_fit_dates) < minimum_effective_dates[years]
                )
                folds.append(
                    TrainingViewFoldV1(
                        fold_index=fold.fold_index,
                        fit_start_date=window.fit_dates[0],
                        fit_end_date=window.fit_dates[-1],
                        validation_start_date=fold.validation_dates[0],
                        validation_end_date=fold.validation_dates[-1],
                        test_start_date=fold.test_dates[0],
                        test_end_date=fold.test_dates[-1],
                        fold_training_as_of=fold.fold_training_as_of,
                        eligible_fit_date_count=len(eligible_fit_dates),
                        modelable_fit_date_count=len(modelable_fit_dates),
                        fit_date_set_hash=str(window.fit_date_set_hash),
                        fold_hash=str(fold.fold_hash),
                    )
                )
            coverage = (
                "INSUFFICIENT_CALENDAR_HISTORY"
                if window_incomplete
                else "INSUFFICIENT_EFFECTIVE_DATES"
                if effective_dates_incomplete
                else "COMPLETE"
            )
        views.append(
            TrainingViewManifestV1(
                window_years=years,
                coverage_status=coverage,
                trainable=coverage == "COMPLETE" and len(folds) == 5,
                base_snapshot_id=base_snapshot_id,
                base_snapshot_content_hash=base_snapshot_content_hash,
                feature_snapshot_id=str(feature_snapshot.feature_snapshot_id),
                feature_snapshot_hash=str(feature_snapshot.feature_snapshot_hash),
                request_semantic_hash=str(request.request_semantic_hash),
                split_plan_hash=str(split_plan.split_plan_hash),
                eligible_decision_date_count=len(eligible_dates),
                modelable_decision_date_count=len(
                    {
                        item["feature"].decision_trade_date
                        for item in labeled
                        if item["group_status"] == "MODELABLE"
                    }
                ),
                folds=tuple(folds),
            )
        )
    payload_files: dict[str, bytes] = {
        "training_rows.parquet": training_payload,
        "split_plan.json": (canonical_json_text(split_plan.model_dump(mode="python")) + "\n").encode("utf-8"),
    }
    for view in views:
        payload_files[f"views/{view.window_years}y.json"] = (
            canonical_json_text(view.model_dump(mode="python")) + "\n"
        ).encode("utf-8")
    manifest = TrainingExportManifestV1(
        request_semantic_hash=str(request.request_semantic_hash),
        base_snapshot_id=base_snapshot_id,
        base_snapshot_content_hash=base_snapshot_content_hash,
        feature_snapshot_id=str(feature_snapshot.feature_snapshot_id),
        feature_snapshot_hash=str(feature_snapshot.feature_snapshot_hash),
        label_policy_hash=str(label_policy.label_policy_hash),
        split_plan=split_plan,
        views=tuple(views),
        row_count=len(flat_rows),
        files=build_file_descriptors(payload_files),
    )
    return manifest, payload_files
