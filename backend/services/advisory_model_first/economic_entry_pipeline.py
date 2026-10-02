"""Small artifact-only economic-entry workflow; no DB writes or activation."""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
from pathlib import Path
from typing import Any, Mapping
from uuid import uuid4

import pandas as pd

from backend.services.advisory_model_first.economic_entry_contracts import (
    EconomicEntryInputIdentityV1,
    EconomicEntryLabelV1,
    EconomicEntryStudyPlanV1,
    EconomicEntryTrainingRequestV1,
)
from backend.services.advisory_model_first.economic_entry_labels import _fail
from backend.services.advisory_model_first.economic_entry_labels import KEY, build_economic_entry_labels, candidate_roster_sha256
from backend.services.advisory_model_first.economic_entry_training import (
    EconomicEntryTrainingResult,
    train_economic_entry_model,
)
from backend.services.advisory_model_first.policy_contracts import FrozenAdvisoryPolicyDatasetRequestV1
from backend.services.advisory_model_first.research_control import (
    AdvisoryResearchTrialRegistryV1,
    _exclusive_file_lock,
    authorize_research_window_access,
    evidence_reference_for_file,
    load_window_contract,
    research_policy_identity,
)
from backend.services.advisory_model_first.research_control_contracts import (
    ConsumedWindowV1,
    EvidenceReferenceV1,
    build_trial_record,
    build_window_access_request,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


STAGES = ("preregistered", "prepared", "trained", "evaluated")


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False, indent=2) + "\n").encode("utf-8")


def file_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def implementation_sha256() -> str:
    names = ("contracts", "labels", "training", "inference", "sources", "pipeline", "cli")
    paths = [Path(__file__).with_name(f"economic_entry_{name}.py") for name in names]
    if any(not path.is_file() for path in paths):
        _fail("economic implementation is incomplete; no study may be registered")
    return canonical_json_sha256({path.name: file_sha256(path) for path in paths})


def _verify_reference(ref: EvidenceReferenceV1) -> Path:
    path = Path(ref.artifact_uri)
    if not path.is_absolute() or not path.is_file():
        _fail(f"economic source reference is not an existing absolute file: {ref.role}")
    if path.stat().st_size != ref.size_bytes or file_sha256(path) != ref.sha256:
        _fail(f"economic source identity changed: {ref.role}")
    return path


def _study_root(output_root: str | Path, plan: EconomicEntryStudyPlanV1) -> Path:
    declared = Path(output_root)
    root = declared.resolve()
    if not declared.is_absolute() or root.drive.upper() == "C:":
        _fail("economic persistent output root cannot be C or relative")
    target = root / plan.experiment_id
    if target.resolve() != target or not target.is_relative_to(root):
        _fail("economic study output path escapes its declared root")
    return target


def read_stage(path: str | Path, *, stage: str, plan_sha256: str, parent_sha256: str | None) -> dict[str, Any]:
    root = Path(path).resolve()
    if (root / "manifest.json").resolve() != root / "manifest.json":
        _fail("economic stage manifest cannot reference a foreign path")
    try:
        manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        _fail(f"economic stage is not complete/readable: {type(exc).__name__}")
    expected = manifest.get("stage_sha256")
    payload = {key: value for key, value in manifest.items() if key != "stage_sha256"}
    if (
        manifest.get("schema_version") != "economic_entry_stage_v1" or manifest.get("stage") != stage
        or manifest.get("plan_sha256") != plan_sha256 or manifest.get("parent_sha256") != parent_sha256
        or canonical_json_sha256(payload) != expected
    ):
        _fail("economic stage identity or parent chain mismatch")
    files = manifest.get("files")
    if not isinstance(files, dict) or not files:
        _fail("economic stage has no bound artifacts")
    if {file.name for file in root.iterdir()} != set(files) | {"manifest.json"}:
        _fail("economic stage has missing or unregistered artifacts")
    for name, descriptor in files.items():
        if not re.fullmatch(r"[a-z0-9_]+\.(json|jsonl|parquet|txt)", name):
            _fail("economic stage contains an unsafe filename")
        path = root / name
        if path.resolve() != path or not path.is_file():
            _fail("economic stage artifact is a symlink, directory or foreign path")
        if path.stat().st_size != descriptor.get("size_bytes") or file_sha256(path) != descriptor.get("sha256"):
            _fail(f"economic stage artifact hash mismatch: {name}")
    return manifest


def publish_stage(
    *, study_root: str | Path, stage: str, plan_sha256: str, parent_sha256: str | None,
    artifacts: Mapping[str, bytes],
) -> Path:
    """Publish a whole stage atomically; unpublished generations stay recoverable.

    Files here are durable study artifacts, not temporary scratch. No tempfile or
    third-party cache is created. No existing generation or input is overwritten.
    """
    root = Path(study_root).resolve()
    if root.drive.upper() == "C:" or stage not in STAGES or not artifacts:
        _fail("economic publication needs a non-C root and a known nonempty stage")
    files = {}
    for name, content in artifacts.items():
        if not re.fullmatch(r"[a-z0-9_]+\.(json|jsonl|parquet|txt)", name) or name == "manifest.json":
            _fail("economic artifact filename is unsafe")
        if not isinstance(content, bytes) or not content:
            _fail("economic artifacts must be nonempty bytes")
        files[name] = {"sha256": hashlib.sha256(content).hexdigest(), "size_bytes": len(content)}
    manifest = {"schema_version": "economic_entry_stage_v1", "stage": stage, "plan_sha256": plan_sha256,
                "parent_sha256": parent_sha256, "files": files}
    manifest["stage_sha256"] = canonical_json_sha256(manifest)
    target = root / stage
    root.mkdir(parents=True, exist_ok=True)
    with _exclusive_file_lock(root / "publication.lock"):
        if target.exists():
            prior = read_stage(target, stage=stage, plan_sha256=plan_sha256, parent_sha256=parent_sha256)
            if prior["stage_sha256"] != manifest["stage_sha256"]:
                _fail("economic immutable stage conflicts with proposed content")
            return target
        generation = root / "unpublished" / f"{stage}_{uuid4().hex}"
        generation.mkdir(parents=True, exist_ok=False)
        for name, content in {**artifacts, "manifest.json": _json_bytes(manifest)}.items():
            with (generation / name).open("xb") as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
        read_stage(generation, stage=stage, plan_sha256=plan_sha256, parent_sha256=parent_sha256)
        os.rename(generation, target)  # Same volume, directory-level visibility, no overwrite.
    return target


def _authorize(plan: EconomicEntryStudyPlanV1) -> dict[str, Any]:
    contract = load_window_contract(_verify_reference(plan.window_contract_ref))
    request = build_window_access_request(
        contract_sha256=contract.contract_sha256, study_type=plan.study_type,
        objective_contract=plan.objective_contract, decision_use=plan.decision_use,
        dataset_identity=plan.dataset_identity, policy_identity=plan.policy_identity,
        start_date=plan.configuration.train_start, end_date=plan.configuration.label_cutoff,
    )
    return authorize_research_window_access(contract=contract, request=request)


def verify_economic_dataset(plan: EconomicEntryStudyPlanV1) -> tuple[dict[str, Any], FrozenAdvisoryPolicyDatasetRequestV1]:
    path = _verify_reference(plan.dataset_manifest_ref)
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if manifest.get("policy_dataset_bundle_id") != plan.dataset_identity:
        _fail("study dataset identity differs from its manifest")
    for name in ("request.json", "candidate_rankings.parquet", "candidate_episode_labels.parquet"):
        descriptor = manifest.get("files", {}).get(name, {})
        file = path.parent / name
        if not file.is_file() or file.stat().st_size != descriptor.get("size_bytes") or file_sha256(file) != descriptor.get("sha256"):
            _fail(f"frozen economic dataset file changed: {name}")
    request = FrozenAdvisoryPolicyDatasetRequestV1.model_validate_json((path.parent / "request.json").read_text(encoding="utf-8"))
    for field in ("program_id", "binding_version_id", "package_id", "manifest_sha256", "request_id", "request_sha256", "shadow_policy_sha256", "cost_policy_sha256"):
        if manifest.get(field) != getattr(request, field):
            _fail(f"frozen economic dataset request/manifest contradiction: {field}")
    contract = load_window_contract(_verify_reference(plan.window_contract_ref))
    for field in ("package_id", "manifest_sha256", "baseline_policy_sha256", "shadow_policy_sha256", "cost_policy_sha256"):
        if getattr(contract, field) != getattr(request, field):
            _fail(f"economic dataset/window identity contradiction: {field}")
    if contract.runtime_semantics_hash != request.selection_runtime_semantics_hash:
        _fail("economic dataset/window runtime semantics contradiction")
    if request.decision_date_start != plan.configuration.train_start.isoformat() or request.decision_date_end != plan.configuration.test_end.isoformat():
        _fail("economic first study must preserve the full original candidate decision window")
    if request.data_cutoff != plan.configuration.label_cutoff.isoformat():
        _fail("economic study label cutoff differs from the declared source cutoff")
    return manifest, request


def _register(plan: EconomicEntryStudyPlanV1, root: Path, stage: str, evidence_path: Path, *, generated=0, evaluated=0) -> dict[str, Any]:
    record = build_trial_record(
        experiment_id=plan.experiment_id, attempt_id="exact_attempt_v1", research_stage=stage,
        study_type=plan.study_type, hypothesis_family_id="economic_actual_open_entry_value_v1",
        parent_lineage=plan.parent_lineage, unique_variable="actual_open_conditioned_entry_vs_fixed_slot_cash",
        objective_contract=plan.objective_contract, dataset_identity=plan.dataset_identity,
        schema_identity="economic_entry_label_v1", policy_identity=plan.policy_identity,
        planned_trial_count=plan.configuration.model_trial_count,
        generated_trial_count=generated, evaluated_trial_count=evaluated, selected_trial_count=0,
        consumed_windows=(ConsumedWindowV1(
            window_id="P0C_DEVELOPMENT_V1", dataset_identity=plan.dataset_identity,
            start_date=plan.configuration.train_start, end_date=plan.configuration.label_cutoff,
        ),),
        result_class="CONTROL_READY" if stage in {"PREREGISTERED", "PREPARED"} else "EXPLORATORY",
        decision_use=plan.decision_use,
        evidence_refs=(evidence_reference_for_file(evidence_path, role=f"economic_{stage.lower()}"),),
    )
    return AdvisoryResearchTrialRegistryV1(root.parent / "trial_registry.jsonl").append_batch((record,))


def _check_registered_stage(plan: EconomicEntryStudyPlanV1, root: Path, stage: str) -> None:
    records = AdvisoryResearchTrialRegistryV1(root.parent / "trial_registry.jsonl").read()
    matching = [record for record in records if record.experiment_id == plan.experiment_id and record.research_stage == stage.upper()]
    manifest_path = root / stage.lower() / "manifest.json"
    if len(matching) != 1 or len(matching[0].evidence_refs) != 1 or matching[0].evidence_refs[0].sha256 != file_sha256(manifest_path):
        _fail("economic stage differs from its bound ledger evidence; use explicit exact retry for interrupted registration")


def preregister_economic_study(*, plan: EconomicEntryStudyPlanV1, output_root: str | Path) -> Path:
    plan = EconomicEntryStudyPlanV1.model_validate(plan.model_dump())
    if plan.implementation_sha256 != implementation_sha256():
        _fail("study plan does not bind the current economic implementation")
    _verify_reference(plan.dataset_manifest_ref)
    _verify_reference(plan.feature_ref)
    access = _authorize(plan)
    manifest, frozen_request = verify_economic_dataset(plan)
    policy_identity = research_policy_identity(
        baseline_policy_sha256=frozen_request.baseline_policy_sha256,
        shadow_policy_sha256=manifest["shadow_policy_sha256"], cost_policy_sha256=manifest["cost_policy_sha256"],
    )
    if policy_identity != plan.policy_identity:
        _fail("study policy identity differs from its frozen dataset")
    root = _study_root(output_root, plan)
    path = publish_stage(study_root=root, stage="preregistered", plan_sha256=plan.plan_sha256, parent_sha256=None,
                         artifacts={"plan.json": _json_bytes(plan.model_dump(mode="json")), "window_access.json": _json_bytes(access)})
    _register(plan, root, "PREREGISTERED", path / "manifest.json")
    return path / "plan.json"


def load_economic_study(plan_path: str | Path, *, output_root: str | Path) -> tuple[EconomicEntryStudyPlanV1, Path]:
    plan = EconomicEntryStudyPlanV1.model_validate_json(Path(plan_path).read_text(encoding="utf-8"))
    root = _study_root(output_root, plan)
    if Path(plan_path).resolve() != root / "preregistered" / "plan.json":
        _fail("economic plan path is not the registered study identity")
    read_stage(root / "preregistered", stage="preregistered", plan_sha256=plan.plan_sha256, parent_sha256=None)
    if plan.implementation_sha256 != implementation_sha256():
        _fail("economic implementation changed after pre-registration")
    _authorize(plan)
    _check_registered_stage(plan, root, "PREREGISTERED")
    return plan, root


def _parquet_bytes(frame: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    return buffer.getvalue()


def publish_prepared_economic_inputs(
    *, plan_path: str | Path, output_root: str | Path, identity: EconomicEntryInputIdentityV1,
    labels: tuple[EconomicEntryLabelV1, ...], features: pd.DataFrame,
    feature_source_sha256: str, source_receipt: dict[str, Any], source_artifacts: Mapping[str, bytes],
) -> Path:
    plan, root = load_economic_study(plan_path, output_root=output_root)
    manifest, frozen = verify_economic_dataset(plan)
    if identity.dataset_manifest_sha256 != plan.dataset_manifest_ref.sha256:
        _fail("prepared dataset does not match the registered manifest")
    if identity.shadow_policy["stop_loss_bps"] != plan.configuration.downside_budget_bps:
        _fail("prepared risk budget does not match the original policy")
    if len(labels) != manifest["files"]["candidate_episode_labels.parquet"]["row_count"]:
        _fail("prepared labels do not preserve the original full candidate count")
    for field in identity.episode_identity():
        if getattr(identity, field) != getattr(frozen, field):
            _fail(f"prepared identity differs from the original dataset: {field}")
    for target, original in (("frozen_episodes.parquet", "candidate_episode_labels.parquet"),
                             ("frozen_rankings.parquet", "candidate_rankings.parquet"),
                             ("frozen_request.json", "request.json")):
        content = source_artifacts.get(target)
        if content is None or hashlib.sha256(content).hexdigest() != manifest["files"][original]["sha256"]:
            _fail(f"prepared source snapshot differs from the frozen original: {target}")
    request = EconomicEntryTrainingRequestV1(
        **plan.configuration.model_dump(), input_identity_sha256=identity.identity_sha256,
        feature_source_sha256=feature_source_sha256, implementation_sha256=plan.implementation_sha256,
    )
    # Validates roster, clocks and label-end purge before source publication.
    from backend.services.advisory_model_first.economic_entry_training import prepare_economic_training_rows
    rows = prepare_economic_training_rows(features=features, labels=labels, request=request)
    parent = read_stage(root / "preregistered", stage="preregistered", plan_sha256=plan.plan_sha256, parent_sha256=None)
    artifacts = {
        **source_artifacts, "identity.json": _json_bytes(identity.model_dump(mode="json")),
        "training_request.json": _json_bytes(request.model_dump(mode="json")),
        "labels.json": _json_bytes([label.model_dump(mode="json") for label in labels]),
        "features.parquet": _parquet_bytes(features), "source_receipt.json": _json_bytes(source_receipt),
        "split_receipt.parquet": _parquet_bytes(rows.loc[:, ["decision_as_of_trade_date", "instrument", "split", "status", "training_eligible"]]),
    }
    path = publish_stage(study_root=root, stage="prepared", plan_sha256=plan.plan_sha256,
                         parent_sha256=parent["stage_sha256"], artifacts=artifacts)
    _register(plan, root, "PREPARED", path / "manifest.json")
    return path


def _prepared(root: Path, plan: EconomicEntryStudyPlanV1) -> dict[str, Any]:
    parent = read_stage(root / "preregistered", stage="preregistered", plan_sha256=plan.plan_sha256, parent_sha256=None)
    return read_stage(root / "prepared", stage="prepared", plan_sha256=plan.plan_sha256, parent_sha256=parent["stage_sha256"])


def prepare_economic_study(*, plan_path: str | Path, output_root: str | Path, daily_source=None) -> Path:
    plan, root = load_economic_study(plan_path, output_root=output_root)
    if (root / "prepared").exists():
        _prepared(root, plan)
        _register(plan, root, "PREPARED", root / "prepared" / "manifest.json")
        return root / "prepared"
    manifest, frozen = verify_economic_dataset(plan)
    feature_path = _verify_reference(plan.feature_ref)
    source_root = Path(plan.dataset_manifest_ref.artifact_uri).parent
    episodes = pd.read_parquet(source_root / "candidate_episode_labels.parquet")
    rankings = pd.read_parquet(source_root / "candidate_rankings.parquet")
    candidates = rankings.loc[rankings["is_candidate_decision"].eq(True) & rankings["selection_effective_rank"].le(20)].copy()
    if candidates.empty or len(candidates) != len(episodes):
        _fail("frozen economic candidate count differs from original episode roster")
    from backend.services.advisory_model_first.economic_entry_contracts import ECONOMIC_FEATURE_NAMES
    identity_names = ["program_id", "binding_version_id", "package_id", "manifest_sha256", "selection_runtime_semantics_hash"]
    feature_names = [name for name in ECONOMIC_FEATURE_NAMES if name != "query_gap_bps"]
    frozen_features = pd.read_parquet(feature_path, columns=KEY + identity_names + feature_names)
    if frozen_features.duplicated(KEY).any():
        _fail("frozen feature source has duplicate candidate identities")
    for field in identity_names:
        expected = getattr(frozen, field)
        if not candidates[field].eq(expected).all() or not frozen_features[field].eq(expected).all():
            _fail(f"economic candidate/feature identity differs from the original package: {field}")
    roster_sha = candidate_roster_sha256(candidates)
    # Preserve all original rows. Absence in the old dropped-date feature artifact
    # remains explicit unknown; no replacement stock, date deletion or new feature.
    features = candidates.loc[:, KEY].merge(frozen_features.loc[:, KEY + feature_names],
                                          on=KEY, how="left", validate="one_to_one", indicator=True)
    missing_feature_count = int(features["_merge"].eq("left_only").sum())
    features = features.drop(columns="_merge")
    feature_source = canonical_json_sha256({"original_file_sha256": plan.feature_ref.sha256,
                                           "roster_sha256": roster_sha, "missing_policy": plan.missing_feature_policy})
    features["feature_source_sha256"] = feature_source
    # This is a decision information boundary, NOT a backfilled capture timestamp.
    features["feature_visible_through"] = features[KEY[0]]
    from backend.services.advisory_model_first.economic_entry_sources import (
        EconomicEntryReadonlyDailySource, project_economic_price_inputs,
    )
    source = daily_source or EconomicEntryReadonlyDailySource()
    snapshot = source.load(symbols=tuple(sorted(candidates["instrument"].unique())),
                           start_date=plan.configuration.train_start, end_date=plan.configuration.label_cutoff,
                           maximum_rows=plan.resource_max_market_rows)
    prices, references, coordinate_sha, coordinate_receipt = project_economic_price_inputs(
        snapshot=snapshot, candidates=candidates, episodes=episodes,
    )
    identity = EconomicEntryInputIdentityV1(
        dataset_manifest_sha256=plan.dataset_manifest_ref.sha256,
        **{field: getattr(frozen, field) for field in ("request_id", "request_sha256", *identity_names)},
        universe_identity_sha256=canonical_json_sha256({"historical_definition": frozen.selection_runtime_semantics}),
        candidate_roster_sha256=roster_sha, price_coordinate_sha256=coordinate_sha,
        price_source_sha256=snapshot.source_sha256, reference_source_sha256=snapshot.source_sha256,
        shadow_policy=frozen.shadow_policy, shadow_policy_sha256=frozen.shadow_policy_sha256,
        cost_policy=frozen.cost_policy, cost_policy_sha256=frozen.cost_policy_sha256,
        source_evidence="RECOVERED_LIMITED", evidence_limitations=(
            "original full-market membership/native capture receipts not reconstructed",
            "current readonly DB historical values and restored coordinate parity are not original vintage capture",
            "previously consumed development window, no independent OOS or production activation",
            "old missing feature rows preserved as unavailable, not backfilled",
        ),
    )
    labels = build_economic_entry_labels(candidates=candidates, episodes=episodes, prices=prices,
                                        references=references, trading_calendar=snapshot.calendar,
                                        label_cutoff=pd.Timestamp(plan.configuration.label_cutoff), identity=identity)
    source_receipt = {"plan_sha256": plan.plan_sha256, "input_identity_sha256": identity.identity_sha256,
                      "candidate_count": len(candidates), "label_count": len(labels),
                      "missing_frozen_feature_rows": missing_feature_count,
                      "label_status_counts": {key: int(value) for key, value in pd.Series([label.status for label in labels]).value_counts().items()},
                      "db_source": snapshot.source_receipt, "coordinate_parity": coordinate_receipt,
                      "feature_source_sha256": feature_source, "source_metadata": frozen.model_dump(mode="json"),
                      "database_written": False, "selection_regenerated": False, "qe_experiment_submitted": False}
    source_artifacts = {
        "raw_daily.parquet": _parquet_bytes(snapshot.daily), "suspend_inputs.parquet": _parquet_bytes(snapshot.suspend_rows),
        "raw_suspend_inputs.parquet": _parquet_bytes(snapshot.raw_suspend_rows if snapshot.raw_suspend_rows is not None else snapshot.suspend_rows),
        "dividend_inputs.parquet": _parquet_bytes(snapshot.dividends), "calendar.json": _json_bytes([day.date().isoformat() for day in snapshot.calendar]),
        "prices.parquet": _parquet_bytes(prices), "references.parquet": _parquet_bytes(references),
        "frozen_rankings.parquet": (source_root / "candidate_rankings.parquet").read_bytes(),
        "frozen_episodes.parquet": (source_root / "candidate_episode_labels.parquet").read_bytes(),
        "frozen_request.json": (source_root / "request.json").read_bytes(),
        "dataset_manifest.json": _json_bytes(manifest),
    }
    return publish_prepared_economic_inputs(plan_path=plan_path, output_root=output_root, identity=identity,
                                           labels=labels, features=features, feature_source_sha256=feature_source,
                                           source_receipt=source_receipt, source_artifacts=source_artifacts)


def train_economic_study(*, plan_path: str | Path, output_root: str | Path) -> Path:
    plan, root = load_economic_study(plan_path, output_root=output_root)
    parent = _prepared(root, plan)
    if (root / "trained").exists():
        read_stage(root / "trained", stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=parent["stage_sha256"])
        _register(plan, root, "TRAINED", root / "trained" / "manifest.json", generated=1)
        return root / "trained"
    request = EconomicEntryTrainingRequestV1.model_validate_json((root / "prepared" / "training_request.json").read_text(encoding="utf-8"))
    if request.implementation_sha256 != plan.implementation_sha256:
        _fail("prepared training implementation differs from its preregistered plan")
    identity = EconomicEntryInputIdentityV1.model_validate_json((root / "prepared" / "identity.json").read_text(encoding="utf-8"))
    if identity.identity_sha256 != request.input_identity_sha256:
        _fail("prepared training input identity is inconsistent")
    if request.model_dump(exclude={"schema_version", "input_identity_sha256", "feature_source_sha256", "implementation_sha256"}) != plan.configuration.model_dump():
        _fail("prepared training configuration differs from the preregistered plan")
    labels = tuple(EconomicEntryLabelV1.model_validate(row) for row in json.loads((root / "prepared" / "labels.json").read_text(encoding="utf-8")))
    fitted = train_economic_entry_model(features=pd.read_parquet(root / "prepared" / "features.parquet"), labels=labels, request=request)
    import lightgbm as lgb
    metadata = {"request_sha256": request.request_sha256, "feature_names": list(request.feature_names),
                "feature_bounds": fitted.feature_bounds, "price_support": fitted.price_support,
                "diagnostics": fitted.diagnostics, "effective_parameters": request.effective_parameters,
                "lightgbm_version": lgb.__version__, "deployable": False}
    path = publish_stage(study_root=root, stage="trained", plan_sha256=plan.plan_sha256,
                         parent_sha256=parent["stage_sha256"], artifacts={
                             "return_model.txt": fitted.return_model.model_to_string().encode("utf-8"),
                             "risk_model.txt": fitted.risk_model.model_to_string().encode("utf-8"),
                             "model_metadata.json": _json_bytes(metadata),
                             "split_receipt.parquet": _parquet_bytes(fitted.split_receipt),
                         })
    _register(plan, root, "TRAINED", path / "manifest.json", generated=1)
    return path


def load_fitted_economic_study(*, plan_path: str | Path, output_root: str | Path) -> EconomicEntryTrainingResult:
    import lightgbm as lgb
    plan, root = load_economic_study(plan_path, output_root=output_root)
    parent = _prepared(root, plan)
    _check_registered_stage(plan, root, "PREPARED")
    read_stage(root / "trained", stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=parent["stage_sha256"])
    _check_registered_stage(plan, root, "TRAINED")
    request = EconomicEntryTrainingRequestV1.model_validate_json((root / "prepared" / "training_request.json").read_text(encoding="utf-8"))
    metadata = json.loads((root / "trained" / "model_metadata.json").read_text(encoding="utf-8"))
    if metadata["request_sha256"] != request.request_sha256 or tuple(metadata["feature_names"]) != request.feature_names:
        _fail("trained model metadata differs from the bound request")
    models = [lgb.Booster(model_file=str(root / "trained" / name)) for name in ("return_model.txt", "risk_model.txt")]
    if any(tuple(model.feature_name()) != request.feature_names for model in models):
        _fail("loaded economic model feature schema is incompatible")
    return EconomicEntryTrainingResult(
        *models, request, {key: tuple(value) for key, value in metadata["feature_bounds"].items()},
        {int(key): value for key, value in metadata["price_support"].items()}, metadata["diagnostics"],
        pd.read_parquet(root / "trained" / "split_receipt.parquet"),
    )
