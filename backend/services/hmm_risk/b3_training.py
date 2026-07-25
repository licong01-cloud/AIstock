from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np

from backend.services.hmm_risk.b3_acceptance import (
    D3_CONTRACT_VERSION,
    D5_SELECTION_VERSION,
    D6_SEMANTIC_VERSION,
    L2_RETRAIN_VERSION,
    RESTART_SCHEDULE,
    evaluate_covariance_acceptance,
    evaluate_likelihood_acceptance,
    evaluate_semantic_validation,
    evaluate_train_occupancy,
)
from backend.services.hmm_risk.state_model_set import (
    ALL_CORE_FEATURES,
    BASE_FEATURES,
    C008_B3_DIAG04_NU,
    HMM_N_ITER,
    L1TrainingSeries,
    StateModelSetError,
    _apply_preprocess,
    _b3_diag04_covariance_evidence,
    _finite_array,
    _fit_preprocess,
    _manual_b3_diag04_initialization,
    _monitor_diagnostic,
    _probability_vector,
    _sector_local_reference_variance,
    _transition_matrix,
    _write_immutable,
    c008_b3_diag04_fixed_numeric_environment,
    c008_b3_diag04_parameter_profile,
    canonical_json_bytes,
    canonical_sha256,
    causal_forward_posteriors,
)


class B3TrainingStageError(StateModelSetError):
    """Expected candidate-local failure with a stable stage and reason code."""

    def __init__(self, stage: str, reason_code: str, cause: Exception) -> None:
        super().__init__(f"{stage}: {cause}")
        self.stage = stage
        self.reason_code = reason_code
        self.cause_type = type(cause).__name__


@dataclass(frozen=True)
class B3FittedModel:
    family: str
    level: str
    seed: int
    sector_code: str
    feature_names: tuple[str, ...]
    preprocess: Mapping[str, Any]
    startprob: np.ndarray
    transmat: np.ndarray
    means: np.ndarray
    covars: np.ndarray
    parameter_profile_sha256: str
    numeric_environment_sha256: str
    observation_manifest_hash: str
    pit_constituent_manifest_hash: str
    model_payload_sha256: str

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": "hmm_risk_b3_fitted_model_v1",
            "contract_version": D3_CONTRACT_VERSION,
            "family": self.family,
            "level": self.level,
            "seed": self.seed,
            "sector_code": self.sector_code,
            "feature_names": list(self.feature_names),
            "preprocess": dict(self.preprocess),
            "startprob": self.startprob.tolist(),
            "transmat": self.transmat.tolist(),
            "means": self.means.tolist(),
            "covariance_type": "diag",
            "covars": self.covars.tolist(),
            "parameter_profile_sha256": self.parameter_profile_sha256,
            "numeric_environment_sha256": self.numeric_environment_sha256,
            "observation_manifest_hash": self.observation_manifest_hash,
            "pit_constituent_manifest_hash": self.pit_constituent_manifest_hash,
            "model_payload_sha256": self.model_payload_sha256,
        }


@dataclass(frozen=True)
class B3TrainOnlySeries:
    sector_code: str
    sector_name: str
    train_observations: np.ndarray
    train_dates: tuple[date, ...]
    pit_l2_constituents: tuple[str, ...]
    pit_constituent_manifest_hash: str
    observation_manifest_hash: str

    def validate(self, feature_count: int) -> None:
        if not self.sector_code.strip() or not self.sector_name.strip():
            raise StateModelSetError("B3 train-only sector code/name must be non-empty")
        train = np.asarray(self.train_observations, dtype=np.float64)
        if train.ndim != 2 or train.shape[1] != feature_count or train.shape[0] < 120:
            raise StateModelSetError(f"{self.sector_code} has insufficient train-only observations")
        if not np.isfinite(train).all():
            raise StateModelSetError(f"{self.sector_code} train-only observations are non-finite")
        if len(self.train_dates) != train.shape[0]:
            raise StateModelSetError(f"{self.sector_code} train-only dates do not align")
        if tuple(sorted(self.train_dates)) != self.train_dates or len(set(self.train_dates)) != len(self.train_dates):
            raise StateModelSetError(f"{self.sector_code} train-only dates must be strictly increasing")
        if not self.pit_l2_constituents:
            raise StateModelSetError(f"{self.sector_code} train-only constituent identity is missing")


def build_train_only_series(
    panel: Any,
    *,
    feature_names: Sequence[str],
    train_start: date,
    train_end: date,
    constituent_manifest: Mapping[str, Mapping[str, Any]],
    expected_sector_count: int,
    direct_sector_level: str,
) -> dict[str, B3TrainOnlySeries]:
    """Freeze only D3/D4/D5 train inputs; validation and future utility remain unread."""

    features = tuple(str(value) for value in feature_names)
    if features not in {BASE_FEATURES, ALL_CORE_FEATURES}:
        raise StateModelSetError("B3 train-only feature family is invalid")
    if direct_sector_level not in {"L1", "L2"} or expected_sector_count not in {31, 131}:
        raise StateModelSetError("B3 train-only level/count contract is invalid")
    output: dict[str, B3TrainOnlySeries] = {}
    for code in sorted(panel.index.get_level_values("l1_code").unique()):
        sector = panel.xs(code, level="l1_code")
        sector_dates = sector.index.date
        train = sector.loc[
            (sector_dates >= train_start) & (sector_dates <= train_end),
            list(features),
        ].dropna()
        if len(train) < 120:
            raise StateModelSetError(f"{code} train-only observation coverage is insufficient: {len(train)}")
        constituent = constituent_manifest.get(str(code))
        if not isinstance(constituent, Mapping):
            raise StateModelSetError(f"{code} train-only constituent manifest is missing")
        l2_codes = tuple(sorted(str(value) for value in constituent.get("l2_codes") or ()))
        if not l2_codes:
            raise StateModelSetError(f"{code} train-only L2 identity is missing")
        body = {
            "contract_version": D3_CONTRACT_VERSION,
            "direct_sector_level": direct_sector_level,
            "sector_code": str(code),
            "feature_names": list(features),
            "train_dates": [item.date().isoformat() for item in train.index],
            "train_sha256": canonical_sha256(train.to_numpy(dtype=np.float64).tolist()),
        }
        output[str(code)] = B3TrainOnlySeries(
            sector_code=str(code),
            sector_name=str(sector["l1_name"].dropna().iloc[-1]),
            train_observations=train.to_numpy(dtype=np.float64),
            train_dates=tuple(item.date() for item in train.index),
            pit_l2_constituents=l2_codes,
            pit_constituent_manifest_hash=canonical_sha256(constituent),
            observation_manifest_hash=canonical_sha256(body),
        )
    if len(output) != expected_sector_count:
        raise StateModelSetError(
            f"B3 train-only requires {expected_sector_count} direct {direct_sector_level} sectors; actual={len(output)}"
        )
    return output


def formal_b3_parameter_profile() -> dict[str, Any]:
    diagnostic_profile = c008_b3_diag04_parameter_profile()
    return {
        **diagnostic_profile,
        "schema_version": "hmm_risk_b3_parameter_profile_v1",
        "contract": D3_CONTRACT_VERSION,
        "numeric_contract_status": "USER_APPROVED_FORMAL_CONTRACT",
        "formal_acceptance_thresholds_applied_by_independent_d4_receipts": True,
        "selection_performed_by_profile": False,
    }


def _fit_b3_train_only(
    item: B3TrainOnlySeries,
    *,
    family: str,
    level: str,
    feature_names: tuple[str, ...],
    preprocess: Mapping[str, Any],
    seed: int,
    numeric_environment: Mapping[str, Any],
) -> tuple[dict[str, Any], B3FittedModel]:
    """Fit one formal B3 entry without touching validation or future utility."""

    try:
        from hmmlearn.hmm import GaussianHMM
    except ImportError as exc:  # pragma: no cover - dependency gate is explicit.
        raise StateModelSetError("hmmlearn==0.3.3 is required for formal B3 training") from exc
    item.validate(len(feature_names))
    try:
        train = _apply_preprocess(item.train_observations, preprocess)
        reference = _sector_local_reference_variance(train)
        startprob, transmat, means, initialized_covars, initialization = _manual_b3_diag04_initialization(
            train,
            sector_reference_variance=reference,
            random_seed=seed,
        )
    except (StateModelSetError, ValueError, FloatingPointError, np.linalg.LinAlgError) as exc:
        raise B3TrainingStageError(
            "initialization",
            "hmm_risk_model_initialization_failed",
            exc,
        ) from exc
    initialization = {
        **initialization,
        "schema_version": "hmm_risk_b3_manual_initialization_v1",
        "contract_version": D3_CONTRACT_VERSION,
        "diagnostic_source_contract": initialization.get("schema_version"),
        "formal_initialization_contract_applied": True,
    }
    prior = C008_B3_DIAG04_NU * np.broadcast_to(reference, (3, len(feature_names))).copy()
    try:
        model = GaussianHMM(
            n_components=3,
            covariance_type="diag",
            min_covar=0.0,
            startprob_prior=1.0,
            transmat_prior=1.0,
            means_prior=0.0,
            means_weight=0.0,
            covars_prior=prior,
            covars_weight=C008_B3_DIAG04_NU + 1.0,
            algorithm="viterbi",
            random_state=seed,
            n_iter=HMM_N_ITER,
            tol=0.01,
            verbose=False,
            params="stmc",
            init_params="",
            implementation="log",
        )
        model.startprob_ = startprob.copy()
        model.transmat_ = transmat.copy()
        model.means_ = means.copy()
        model.covars_ = initialized_covars.copy()
        model.fit(train)
    except (ValueError, FloatingPointError, np.linalg.LinAlgError) as exc:
        raise B3TrainingStageError("fit", "hmm_risk_model_fit_failed", exc) from exc
    try:
        monitor_evidence = _monitor_diagnostic(model)
        likelihood = evaluate_likelihood_acceptance(monitor_evidence)
    except (StateModelSetError, ValueError, FloatingPointError) as exc:
        raise B3TrainingStageError(
            "likelihood",
            "hmm_risk_model_likelihood_evidence_invalid",
            exc,
        ) from exc
    try:
        raw_covars = np.asarray(model._covars_, dtype=np.float64)
        covariance_evidence, _, smoothed_audit_log_likelihood = _b3_diag04_covariance_evidence(
            model,
            train,
            raw_covars=raw_covars,
            sector_reference_variance=reference,
        )
    except (StateModelSetError, ValueError, FloatingPointError, np.linalg.LinAlgError) as exc:
        raise B3TrainingStageError(
            "covariance",
            "hmm_risk_model_covariance_invalid",
            exc,
        ) from exc
    covariance_evidence = {
        **covariance_evidence,
        "train_rows": int(train.shape[0]),
        "postfit_projection_performed": False,
        "smoothed_audit_log_likelihood": smoothed_audit_log_likelihood,
    }
    covariance = evaluate_covariance_acceptance(covariance_evidence)
    try:
        fitted_startprob = _probability_vector(model.startprob_, f"{item.sector_code}.startprob", 3)
        fitted_transmat = _transition_matrix(model.transmat_, f"{item.sector_code}.transmat", 3)
        fitted_means = _finite_array(model.means_, f"{item.sector_code}.means", ndim=2)
        train_posteriors = causal_forward_posteriors(
            train,
            startprob=fitted_startprob,
            transmat=fitted_transmat,
            means=fitted_means,
            covars=raw_covars,
        )
        occupancy = evaluate_train_occupancy(train_posteriors, item.train_dates)
    except (StateModelSetError, ValueError, FloatingPointError, np.linalg.LinAlgError) as exc:
        raise B3TrainingStageError(
            "train_posterior",
            "hmm_risk_model_train_posterior_invalid",
            exc,
        ) from exc
    monitor_history = list(monitor_evidence.get("history") or ())
    terminal_likelihood = monitor_history[-1] if monitor_history else None
    model_body = {
        "schema_version": "hmm_risk_b3_fitted_model_v1",
        "contract_version": D3_CONTRACT_VERSION,
        "family": family,
        "level": level,
        "seed": seed,
        "sector_code": item.sector_code,
        "feature_names": list(feature_names),
        "preprocess": dict(preprocess),
        "startprob": fitted_startprob.tolist(),
        "transmat": fitted_transmat.tolist(),
        "means": fitted_means.tolist(),
        "covariance_type": "diag",
        "covars": raw_covars.tolist(),
        "parameter_profile_sha256": canonical_sha256(formal_b3_parameter_profile()),
        "numeric_environment_sha256": canonical_sha256(dict(numeric_environment)),
        "observation_manifest_hash": item.observation_manifest_hash,
        "pit_constituent_manifest_hash": item.pit_constituent_manifest_hash,
    }
    model_hash = canonical_sha256(model_body)
    fitted = B3FittedModel(
        family=family,
        level=level,
        seed=seed,
        sector_code=item.sector_code,
        feature_names=feature_names,
        preprocess=dict(preprocess),
        startprob=fitted_startprob,
        transmat=fitted_transmat,
        means=fitted_means,
        covars=raw_covars,
        parameter_profile_sha256=model_body["parameter_profile_sha256"],
        numeric_environment_sha256=model_body["numeric_environment_sha256"],
        observation_manifest_hash=item.observation_manifest_hash,
        pit_constituent_manifest_hash=item.pit_constituent_manifest_hash,
        model_payload_sha256=model_hash,
    )
    entry_body = {
        "schema_version": "hmm_risk_b3_training_entry_receipt_v1",
        "contract_version": D3_CONTRACT_VERSION,
        "retrain_contract_version": L2_RETRAIN_VERSION if level == "L2" else None,
        "family": family,
        "level": level,
        "seed": seed,
        "sector_code": item.sector_code,
        "feature_count": len(feature_names),
        "training_rows": int(train.shape[0]),
        "fit_status": "accepted",
        "initialization_evidence": initialization,
        "parameter_profile": formal_b3_parameter_profile(),
        "numeric_environment": dict(numeric_environment),
        "monitor_evidence": monitor_evidence,
        "likelihood": likelihood,
        "covariance": covariance,
        "train_occupancy": occupancy,
        "final_train_log_likelihood": terminal_likelihood,
        "final_train_log_likelihood_source": "monitor_history_terminal_value",
        "model_payload_sha256": model_hash,
        "validation_accessed": False,
        "future_utility_accessed": False,
        "semantic_labelability_accessed": False,
        "d6_status_accessed": False,
        "artifact_write_performed": False,
        "postfit_projection_performed": False,
    }
    return {**entry_body, "entry_receipt_sha256": canonical_sha256(entry_body)}, fitted


def run_level_repeat(
    series: Mapping[str, B3TrainOnlySeries],
    *,
    family: str,
    level: str,
    feature_names: Sequence[str],
    preprocess_family: str,
    process_identity: str,
) -> tuple[dict[str, Any], dict[tuple[int, str], B3FittedModel]]:
    """Run the complete 8-seed level grid; failures are retained and never trigger early stop."""

    features = tuple(str(value) for value in feature_names)
    if features not in {BASE_FEATURES, ALL_CORE_FEATURES}:
        raise StateModelSetError("formal B3 feature_names must be the approved 7/20 dimensional family")
    expected_count = 31 if level == "L1" else 131 if level == "L2" else 0
    codes = tuple(sorted(series))
    if expected_count == 0 or len(codes) != expected_count or len(set(codes)) != expected_count:
        raise StateModelSetError(f"formal B3 {level} requires exactly {expected_count} canonical sectors")
    for item in series.values():
        item.validate(len(features))
    environment = c008_b3_diag04_fixed_numeric_environment()
    package_version = str(environment.get("packages", {}).get("hmmlearn") or "")
    if package_version != "0.3.3":
        raise StateModelSetError(f"formal B3 requires hmmlearn==0.3.3 actual={package_version}")
    preprocess = _fit_preprocess(series, preprocess_family=preprocess_family)
    entries: list[dict[str, Any]] = []
    models: dict[tuple[int, str], B3FittedModel] = {}
    for seed in RESTART_SCHEDULE:
        for code in codes:
            item = series[code]
            try:
                entry, fitted = _fit_b3_train_only(
                    item,
                    family=family,
                    level=level,
                    feature_names=features,
                    preprocess=preprocess,
                    seed=seed,
                    numeric_environment=environment,
                )
            except B3TrainingStageError as exc:
                failure_body = {
                    "schema_version": "hmm_risk_b3_training_entry_receipt_v1",
                    "contract_version": D3_CONTRACT_VERSION,
                    "retrain_contract_version": L2_RETRAIN_VERSION if level == "L2" else None,
                    "family": family,
                    "level": level,
                    "seed": seed,
                    "sector_code": code,
                    "feature_count": len(features),
                    "training_rows": int(item.train_observations.shape[0]),
                    "fit_status": "failed",
                    "failure_stage": exc.stage,
                    "failure_reason_codes": [exc.reason_code],
                    "failure_type": exc.cause_type,
                    "failure_message": str(exc),
                    "validation_accessed": False,
                    "future_utility_accessed": False,
                    "semantic_labelability_accessed": False,
                    "d6_status_accessed": False,
                    "artifact_write_performed": False,
                }
                entries.append({**failure_body, "entry_receipt_sha256": canonical_sha256(failure_body)})
                continue
            entries.append(entry)
            models[(seed, code)] = fitted
    model_payloads = [models[key].payload() for key in sorted(models)]
    candidate_payload = {
        "family": family,
        "level": level,
        "schedule": list(RESTART_SCHEDULE),
        "canonical_sector_codes": list(codes),
        "feature_names": list(features),
        "preprocess": preprocess,
        "numeric_environment": environment,
        "entries": entries,
        "models": model_payloads,
    }
    payload = {
        "schema_version": "hmm_risk_b3_level_repeat_receipt_v1",
        "contract_version": D3_CONTRACT_VERSION,
        "retrain_contract_version": L2_RETRAIN_VERSION if level == "L2" else None,
        "process_identity": process_identity,
        "family": family,
        "level": level,
        "schedule": list(RESTART_SCHEDULE),
        "canonical_sector_codes": list(codes),
        "canonical_sector_set_sha256": canonical_sha256(list(codes)),
        "feature_names": list(features),
        "preprocess": preprocess,
        "numeric_environment": environment,
        "entries": entries,
        "models": model_payloads,
        "entry_count": len(entries),
        "expected_entry_count": len(RESTART_SCHEDULE) * expected_count,
        "validation_accessed": False,
        "future_utility_accessed": False,
        "semantic_labelability_accessed": False,
        "d6_status_accessed": False,
        "selection_performed": False,
        "artifact_write_performed": False,
    }
    return {
        **payload,
        "entry_payload_sha256": canonical_sha256(entries),
        "model_payload_sha256": canonical_sha256(model_payloads),
        "candidate_payload_sha256": canonical_sha256(candidate_payload),
        "repeat_receipt_sha256": canonical_sha256(payload),
    }, models


def models_from_repeat(repeat: Mapping[str, Any]) -> dict[tuple[int, str], B3FittedModel]:
    models: dict[tuple[int, str], B3FittedModel] = {}
    repeat_family = str(repeat.get("family") or "")
    repeat_level = str(repeat.get("level") or "")
    expected_codes = tuple(str(value) for value in repeat.get("canonical_sector_codes") or ())
    expected_features = tuple(str(value) for value in repeat.get("feature_names") or ())
    for raw in repeat.get("models") or ():
        feature_names = tuple(str(value) for value in raw.get("feature_names") or ())
        startprob = _probability_vector(raw.get("startprob"), "repeat.startprob", 3)
        transmat = _transition_matrix(raw.get("transmat"), "repeat.transmat", 3)
        means = _finite_array(raw.get("means"), "repeat.means", ndim=2)
        covars = _finite_array(raw.get("covars"), "repeat.covars", ndim=2)
        if means.shape != covars.shape or means.shape != (3, len(feature_names)) or np.any(covars <= 0.0):
            raise StateModelSetError("repeat model parameter shape is invalid")
        expected_hash = str(raw.get("model_payload_sha256") or "")
        body = {key: value for key, value in raw.items() if key != "model_payload_sha256"}
        if canonical_sha256(body) != expected_hash:
            raise StateModelSetError("repeat model payload hash mismatch")
        fitted = B3FittedModel(
            family=str(raw.get("family") or ""),
            level=str(raw.get("level") or ""),
            seed=int(raw.get("seed")),
            sector_code=str(raw.get("sector_code") or ""),
            feature_names=feature_names,
            preprocess=dict(raw.get("preprocess") or {}),
            startprob=startprob,
            transmat=transmat,
            means=means,
            covars=covars,
            parameter_profile_sha256=str(raw.get("parameter_profile_sha256") or ""),
            numeric_environment_sha256=str(raw.get("numeric_environment_sha256") or ""),
            observation_manifest_hash=str(raw.get("observation_manifest_hash") or ""),
            pit_constituent_manifest_hash=str(raw.get("pit_constituent_manifest_hash") or ""),
            model_payload_sha256=expected_hash,
        )
        if (
            fitted.family != repeat_family
            or fitted.level != repeat_level
            or fitted.seed not in RESTART_SCHEDULE
            or fitted.sector_code not in expected_codes
            or fitted.feature_names != expected_features
        ):
            raise StateModelSetError("repeat model identity differs from its level receipt")
        key = (fitted.seed, fitted.sector_code)
        if key in models:
            raise StateModelSetError("repeat contains duplicate model identity")
        models[key] = fitted
    if canonical_sha256([models[key].payload() for key in sorted(models)]) != repeat.get("model_payload_sha256"):
        raise StateModelSetError("repeat aggregate model hash mismatch")
    return models


def build_selected_level_artifact(
    selection: Mapping[str, Any],
    models: Mapping[tuple[int, str], B3FittedModel],
    series: Mapping[str, L1TrainingSeries],
) -> dict[str, Any]:
    """Run D6 only for the frozen D5 selection; semantic failure never reselects another seed."""

    if not bool(selection.get("level_selection_valid")):
        raise StateModelSetError("D6 cannot run before an accepted D5 level selection")
    selected_seed = selection.get("evidence", {}).get("selected_seed")
    if selected_seed not in RESTART_SCHEDULE:
        raise StateModelSetError("D5 selected seed is missing from the approved schedule")
    entries: list[dict[str, Any]] = []
    for code in sorted(series):
        fitted = models.get((int(selected_seed), code))
        if fitted is None:
            raise StateModelSetError(f"selected fitted model is missing for {code}")
        item = series[code]
        validation = _apply_preprocess(item.validation_observations, fitted.preprocess)
        posterior = causal_forward_posteriors(
            validation,
            startprob=fitted.startprob,
            transmat=fitted.transmat,
            means=fitted.means,
            covars=fitted.covars,
        )
        semantic = evaluate_semantic_validation(
            posterior,
            item.validation_dates,
            {
                **item.validation_future_components,
                "source_cutoff": (
                    None
                    if item.validation_utility_source_cutoff is None
                    else item.validation_utility_source_cutoff.isoformat()
                ),
                "formula_version": item.validation_utility_formula_version,
            },
        )
        entry_body = {
            **fitted.payload(),
            "semantic": semantic,
            "validation_accessed_after_selection": True,
            "future_utility_accessed_after_selection": True,
            "selection_reexecuted": False,
            "semantic_mapping": semantic.get("semantic_mapping"),
        }
        entries.append({**entry_body, "selected_entry_sha256": canonical_sha256(entry_body)})
    valid = all(
        entry["semantic"]["assignment"]["semantic_assignment_valid"]
        and entry["semantic"]["semantic_evidence"]["semantic_evidence_valid"]
        for entry in entries
    )
    body = {
        "schema_version": "hmm_risk_b3_selected_level_artifact_v1",
        "family": selection.get("evidence", {}).get("family"),
        "level": selection.get("evidence", {}).get("level"),
        "selected_seed": selected_seed,
        "selection_receipt_sha256": selection.get("receipt_sha256"),
        "status": "accepted" if valid else "blocked",
        "entry_count": len(entries),
        "entries": entries,
        "selection_reexecuted": False,
        "ready": False,
    }
    return {**body, "artifact_sha256": canonical_sha256(body)}


def _require_canonical_receipt_hash(receipt: Mapping[str, Any], *, field: str, label: str) -> None:
    expected = str(receipt.get(field) or "")
    body = {key: value for key, value in receipt.items() if key != field}
    if len(expected) != 64 or canonical_sha256(body) != expected:
        raise StateModelSetError(f"{label} canonical receipt hash mismatch")


def _require_hex_identity(value: str, *, length: int, label: str) -> None:
    if len(value) != length or any(character not in "0123456789abcdef" for character in value.lower()):
        raise StateModelSetError(f"{label} identity is invalid")


def _validate_ready_layer(
    artifact: Mapping[str, Any],
    selection: Mapping[str, Any],
    *,
    family: str,
    level: str,
    expected_count: int,
) -> None:
    if artifact.get("schema_version") != "hmm_risk_b3_selected_level_artifact_v1":
        raise StateModelSetError(f"B3 READY selected artifact schema is invalid for {family}/{level}")
    if artifact.get("family") != family or artifact.get("level") != level:
        raise StateModelSetError(f"B3 READY selected artifact identity is invalid for {family}/{level}")
    _require_canonical_receipt_hash(artifact, field="artifact_sha256", label=f"{family}/{level} artifact")
    _require_canonical_receipt_hash(selection, field="receipt_sha256", label=f"{family}/{level} selection")
    evidence = selection.get("evidence")
    selected_seed = artifact.get("selected_seed")
    if not isinstance(evidence, Mapping):
        raise StateModelSetError(f"B3 READY selection evidence is missing for {family}/{level}")
    if (
        selection.get("contract_version") != D5_SELECTION_VERSION
        or selection.get("level_selection_status") != "accepted"
        or selection.get("level_selection_valid") is not True
        or evidence.get("family") != family
        or evidence.get("level") != level
        or evidence.get("selected_seed") != selected_seed
        or selected_seed not in RESTART_SCHEDULE
        or artifact.get("selection_receipt_sha256") != selection.get("receipt_sha256")
    ):
        raise StateModelSetError(f"B3 READY selection contract is invalid for {family}/{level}")
    entries = list(artifact.get("entries") or ())
    if artifact.get("status") != "accepted" or artifact.get("entry_count") != expected_count:
        raise StateModelSetError(f"B3 READY blocked by incomplete semantic evidence for {family}/{level}")
    if len(entries) != expected_count:
        raise StateModelSetError(f"B3 READY selected entry count is invalid for {family}/{level}")
    codes: set[str] = set()
    model_keys = (
        "schema_version",
        "contract_version",
        "family",
        "level",
        "seed",
        "sector_code",
        "feature_names",
        "preprocess",
        "startprob",
        "transmat",
        "means",
        "covariance_type",
        "covars",
        "parameter_profile_sha256",
        "numeric_environment_sha256",
        "observation_manifest_hash",
        "pit_constituent_manifest_hash",
    )
    for entry in entries:
        if not isinstance(entry, Mapping):
            raise StateModelSetError(f"B3 READY selected entry is invalid for {family}/{level}")
        _require_canonical_receipt_hash(entry, field="selected_entry_sha256", label=f"{family}/{level} entry")
        if any(key not in entry for key in model_keys):
            raise StateModelSetError(f"B3 READY model payload is incomplete for {family}/{level}")
        model_body = {key: entry[key] for key in model_keys}
        if canonical_sha256(model_body) != entry.get("model_payload_sha256"):
            raise StateModelSetError(f"B3 READY model payload hash mismatch for {family}/{level}")
        code = str(entry.get("sector_code") or "")
        semantic = entry.get("semantic")
        if (
            entry.get("family") != family
            or entry.get("level") != level
            or entry.get("seed") != selected_seed
            or not code
            or code in codes
            or not isinstance(semantic, Mapping)
            or semantic.get("contract_version") != D6_SEMANTIC_VERSION
            or entry.get("semantic_mapping") != semantic.get("semantic_mapping")
        ):
            raise StateModelSetError(f"B3 READY selected entry identity is invalid for {family}/{level}")
        assignment = semantic.get("assignment")
        semantic_evidence = semantic.get("semantic_evidence")
        if not isinstance(assignment, Mapping) or not isinstance(semantic_evidence, Mapping):
            raise StateModelSetError(f"B3 READY semantic receipt is missing for {family}/{level}")
        _require_canonical_receipt_hash(
            assignment,
            field="receipt_sha256",
            label=f"{family}/{level}/{code} assignment",
        )
        _require_canonical_receipt_hash(
            semantic_evidence,
            field="receipt_sha256",
            label=f"{family}/{level}/{code} semantic evidence",
        )
        if (
            assignment.get("semantic_assignment_status") != "accepted"
            or assignment.get("semantic_assignment_valid") is not True
            or semantic_evidence.get("semantic_evidence_status") != "accepted"
            or semantic_evidence.get("semantic_evidence_valid") is not True
            or not isinstance(semantic.get("semantic_mapping"), Mapping)
            or set(semantic["semantic_mapping"].values()) != {"fading", "neutral", "trending"}
        ):
            raise StateModelSetError(f"B3 READY semantic evidence is not accepted for {family}/{level}/{code}")
        codes.add(code)


def write_b3_ready_model_set(
    output_root: str | Path,
    *,
    selected_artifacts: Mapping[tuple[str, str], Mapping[str, Any]],
    selection_receipts: Mapping[tuple[str, str], Mapping[str, Any]],
    dataset_manifest_hash: str,
    mapping_manifest_hash: str,
    producer_commit: str,
) -> Path:
    """Write a complete four-level READY set; blocked or partial inputs write nothing."""

    required = {(family, level) for family in ("legacy_covfix", "autocycle_all_core") for level in ("L1", "L2")}
    if set(selected_artifacts) != required or set(selection_receipts) != required:
        raise StateModelSetError("B3 READY requires both families and both direct levels")
    _require_hex_identity(dataset_manifest_hash, length=64, label="dataset manifest hash")
    _require_hex_identity(mapping_manifest_hash, length=64, label="mapping manifest hash")
    _require_hex_identity(producer_commit, length=40, label="producer commit")
    layers: dict[str, Any] = {}
    payloads: dict[str, bytes] = {}
    for family, level in sorted(required):
        artifact = dict(selected_artifacts[(family, level)])
        expected_count = 31 if level == "L1" else 131
        selection = selection_receipts[(family, level)]
        _validate_ready_layer(
            artifact,
            selection,
            family=family,
            level=level,
            expected_count=expected_count,
        )
        payload = canonical_json_bytes(artifact)
        payload_sha = canonical_sha256(artifact)
        key = f"{family}:{level}"
        relative = f"artifacts/{payload_sha}.{family}.{level.lower()}.json"
        payloads[relative] = payload
        layers[key] = {
            "family": family,
            "level": level,
            "status": "accepted",
            "sector_count": expected_count,
            "artifact_uri": relative,
            "artifact_sha256": payload_sha,
            "selection_receipt_sha256": selection.get("receipt_sha256"),
            "selected_seed": selection.get("evidence", {}).get("selected_seed"),
        }
    body = {
        "schema_version": "hmm_risk_state_model_set_v2",
        "status": "READY",
        "producer_commit": producer_commit,
        "dataset_manifest_hash": dataset_manifest_hash,
        "mapping_manifest_hash": mapping_manifest_hash,
        "contracts": {
            "d3": D3_CONTRACT_VERSION,
            "l2_retrain": L2_RETRAIN_VERSION,
        },
        "layers": layers,
        "selection_receipts": {
            f"{family}:{level}": selection_receipts[(family, level)] for family, level in sorted(required)
        },
        "ready_requires_both_families": True,
        "ready_requires_direct_l1_and_l2": True,
    }
    set_hash = canonical_sha256(body)
    manifest = {
        **body,
        "state_model_set_id": f"hmms_{set_hash[:24]}",
        "state_model_set_hash": set_hash,
    }
    root = Path(output_root).resolve() / manifest["state_model_set_id"]
    for relative, payload in sorted(payloads.items()):
        _write_immutable(root / relative, payload)
    manifest_path = root / "manifest.json"
    _write_immutable(manifest_path, canonical_json_bytes(manifest))
    return manifest_path
