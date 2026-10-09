"""GP5-PARENT-SCORE-CONDITION-1: explicit Advisory study, not QE admission."""
from datetime import date
from pathlib import Path, PureWindowsPath
import os
import re
from types import MappingProxyType
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import FEATURES, POLICY_SHA256
from backend.services.advisory_model_first.generic_price_5td_valuation_v2 import VALUATION_POLICY_SHA256
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

ARMS = ("matched_core", "candidate_parent_score")
BASE_ORDER = (*FEATURES, *(f"missing_{n}" for n in FEATURES), "scenario_gap_bps/100", "package_TCN")
MATRIX_ORDERS = {ARMS[0]: BASE_ORDER, ARMS[1]: (*BASE_ORDER, "parent_score_z", "missing_parent_score")}
PARAMETERS = MappingProxyType(dict(n_estimators=128, max_depth=6, min_samples_leaf=30,
    max_features=1., bootstrap=True, random_state=20261007, n_jobs=2, criterion="squared_error"))
STATISTICS = dict(block_sessions=5, bootstrap_count=2000, bootstrap_seed=20261008,
    primary_endpoints=2, marginal_diagnostic_alpha=.05, bonferroni_endpoint_alpha=.025)
SCHEMA = "parent_score_price_5td_v1"
SCHEMA_SHA256 = sha(dict(schema=SCHEMA, orders=MATRIX_ORDERS, parameters=dict(PARAMETERS),
    policy_sha256=POLICY_SHA256, valuation_policy_sha256=VALUATION_POLICY_SHA256,
    distribution="HONEST_PAIRED_CLUSTER_MASS_EQUAL_NONEMPTY_TREES", quantile="DIRECT_RISK_INVERTED_CDF"))


def node_path(uri):
    """Portable URI identity; Windows F/X paths map to the existing WSL mounts."""
    if not isinstance(uri, (str, Path)) or not str(uri):
        raise ValueError("an explicit non-C URI is required")
    windows = PureWindowsPath(str(uri))
    path = Path(uri)
    if windows.drive:
        if windows.drive.upper() == "C:" or len(windows.drive) != 2 or not windows.is_absolute():
            raise ValueError("C/UNC/relative study paths are forbidden")
        if os.name != "nt":
            path = Path("/mnt", windows.drive[0].lower(), *windows.parts[1:])
    if (not path.is_absolute() or path.resolve() != path or str(path).lower() == "/mnt/c"
            or str(path).lower().startswith("/mnt/c/")):
        raise ValueError("study URI must be absolute, non-C, and not a symlink")
    return path


class ParentScoreSourceV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    source_id: str = Field(min_length=1)
    package_id: str = Field(min_length=1)
    manifest_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    run_id: str = Field(min_length=1)
    context_bit: Literal[0, 1]
    prediction_source: dict
    universe_identity: dict | None = None

    @field_validator("context_bit", mode="before")
    @classmethod
    def strict_bit(cls, value):
        if type(value) is not int:
            raise ValueError("package context bit must be an integer, not bool/float")
        return value

    @model_validator(mode="after")
    def identity(self):
        expected = dict(package_id=self.package_id, manifest_sha256=self.manifest_sha256,
                        run_id=self.run_id, status="FROZEN_SCORE_AVAILABLE")
        if any(self.prediction_source.get(k) != v for k, v in expected.items()):
            raise ValueError("parent score descriptor contradicts the original package/run")
        descriptor = self.prediction_source.get("descriptor", {})
        if (not isinstance(descriptor.get("sha256"), str) or not re.fullmatch(r"[a-f0-9]{64}", descriptor["sha256"])
                or type(descriptor.get("size_bytes")) is not int
                or not 0 < descriptor["size_bytes"] <= 128*1024**2):
            raise ValueError("parent score must reference an existing bounded score artifact")
        return self


class ParentScoreStudyPlanV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal["parent_score_price_5td_study_v1"] = "parent_score_price_5td_study_v1"
    sources: tuple[ParentScoreSourceV1, ParentScoreSourceV1]
    calendar_ref: EvidenceReferenceV1
    train_start: date
    train_end: date
    evaluation_start: date
    evaluation_end: date
    implementation_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    source_commit: str = Field(pattern=r"^[a-f0-9]{40}$")
    node_python_uri: str = Field(min_length=1)
    qe_api_base: str = Field(min_length=1)
    library_versions: dict[str, str]
    study_type: Literal["EXPLORATORY_SCREEN"] = "EXPLORATORY_SCREEN"
    decision_use: Literal["NAVIGATION_ONLY"] = "NAVIGATION_ONLY"
    objective_contract: Literal["RISK_MANAGED_ADVISORY"] = "RISK_MANAGED_ADVISORY"

    @model_validator(mode="after")
    def specification(self):
        from backend.mcp.common import assert_loopback_url
        assert_loopback_url(self.qe_api_base)
        if (not self.train_start <= self.train_end < self.evaluation_start <= self.evaluation_end
                or len({s.package_id for s in self.sources}) != 2
                or len({s.source_id for s in self.sources}) != 2
                or {s.context_bit for s in self.sources} != {0, 1}
                or set(self.library_versions) != {"numpy", "pandas", "scikit-learn", "pyarrow", "pydantic"}
                or any(not v.strip() for v in self.library_versions.values())):
            raise ValueError("parent score study windows/sources/existing libraries differ")
        if (self.train_start, self.train_end, self.evaluation_start, self.evaluation_end) != (
                date(2024, 7, 4), date(2025, 5, 30), date(2025, 6, 3), date(2025, 9, 30)):
            raise ValueError("this study requires its four approved preregistered windows; no sealed finance")
        return self

    @property
    def plan_sha256(self):
        return sha(dict(plan=self.model_dump(mode="json"), hypothesis="GP5-PARENT-SCORE-CONDITION-1",
            schema_sha256=SCHEMA_SHA256, policy_sha256=POLICY_SHA256,
            valuation_policy_sha256=VALUATION_POLICY_SHA256, statistics=STATISTICS, physical_fit_budget=2))

    @property
    def experiment_id(self):
        return "advgp5parentscore_"+self.plan_sha256[:24]
