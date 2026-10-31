"""Independent, immutable admission for the approved natural-entry experiment."""

from copy import deepcopy
from types import MappingProxyType

if __package__:
    from .qe_l2_relative_admission import ADMISSIONS as RELATIVE_ADMISSIONS, identity
    from .qe_l2_continuous_admission import validate_projection
else:
    from qe_l2_relative_admission import ADMISSIONS as RELATIVE_ADMISSIONS, identity
    from qe_l2_continuous_admission import validate_projection

CONTRACT = "QE-L2-LOSS-NATURAL-ENTRY-V1"
VERSION = "qe_l2_loss_natural_entry_v1"
BASE_SOURCE_SHA = "1a3d00c2c6ed160ac6a1114f740259505b5cf140281a59c40679f3257d78c1bb"


def _record(node):
    value = deepcopy(dict(RELATIVE_ADMISSIONS[f"l2-relative-590-c1-20261030-{node}-v1"]))
    value["base_source_sha256"] = BASE_SOURCE_SHA
    return value


ADMISSIONS = MappingProxyType(
    {f"l2-natural-entry-590-c1-20261031-{node}-v1": _record(node) for node in ("rdagent-node1", "wsl2-5080")}
)


def validate_admission(m):
    if (
        not isinstance(m, dict)
        or m.get("contract") != CONTRACT
        or m.get("schema_version") != VERSION
        or m.get("arm") not in ("B0", "R-entry")
        or m.get("kappa") != 0.10
        or m.get("max_weight") != 0.05
        or "targets" in m.get("assets", {})
        or "target_source" in m
    ):
        raise ValueError("QE_L2_NATURAL_CONTRACT_INVALID")
    version = m.get("asset_admission_version")
    if version not in ADMISSIONS:
        raise ValueError("QE_L2_NATURAL_ADMISSION_UNKNOWN")
    expected = deepcopy(dict(ADMISSIONS[version]))
    af = expected.pop("AF_predictions_by_seed")
    projections = expected.pop("AF_projection_by_seed")
    if identity(m) != expected:
        raise ValueError("QE_L2_NATURAL_ADMISSION_MISMATCH")
    seed = m.get("seed")
    if type(seed) is not int or af.get(str(seed)) != m["assets"]["prediction"]["sha256"]:
        raise ValueError("QE_L2_NATURAL_AF_IDENTITY")
    if m.get("prediction_projection") != projections.get(str(seed)):
        raise ValueError("QE_L2_NATURAL_AF_PROJECTION")
    validate_projection(m["prediction_projection"], source_sha256=af[str(seed)])
    return version
