"""Versioned QE loss admission; no DB, training or automatic latest binding."""
from copy import deepcopy
from types import MappingProxyType

if __package__:
    from .qe_l2_continuous_admission import ADMISSIONS as CONTINUOUS_ADMISSIONS, validate_projection
    from .qe_l2_risk_admission import canonical_sha, physical_binding
else:
    from qe_l2_continuous_admission import ADMISSIONS as CONTINUOUS_ADMISSIONS, validate_projection
    from qe_l2_risk_admission import canonical_sha, physical_binding

CONTRACT = "QE-L2-LOSS-RELATIVE-ALLOCATION-V1"
VERSION = "qe_l2_loss_relative_v1"
LOSS_SHA = "75250a22ea24aef564fc40c265744855122c9f95d244d9a03416e8025e9dd267"
MODELS = {
    "2026Q2": "6f9154f96002c2a016edbe9c45daa103a48259057a52e1f1b04fd7f5d0a973ee",
    "2026Q3": "8ab99fd1c2361d4f90a289718c8e169aa98edd860de7062c3e7264d092cc5bcc",
}
BASE_SOURCE_SHA = "20f1d21b0f179c5f7f909a73b3f3c4d374affbcca66b754624a8c9c8b47fe0bf"


def _record(node):
    # Exact reviewed historical admission, never the most recently registered one.
    old = CONTINUOUS_ADMISSIONS[f"l2-continuous-590-c1-20261028-{node}-v1"]
    value = deepcopy(dict(old))
    del value["M0_sha256"], value["M1_sha256"]
    value.update(loss_sha256=LOSS_SHA, models=dict(MODELS), base_source_sha256=BASE_SOURCE_SHA)
    return value


ADMISSIONS = MappingProxyType({
    f"l2-relative-590-c1-20261030-{node}-v1": _record(node)
    for node in ("rdagent-node1", "wsl2-5080")
})


def identity(m):
    return {
        "loss_sha256": m["assets"]["loss"]["sha256"],
        "models": m["models"],
        "base_contract_sha256": m["base_contract_sha256"],
        "base_source_sha256": m["base_source_sha256"],
        "dataset_binding_sha256": canonical_sha(physical_binding(m["dataset_binding"])),
        "release_identity_sha256": canonical_sha(m["release_identity"]),
        "authority_sha256": m["assets"]["authority"]["sha256"],
        "mapping_sha256": m["assets"]["mapping"]["sha256"],
        "mapping_receipt_sha256": m["assets"]["mapping_receipt"]["sha256"],
    }


def validate_admission(m):
    if (m.get("contract") != CONTRACT or m.get("schema_version") != VERSION
            or m.get("arm") not in ("BSTAR", "R") or m.get("kappa") != 0.10
            or m.get("max_weight") != 0.05):
        raise ValueError("QE_L2_RELATIVE_CONTRACT_INVALID")
    version = m.get("asset_admission_version")
    if version not in ADMISSIONS:
        raise ValueError("QE_L2_RELATIVE_ADMISSION_UNKNOWN")
    expected = deepcopy(dict(ADMISSIONS[version]))
    af = expected.pop("AF_predictions_by_seed")
    projections = expected.pop("AF_projection_by_seed")
    if identity(m) != expected:
        raise ValueError("QE_L2_RELATIVE_ADMISSION_MISMATCH")
    seed = m.get("seed")
    if type(seed) is not int or af.get(str(seed)) != m["assets"]["prediction"]["sha256"]:
        raise ValueError("QE_L2_RELATIVE_AF_IDENTITY")
    if m.get("prediction_projection") != projections.get(str(seed)):
        raise ValueError("QE_L2_RELATIVE_AF_PROJECTION")
    validate_projection(m["prediction_projection"], source_sha256=af[str(seed)])
    return version
