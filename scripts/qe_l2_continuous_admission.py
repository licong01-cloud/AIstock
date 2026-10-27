"""Source-reviewed identities for optional QE continuous L2 studies."""

from copy import deepcopy
from types import MappingProxyType

if __package__:
    from .qe_l2_risk_admission import L2AdmissionError, canonical_sha, physical_binding
else:
    from qe_l2_risk_admission import L2AdmissionError, canonical_sha, physical_binding

CONTRACT = "QE-L2-CONTINUOUS-RISK-V1"
VERSION = "qe_l2_continuous_v1"
# Add only an actual matching producer delivery and the reviewed shared AF pins.
# No rolling delivery has been accepted by QE at source publication time.
ADMISSIONS = MappingProxyType({})


def identity(manifest):
    return {
        "M0_sha256": manifest["assets"]["M0"]["sha256"],
        "M1_sha256": manifest["assets"]["M1"]["sha256"],
        "models": deepcopy(manifest["models"]),
        "base_contract_sha256": manifest["base_contract_sha256"],
        "dataset_binding_sha256": canonical_sha(physical_binding(manifest["dataset_binding"])),
        "release_identity_sha256": canonical_sha(manifest["release_identity"]),
        "authority_sha256": manifest["assets"]["authority"]["sha256"],
        "mapping_sha256": manifest["assets"]["mapping"]["sha256"],
        "mapping_receipt_sha256": manifest["assets"]["mapping_receipt"]["sha256"],
    }


def validate_admission(manifest):
    if manifest.get("contract") != CONTRACT:
        raise L2AdmissionError("QE_L2_CONTINUOUS_CONTRACT_INVALID")
    version = manifest.get("asset_admission_version")
    if not isinstance(version, str) or version not in ADMISSIONS:
        raise L2AdmissionError("QE_L2_CONTINUOUS_ASSET_ADMISSION_UNKNOWN", str(version))
    expected = dict(ADMISSIONS[version])
    af = expected.pop("AF_predictions_by_seed")
    if identity(manifest) != expected:
        raise L2AdmissionError("QE_L2_CONTINUOUS_ASSET_ADMISSION_MISMATCH", version)
    seed = manifest.get("seed")
    if type(seed) is not int or af.get(str(seed)) != manifest["assets"]["prediction"]["sha256"]:
        raise L2AdmissionError("QE_L2_CONTINUOUS_AF_IDENTITY_MISMATCH")
    return version


def validate_reference(ref, manifest, as_of):
    def require(ok, code):
        if not ok:
            raise L2AdmissionError(code)

    def finite(value, label, *, maximum):
        import math

        require(
            type(value) in (int, float) and math.isfinite(value) and 0 <= value <= maximum,
            "QE_L2_CONTINUOUS_NUMBER_INVALID",
        )
        return float(value)

    expected_arm = "M0" if manifest["arm"] == "X0" else "M1"
    require(
        set(ref)
        == {
            "schema_version",
            "contract",
            "source_arm",
            "seed",
            "AF_prediction_sha256",
            "dataset_identity_sha256",
            "source_risk_sha256",
            "base_contract_sha256",
            "rows",
        },
        "QE_L2_CONTINUOUS_REFERENCE_FIELDS",
    )
    require(
        ref["schema_version"] == VERSION + "_exposure"
        and ref["contract"] == CONTRACT
        and ref["source_arm"] == expected_arm
        and ref["seed"] == manifest["seed"]
        and ref["AF_prediction_sha256"] == manifest["assets"]["prediction"]["sha256"]
        and ref["dataset_identity_sha256"] == canonical_sha(manifest["release_identity"])
        and ref["source_risk_sha256"] == manifest["assets"][expected_arm]["sha256"],
        "QE_L2_CONTINUOUS_REFERENCE_IDENTITY",
    )
    require(ref["base_contract_sha256"] == manifest["base_contract_sha256"], "QE_L2_CONTINUOUS_REFERENCE_BASE_CONTRACT")
    rows = {}
    for row in ref["rows"]:
        require(
            set(row) == {"trade_date", "as_of_date", "target_exposure"}
            and row["trade_date"] in as_of
            and row["as_of_date"] == as_of[row["trade_date"]]
            and row["trade_date"] not in rows,
            "QE_L2_CONTINUOUS_REFERENCE_CLOCK",
        )
        rows[row["trade_date"]] = finite(row["target_exposure"], "target_exposure", maximum=1)
    require(set(rows) == set(as_of), "QE_L2_CONTINUOUS_REFERENCE_GRID")
    return rows
