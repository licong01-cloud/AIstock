"""Independent reviewed member-loss admissions; never an incoming allowlist."""
from copy import deepcopy
from types import MappingProxyType

if __package__:
    from .qe_l2_risk_admission import canonical_sha, physical_binding
else:
    from qe_l2_risk_admission import canonical_sha, physical_binding

CONTRACT = "QE-L2-MEMBER-LOSS-NATURAL-ENTRY-V1"
VERSION = "qe_l2_member_loss_natural_entry_v1"
BASE_SOURCE_SHA = "1a3d00c2c6ed160ac6a1114f740259505b5cf140281a59c40679f3257d78c1bb"
TARGET_MANIFEST = "772112c58f959748787b939e33ec874fb0766ca921d88eddbd49c131c3b3c43d"
# No matching C/Q4 delivery has been admitted. Append only real reviewed pins;
# an empty registry rejects requests and does not signify an accepted asset.
ADMISSIONS = MappingProxyType({})


def identity(m):
    return {
        "dataset_binding_sha256": canonical_sha(physical_binding(m["dataset_binding"])),
        "release_identity_sha256": canonical_sha(m["release_identity"]),
        "risk_binding_sha256": canonical_sha(m["risk_binding"]),
        "base_source_sha256": m["base_source_sha256"],
        "base_contract_sha256": m["base_contract_sha256"],
        "authority_canonical_sha256": m["authority_canonical_sha256"],
        "asset_sha256": {k: v["sha256"] for k, v in m["assets"].items() if k not in ("prediction", "AF_receipt")},
    }


def validate_projection(value, source_sha256):
    import re

    fields = {"schema_version", "source_prediction_sha256", "source_start", "source_end", "start", "end", "panel_sha256"}
    if (
        not isinstance(value, dict) or set(value) != fields
        or value["schema_version"] != "qe_l2_member_AF_projection_v1"
        or value["source_prediction_sha256"] != source_sha256
        or (value["source_start"], value["source_end"]) != ("2026-01-05", "2026-10-30")
        or (value["start"], value["end"]) != ("2026-03-31", "2026-10-30")
        or not re.fullmatch(r"[0-9a-f]{64}", str(value["panel_sha256"]))
    ):
        raise ValueError("QE_L2_MEMBER_AF_PROJECTION")
    return value


def validate_admission(m):
    if (
        not isinstance(m, dict) or m.get("contract") != CONTRACT or m.get("schema_version") != VERSION
        or m.get("arm") not in ("B0", "C") or type(m.get("seed")) is not int or m["seed"] not in (42, 2024, 2026)
        or m.get("kappa") != 0.10 or m.get("max_weight") != 0.05
        or (m.get("start"), m.get("end"), m.get("decision_days"), m.get("signal_end"), m.get("signal_days"))
        != ("2026-04-01", "2026-10-29", 141, "2026-10-30", 142)
        or m.get("base_source_sha256") != BASE_SOURCE_SHA
        or m.get("release_identity", {}).get("dataset_manifest_sha256") != TARGET_MANIFEST
        or "target_source" in m
        or set(m.get("assets", {})) != {"loss", "prediction", "AF_receipt", "mapping", "mapping_receipt", "authority", "calendar", "pit"}
    ):
        raise ValueError("QE_L2_MEMBER_CONTRACT")
    version = m.get("asset_admission_version")
    if not isinstance(version, str) or version not in ADMISSIONS:
        raise ValueError("QE_L2_MEMBER_ADMISSION_UNKNOWN")
    expected = deepcopy(dict(ADMISSIONS[version]))
    seeds = expected.pop("AF_by_seed")
    if identity(m) != expected:
        raise ValueError("QE_L2_MEMBER_ADMISSION_MISMATCH")
    seed_record = seeds.get(str(m["seed"]))
    observed = {"prediction_sha256": m["assets"]["prediction"]["sha256"],
                "receipt_sha256": m["assets"]["AF_receipt"]["sha256"], "projection": m["prediction_projection"]}
    if observed != seed_record:
        raise ValueError("QE_L2_MEMBER_AF_ADMISSION")
    validate_projection(m["prediction_projection"], observed["prediction_sha256"])
    return version
