"""Source-reviewed identities for optional QE continuous L2 studies."""

from copy import deepcopy
from types import MappingProxyType

if __package__:
    from .qe_l2_risk_admission import L2AdmissionError, canonical_sha, physical_binding
else:
    from qe_l2_risk_admission import L2AdmissionError, canonical_sha, physical_binding

CONTRACT = "QE-L2-CONTINUOUS-RISK-V1"
VERSION = "qe_l2_continuous_v1"
# Actual source-reviewed C1 AF and quarterly producer identities; research only.
# Source admission, API activation and economics are reported separately.
ADMISSIONS = MappingProxyType({'l2-continuous-590-c1-20261028-rdagent-node1-v1': {'AF_predictions_by_seed': {'2024': '9b87acc8f7be7e4a7f51766ba7d73897f41bb6576289cbf493111805baa8fab0',
                                                                               '2026': '29a9b34e8dc6168b37e4964f8cb0039892bcda09ac9660490b8060466f2f6b1c',
                                                                               '42': '215535ded832bda646be7a86ceec0a6685ac75b7d4903b504007581e808c9322'},
                                                    'AF_projection_by_seed': {'2024': {'end': '2026-09-30',
                                                                                       'panel_sha256': '2e8e76c4e679b7f30d6ad6d0304a5e265c01877e70fa6218be6fa9907c634219',
                                                                                       'schema_version': 'qe_l2_af_projection_v1',
                                                                                       'source_end': '2026-09-30',
                                                                                       'source_prediction_sha256': '9b87acc8f7be7e4a7f51766ba7d73897f41bb6576289cbf493111805baa8fab0',
                                                                                       'source_start': '2026-01-05',
                                                                                       'start': '2026-03-31'},
                                                                              '2026': {'end': '2026-09-30',
                                                                                       'panel_sha256': 'e5dc029948ffd081cc558d91d684ab9840956979126baa2632f18ea56396364f',
                                                                                       'schema_version': 'qe_l2_af_projection_v1',
                                                                                       'source_end': '2026-09-30',
                                                                                       'source_prediction_sha256': '29a9b34e8dc6168b37e4964f8cb0039892bcda09ac9660490b8060466f2f6b1c',
                                                                                       'source_start': '2026-01-05',
                                                                                       'start': '2026-03-31'},
                                                                              '42': {'end': '2026-09-30',
                                                                                     'panel_sha256': '147c36a9db29cdd5931a7701065d8d810b18e84a98b4bb03c52cafe433dd4e78',
                                                                                     'schema_version': 'qe_l2_af_projection_v1',
                                                                                     'source_end': '2026-09-30',
                                                                                     'source_prediction_sha256': '215535ded832bda646be7a86ceec0a6685ac75b7d4903b504007581e808c9322',
                                                                                     'source_start': '2026-01-05',
                                                                                     'start': '2026-03-31'}},
                                                    'M0_sha256': 'a8b58217469682ae1a6ad0aac141a516b81724eb71b5aa8f548e405204debbd8',
                                                    'M1_sha256': '16a92d69367d2f418b2bce1e7c0bf96218a09a0d0f6c2b4bf357c5a1fafe349c',
                                                    'authority_sha256': 'bf434c6e66f99b53b65373daac84ea88ee29a0c8794854bf096cd628aadb06a2',
                                                    'base_contract_sha256': '6f8253d81155492b0f6f13ceee0b4ff1ba1d6e119aec1b65e16f845986d0b8a9',
                                                    'dataset_binding_sha256': 'f33c238789d9c20380303e4f57135c91716997f1558d0238ab8895982d1f65e1',
                                                    'mapping_receipt_sha256': '04b502f983b538aa85f4da6fb08be99bb4264aa71b64e012cbc9efdc1622a539',
                                                    'mapping_sha256': 'b0d4fef1ddc65dbea05f9c6ed03cc5395d9b2343a731bbda3c7dd5d7e854fe42',
                                                    'models': {'2026Q2': '18f76cb27fab89f08283eb109c28f1c6da32fc9a1561cb8482dd987e6128ebd5',
                                                               '2026Q3': '02172914807f80913582a09931731949829ccc78345adbb88dd0aadfe1070a20',
                                                               'M0': '37259b5e9ca2c6eee2845cf0f1f02932a8cfd6cf21ad29080d6570d274b8038d'},
                                                    'release_identity_sha256': 'b321443a7567562844fd6ef51a9445ba64f11c1eaadc1f8e49bfd0e1a5d75879'},
 'l2-continuous-590-c1-20261028-wsl2-5080-v1': {'AF_predictions_by_seed': {'2024': '9b87acc8f7be7e4a7f51766ba7d73897f41bb6576289cbf493111805baa8fab0',
                                                                           '2026': '29a9b34e8dc6168b37e4964f8cb0039892bcda09ac9660490b8060466f2f6b1c',
                                                                           '42': '215535ded832bda646be7a86ceec0a6685ac75b7d4903b504007581e808c9322'},
                                                'AF_projection_by_seed': {'2024': {'end': '2026-09-30',
                                                                                   'panel_sha256': '2e8e76c4e679b7f30d6ad6d0304a5e265c01877e70fa6218be6fa9907c634219',
                                                                                   'schema_version': 'qe_l2_af_projection_v1',
                                                                                   'source_end': '2026-09-30',
                                                                                   'source_prediction_sha256': '9b87acc8f7be7e4a7f51766ba7d73897f41bb6576289cbf493111805baa8fab0',
                                                                                   'source_start': '2026-01-05',
                                                                                   'start': '2026-03-31'},
                                                                          '2026': {'end': '2026-09-30',
                                                                                   'panel_sha256': 'e5dc029948ffd081cc558d91d684ab9840956979126baa2632f18ea56396364f',
                                                                                   'schema_version': 'qe_l2_af_projection_v1',
                                                                                   'source_end': '2026-09-30',
                                                                                   'source_prediction_sha256': '29a9b34e8dc6168b37e4964f8cb0039892bcda09ac9660490b8060466f2f6b1c',
                                                                                   'source_start': '2026-01-05',
                                                                                   'start': '2026-03-31'},
                                                                          '42': {'end': '2026-09-30',
                                                                                 'panel_sha256': '147c36a9db29cdd5931a7701065d8d810b18e84a98b4bb03c52cafe433dd4e78',
                                                                                 'schema_version': 'qe_l2_af_projection_v1',
                                                                                 'source_end': '2026-09-30',
                                                                                 'source_prediction_sha256': '215535ded832bda646be7a86ceec0a6685ac75b7d4903b504007581e808c9322',
                                                                                 'source_start': '2026-01-05',
                                                                                 'start': '2026-03-31'}},
                                                'M0_sha256': 'a8b58217469682ae1a6ad0aac141a516b81724eb71b5aa8f548e405204debbd8',
                                                'M1_sha256': '16a92d69367d2f418b2bce1e7c0bf96218a09a0d0f6c2b4bf357c5a1fafe349c',
                                                'authority_sha256': 'bf434c6e66f99b53b65373daac84ea88ee29a0c8794854bf096cd628aadb06a2',
                                                'base_contract_sha256': '6f8253d81155492b0f6f13ceee0b4ff1ba1d6e119aec1b65e16f845986d0b8a9',
                                                'dataset_binding_sha256': '0c76e9583a99d2a624ec7fa877cfcad6ac5ab9fedaf53f5789024215e403620b',
                                                'mapping_receipt_sha256': '04b502f983b538aa85f4da6fb08be99bb4264aa71b64e012cbc9efdc1622a539',
                                                'mapping_sha256': 'b0d4fef1ddc65dbea05f9c6ed03cc5395d9b2343a731bbda3c7dd5d7e854fe42',
                                                'models': {'2026Q2': '18f76cb27fab89f08283eb109c28f1c6da32fc9a1561cb8482dd987e6128ebd5',
                                                           '2026Q3': '02172914807f80913582a09931731949829ccc78345adbb88dd0aadfe1070a20',
                                                           'M0': '37259b5e9ca2c6eee2845cf0f1f02932a8cfd6cf21ad29080d6570d274b8038d'},
                                                'release_identity_sha256': 'b321443a7567562844fd6ef51a9445ba64f11c1eaadc1f8e49bfd0e1a5d75879'}})


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
    projections = expected.pop("AF_projection_by_seed", {})
    if identity(manifest) != expected:
        raise L2AdmissionError("QE_L2_CONTINUOUS_ASSET_ADMISSION_MISMATCH", version)
    seed = manifest.get("seed")
    if type(seed) is not int or af.get(str(seed)) != manifest["assets"]["prediction"]["sha256"]:
        raise L2AdmissionError("QE_L2_CONTINUOUS_AF_IDENTITY_MISMATCH")
    projection = manifest.get("prediction_projection")
    if projection != projections.get(str(seed)):
        raise L2AdmissionError("QE_L2_CONTINUOUS_AF_PROJECTION_ADMISSION_MISMATCH")
    if projection is not None:
        validate_projection(projection, source_sha256=af[str(seed)])
    return version


def validate_projection(value, *, source_sha256):
    """A source-reviewed date projection; never an arbitrary runtime filter."""
    import re

    fields = {"schema_version", "source_prediction_sha256", "source_start", "source_end", "start", "end", "panel_sha256"}
    if (not isinstance(value, dict) or set(value) != fields
            or value["schema_version"] != "qe_l2_af_projection_v1"
            or value["source_prediction_sha256"] != source_sha256
            or (value["source_start"], value["source_end"]) != ("2026-01-05", "2026-09-30")
            or (value["start"], value["end"]) != ("2026-03-31", "2026-09-30")
            or not re.fullmatch(r"[0-9a-f]{64}", str(value["panel_sha256"]))):
        raise L2AdmissionError("QE_L2_CONTINUOUS_AF_PROJECTION_INVALID")
    return value


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
