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
SAVED_AF_MODE = "saved_asset_version_bridge_v1"
STRICT_AF_MODE = "strict_frozen_inference_v1"
# Reviewed saved AF/C file assets for this fixed research contract only.
# Original inference integrity stays UNKNOWN in the separately pinned evidence.
ADMISSIONS = MappingProxyType({'member-loss-772-saved-af-20261107-rdagent-node1-v1': {'AF_by_seed': {'2024': {'prediction_sha256': '6e162610352519b50c8c1dbf65d7d784aa140aa3683e03a4d02bdaf8b6778755',
                                                                                'projection': {'end': '2026-10-30',
                                                                                               'panel_sha256': 'ae7e5f4eaf1f5394adc8acde15e17f0fca3f972d3a33d45c1bd87e13cddc7fc9',
                                                                                               'schema_version': 'qe_l2_member_AF_projection_v1',
                                                                                               'source_end': '2026-10-30',
                                                                                               'source_prediction_sha256': '6e162610352519b50c8c1dbf65d7d784aa140aa3683e03a4d02bdaf8b6778755',
                                                                                               'source_start': '2026-01-05',
                                                                                               'start': '2026-03-31'},
                                                                                'receipt_sha256': 'dd68982daa2487788b70de22ffb6472eae075d92ca10006e5068c975c5a999bc'},
                                                                       '2026': {'prediction_sha256': '06e6b26dae1569af83f3093dc79cd023b950fc65879e9c748bf2df1bf4ea4e11',
                                                                                'projection': {'end': '2026-10-30',
                                                                                               'panel_sha256': '6cb36a14fbad306ef5f407286c03eefda3d14355d772f77935cefaa7f724ad97',
                                                                                               'schema_version': 'qe_l2_member_AF_projection_v1',
                                                                                               'source_end': '2026-10-30',
                                                                                               'source_prediction_sha256': '06e6b26dae1569af83f3093dc79cd023b950fc65879e9c748bf2df1bf4ea4e11',
                                                                                               'source_start': '2026-01-05',
                                                                                               'start': '2026-03-31'},
                                                                                'receipt_sha256': '0bf48cd3dfbf9860ee30f52463b9f0365e0f93412877e74244fc63dd6b22fcfd'},
                                                                       '42': {'prediction_sha256': 'd6ade07c58d4a412d67521d4d1451473f2373a17c6beb2e3f86ca922bfb0c1de',
                                                                              'projection': {'end': '2026-10-30',
                                                                                             'panel_sha256': '62ca0128fe76c63636f720126861a86b80868aa38143fb07c9b43fa9a251561f',
                                                                                             'schema_version': 'qe_l2_member_AF_projection_v1',
                                                                                             'source_end': '2026-10-30',
                                                                                             'source_prediction_sha256': 'd6ade07c58d4a412d67521d4d1451473f2373a17c6beb2e3f86ca922bfb0c1de',
                                                                                             'source_start': '2026-01-05',
                                                                                             'start': '2026-03-31'},
                                                                              'receipt_sha256': '7d2ac32c50705fa0ae6654d5a66ce17fe9fe6e60c3211b318f16db784b55a7aa'}},
                                                        'af_evidence_mode': 'saved_asset_version_bridge_v1',
                                                        'asset_sha256': {'authority': 'd9aa2aec1af7db0f958643e2b1b4bd4b97bc7d699c77b0c768e13bbadaba43b3',
                                                                         'calendar': '7ea1bb6e038cce8ff792e6ecbc13a7dd1a1168fc5ca23fc24c995bff195a08ed',
                                                                         'loss': '955508b7d9da4a63b434389f68ac65dae2eb36891e9bfd40d08a6b7fa6861be2',
                                                                         'mapping': 'ae4b5d0d1bfcc818dce3e3ac4461aea24beea2082381fc6a37448c25f83fbbcb',
                                                                         'mapping_receipt': 'ff0d7702357c93cbd8c8551ef0245e24da7470c8a997cf8442b9cfa12318a156',
                                                                         'pit': 'f242acb266d585306e63533c52c478d743b9526c2405bd7e1180f5a784531af5'},
                                                        'authority_canonical_sha256': '505ede3b457014b0b125cad557b1404fd1d8237b96481494a55c5c15895247de',
                                                        'base_contract_sha256': '6f8253d81155492b0f6f13ceee0b4ff1ba1d6e119aec1b65e16f845986d0b8a9',
                                                        'base_source_sha256': '1a3d00c2c6ed160ac6a1114f740259505b5cf140281a59c40679f3257d78c1bb',
                                                        'dataset_binding_sha256': 'ea40120300dd3fe1a638fe2d897a10fc7a5afd3a1b1e9e6abebbefd360c9e8a7',
                                                        'release_identity_sha256': '4038a37f5fdca18adc50c6e6f381c19ff9d02104a5804b58b6854c9776e466ed',
                                                        'risk_binding_sha256': 'bcbad2e834314d1d34e4a5d6ca02e0305fd3c0dc54b7acb968272f50172c1963'},
 'member-loss-772-saved-af-20261107-wsl2-5080-v1': {'AF_by_seed': {'2024': {'prediction_sha256': '6e162610352519b50c8c1dbf65d7d784aa140aa3683e03a4d02bdaf8b6778755',
                                                                            'projection': {'end': '2026-10-30',
                                                                                           'panel_sha256': 'ae7e5f4eaf1f5394adc8acde15e17f0fca3f972d3a33d45c1bd87e13cddc7fc9',
                                                                                           'schema_version': 'qe_l2_member_AF_projection_v1',
                                                                                           'source_end': '2026-10-30',
                                                                                           'source_prediction_sha256': '6e162610352519b50c8c1dbf65d7d784aa140aa3683e03a4d02bdaf8b6778755',
                                                                                           'source_start': '2026-01-05',
                                                                                           'start': '2026-03-31'},
                                                                            'receipt_sha256': 'dd68982daa2487788b70de22ffb6472eae075d92ca10006e5068c975c5a999bc'},
                                                                   '2026': {'prediction_sha256': '06e6b26dae1569af83f3093dc79cd023b950fc65879e9c748bf2df1bf4ea4e11',
                                                                            'projection': {'end': '2026-10-30',
                                                                                           'panel_sha256': '6cb36a14fbad306ef5f407286c03eefda3d14355d772f77935cefaa7f724ad97',
                                                                                           'schema_version': 'qe_l2_member_AF_projection_v1',
                                                                                           'source_end': '2026-10-30',
                                                                                           'source_prediction_sha256': '06e6b26dae1569af83f3093dc79cd023b950fc65879e9c748bf2df1bf4ea4e11',
                                                                                           'source_start': '2026-01-05',
                                                                                           'start': '2026-03-31'},
                                                                            'receipt_sha256': '0bf48cd3dfbf9860ee30f52463b9f0365e0f93412877e74244fc63dd6b22fcfd'},
                                                                   '42': {'prediction_sha256': 'd6ade07c58d4a412d67521d4d1451473f2373a17c6beb2e3f86ca922bfb0c1de',
                                                                          'projection': {'end': '2026-10-30',
                                                                                         'panel_sha256': '62ca0128fe76c63636f720126861a86b80868aa38143fb07c9b43fa9a251561f',
                                                                                         'schema_version': 'qe_l2_member_AF_projection_v1',
                                                                                         'source_end': '2026-10-30',
                                                                                         'source_prediction_sha256': 'd6ade07c58d4a412d67521d4d1451473f2373a17c6beb2e3f86ca922bfb0c1de',
                                                                                         'source_start': '2026-01-05',
                                                                                         'start': '2026-03-31'},
                                                                          'receipt_sha256': '7d2ac32c50705fa0ae6654d5a66ce17fe9fe6e60c3211b318f16db784b55a7aa'}},
                                                    'af_evidence_mode': 'saved_asset_version_bridge_v1',
                                                    'asset_sha256': {'authority': 'd9aa2aec1af7db0f958643e2b1b4bd4b97bc7d699c77b0c768e13bbadaba43b3',
                                                                     'calendar': '7ea1bb6e038cce8ff792e6ecbc13a7dd1a1168fc5ca23fc24c995bff195a08ed',
                                                                     'loss': '955508b7d9da4a63b434389f68ac65dae2eb36891e9bfd40d08a6b7fa6861be2',
                                                                     'mapping': 'ae4b5d0d1bfcc818dce3e3ac4461aea24beea2082381fc6a37448c25f83fbbcb',
                                                                     'mapping_receipt': 'ff0d7702357c93cbd8c8551ef0245e24da7470c8a997cf8442b9cfa12318a156',
                                                                     'pit': 'f242acb266d585306e63533c52c478d743b9526c2405bd7e1180f5a784531af5'},
                                                    'authority_canonical_sha256': '505ede3b457014b0b125cad557b1404fd1d8237b96481494a55c5c15895247de',
                                                    'base_contract_sha256': '6f8253d81155492b0f6f13ceee0b4ff1ba1d6e119aec1b65e16f845986d0b8a9',
                                                    'base_source_sha256': '1a3d00c2c6ed160ac6a1114f740259505b5cf140281a59c40679f3257d78c1bb',
                                                    'dataset_binding_sha256': 'db38443b80ccfec71ff26c59dd8785ff9d8176cebd1abdb98eb8388bc790f2c7',
                                                    'release_identity_sha256': '4038a37f5fdca18adc50c6e6f381c19ff9d02104a5804b58b6854c9776e466ed',
                                                    'risk_binding_sha256': 'bcbad2e834314d1d34e4a5d6ca02e0305fd3c0dc54b7acb968272f50172c1963'}})


def identity(m):
    result = {
        "dataset_binding_sha256": canonical_sha(physical_binding(m["dataset_binding"])),
        "release_identity_sha256": canonical_sha(m["release_identity"]),
        "risk_binding_sha256": canonical_sha(m["risk_binding"]),
        "base_source_sha256": m["base_source_sha256"],
        "base_contract_sha256": m["base_contract_sha256"],
        "authority_canonical_sha256": m["authority_canonical_sha256"],
        "asset_sha256": {k: v["sha256"] for k, v in m["assets"].items() if k not in ("prediction", "AF_receipt")},
    }
    # The selected evidence mode is part of the immutable admission identity.
    # Absent means the existing strict path, never schema-based auto-detection.
    if "af_evidence_mode" in m:
        result["af_evidence_mode"] = m["af_evidence_mode"]
    return result


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
        or m.get("af_evidence_mode", STRICT_AF_MODE) not in (STRICT_AF_MODE, SAVED_AF_MODE)
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
