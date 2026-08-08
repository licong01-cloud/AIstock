from __future__ import annotations

from typing import Final

from backend.services.strategy_package.runtime_variant import canonical_json_sha256

PACKAGE_ID: Final = "pkg_ma_8ec5e389fa2c5e484a1ac7e9"
MANIFEST_SHA256: Final = "f5b008d09fa1c36a1f3604333dee62fa66ba3c692fa07239b57e5690debb6016"
PROGRAM_ID: Final = "advp_3126dd77f9774d94850f37ad012f640f"
BINDING_VERSION_ID: Final = "advb_f860140caa314665ad60ac089ed84b3f"
STYLE_PROFILE_ID: Final = "short_rebound_pkg_ma_8ec5e389_v1"
EFFECTIVE_PACKAGE_OOS_CUTOFF: Final = "2026-07-20"
STYLE_PROFILE_PAYLOAD: Final = {
    "schema_version": "advisory_style_profile_v1",
    "style_profile_id": STYLE_PROFILE_ID,
    "package_id": PACKAGE_ID,
    "manifest_sha256": MANIFEST_SHA256,
    "style": "SHORT_REBOUND",
    "effective_package_oos_cutoff": EFFECTIVE_PACKAGE_OOS_CUTOFF,
}
STYLE_PROFILE_HASH: Final = canonical_json_sha256(STYLE_PROFILE_PAYLOAD)

LSTM_LEG_ID: Final = "a1_plus3_LSTM_h20"
FUND_LEG_ID: Final = "new_FUNDGROWTH_h20"
LEG_IDS: Final = (LSTM_LEG_ID, FUND_LEG_ID)

LSTM_SEED_RUN_IDS: Final = (
    "qear_run_fc5d506390b8f70651a790e6",
    "qear_run_9a1defd5a1e2257a7255b78d",
    "qear_run_a897779cf0a3c30e41a37efc",
    "qear_run_2ab298df84dcf5a024dc6bd5",
    "qear_run_21afc18b61dddd3a53f2fdac",
    "qear_run_1099a628e0322a96d46faf93",
    "qear_run_4ebb5ff58e47f5065bb82829",
    "qear_run_a8ae1bdd6146ea632d3dcae7",
    "qear_run_eaf48dfbe26a95bf58a4bb3b",
    "qear_run_9487b5f53ff0913f5f09bf47",
    "qear_run_6687739da093e3284a26e306",
    "qear_run_c478082a3afbb4a1d98b8865",
    "qear_run_b7323fab9f5255541a025982",
    "qear_run_59126620bc7dcf0b175c2071",
    "qear_run_f485b00a928d3b70b7360f19",
    "qear_run_eb749aa2cd9c5e221148830d",
    "qear_run_a34f16978092d3ecedf05b2b",
    "qear_run_6b24a283dc4ff3bd6e9d68d1",
    "qear_run_433114f81a72801452c926a0",
    "qear_run_eda52a9488df4aa8553e634b",
    "qear_run_e573090215b24806664ebde8",
    "qear_run_b524652839421fb0d80d72a4",
    "qear_run_25c421dd28515128f9d89486",
    "qear_run_dcf9b2f0bca2979fb3f92acf",
    "qear_run_150bfa41e8a6a2d1664b3b07",
    "qear_run_a5e4e0c0e5caa938e93df68d",
    "qear_run_bdfd65618510eeb5d940f205",
    "qear_run_7d96269f3e2fba256ab904a3",
    "qear_run_16bbe11ea1794c462ccab2b3",
    "qear_run_adfecf69242b697021d6d56d",
    "qear_run_3ed05fccaa8a6cd0062e0d7a",
    "qear_run_e262916978fa9f7844422584",
    "qear_run_880feffe3961738b775f4573",
)
FUND_SEED_RUN_IDS: Final = (
    "qe_20260622_035058_ec76_L5",
    "qe_20260622_035058_ec76_L6",
    "qe_20260622_171346_0e41_L1",
    "qe_20260622_171346_0e41_L2",
    "qe_20260622_171346_0e41_L3",
)
FULL_SEED_ROSTER: Final = {
    LSTM_LEG_ID: LSTM_SEED_RUN_IDS,
    FUND_LEG_ID: FUND_SEED_RUN_IDS,
}
REPRESENTATIVE_SEED_RUN_IDS: Final = {
    LSTM_LEG_ID: LSTM_SEED_RUN_IDS[0],
    FUND_LEG_ID: FUND_SEED_RUN_IDS[0],
}
REPRESENTATIVE_MODEL_ASSET_SHA256: Final = {
    LSTM_LEG_ID: "9c65fe85fa1e3e31a544c2f59608f6c295de9b7943c4b04aadef3fc34aac87fc",
    FUND_LEG_ID: "5f255fc454f02ace754c7c6bfbd8362b37696f18eb1729a488f942ebe0396620",
}
TERMINAL_WEIGHTS: Final = {
    LSTM_LEG_ID: 0.6966591521,
    FUND_LEG_ID: 0.3033408479,
}

RUNTIME_SEMANTICS_ID: Final = "advisory_multi_alpha_representative_terminal_top25_to20_v1"
RUNTIME_SEMANTICS_PAYLOAD: Final = {
    "decision_clock_version": "advisory_previous_close_target_next_trade_v1",
    "hmm_enabled": False,
    "industry_blacklist": [],
    "normalization_method": "zscore",
    "provider_version": "multi_alpha_live_selection_provider_v3",
    "raw_top_k": 25,
    "representative_seed_run_ids": REPRESENTATIVE_SEED_RUN_IDS,
    "risk_policy_enabled": False,
    "schema_version": "advisory_selection_runtime_semantics_v1",
    "target_count": 20,
    "target_day_suspend_filter": False,
    "terminal_weights": TERMINAL_WEIGHTS,
    "weight_policy_mode": "frozen_backtest_terminal_weights",
}
RUNTIME_SEMANTICS_HASH: Final = canonical_json_sha256(RUNTIME_SEMANTICS_PAYLOAD)
EXPECTED_RUNTIME_SEMANTICS_HASH: Final = "83fc0475964df75a9a23db597567af5bf31543f6980170f9d924c650ea3eb692"
if RUNTIME_SEMANTICS_HASH != EXPECTED_RUNTIME_SEMANTICS_HASH:
    raise RuntimeError(
        "model-first runtime semantics hash drift: "
        f"expected={EXPECTED_RUNTIME_SEMANTICS_HASH} actual={RUNTIME_SEMANTICS_HASH}"
    )
