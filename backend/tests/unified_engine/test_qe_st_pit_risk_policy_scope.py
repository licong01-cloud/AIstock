from __future__ import annotations

import pytest
from pydantic import ValidationError

from backend.services.quantevolver.config_composer import ConfigComposer
from backend.services.quantevolver.qe_dataset_contract import QE_ST_PIT_UNIVERSE_KEY
from backend.services.selection_center.runtime_profile import RuntimeRiskPolicyProfile
from backend.services.stock_universe_pit_service import DEFAULT_ST_PIT_UNIVERSE_KEY


def _policy(universe_key: str) -> dict:
    return {
        "enabled": True,
        "policy_version": "stock_event_risk_policy_v1",
        "providers": ["st_pit"],
        "st_universe_key": universe_key,
        "hard_actions": ["block_buy", "force_exit"],
        "visible_time_mode": "next_trading_session",
        "strict_data_ready": True,
        "score_overlay": {"enabled": False},
    }


def test_live_risk_policy_default_still_rejects_qe_dataset_namespace() -> None:
    with pytest.raises(ValidationError, match="live Selection/Paper/simulation ST PIT"):
        RuntimeRiskPolicyProfile.model_validate(_policy(QE_ST_PIT_UNIVERSE_KEY))

    profile = RuntimeRiskPolicyProfile.model_validate(_policy(DEFAULT_ST_PIT_UNIVERSE_KEY))
    assert profile.st_universe_key == DEFAULT_ST_PIT_UNIVERSE_KEY


def test_qe_risk_policy_context_accepts_only_immutable_dataset_namespace() -> None:
    profile = RuntimeRiskPolicyProfile.model_validate(
        _policy(QE_ST_PIT_UNIVERSE_KEY),
        context={"st_pit_namespace_scope": "qe_immutable"},
    )
    assert profile.st_universe_key == QE_ST_PIT_UNIVERSE_KEY

    with pytest.raises(ValidationError, match="QE ST PIT must use an immutable dataset namespace"):
        RuntimeRiskPolicyProfile.model_validate(
            _policy(DEFAULT_ST_PIT_UNIVERSE_KEY),
            context={"st_pit_namespace_scope": "qe_immutable"},
        )


def test_config_composer_uses_explicit_qe_immutable_validation_scope() -> None:
    profile = ConfigComposer._risk_policy_profile(
        {"risk_policy": _policy(QE_ST_PIT_UNIVERSE_KEY)}
    )

    assert profile.enabled is True
    assert profile.st_universe_key == QE_ST_PIT_UNIVERSE_KEY
    assert ConfigComposer._is_qe_risk_policy_enabled(
        {"risk_policy": _policy(QE_ST_PIT_UNIVERSE_KEY)}
    ) is True
