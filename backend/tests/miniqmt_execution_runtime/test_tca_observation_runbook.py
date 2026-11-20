from __future__ import annotations

from pathlib import Path


def test_tca_observation_runbook_separates_code_config_activation_and_evidence() -> None:
    runbook = Path(__file__).resolve().parents[3] / "docs" / "runbooks" / "miniqmt_phase0a_tca_observation.md"
    content = runbook.read_text(encoding="utf-8")

    for required in (
        "## 1. Code merged is not activation",
        "## 2. Configuration persistence",
        "## 3. Activation requires explicit authorization",
        "## 4. Evidence collection",
        "## 6. Rollback and Monday B0 verification",
        "MINIQMT_TCA_EOD_OBSERVATION_ENABLED=false",
        "A real prospective SIM receipt is required",
        "Monday's B0 SIM verification is separate from TCA activation",
        "The user alone performs service restarts.",
    ):
        assert required in content
