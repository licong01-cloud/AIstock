from __future__ import annotations

import json

from backend.services.qmt_strategy_ledger.tca_models import canonical_json_sha256
from backend.services.qmt_strategy_ledger.tca_rebuild import TcaRebuildOutcome
from backend.services.qmt_strategy_ledger.tca_read_service import TcaReadRuntimeConfig
from scripts.rebuild_miniqmt_execution_tca import main


def test_manual_rebuild_cli_is_dry_run_by_default_and_execute_is_explicit(capsys) -> None:
    rebuild = _RebuildService()
    args = [
        "--binding-id",
        "binding-1",
        "--trade-date",
        "2026-07-10",
        "--account-id",
        "account-raw",
        "--as-of",
        "2026-07-10T15:05:00+08:00",
        "--code-commit",
        "a" * 40,
        "--operator-pseudonym",
        "operator_test_v1",
    ]

    dry_run = main(
        args,
        rebuild_service_factory=lambda: rebuild,
        config_provider=_config,
    )
    dry_payload = json.loads(capsys.readouterr().out)
    assert rebuild.requests == []
    executed = main(
        [*args, "--execute"],
        rebuild_service_factory=lambda: rebuild,
        config_provider=_config,
    )
    execute_payload = json.loads(capsys.readouterr().out)

    assert dry_run == 0
    assert dry_payload["dry_run"] is True
    assert dry_payload["environment"] == "SIM"
    assert "account-raw" not in json.dumps(dry_payload, sort_keys=True)
    assert executed == 0
    assert execute_payload["dry_run"] is False
    assert execute_payload["receipt_status"] == "COMPLETED"
    assert len(rebuild.requests) == 1
    request = rebuild.requests[0]
    assert request.scope.environment == "SIM"
    assert request.scope.binding_ids == ("binding-1",)
    assert request.account_pseudonyms["account-raw"].startswith("acct_test-v1_")


def _config() -> TcaReadRuntimeConfig:
    version = {
        "calculator_version": "calculator-v1",
        "formula_version": "formula-v1",
        "schema_version": "schema-v1",
        "query_version": "query-v1",
        "benchmark_policy_version": "benchmark-v1",
        "mark_policy_version": "mark-v1",
        "fee_policy_version": "fee-v1",
        "trade_provenance_policy_version": "trade-v1",
    }
    return TcaReadRuntimeConfig.from_environ(
        {
            "MINIQMT_TCA_ACTIVE_READ_VERSION": json.dumps(
                {**version, "config_sha256": canonical_json_sha256(version)}, sort_keys=True
            ),
            "AISTOCK_TCA_EXPORT_HMAC_KEY": "test-key",
            "AISTOCK_TCA_EXPORT_HMAC_KEY_VERSION": "test-v1",
        }
    )


class _RebuildService:
    def __init__(self) -> None:
        self.requests: list[object] = []

    def rebuild(self, request: object) -> TcaRebuildOutcome:
        self.requests.append(request)
        return TcaRebuildOutcome(
            receipt_id="receipt-1",
            receipt_status="COMPLETED",
            reused=False,
            receipt_generation=1,
            result_ids=("result-1",),
            canonical_input_sha256="a" * 64,
            canonical_output_sha256="b" * 64,
        )
