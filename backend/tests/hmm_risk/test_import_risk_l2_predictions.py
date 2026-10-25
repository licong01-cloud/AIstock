import pytest

from scripts.hmm_risk import import_risk_l2_predictions as cli
from backend.services.hmm_risk.risk_l2_prediction import RiskL2PredictionError, REASON_INPUT


def test_write_requires_explicit_target_before_asset_or_db_access(monkeypatch):
    monkeypatch.setattr(cli, "load_product", lambda *_a: pytest.fail("must parse explicit write target first"))
    with pytest.raises(SystemExit) as exc:
        cli.main(["--request", "missing.json", "--mode", "write"])
    assert exc.value.code == 2


def test_cli_invalid_evidence_is_typed_failure_no_default_database(monkeypatch, capsys):
    def fail(*_a):
        raise RiskL2PredictionError(REASON_INPUT, "invalid pinned evidence")

    monkeypatch.setattr(cli, "load_product", fail)
    monkeypatch.setattr(cli, "target_connection", lambda *_a: pytest.fail("DB forbidden"))
    assert cli.main(["--request", "missing.json", "--mode", "validate"]) == 1
    assert '"status": "FAILED"' in capsys.readouterr().out
