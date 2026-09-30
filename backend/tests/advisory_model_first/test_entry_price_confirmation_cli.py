import json

from backend.services.advisory_model_first import entry_price_confirmation_cli as cli
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError


def test_success_exit_code_does_not_promote_negative_result(monkeypatch, capsys, tmp_path):
    class Service:
        def evaluate(self, **kwargs):
            return {"stage": "CONSUMED", "evaluation": {"status": "NOT_CONFIRMED", "binding_activated": False}}
    monkeypatch.setattr(cli, "AdvisoryEntryPriceConfirmationService", Service)
    slot = tmp_path / "slot.json"
    slot.write_text("{}", encoding="utf-8")
    assert cli.main(["evaluate", "--request", "request.json", "--model-root", str(tmp_path), "--output-root", str(tmp_path), "--qe-exclusive-slot", str(slot)]) == 0
    assert json.loads(capsys.readouterr().out)["evaluation"]["status"] == "NOT_CONFIRMED"


def test_unready_input_and_invalid_contract_are_distinct(monkeypatch, capsys, tmp_path):
    env = tmp_path / "empty.env"
    env.touch()
    class Service:
        reason = "ADVISORY_PRICE_PROSPECTIVE_OUTCOME_NOT_MATURE"
        def settle(self, **kwargs):
            raise AdvisoryModelFirstError("test", reason_code=self.reason)
    monkeypatch.setattr(cli, "AdvisoryEntryPriceConfirmationService", Service)
    slot = tmp_path / "slot.json"
    slot.write_text("{}", encoding="utf-8")
    args = ["settle", "--request", "request.json", "--model-root", str(tmp_path), "--output-root", str(tmp_path), "--env-file", str(env), "--qe-exclusive-slot", str(slot)]
    assert cli.main(args) == 3
    assert json.loads(capsys.readouterr().out)["status"] == "WAITING"
    Service.reason = "ADVISORY_ENTRY_CONFIRMATION_IDENTITY_MISMATCH"
    assert cli.main(args) == 2
    assert json.loads(capsys.readouterr().out)["status"] == "ERROR"


def test_inspect_missing_request_does_not_create_output(capsys, tmp_path):
    output = tmp_path / "untouched"
    assert cli.main(["inspect", "--request", str(tmp_path / "missing.json"), "--model-root", str(output), "--output-root", str(output)]) == 2
    assert not output.exists()
    assert json.loads(capsys.readouterr().out)["reason_code"] == "ADVISORY_ENTRY_CONFIRMATION_INPUT_INVALID"
