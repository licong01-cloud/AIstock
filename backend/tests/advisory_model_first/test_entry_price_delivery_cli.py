import json

import pytest

from backend.services.advisory_model_first import entry_price_delivery_cli as cli


def test_inspect_no_config_is_readonly_and_not_error(tmp_path, capsys):
    root = tmp_path / "models"
    assert cli.main(["inspect", "--model-root", str(root), "--program-id", "advp_test", "--binding-version-id", "advb_test"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert not payload["configured"] and not payload["enabled"] and not root.exists()
    assert not payload["database_written"] and not payload["backend_restarted"]


def test_apply_requires_explicit_cas_and_authorization_arguments():
    with pytest.raises(SystemExit) as result:
        cli.build_parser().parse_args(["apply-binding", "--model-root", "/model", "--program-id", "advp_test", "--binding-version-id", "advb_test", "--env-file", "dev.env", "--spec", "request.json"])
    assert result.value.code == 2


def test_cli_does_not_treat_none_as_a_rollback_target(tmp_path, capsys):
    env = tmp_path / "empty.env"
    env.touch()
    code = cli.main(["rollback-binding", "--model-root", str(tmp_path), "--program-id", "advp_test", "--binding-version-id", "advb_test",
                     "--env-file", str(env), "--expected-current-hash", "a" * 64, "--authorization-ref", "fixture", "--target-role-hash", "NONE"])
    assert code == 2
    assert json.loads(capsys.readouterr().out)["reason_code"] == "ADVISORY_ENTRY_ROLE_INPUT_INVALID"
