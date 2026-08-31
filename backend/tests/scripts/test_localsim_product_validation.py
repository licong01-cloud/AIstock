from argparse import Namespace

import pytest

from scripts import localsim_product_validation as subject


def _args(**overrides: object) -> Namespace:
    values = {
        "api_base": "http://127.0.0.1:8001/api/v1",
        "account_id": None,
        "replay_id": None,
        "expected_source_commit": "a" * 40,
        "timeout_seconds": 1.0,
    }
    values.update(overrides)
    return Namespace(**values)


def test_validation_is_read_only_and_uses_successor_product_surface(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, str]] = []

    def fake_get(self: subject.ApiClient, path: str) -> dict[str, object]:
        calls.append((self.base_url, path))
        if path == "/openapi.json":
            return {"paths": {name: {"get": {}} for name in subject.REQUIRED_PATHS}}
        if path.endswith("/cutover-readiness"):
            return {"readiness": {"schema_version": "localsim_cutover_readiness_v1", "ready": True}}
        if "/accounts/" in path:
            return {"schema_version": "localsim_account_detail_v1", "items": []}
        if "/replays/" in path:
            return {"schema_version": "localsim_replay_job_v1"}
        if path.startswith("/simulation-runtime/localsim/accounts"):
            return {"schema_version": "localsim_list_response_v1", "items": []}
        if path.startswith("/simulation-runtime/localsim/replays"):
            return {"schema_version": "localsim_list_response_v1", "items": []}
        if path.endswith("/scheduler/status"):
            return {"scheduler": {"scheduler_control_api_enabled": False}}
        return {"verification": {"status": "available"}}

    monkeypatch.setattr(subject.ApiClient, "get", fake_get)
    receipt = subject.validate(_args(account_id="simacct_1", replay_id="lsreplay_1"))

    assert receipt["ok"] is True
    assert receipt["read_only"] is True
    assert receipt["scheduler_control_api_enabled"] is False
    assert all(path == "/openapi.json" or path.startswith("/simulation-runtime/") for _, path in calls)
    assert not any("/paper-v2" in path for _, path in calls)


def test_validation_fails_closed_when_legacy_or_scheduler_mutation_path_remains(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_get(self: subject.ApiClient, path: str) -> dict[str, object]:
        del self
        if path == "/openapi.json":
            paths = {name: {"get": {}} for name in subject.REQUIRED_PATHS}
            paths["/api/v1/paper-v2/sessions"] = {"post": {}}
            return {"paths": paths}
        raise AssertionError("runtime reads must not start after OpenAPI rejection")

    monkeypatch.setattr(subject.ApiClient, "get", fake_get)
    with pytest.raises(subject.LocalSimValidationError, match="legacy or mutation"):
        subject.validate(_args())


def test_parser_has_no_mutating_switch() -> None:
    destinations = {action.dest for action in subject.parser()._actions}
    assert not {"apply", "create", "tick", "start", "stop"} & destinations
