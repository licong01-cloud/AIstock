import json

import pytest

from backend.routers import quantevolver as qt


class _UpdateCursor:
    def __init__(self):
        self.executed = []
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        if "UPDATE qe_experiments" in sql:
            self.rowcount = 1


class _UpdateConn:
    def __init__(self, cursor):
        self.cursor_obj = cursor
        self.committed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self, *args, **kwargs):
        return self.cursor_obj

    def commit(self):
        self.committed = True


def test_single_pending_create_rejects_multi_alpha_payload():
    req = qt.SingleExperimentPendingCreateRequest(
        factor_names=["alpha_a"],
        alpha_mode="multi",
        multi_alpha_config={"alpha_groups": []},
        custom_params={"random_seed": 42},
    )

    with pytest.raises(qt.HTTPException) as exc:
        qt.create_pending_experiment(req)

    assert exc.value.status_code == 400
    assert "only supports single" in str(exc.value.detail)


def test_single_pending_edit_preserves_mcp_provenance_and_factor_sources(monkeypatch):
    existing = {
        "experiment_id": "exp_pending",
        "experiment_name": "old",
        "status": "created",
        "alpha_mode": "single",
        "factor_names": json.dumps(["old_alpha"]),
        "model_id": "old_model",
        "strategy_id": "old_strategy",
        "data_split": json.dumps({"train_start": "2020-01-01"}),
        "custom_params": json.dumps(
            {
                "qe_mcp_provenance": {"created_by_name": "Claude Code"},
                "qe_factor_sources": {"old_alpha": "legacy_source"},
                "random_seed": 1,
            }
        ),
    }
    updated = {**existing, "factor_names": json.dumps(["new_alpha"])}

    class FakeComposer:
        def __init__(self):
            self.calls = 0

        def _get_experiment_record(self, experiment_id):
            self.calls += 1
            return existing if self.calls == 1 else updated

    cursor = _UpdateCursor()
    conn = _UpdateConn(cursor)
    monkeypatch.setattr(qt, "get_conn", lambda: conn)
    monkeypatch.setattr(qt, "_validate_qe_catalog_refs", lambda strategy_id, model_id: None)
    monkeypatch.setattr(
        qt,
        "_normalize_single_experiment_custom_params",
        lambda req, source: {"random_seed": req.custom_params["random_seed"]},
    )
    monkeypatch.setattr("backend.services.quantevolver.config_composer.ConfigComposer", FakeComposer)

    req = qt.SingleExperimentConfigUpdateRequest(
        factor_names=["new_alpha"],
        model_id="new_model",
        strategy_id="new_strategy",
        data_split={"train_start": "2021-01-01"},
        custom_params={"random_seed": 42},
    )

    result = qt.update_experiment_editable_config("exp_pending", req)

    update_params = next(params for sql, params in cursor.executed if "UPDATE qe_experiments" in sql)
    saved_custom_params = json.loads(update_params[5])
    assert conn.committed is True
    assert result["operation"] == "update_pending_config"
    assert saved_custom_params["random_seed"] == 42
    assert saved_custom_params["qe_mcp_provenance"] == {"created_by_name": "Claude Code"}
    assert saved_custom_params["qe_factor_sources"] == {"old_alpha": "legacy_source"}
