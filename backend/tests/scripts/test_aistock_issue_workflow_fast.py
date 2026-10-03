from __future__ import annotations

import argparse
import hashlib
import io
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import scripts.aistock_issue_workflow as workflow
from scripts.aistock_bug_id_allocator import compact_terminal_reservation


@pytest.mark.parametrize("executable", ["gh", "C:/tools/gh.exe"])
@pytest.mark.parametrize("existing", ["localhost", "api.github.com", "*"])
def test_gh_environment_bypasses_only_api_and_preserves_parent(monkeypatch, executable, existing):
    parent = {"NO_PROXY": existing, "no_proxy": "127.0.0.1", "HTTPS_PROXY": "http://127.0.0.1:7896"}
    monkeypatch.setattr(workflow.os, "environ", parent)
    env = workflow._subprocess_env([executable, "api", "graphql"])
    assert env is not None and env is not parent
    assert env["NO_PROXY"] == env["no_proxy"]
    assert set(env["NO_PROXY"].split(",")) == {existing, "127.0.0.1", "api.github.com"}
    assert env["HTTPS_PROXY"] == parent["HTTPS_PROXY"]
    assert parent == {"NO_PROXY": existing, "no_proxy": "127.0.0.1", "HTTPS_PROXY": "http://127.0.0.1:7896"}


@pytest.mark.parametrize("args", [[], ["python"], ["curl.exe"], ["not-gh"]])
def test_non_gh_commands_keep_inherited_network_environment(args):
    assert workflow._subprocess_env(args) is None


@pytest.mark.parametrize("command", ["promote-ci-issue", "ci-issue-janitor"])
def test_metadata_cli_starts_without_process_dependency(command):
    import sys

    code = (
        "import importlib.abc, runpy, sys\n"
        "class NoPsutil(importlib.abc.MetaPathFinder):\n"
        "    def find_spec(self, fullname, path=None, target=None):\n"
        "        if fullname == 'psutil':\n"
        "            raise ModuleNotFoundError('No psutil in metadata runner', name='psutil')\n"
        "sys.meta_path.insert(0, NoPsutil())\n"
        f"sys.argv = ['aistock_issue_workflow.py', {command!r}, '--help']\n"
        "runpy.run_path('scripts/aistock_issue_workflow.py', run_name='__main__')\n"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stdout + result.stderr
    assert command in result.stdout


def test_process_probe_fails_closed_without_prebuilt_psutil(monkeypatch):
    monkeypatch.setattr(workflow, "psutil", None)
    with pytest.raises(workflow.WorkflowError, match="requires the prebuilt psutil"):
        workflow._monthly_release_worker_process_snapshot({})


def _monthly_ready_payload() -> dict[str, Any]:
    from backend.services.dataset_release.monthly_unified import STAGES
    return {
        "schema_version": "aistock_monthly_release_status_v1",
        "data": {
            "schema_version": "aistock_monthly_release_state_v1",
            "operation_id": "dmr_" + "a" * 32,
            "status": "READY_TO_ACTIVATE", "attempt": 1,
            "plan_sha256": "b" * 64, "ready_receipt_sha256": "c" * 64,
            "cancel_requested": False, "last_error": None, "current_stage": None,
            "checkpoints": dict.fromkeys(STAGES, True),
        },
    }


def _monthly_verdict(payload):
    return workflow._evaluate_business_smoke_semantics(
        "http://127.0.0.1:8001/api/v1/qlib/monthly-releases/dmr_" + "a" * 32,
        json.dumps(payload), response_sha256="d" * 64,
    )[1]


def test_monthly_ready_probe_requires_operation_bound_success() -> None:
    verdict = _monthly_verdict(_monthly_ready_payload())
    assert verdict["contract_id"] == "monthly_release_ready"
    assert verdict["verdict"] == "passed"


@pytest.mark.parametrize(("key", "value"), [
    ("operation_id", "dmr_" + "f" * 32), ("status", "FAILED"),
    ("status", "SOURCE_READY"), ("status", "ACTIVATED_VERIFY_FAILED"),
    ("status", []),
    ("cancel_requested", True), ("cancel_requested", 0), ("attempt", True),
    ("ready_receipt_sha256", ""), ("plan_sha256", "invalid"),
    ("current_stage", "SOURCE"), ("last_error", {"code": "SOURCE_INCOMPLETE"}),
])
def test_monthly_probe_rejects_cross_operation_or_unready_state(key, value) -> None:
    payload = _monthly_ready_payload()
    payload["data"][key] = value
    assert _monthly_verdict(payload)["verdict"] == "failed"


@pytest.mark.parametrize("case", ["missing", "non_bool", "extra", "failed", "outer_schema", "state_schema"])
def test_monthly_probe_checkpoint_and_schema_closure(case) -> None:
    payload = _monthly_ready_payload()
    points = payload["data"]["checkpoints"]
    if case == "missing":
        del points["SOURCE"]
    elif case == "non_bool":
        points["SOURCE"] = 1
    elif case == "extra":
        points["UNREVIEWED"] = True
    elif case == "failed":
        points["SOURCE"] = False
    elif case == "outer_schema":
        payload["schema_version"] = "other"
    else:
        payload["data"]["schema_version"] = "other"
    assert _monthly_verdict(payload)["verdict"] == "failed"


def test_monthly_read_only_probe_uses_scoped_operator_file(monkeypatch, tmp_path) -> None:
    token = hashlib.sha256(b"monthly-probe-credential-fixture").hexdigest()
    secret_file = tmp_path / "operator.token"
    secret_file.write_text(token, encoding="utf-8")
    monkeypatch.setenv("DATASET_RELEASE_OPERATOR_TOKEN_FILE", str(secret_file))
    captured = []

    def open_probe(request, **_kwargs):
        captured.append(request)
        return io.BytesIO(b'{"status":"FAILED"}')

    monkeypatch.setattr(workflow, "_open_read_only_url", open_probe)
    url = "http://127.0.0.1:8001/api/v1/qlib/monthly-releases/dmr_" + "a" * 32
    receipt = workflow._read_only_http_probe("business_smoke_ref", url, allowed_origins=["http://127.0.0.1:8001"])
    assert captured[0].get_header("X-dataset-release-operator-token") == token
    assert captured[0].method == "GET"
    assert token not in json.dumps(receipt)
    assert receipt["_response_body"] == '{"status":"FAILED"}'  # HTTP success is not business success.


@pytest.mark.parametrize("url", [
    "http://node1:8001/api/v1/qlib/monthly-releases/dmr_" + "a" * 32,
    "http://127.0.0.1:8001/health",
    "http://127.0.0.1:8001/api/v1/qlib/monthly-releases/dmr_" + "a" * 32 + "/activate",
    "http://127.0.0.1:8001/api/v1/qlib/monthly-releases/dmr_" + "a" * 32 + "?redirect=evil",
])
def test_other_probes_never_read_or_forward_operator_secret(monkeypatch, url) -> None:
    from scripts import monthly_unified_dataset_release as release
    monkeypatch.setattr(release, "_token", lambda: pytest.fail("must not load secret"))
    captured = []
    monkeypatch.setattr(workflow, "_open_read_only_url", lambda request, **kw: (captured.append(request) or io.BytesIO(b"{}")))
    origin = workflow._normalized_http_origin(url)
    workflow._read_only_http_probe("business_smoke_ref", url, allowed_origins=[origin])
    assert all(request.get_header("X-dataset-release-operator-token") is None for request in captured)


def test_monthly_probe_missing_secret_fails_closed_before_network(monkeypatch) -> None:
    monkeypatch.delenv("DATASET_RELEASE_OPERATOR_TOKEN_FILE", raising=False)
    monkeypatch.setattr(workflow, "_open_read_only_url", lambda *a, **kw: pytest.fail("must not call API"))
    url = "http://127.0.0.1:8001/api/v1/qlib/monthly-releases/dmr_" + "a" * 32
    receipt = workflow._read_only_http_probe("business_smoke_ref", url, allowed_origins=["http://127.0.0.1:8001"])
    assert receipt["status"] == "blocked"


@pytest.mark.parametrize("reflect_in_error", [False, True])
def test_authenticated_probe_never_records_reflected_secret(monkeypatch, reflect_in_error) -> None:
    from scripts import monthly_unified_dataset_release as release
    import urllib.error
    token = hashlib.sha256(b"monthly-probe-reflected-fixture").hexdigest()
    monkeypatch.setattr(release, "_token", lambda: token)

    def open_probe(*args, **kwargs):
        if reflect_in_error:
            raise urllib.error.URLError(token)
        return io.BytesIO(json.dumps({"echo": token}).encode())

    monkeypatch.setattr(workflow, "_open_read_only_url", open_probe)
    url = "http://127.0.0.1:8001/api/v1/qlib/monthly-releases/dmr_" + "a" * 32 + "/receipts"
    receipt = workflow._read_only_http_probe("business_smoke_ref", url, allowed_origins=["http://127.0.0.1:8001"])
    assert receipt["status"] == "failed"
    assert token not in json.dumps(receipt)
    assert "_response_body" not in receipt


def test_pre_pr_gate_reuses_exact_ci_classifier_and_blocks_before_push(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(workflow, "_git_status_paths", lambda _root: [])
    monkeypatch.setattr(
        workflow,
        "_run_ci_changed_file_classifier",
        lambda _paths, root: {
            "workflow_gate": "blocked",
            "classification": "unexecuted_test_blocked",
            "blocking": ["changed test files are not executed by any selected CI plan: ['backend/tests/new_test.py']"],
        },
    )

    gate = workflow._pre_pr_gate(
        finish={
            "changed_files": ["backend/tests/new_test.py"],
            "scope_check": {"status": "passed"},
            "fast_path": {"ownership": {}},
            "closure_ready": True,
        },
        validation_evidence=["pytest backend/tests/new_test.py -> passed"],
        root=Path.cwd(),
        run_lint=False,
    )

    assert gate["workflow_gate"] == "blocked"
    assert gate["ci_classifier"]["classification"] == "unexecuted_test_blocked"
    assert any("local CI classifier" in item for item in gate["blocking"])


def _local_data_payload(overview: bool = False) -> dict[str, Any]:
    row = {'data_kind': 'adj_factor', 'stats_max_date': '2026-08-11',
           'audit_ready_date': '2026-09-29', 'ready_date': '2026-09-29',
           'audit_quality_status': 'ok', 'physical_max_date': None,
           'physical_max_date_source': 'not_probed', 'stats_date_source': 'data_stats_cache',
           'readiness_source': 'dataset_date_refresh_audit', 'cache_state': 'stale',
           'readiness_status': 'audit_success', 'operator_action_required': False}
    data: dict[str, Any] = {'items': [row]}
    if overview:
        data = {'datasets': [row], 'dataset_count': 1, 'stale_dataset_count': 1,
                'stale_stats_cache_count': 1, 'readiness_unknown_count': 0,
                'quality_blocked_dataset_count': 0, 'running_job_count': 0,
                'active_alert_count': 0, 'blocked_target_count': 0,
                'retry_target_count': 0, 'status': 'yellow'}
    return {'success': True, 'operation': 'local_data_health_overview' if overview else 'local_data_list_data_stats',
            'risk_level': 'read_only', 'data': data}


@pytest.mark.parametrize('overview', [False, True])
def test_local_data_freshness_separates_cache_and_readiness(overview: bool) -> None:
    endpoint = 'overview' if overview else 'data-stats'
    _, verdict = workflow._evaluate_business_smoke_semantics(
        f'http://127.0.0.1:8001/api/v1/local-data/{endpoint}',
        json.dumps(_local_data_payload(overview)), response_sha256='a'*64)
    assert verdict['verdict'] == 'passed'
    assert verdict['contract_id'] == 'local_data_freshness'
    assert verdict['facts']['stale'] == 1


@pytest.mark.parametrize(('key', 'value'), [
    ('physical_max_date', '2026-09-29'), ('physical_max_date_source', 'cache'),
    ('stats_date_source', 'live'), ('readiness_source', 'cache'),
    ('ready_date', '2026-08-11'), ('cache_state', 'fresh'),
    ('readiness_status', 'unknown'), ('operator_action_required', 0),
    ('audit_ready_date', '2026-09-31'), ('stats_max_date', '2026-8-11'),
    ('audit_quality_status', {}),
])
def test_local_data_freshness_rejects_conflated_evidence(key: str, value: Any) -> None:
    payload = _local_data_payload()
    payload['data']['items'][0][key] = value
    status, _, _ = workflow._validate_local_data_freshness(payload, url='/api/v1/local-data/data-stats')
    assert status == 'failed'


@pytest.mark.parametrize('state', ['unknown', 'quality_blocked'])
def test_local_data_freshness_preserves_non_ready_states(state: str) -> None:
    payload = _local_data_payload()
    row = payload['data']['items'][0]
    row['readiness_status'] = state
    if state == 'unknown':
        row.update(audit_ready_date=None, ready_date=None, cache_state='audit_missing')
    else:
        row['audit_quality_status'] = 'low_coverage'
    status, _, facts = workflow._validate_local_data_freshness(payload, url='/api/v1/local-data/data-stats')
    assert status == 'passed' and facts[state] == 1


@pytest.mark.parametrize(('key', 'value'), [('success', False), ('operation', 'wrong'), ('risk_level', 'write')])
def test_local_data_freshness_requires_read_only_envelope(key: str, value: Any) -> None:
    payload = _local_data_payload()
    payload[key] = value
    assert workflow._validate_local_data_freshness(payload, url='/api/v1/local-data/data-stats')[0] == 'failed'


def test_local_data_overview_validates_counters_without_hiding_real_alerts() -> None:
    payload = _local_data_payload(True)
    data = payload['data']
    data.update(status='red', blocked_target_count=4, active_alert_count=4)
    assert workflow._validate_local_data_freshness(payload, url='/api/v1/local-data/overview')[0] == 'passed'
    data['status'] = 'green'
    assert workflow._validate_local_data_freshness(payload, url='/api/v1/local-data/overview')[0] == 'failed'
    data.update(status='red', dataset_count=2, readiness_unknown_count=1)
    assert workflow._validate_local_data_freshness(payload, url='/api/v1/local-data/overview')[0] == 'passed'
    data['stale_stats_cache_count'] = 0
    assert workflow._validate_local_data_freshness(payload, url='/api/v1/local-data/overview')[0] == 'failed'


def test_local_data_freshness_rejects_missing_and_duplicate_dataset_evidence() -> None:
    payload = _local_data_payload()
    payload['data']['items'].append(dict(payload['data']['items'][0]))
    assert workflow._validate_local_data_freshness(payload, url='/api/v1/local-data/data-stats')[0] == 'failed'
    payload['data']['items'] = []
    assert workflow._validate_local_data_freshness(payload, url='/api/v1/local-data/data-stats')[0] == 'failed'


@pytest.mark.parametrize(('key', 'value'), [('status', {}), ('dataset_count', True),
                                         ('readiness_unknown_count', -1)])
def test_local_data_overview_rejects_malformed_summary(key: str, value: Any) -> None:
    payload = _local_data_payload(True)
    payload['data'][key] = value
    assert workflow._validate_local_data_freshness(payload, url='/api/v1/local-data/overview')[0] == 'failed'


def _result(*, ok: bool = True, stdout: str = "", stderr: str = "", returncode: int = 0) -> dict[str, Any]:
    return {"ok": ok, "stdout": stdout, "stderr": stderr, "returncode": returncode}


_METRICS_PROBE = (
    "http://127.0.0.1:8001/api/v1/factor-metrics/results?factor_name=sample&calc_batch_id=batch1"
    "&eval_window=full&expected_snapshot_date=2026-08-31&expected_universe=pit_v2"
    "&expected_return_horizon=1d&limit=1"
)


def _metrics_payload() -> dict[str, Any]:
    return {"ok": True, "domain": "factor_metrics.result", "summary_first": True, "total": 1,
            "items": [{"id": 1, "factor_name": "sample", "calc_batch_id": "batch1", "eval_window": "full",
                       "snapshot_date": "2026-08-31", "universe": "pit_v2", "return_horizon": "1d",
                       "coverage": .9, "n_trading_days": 100, "ic_mean": -.1, "rank_ic_mean": -.2,
                       "icir": -2., "rank_icir": -3., "ic_positive_ratio": .4,
                       "calculated_at": "2026-09-30T13:04:21+08:00"}],
            "pagination": {"limit": 1, "offset": 0, "next_offset": 1, "total": 1, "has_more": False}}


def test_factor_metrics_semantics_bind_real_results_without_profitability_threshold() -> None:
    _, verdict = workflow._evaluate_business_smoke_semantics(
        _METRICS_PROBE, json.dumps(_metrics_payload()), response_sha256="a" * 64
    )
    assert verdict["verdict"] == "passed" and verdict["contract_id"] == "factor_metrics_results"
    assert verdict["facts"]["calc_batch_id"] == "batch1"
    assert verdict["facts"]["acceptance_scope"] == "bound_metrics_readback_only"
    assert verdict["facts"]["offline_algorithm_acceptance"] == "requires_separate_bug_specific_evidence"


@pytest.mark.parametrize("query", [
    "limit=1", "", "factor_name=sample&limit=1",
    _METRICS_PROBE.split("?", 1)[1] + "&calc_batch_id=other",
    _METRICS_PROBE.split("?", 1)[1].replace("batch1", "other"),
    _METRICS_PROBE.split("?", 1)[1].replace("2026-08-31", "2026-8-31"),
    _METRICS_PROBE.split("?", 1)[1].replace("limit=1", "limit=0"),
    _METRICS_PROBE.split("?", 1)[1].replace("limit=1", "limit=" + "9" * 5000),
    _METRICS_PROBE.split("?", 1)[1] + "&offset=1",
])
def test_factor_metrics_semantics_reject_unbound_or_conflicting_probe(query: str) -> None:
    assert workflow._validate_factor_metrics_results(
        _metrics_payload(), url=_METRICS_PROBE.split("?", 1)[0] + "?" + query
    )[0] == "failed"


@pytest.mark.parametrize(("field", "value"), [
    ("factor_name", "other"), ("calc_batch_id", "old_batch"), ("eval_window", "2024"),
    ("snapshot_date", "2026-06-30"), ("universe", "old_pool"), ("return_horizon", "20d"),
    ("id", True), ("n_trading_days", 0), ("coverage", float("nan")), ("ic_mean", float("inf")),
    ("rank_ic_mean", 2), ("ic_positive_ratio", -1), ("icir", None), ("rank_icir", "1"),
    ("h20_ic_mean", float("nan")), ("ic_mean", 10 ** 500),
    ("calculated_at", "2026-09-30T13:04:21"), ("calculated_at", "2026-08-30T13:04:21+08:00"),
])
def test_factor_metrics_semantics_reject_invalid_business_rows(field: str, value: Any) -> None:
    payload = _metrics_payload()
    payload["items"][0][field] = value
    assert workflow._validate_factor_metrics_results(payload, url=_METRICS_PROBE)[0] == "failed"


@pytest.mark.parametrize(("field", "value"), [
    ("items", []), ("total", True), ("total", 0), ("ok", False), ("domain", "other"),
    ("errors", ["failed"]), ("pagination", {}),
    ("success", False), ("status", "failed"),
    ("pagination", {"limit": 1, "offset": 0, "next_offset": 1, "total": 2, "has_more": False}),
])
def test_factor_metrics_semantics_reject_empty_or_contradictory_envelope(field: str, value: Any) -> None:
    payload = _metrics_payload()
    payload[field] = value
    assert workflow._validate_factor_metrics_results(payload, url=_METRICS_PROBE)[0] == "failed"


def test_factor_metrics_semantics_reject_duplicate_persisted_rows() -> None:
    payload = _metrics_payload()
    payload["items"] *= 2
    payload["total"] = 2
    payload["pagination"].update(limit=2, next_offset=2, total=2)
    assert workflow._validate_factor_metrics_results(payload, url=_METRICS_PROBE.replace("limit=1", "limit=2"))[0] == "failed"


def _entry_price_status_semantic(payload: Any, *, program_id: str = "advp_test") -> dict[str, Any]:
    _schema, semantic = workflow._evaluate_business_smoke_semantics(
        f"http://127.0.0.1:8001/api/v1/advisory/programs/{program_id}/entry-price/status",
        json.dumps(payload),
        response_sha256="e" * 64,
    )
    return semantic


def test_entry_price_status_semantic_contract_accepts_safe_unconfigured_readback() -> None:
    semantic = _entry_price_status_semantic(
        {
            "ok": True,
            "schema_version": "advisory_entry_price_status_v1",
            "configured": False,
            "program_id": "advp_test",
            "status": "NOT_CONFIGURED",
            "database_written": False,
        }
    )

    assert semantic["contract_id"] == "advisory_entry_price_status"
    assert semantic["verdict"] == "passed"
    assert semantic["facts"] == {
        "program_id": "advp_test",
        "configured": False,
        "status": "NOT_CONFIGURED",
        "database_written": False,
    }


def test_entry_price_status_semantic_contract_accepts_non_activating_configured_readback() -> None:
    semantic = _entry_price_status_semantic(
        {
            "ok": True,
            "schema_version": "advisory_entry_price_status_v1",
            "configured": True,
            "program_id": "advp_test",
            "status": "QUALITY_REVIEW_REQUIRED",
            "database_written": False,
            "binding_activated": False,
        }
    )

    assert semantic["verdict"] == "passed"
    assert semantic["facts"]["status"] == "QUALITY_REVIEW_REQUIRED"


@pytest.mark.parametrize(
    ("override", "reason"),
    [
        ({"ok": False}, "ok=true"),
        ({"errors": ["readback failed"]}, "without errors"),
        ({"schema_version": "wrong"}, "schema_version"),
        ({"program_id": "advp_other"}, "does not match"),
        ({"configured": "false"}, "configured must be boolean"),
        ({"status": "CONFIGURED"}, "must be NOT_CONFIGURED"),
        ({"database_written": True}, "database_written=false"),
        (
            {"configured": True, "status": "CONFIGURED", "binding_activated": True},
            "binding_activated=false",
        ),
    ],
)
def test_entry_price_status_semantic_contract_rejects_invalid_or_unsafe_readback(
    override: dict[str, Any],
    reason: str,
) -> None:
    payload = {
        "ok": True,
        "schema_version": "advisory_entry_price_status_v1",
        "configured": False,
        "program_id": "advp_test",
        "status": "NOT_CONFIGURED",
        "database_written": False,
    }
    payload.update(override)

    semantic = _entry_price_status_semantic(payload)

    assert semantic["verdict"] == "failed"
    assert reason in semantic["reason"]


def _rotation_l2_overview_payload(run_id: str = "a" * 64) -> dict[str, Any]:
    return {
        "status": "ok",
        "data": {
            "run_id": run_id,
            "model_hash": "b" * 64,
            "trade_date": "2026-09-26",
            "as_of_date": "2026-09-25",
            "sector_count": 131,
            "available_count": 119,
            "canonical_row_sha256": "c" * 64,
        },
    }


def _rotation_l2_semantic(
    payload: Any,
    *,
    query: str = "run_id=" + "a" * 64,
) -> dict[str, Any]:
    _schema, semantic = workflow._evaluate_business_smoke_semantics(
        f"http://127.0.0.1:8001/api/v1/hmm-risk/rotation-l2/overview?{query}",
        json.dumps(payload),
        response_sha256="d" * 64,
    )
    return semantic


def test_rotation_l2_overview_semantic_contract_binds_complete_run() -> None:
    semantic = _rotation_l2_semantic(_rotation_l2_overview_payload())

    assert semantic["contract_id"] == "hmm_rotation_l2_overview"
    assert semantic["verdict"] == "passed"
    assert semantic["facts"] == {
        "run_id": "a" * 64,
        "model_hash": "b" * 64,
        "canonical_row_sha256": "c" * 64,
        "trade_date": "2026-09-26",
        "as_of_date": "2026-09-25",
        "sector_count": 131,
        "available_count": 119,
    }


@pytest.mark.parametrize(
    "payload",
    [
        {**_rotation_l2_overview_payload(), "status": "failed"},
        {**_rotation_l2_overview_payload(), "ok": False},
        {**_rotation_l2_overview_payload(), "errors": ["readback failed"]},
    ],
)
def test_rotation_l2_overview_semantic_contract_rejects_conflicting_envelope(payload: dict[str, Any]) -> None:
    semantic = _rotation_l2_semantic(payload)

    assert semantic["verdict"] == "failed"
    assert "status=ok" in semantic["reason"]


@pytest.mark.parametrize(
    ("query", "payload", "reason"),
    [
        ("", _rotation_l2_overview_payload(), "exactly one non-empty run_id"),
        (
            "run_id=" + "a" * 64 + "&run_id=" + "a" * 64,
            _rotation_l2_overview_payload(),
            "exactly one non-empty run_id",
        ),
        ("run_id=" + "A" * 64, _rotation_l2_overview_payload("A" * 64), "lowercase SHA-256"),
        ("run_id=" + "a" * 64, _rotation_l2_overview_payload("e" * 64), "does not match"),
    ],
)
def test_rotation_l2_overview_semantic_contract_rejects_unbound_run(
    query: str,
    payload: dict[str, Any],
    reason: str,
) -> None:
    semantic = _rotation_l2_semantic(payload, query=query)

    assert semantic["verdict"] == "failed"
    assert reason in semantic["reason"]


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("sector_count", 130, "complete 131-sector catalog"),
        ("sector_count", True, "complete 131-sector catalog"),
        ("available_count", 132, "outside the sector catalog"),
        ("canonical_row_sha256", None, "lowercase SHA-256"),
        ("trade_date", "2026-09-31", "ISO date"),
        ("trade_date", "2026-9-26", "ISO date"),
        ("as_of_date", "2026-09-26", "must precede"),
    ],
)
def test_rotation_l2_overview_semantic_contract_rejects_invalid_business_data(
    field: str,
    value: Any,
    reason: str,
) -> None:
    payload = _rotation_l2_overview_payload()
    payload["data"][field] = value

    semantic = _rotation_l2_semantic(payload)

    assert semantic["verdict"] == "failed"
    assert reason in semantic["reason"]


def test_unknown_business_smoke_endpoint_remains_fail_closed() -> None:
    _schema, semantic = workflow._evaluate_business_smoke_semantics(
        "http://127.0.0.1:8001/api/v1/hmm-risk/rotation-l2/not-registered",
        json.dumps({"status": "ok", "data": {}}),
        response_sha256="f" * 64,
    )

    assert semantic["contract_id"] is None
    assert semantic["verdict"] == "failed"
    assert "no target-owned business-smoke semantic contract" in semantic["reason"]


def test_ci_issue_classification_ignores_successful_runner_and_no_network_metadata() -> None:
    summary = {
        "diagnostic_status": "complete",
        "failed_jobs": [
            {
                "error_signature": "Nightly failed sessions: validation_center_backend",
                "key_log_excerpt": [
                    "runner_preflight: success",
                    "FAILED backend/tests/scripts/test_issue_flow.py::test_validation_select",
                ],
            }
        ],
    }
    issue = {
        "title": "[P1][validation_center] Nightly failed: validation_center_backend",
        "body": """## Nightly Statuses

- runner_preflight: `success`
- nightly_l3: `failure`

## LLM Triage Advice

- reason: `schema_quality_smoke_no_network`

## Agent Handoff

Restore the self-hosted runner when infrastructure fails.
""",
    }

    assert workflow._classify_ci_issue(summary, issue) == "real_regression_candidate"


@pytest.mark.parametrize(
    ("title", "error_signature"),
    [
        ("P1 Nightly blocked: self-hosted Windows runner unavailable", "Nightly failed"),
        ("P1 Nightly failed", "no online GitHub Actions runner matches required labels"),
        ("P1 Nightly failed", "runner_preflight=failure"),
    ],
)
def test_ci_issue_classification_keeps_explicit_runner_failures_infrastructure(
    title: str,
    error_signature: str,
) -> None:
    summary = {
        "diagnostic_status": "complete",
        "failed_jobs": [{"error_signature": error_signature, "key_log_excerpt": []}],
    }

    assert workflow._classify_ci_issue(summary, {"title": title, "body": ""}) == "infra_blocker"


def test_merge_uses_stable_quality_contract_and_ignores_advisory_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    commands: list[list[str]] = []

    def fake_run(args: list[str], **kwargs: Any) -> dict[str, Any]:
        commands.append(args)
        if args[:3] == ["gh", "pr", "view"]:
            return _result(
                stdout=json.dumps(
                    {
                        "state": "OPEN",
                        "statusCheckRollup": [
                            {"name": "CI verdict", "status": "COMPLETED", "conclusion": "SUCCESS"},
                            {"name": "advisory", "status": "COMPLETED", "conclusion": "FAILURE"},
                        ],
                    }
                )
            )
        if args[:3] == ["gh", "pr", "checks"]:
            return _result(
                stdout=json.dumps(
                    [
                        {"name": name, "state": "SUCCESS", "bucket": "pass", "workflow": "quality"}
                        for name in workflow.MERGE_QUALITY_CHECK_CONTEXTS
                    ]
                    + [{"name": "advisory", "state": "FAILURE", "bucket": "fail", "workflow": "advisory"}]
                )
            )
        if args[:3] == ["gh", "pr", "merge"]:
            return _result()
        raise AssertionError(args)

    monkeypatch.setattr(workflow, "_run_command", fake_run)
    monkeypatch.setattr(
        workflow,
        "_verify_pr_merged",
        lambda pr_url: {"checked": True, "merged": True, "pr": {"mergeCommit": {"oid": "merge123"}}},
    )

    payload = workflow._merge_pr_if_ready("https://github.example/pull/199")

    assert payload["check_summary"]["passed"] == list(workflow.MERGE_QUALITY_CHECK_CONTEXTS)
    assert payload["check_summary"]["failed"] == []
    assert any(args[:3] == ["gh", "pr", "merge"] for args in commands)


def test_required_check_unknown_bucket_fails_closed() -> None:
    summary = workflow._required_pr_check_summary(
        _result(stdout=json.dumps([{"name": "CI verdict", "bucket": "mystery"}]))
    )

    assert summary["failed"] == ["CI verdict"]
    assert summary["passed"] == []


@pytest.mark.parametrize(
    ("changed_file", "expected_impact", "expected_targets"),
    [
        ("backend/main.py", "backend", ["backend-main"]),
        (
            "backend/services/dataset_release/index_contract.py",
            "worker_scheduler",
            ["worker-scheduler"],
        ),
        ("backend/services/hmm_risk/rotation_l1_gbdt.py", "none", []),
        ("scripts/aistock_runner_health.py", "none", []),
    ],
)
def test_repository_runtime_catalog_preserves_representative_roles(
    changed_file: str,
    expected_impact: str,
    expected_targets: list[str],
) -> None:
    payload = workflow._classify_runtime_impact([changed_file])

    assert payload["runtime_impact"] == expected_impact
    assert payload["target_ids"] == expected_targets


def test_monthly_release_sources_select_supervised_process_probe() -> None:
    catalog = workflow._load_runtime_target_catalog()
    target = catalog["targets"]["worker-scheduler"]
    monthly_sources = [
        "backend/services/dataset_release/artifact_ready_build_source.py",
        "backend/services/dataset_release/build_stage.py",
        "backend/services/dataset_release/candidate_validator.py",
        "backend/services/dataset_release/factor_materializer.py",
        "backend/services/dataset_release/monthly_consumer_layout.py",
        "backend/services/dataset_release/monthly_local_validation.py",
        "backend/services/dataset_release/monthly_worker_nodes.py",
        "backend/services/dataset_release/monthly_worker_runtime.py",
    ]

    selected, error = workflow._select_runtime_probe_route(target, runtime_files=monthly_sources)

    assert error is None
    assert selected["probe_route_id"] == "monthly_release_worker_process"
    assert selected["probe_mode"] == workflow._MONTHLY_RELEASE_WORKER_PROCESS_MODE
    assert selected["probes"] == workflow._MONTHLY_RELEASE_WORKER_PROCESS_REFS
    assert selected["probe_origins"] == ["http://127.0.0.1:8001", "http://localhost:8001"]


def test_shared_release_source_without_monthly_anchor_keeps_generic_heartbeat_probe() -> None:
    catalog = workflow._load_runtime_target_catalog()
    target = catalog["targets"]["worker-scheduler"]

    selected, error = workflow._select_runtime_probe_route(
        target,
        runtime_files=["backend/services/dataset_release/build_stage.py"],
    )

    assert error is None
    assert selected["probe_route_id"] == "dataset_release_worker_heartbeat"
    assert selected["probe_mode"] == workflow._DATASET_RELEASE_WORKER_HEARTBEAT_MODE


def test_monthly_release_process_probes_do_not_consult_worker_heartbeat(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = {
        "probe_origins": ["http://127.0.0.1:8001"],
        "probes": workflow._MONTHLY_RELEASE_WORKER_PROCESS_REFS,
    }
    snapshot = {
        "schema_version": "aistock_monthly_release_worker_process_snapshot_v1",
        "backend_listener_count": 1,
        "worker_count": 1,
        "healthy": True,
        "workers": [{"pid": 101, "ppid": 100}],
    }
    monkeypatch.setattr(workflow, "_monthly_release_worker_process_snapshot", lambda _target: snapshot)
    monkeypatch.setattr(
        workflow,
        "_read_only_http_probe",
        lambda name, url, **_kwargs: {
            "name": name,
            "url": url,
            "status": "passed",
            "_response_body": json.dumps({"commit": "a" * 40}),
        },
    )
    monkeypatch.setattr(
        workflow,
        "_read_dataset_release_worker_heartbeat_probes",
        lambda *_args, **_kwargs: pytest.fail("monthly process probe must not read generic heartbeat"),
    )

    results = workflow._read_monthly_release_worker_process_probes(target, 3.0)

    assert [item["name"] for item in results] == ["health_ref", "identity_ref", "business_smoke_ref"]
    assert all(item["status"] == "passed" for item in results)
    assert results[2]["semantic"]["contract_id"] == "monthly_release_worker_supervision"


def test_monthly_release_process_snapshot_binds_worker_to_backend_listener(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent_port = 8_000 + 1
    worker_path = tmp_path / "scripts" / "monthly_unified_dataset_release_worker.py"
    worker_path.parent.mkdir(parents=True)
    worker_path.write_text("# worker\n", encoding="utf-8")

    class FakeChild:
        pid = 102

        @staticmethod
        def ppid() -> int:
            return 101

        @staticmethod
        def cmdline() -> list[str]:
            return ["python", str(worker_path), "--serve", "--poll-seconds", "5.0"]

        @staticmethod
        def cwd() -> str:
            return str(tmp_path)

    class FakeParent:
        pid = 101

        @staticmethod
        def cmdline() -> list[str]:
            return ["python", "-m", "uvicorn", "backend.main:app", "--port", str(parent_port)]

        @staticmethod
        def cwd() -> str:
            return str(tmp_path)

        @staticmethod
        def children(*, recursive: bool) -> list[FakeChild]:
            assert recursive is False
            return [FakeChild()]

    monkeypatch.setattr(workflow, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(
        workflow.psutil,
        "net_connections",
        lambda **_kwargs: [
            SimpleNamespace(
                pid=101,
                status=workflow.psutil.CONN_LISTEN,
                laddr=SimpleNamespace(port=parent_port),
            )
        ],
    )
    monkeypatch.setattr(workflow.psutil, "Process", lambda pid: FakeParent() if pid == 101 else pytest.fail())

    snapshot = workflow._monthly_release_worker_process_snapshot(
        {
            "local_probe": {
                "worker_script": "scripts/monthly_unified_dataset_release_worker.py",
                "worker_mode": "--serve",
                "parent_module": "backend.main:app",
                "parent_port": parent_port,
            }
        }
    )

    assert snapshot["backend_listener_count"] == 1
    assert snapshot["worker_count"] == 1
    assert snapshot["healthy"] is True


def test_monthly_release_process_probe_fails_closed_for_duplicate_workers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = {
        "probe_origins": ["http://127.0.0.1:8001"],
        "probes": workflow._MONTHLY_RELEASE_WORKER_PROCESS_REFS,
    }
    snapshot = {
        "schema_version": "aistock_monthly_release_worker_process_snapshot_v1",
        "backend_listener_count": 1,
        "worker_count": 2,
        "healthy": False,
        "workers": [{"pid": 101, "ppid": 100}, {"pid": 102, "ppid": 100}],
    }
    monkeypatch.setattr(workflow, "_monthly_release_worker_process_snapshot", lambda _target: snapshot)
    monkeypatch.setattr(
        workflow,
        "_read_only_http_probe",
        lambda name, url, **_kwargs: {"name": name, "url": url, "status": "passed"},
    )

    results = workflow._read_monthly_release_worker_process_probes(target, 3.0)

    assert results[0]["status"] == "failed"
    assert results[2]["status"] == "failed"
    assert results[2]["semantic"]["verdict"] == "failed"
    assert "worker_count=2" in results[2]["error"]


def test_repository_runtime_catalog_omits_retired_hmm_sources() -> None:
    catalog = workflow._load_runtime_target_catalog()

    retired = {
        "backend/services/hmm_risk/b3_d1_inactive_dimension.py",
        "backend/services/hmm_risk/b3_mixed_dimension.py",
        "backend/services/hmm_risk/b3_training.py",
        "backend/services/hmm_risk/state_model_set.py",
    }

    assert retired.isdisjoint(catalog["non_runtime_source_paths"])


@pytest.fixture
def invalid_runtime_catalog(monkeypatch: pytest.MonkeyPatch) -> str:
    message = "runtime target catalog contains one stale source"

    def fail_catalog(_root: Path | None = None) -> dict[str, Any]:
        raise workflow.WorkflowError(message)

    monkeypatch.setattr(workflow, "_load_runtime_target_catalog", fail_catalog)
    return message


def test_runtime_classifier_surfaces_catalog_validation_error(invalid_runtime_catalog: str) -> None:

    payload = workflow._classify_runtime_impact(
        ["backend/services/hmm_risk/contracts.py"]
    )

    assert payload["runtime_impact"] == "unknown"
    assert payload["target_ids"] == ["backend-main"]
    assert payload["catalog_error"] == invalid_runtime_catalog


def test_runtime_contract_blocks_on_catalog_validation_error(invalid_runtime_catalog: str) -> None:
    contract = workflow.build_runtime_contract(
        record={
            "runtime_contract": {
                "schema_version": workflow.RUNTIME_CONTRACT_SCHEMA,
                "runtime_impact": "none",
                "target_ids": [],
            }
        },
        changed_files=["backend/services/hmm_risk/contracts.py"],
    )

    assert contract["runtime_impact"] == "unknown"
    assert contract["catalog_validation_error"] == invalid_runtime_catalog
    assert f"runtime target catalog validation failed: {invalid_runtime_catalog}" in contract["blocking"]
    assert contract["pre_pr_ready"] is False


@pytest.mark.parametrize("path,registered", [
    ("/api/v1/audit-unregistered-contract", False),
    ("/api/v1/factor-metrics/results", True),
])
def test_runtime_preflight_checks_semantic_registration_without_http(path, registered) -> None:
    root = workflow.REPO_ROOT
    runbook = next((root / "docs/operations").glob("*.md")).relative_to(root).as_posix()
    contract = workflow.build_runtime_contract(
        record={"runtime_contract": {
            "schema_version": workflow.RUNTIME_CONTRACT_SCHEMA,
            "operator_runbook_ref": runbook,
            "identity_ref": "http://127.0.0.1:8001/api/v1/runtime/identity",
            "business_smoke_ref": "http://127.0.0.1:8001" + path,
            "fresh_process_evidence": ["synthetic-contract-presence-only"],
        }},
        changed_files=["backend/main.py"],
    )
    semantic_errors = [error for error in contract["blocking"] if "semantic contract" in error]
    assert bool(semantic_errors) is not registered
    assert contract["pre_pr_ready"] is registered


def test_bug_1549_active_contract_consumers_remain_backend_main() -> None:
    payload = workflow._classify_runtime_impact(
        [
            "backend/services/hmm_risk/b3_d1_inactive_dimension.py",
            "backend/services/hmm_risk/b3_mixed_dimension.py",
            "backend/services/hmm_risk/b3_training.py",
            "backend/services/hmm_risk/contracts.py",
            "backend/services/hmm_risk/risk_l1_prediction.py",
            "backend/services/hmm_risk/state_model_set.py",
            "backend/services/hmm_risk/stock_fact_observation.py",
        ]
    )

    assert payload["runtime_impact"] == "backend"
    assert payload["target_ids"] == ["backend-main"]
    assert payload["catalog_error"] is None


def test_find_bug_record_parses_only_matching_or_opaque_filenames(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    matching = tmp_path / "20260813_BUG-199-example.json"
    unrelated = tmp_path / "20260813_BUG-200-example.json"
    opaque = tmp_path / "legacy.json"
    matching.write_text(json.dumps({"bug_id": "BUG-199"}), encoding="utf-8")
    unrelated.write_text(json.dumps({"bug_id": "BUG-200"}), encoding="utf-8")
    opaque.write_text(json.dumps({"bug_id": "BUG-201"}), encoding="utf-8")
    loaded: list[Path] = []
    original = workflow._load_json

    def recording_load(path: Path) -> dict[str, Any]:
        loaded.append(path)
        return original(path)

    monkeypatch.setattr(workflow, "_bug_files", lambda: [matching, unrelated, opaque])
    monkeypatch.setattr(workflow, "_load_json", recording_load)

    record, path = workflow.find_bug_record("BUG-199")

    assert record["bug_id"] == "BUG-199"
    assert path == matching
    assert loaded == [matching, opaque]


def test_compact_terminal_reservation_is_exact_and_keeps_other_records(tmp_path: Path) -> None:
    target = tmp_path / "BUG-199.json"
    other = tmp_path / "BUG-200.json"
    target.write_text(json.dumps({"bug_id": "BUG-199", "status": "registered"}), encoding="utf-8")
    other.write_text(json.dumps({"bug_id": "BUG-200", "status": "registered"}), encoding="utf-8")

    removed = compact_terminal_reservation(tmp_path, "BUG-199", min_age_seconds=0)

    assert removed == str(target)
    assert not target.exists()
    assert other.exists()


def test_read_command_retry_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    def fake_run(args: list[str], **kwargs: Any) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return _result(ok=calls == 3, stderr="TLS EOF" if calls < 3 else "", returncode=0 if calls == 3 else 1)

    monkeypatch.setattr(workflow, "_run_command", fake_run)
    monkeypatch.setattr(workflow.time, "sleep", lambda seconds: None)

    result = workflow._run_read_command_with_retry(["gh", "pr", "view", "1"], attempts=3)

    assert result["ok"] is True
    assert result["attempts"] == 3
    assert calls == 3


def test_github_issue_create_retries_missing_nested_module_label_with_parent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands: list[list[str]] = []

    def fake_run(args: list[str], **kwargs: Any) -> dict[str, Any]:
        commands.append(args)
        if len(commands) == 1:
            return _result(
                ok=False,
                stderr="could not add label: 'module:validation.workflow_automation' not found",
                returncode=1,
            )
        return _result(stdout="https://github.com/licong01-cloud/AIstock/issues/4601\n")

    monkeypatch.setattr(workflow, "_run_command", fake_run)
    body = tmp_path / "body.md"
    body.write_text("issue", encoding="utf-8")

    result = workflow._create_github_issue_with_recovery(
        bug_id="BUG-1461",
        title="BUG-1461 P2: example",
        body_path=body,
        labels=["aistock:bug", "module:validation.workflow_automation", "status:open"],
        cwd=tmp_path,
    )

    assert result["number"] == 4601
    assert result["warnings"] == [
        "GitHub label module:validation.workflow_automation was unavailable; used module:validation"
    ]
    assert len(commands) == 2
    assert "module:validation.workflow_automation" in commands[0][-1]
    assert "module:validation" in commands[1][-1]
    assert "module:validation.workflow_automation" not in commands[1][-1]


def test_github_issue_create_does_not_retry_unknown_top_level_module_label(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def fake_run(args: list[str], **kwargs: Any) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return _result(ok=False, stderr="could not add label: 'module:unknown' not found", returncode=1)

    monkeypatch.setattr(workflow, "_run_command", fake_run)
    body = tmp_path / "body.md"
    body.write_text("issue", encoding="utf-8")

    with pytest.raises(workflow.WorkflowError, match="module:unknown"):
        workflow._create_github_issue_with_recovery(
            bug_id="BUG-199",
            title="BUG-199 P2: example",
            body_path=body,
            labels=["aistock:bug", "module:unknown", "status:open"],
            cwd=tmp_path,
        )

    assert calls == 1


def test_workflow_smoke_does_not_call_full_doctor(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    issue = tmp_path / "bug.json"
    issue.write_text(json.dumps({"bug_id": "BUG-199"}), encoding="utf-8")
    monkeypatch.setattr(workflow, "build_doctor_report", lambda **kwargs: pytest.fail("full doctor called"))
    monkeypatch.setattr(workflow, "_git_status_paths", lambda root: [])
    monkeypatch.setattr(workflow, "build_fast_path_plan", lambda **kwargs: {"workflow_gate": "planned"})
    monkeypatch.setattr(workflow, "build_start_plan", lambda **kwargs: {"bug_id": "BUG-199"})
    monkeypatch.setattr(
        workflow,
        "build_finish_plan",
        lambda **kwargs: {"workflow_gate": "plan_ready", "artifact_metrics": {}},
    )
    monkeypatch.setattr(workflow, "_workflow_timing_summary", lambda *args, **kwargs: {"event_count": 0})

    payload = workflow.build_workflow_smoke_plan(
        bug_id="BUG-199",
        issue_json=str(issue),
        changed_files=["scripts/aistock_issue_workflow.py"],
        module="validation",
    )

    assert payload["workflow_gate"] == "passed"
    assert payload["client_manifest"] is None


def test_runtime_pending_close_sync_does_not_create_intermediate_pr(monkeypatch: pytest.MonkeyPatch) -> None:
    emitted: dict[str, Any] = {}
    monkeypatch.setattr(
        workflow,
        "build_close_sync_plan",
        lambda **kwargs: {"bug_id": "BUG-199", "workflow_gate": "fixed_source_pending_user_restart"},
    )
    monkeypatch.setattr(
        workflow,
        "_maybe_commit_and_pr_close_sync",
        lambda **kwargs: pytest.fail("intermediate close-sync PR created"),
    )
    monkeypatch.setattr(workflow, "_production_gates_payload", lambda args=None: {})
    monkeypatch.setattr(workflow, "_emit_args", lambda payload, args: emitted.update(payload))
    args = argparse.Namespace(
        bug_id="BUG-199",
        issue_json=None,
        pr_url="https://github.example/pull/199",
        apply=True,
        allow_missing_linkage=False,
        validation_evidence=["pytest -> passed"],
        merge_commit="a" * 40,
        skip_github_check=False,
        create_registry_worktree=True,
        allow_current_worktree=False,
        post_restart_receipt=None,
        create_pr=True,
    )

    assert workflow.cmd_close_sync(args) == 0
    assert emitted["close_sync_commit"]["workflow_gate"] == "deferred_runtime_verification"


@pytest.mark.parametrize("pr_number,commit,accepted", [(199, "a", True), (200, "a", False), (199, "b", False)])
def test_recoverable_close_sync_dirty_record_requires_exact_source_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    pr_number: int,
    commit: str,
    accepted: bool,
) -> None:
    issue = tmp_path / "tests" / "aistock_validation" / "bugs" / "BUG-199.json"
    issue.parent.mkdir(parents=True)
    issue.write_text(
        json.dumps(
            {
                "bug_id": "BUG-199",
                "status": "fixed",
                "fix_commit": "a" * 40,
                "pr_url": "https://github.example/pull/199",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        workflow,
        "_dirty_files",
        lambda _root: ["tests/aistock_validation/bugs/BUG-199.json"],
    )

    recovered = workflow._recoverable_close_sync_dirty_record(
        tmp_path,
        "BUG-199",
        issue,
        source_pr_url=f"https://github.example/pull/{pr_number}",
        merge_commit=commit * 40,
    )
    if accepted:
        assert recovered is not None
        assert recovered["path"] == "tests/aistock_validation/bugs/BUG-199.json"
    else:
        assert recovered is None


@pytest.mark.parametrize("recovery_number,accepted", [(199, True), (200, False)])
def test_close_sync_apply_guard_allows_only_the_exact_recoverable_dirty_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    recovery_number: int,
    accepted: bool,
) -> None:
    dirty_path = "tests/aistock_validation/bugs/BUG-199.json"
    monkeypatch.setattr(
        workflow,
        "_validate_registry_apply_target",
        lambda _root: {
            "blocking": ["registry target is dirty (1 file(s)); start from a clean task worktree"],
            "warnings": [],
            "git": {"dirty": True, "dirty_count": 1},
        },
    )
    monkeypatch.setattr(workflow, "_dirty_files", lambda _root: [dirty_path])
    recovery = {
        "bug_id": "BUG-199",
        "path": f"tests/aistock_validation/bugs/BUG-{recovery_number}.json",
        "status": "fixed",
        "fix_commit": "a" * 40,
        "pr_url": "https://github.example/pull/199",
    }

    result = workflow._validate_close_sync_apply_target(
        tmp_path,
        recoverable_dirty_record=recovery,
    )
    if accepted:
        assert result["blocking"] == []
        assert result["recoverable_dirty_record"] == recovery
    else:
        assert result["blocking"] == ["registry target is dirty (1 file(s)); start from a clean task worktree"]


def test_windows_process_scan_builds_full_caller_ancestor_exclusion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(workflow.os, "name", "nt")

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        captured["args"] = args
        captured["env"] = kwargs["env"]
        return subprocess.CompletedProcess(args=args, returncode=0, stdout="[]", stderr="")

    monkeypatch.setattr(workflow.shutil, "which", lambda name: "powershell.exe")
    monkeypatch.setattr(workflow.subprocess, "run", fake_run)

    profile = workflow._worktree_active_process_profile(tmp_path)

    assert profile["reference_count"] == 0
    assert "AISTOCK_CLEANUP_CALLER_PID" in captured["env"]
    assert "ParentProcessId" in captured["args"][-1]
    assert "AISTOCK_CLEANUP_EXCLUDE_PIDS" not in captured["env"]


def test_backend_lifespan_logs_are_transient_only_for_exact_bounded_format(tmp_path: Path) -> None:
    log_root = tmp_path / "backend" / "logs"
    log_root.mkdir(parents=True)
    (log_root / "aistock.log").write_text(
        "2026-08-13 03:10:31 INFO [backend.main] lifespan validation started\n",
        encoding="utf-8",
    )
    (log_root / "errors.log").write_text("", encoding="utf-8")

    accepted, reason = workflow._validated_backend_lifespan_log_transient_paths(
        ["backend/logs/aistock.log", "backend/logs/errors.log"],
        worktree_path=tmp_path,
    )

    assert accepted == {"backend/logs/aistock.log", "backend/logs/errors.log"}
    assert reason == "bounded_test_created_backend_lifespan_log"

    (log_root / "aistock.log.1").write_text("rotated evidence", encoding="utf-8")
    rejected, reject_reason = workflow._validated_backend_lifespan_log_transient_paths(
        ["backend/logs/aistock.log", "backend/logs/errors.log", "backend/logs/aistock.log.1"],
        worktree_path=tmp_path,
    )

    assert rejected == set()
    assert reject_reason == "backend_lifespan_log_inventory_mismatch"


def test_backend_lifespan_log_format_mismatch_stays_unknown(tmp_path: Path) -> None:
    log_root = tmp_path / "backend" / "logs"
    log_root.mkdir(parents=True)
    (log_root / "aistock.log").write_text("Traceback: retain this evidence\n", encoding="utf-8")

    accepted, reason = workflow._validated_backend_lifespan_log_transient_paths(
        ["backend/logs/aistock.log"],
        worktree_path=tmp_path,
    )

    assert accepted == set()
    assert reason == "backend_lifespan_log_format_mismatch"


def test_cleanup_discovers_registered_worktree_when_argument_is_omitted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    branch = "bug/BUG-199-workflow"
    task_worktree = tmp_path / "task-worktree"
    task_worktree.mkdir()

    def fake_git(args: list[str], **kwargs: Any) -> str:
        if args[:2] == ["branch", "--show-current"]:
            return "main"
        if args[:3] == ["for-each-ref", "--format=%(refname:short)", "refs/heads"]:
            return branch
        if args[:3] == ["branch", "--format=%(refname:short)", "--merged"]:
            return branch
        return ""

    def fake_run(args: list[str], **kwargs: Any) -> dict[str, Any]:
        if args[:2] == ["git", "status"]:
            return _result()
        if args[:2] == ["git", "ls-files"]:
            return _result(stdout="")
        if args[:3] == ["git", "ls-remote", "--heads"]:
            return _result(stdout="")
        raise AssertionError(args)

    monkeypatch.setattr(workflow, "_canonical_root", lambda: tmp_path)
    monkeypatch.setattr(workflow, "_registered_worktree_for_branch", lambda value, cwd=None: task_worktree)
    monkeypatch.setattr(workflow, "_git", fake_git)
    monkeypatch.setattr(workflow, "_run_command", fake_run)
    monkeypatch.setattr(workflow, "_path_is_registered_worktree", lambda path, cwd=None: True)
    monkeypatch.setattr(workflow, "_git_snapshot", lambda root: {"branch": "main", "dirty": False})
    monkeypatch.setattr(workflow, "_dirty_files", lambda root: [])
    monkeypatch.setattr(workflow, "_cleanup_protected_receipt_paths", lambda bug_id: set())
    monkeypatch.setattr(
        workflow,
        "_cleanup_evidence_finalization",
        lambda bug_id: {"durable_receipt_present": True, "status": "finalized_structured_receipt"},
    )

    payload = workflow.build_cleanup_after_merge_plan(branch=branch, apply=False)

    assert payload["workflow_gate"] == "ready_for_cleanup"
    assert payload["worktree"] == str(task_worktree)
    assert payload["worktree_registered"] is True


def test_cleanup_retry_accepts_merged_pr_head_ancestry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    head = "a" * 40
    merge_commit = "b" * 40
    verified_pr_check = {
        "checked": True,
        "merged": True,
        "pr": {
            "url": "https://github.example/pull/199",
            "headRefName": "bug/BUG-199-already-partially-cleaned",
            "headRefOid": head,
            "mergeCommit": {"oid": merge_commit},
        },
    }
    monkeypatch.setattr(workflow, "_verify_pr_merged", lambda pr_url: pytest.fail("cached PR check was reread"))
    monkeypatch.setattr(
        workflow,
        "_git_commit_is_ancestor",
        lambda ancestor, descendant, root: ancestor == head and descendant == merge_commit,
    )

    result = workflow._cleanup_merge_verification(
        "bug/BUG-199-already-partially-cleaned",
        "https://github.example/pull/199",
        False,
        cwd=tmp_path,
        verified_pr_check=verified_pr_check,
    )

    assert result["verified"] is True
    assert result["method"] == "merged_pr_head_is_ancestor_of_merge_commit"
    assert result["tree_equivalence_ref"] == head


def test_cleanup_preflight_reuses_successful_same_finalizer_fetch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cached = {"status": "fetched", "command": "git fetch origin --prune", "result": _result()}
    monkeypatch.setattr(
        workflow,
        "_cleanup_preflight_fetch_origin",
        lambda root, apply: pytest.fail("successful cached fetch was repeated"),
    )

    result = workflow._cleanup_preflight_fetch_for_plan(tmp_path, apply=True, cached=cached)

    assert result["status"] == "fetched"
    assert result["reused"] is True


def test_cleanup_pr_cache_mismatch_forces_exact_readback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    branch = "bug/BUG-199-target"
    pr_url = "https://github.example/pull/199"
    head = "a" * 40
    merge_commit = "b" * 40
    readbacks = 0

    def readback(value: str) -> dict[str, Any]:
        nonlocal readbacks
        readbacks += 1
        assert value == pr_url
        return {
            "checked": True,
            "merged": True,
            "pr": {
                "url": pr_url,
                "headRefName": branch,
                "headRefOid": head,
                "mergeCommit": {"oid": merge_commit},
            },
        }

    monkeypatch.setattr(workflow, "_verify_pr_merged", readback)
    monkeypatch.setattr(workflow, "_git_commit_is_ancestor", lambda ancestor, descendant, root: True)

    result = workflow._cleanup_merge_verification(
        branch,
        pr_url,
        False,
        cwd=tmp_path,
        verified_pr_check={
            "checked": True,
            "merged": True,
            "pr": {
                "url": "https://github.example/pull/200",
                "headRefName": "bug/BUG-200-other",
                "headRefOid": "c" * 40,
                "mergeCommit": {"oid": "d" * 40},
            },
        },
    )

    assert result["verified"] is True
    assert readbacks == 1


@pytest.mark.parametrize("stale", [False, True])
def test_merge_aftercare_publishes_changed_and_existing_stale_client_lanes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stale: bool,
) -> None:
    events: list[str] = []
    monkeypatch.setattr(workflow, "_canonical_root", lambda: tmp_path)
    monkeypatch.setattr(
        workflow,
        "_cleanup_preflight_fetch_origin",
        lambda root, apply: {"status": "fetched", "result": _result()},
    )
    monkeypatch.setattr(
        workflow,
        "_git_snapshot",
        lambda root: {"branch": "main", "dirty": False, "head": "same" if stale else "old", "origin_main": "same" if stale else "new"},
    )
    monkeypatch.setattr(
        workflow,
        "_run_command",
        lambda args, **kwargs: events.append(
            "root_sync" if args[:2] == ["git", "merge"] else "merge_containment"
        )
        or _result(),
    )
    monkeypatch.setattr(
        workflow,
        "_merge_commit_changed_files",
        lambda merge_commit, root: {
            "ok": True,
            "files": [".codex/skills/aistock-merge-aftercare/SKILL.md"] if stale else [
                ".codex/skills/aistock-merge-aftercare/SKILL.md", ".claude/commands/aistock-task-router.md",
                "docs/standards/README.md"],
        },
    )

    def fake_install(*, apply: bool, selected_lane: str, **kwargs: Any) -> dict[str, Any]:
        events.append(f"install:{selected_lane}")
        return {"workflow_gate": "installed", "blocking": []}

    monkeypatch.setattr(workflow, "build_client_install_plan", fake_install)
    monkeypatch.setattr(workflow, "_ClientInstallLock", lambda: workflow.contextlib.nullcontext())
    monkeypatch.setattr(workflow, "_client_manifest", lambda: {
        "codex_entries": {"merge_aftercare": {"status": "current"}, "validation_delegation": {"status": "stale"}},
        "claude_entries": {"validation_delegation": {"status": "stale_global"}, "readonly_triage": {"status": "missing_global"}},
    } if stale else {})
    monkeypatch.setattr(
        workflow,
        "_client_lane_verification",
        lambda manifest, selected_lane, verify_codex, verify_claude: {"ready": True, "blocking": []},
    )

    result = workflow._publish_changed_clients_after_merge(
        merge_commit="c" * 40,
        sync_root=True,
        apply=True,
    )

    assert result["workflow_gate"] == "installed_and_verified"
    if stale:
        assert result["changed_lanes"] == ["merge_aftercare"]
        assert result["stale_lanes_before"] == ["readonly_triage", "validation_delegation"]
        assert [event for event in events if event.startswith("install:")] == [
            "install:merge_aftercare", "install:readonly_triage", "install:validation_delegation"]
    else:
        assert result["selected_lanes"] == ["merge_aftercare", "router"]
        assert events == ["root_sync", "merge_containment", "install:merge_aftercare", "install:router"]
        assert result["merge_commit_containment"]["ok"] is True


def test_merge_finalizer_stops_before_close_sync_when_client_publish_blocks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        workflow,
        "_publish_changed_clients_after_merge",
        lambda **kwargs: {"workflow_gate": "blocked", "blocking": ["client publish failed"]},
    )
    monkeypatch.setattr(
        workflow,
        "build_close_sync_plan",
        lambda **kwargs: pytest.fail("close-sync started before client publish"),
    )

    payload = workflow.build_merge_finalizer_plan(
        bug_id="BUG-199",
        source_pr_url="https://github.example/pull/199",
        source_branch=None,
        source_worktree=None,
        validation_evidence=["pytest -> passed"],
        sync_root=True,
        apply=True,
        source_pr_check={
            "checked": True,
            "merged": True,
            "pr": {"mergeCommit": {"oid": "d" * 40}},
        },
    )

    assert payload["workflow_gate"] == "blocked"
    assert payload["blocking"] == ["client publish failed"]
