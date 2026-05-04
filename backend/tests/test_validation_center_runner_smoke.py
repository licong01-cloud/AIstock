from __future__ import annotations

import json
from pathlib import Path

from scripts import validation_center_runner_smoke as smoke


def _envelope(data: dict) -> tuple[int, dict, None]:
    return 200, {"status": "success", "data": data}, None


def test_runner_smoke_starts_job_and_reads_archive(monkeypatch, tmp_path: Path) -> None:
    job_id = "valjob_20260504_220000_abcdef12"
    run_id = "validation_center_runner__run123"
    calls: list[tuple[str, str]] = []

    def route(_api_base: str, path: str, *, method: str = "GET", body=None, timeout=5.0):
        path_only = path.split("?", 1)[0]
        calls.append((method, path_only))
        if method == "POST" and path_only == "/validation/executions":
            assert body["plan_key"] == "guardrail_changed_files"
            return _envelope({"job_id": job_id, "status": "queued", "archive": {"status": "pending"}})
        if method == "GET" and path_only == f"/validation/executions/{job_id}":
            return _envelope(
                {
                    "job_id": job_id,
                    "status": "passed",
                    "archive": {
                        "status": "archived",
                        "run_id": run_id,
                        "run_record_path": "tests/aistock_validation/history/validation_center/demo.md",
                    },
                }
            )
        if method == "GET" and path_only == f"/validation/executions/{job_id}/log":
            return _envelope({"job_id": job_id, "content": "runner ok\n"})
        if method == "GET" and path_only == f"/validation/executions/{job_id}/evidence":
            return _envelope({"job_id": job_id, "standard_evidence": {"schema_version": "aistock_validation_evidence_manifest_v1"}})
        if method == "GET" and path_only == f"/validation/runs/{run_id}":
            return _envelope({"run_id": run_id, "metadata_missing": False})
        return None, None, f"unexpected path: {method} {path}"

    monkeypatch.setattr(smoke, "_request_json", route)
    output = tmp_path / "runner_smoke.json"

    exit_code = smoke.run_smoke(
        api_base="http://127.0.0.1:8012/api/v1",
        output=output,
        poll_interval_seconds=0,
    )

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert exit_code == 0
    assert payload["schema_version"] == smoke.SCHEMA_VERSION
    assert payload["status"] == "passed"
    assert payload["write_methods_sent"] == ["POST /validation/executions"]
    assert payload["job"]["archive"]["run_id"] == run_id
    assert ("GET", f"/validation/runs/{run_id}") in calls


def test_runner_smoke_blocks_production_8001(tmp_path: Path) -> None:
    output = tmp_path / "blocked.json"

    exit_code = smoke.run_smoke(api_base="http://127.0.0.1:8001/api/v1", output=output)

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert exit_code == 1
    assert payload["status"] == "failed"
    assert "refusing to touch production backend port 8001" in payload["failures"]
    assert payload["write_methods_sent"] == []
