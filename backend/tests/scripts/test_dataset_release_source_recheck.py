from __future__ import annotations

import pytest

from scripts.dataset_release_source_recheck import main


@pytest.mark.parametrize("forbidden", ["TUSHARE_TOKEN", "TDX_HTTP_PORT"])
def test_source_recheck_rejects_provider_or_http_authority_before_scan(
    monkeypatch,
    tmp_path,
    forbidden: str,
) -> None:
    monkeypatch.setenv(forbidden, "must-not-enter-db-only-child")

    with pytest.raises(
        ValueError,
        match="provider credential/HTTP authority is forbidden",
    ):
        main(
            [
                "--profile",
                str(tmp_path / "missing-profile.yaml"),
                "--control-root",
                str(tmp_path / "control"),
                "--cutoff",
                "2026-07-31",
                "--artifact-ready-contract-ref",
                "a" * 64,
                "--run-id",
                "run-fixture",
                "--attempt-id",
                "attempt-fixture",
                "--attempt-fence",
                "1",
                "--execution-id",
                "prepublish-source-recheck",
                "--result-path",
                str(tmp_path / "semantic_result.json"),
                "--stage-timeout-seconds",
                "300",
                "--pressure-rung",
                "0",
            ]
        )
