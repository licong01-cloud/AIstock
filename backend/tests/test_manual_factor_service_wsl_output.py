from __future__ import annotations

from pathlib import Path

from backend.services.manual_factor_service import _build_wsl_copy_file_script


def test_wsl_result_copy_script_uses_windows_mount_not_unc() -> None:
    script = _build_wsl_copy_file_script(
        "/home/lc999/factor_workspace/_factor_demo/result.h5",
        Path("F:/Dev/AIstock/.codex_tmp/result.h5"),
    )

    assert "\\\\wsl" not in script.lower()
    assert "wsl.localhost" not in script.lower()
    assert "/mnt/f/Dev/AIstock/.codex_tmp/result.h5" in script
    assert "cp /home/lc999/factor_workspace/_factor_demo/result.h5" in script
