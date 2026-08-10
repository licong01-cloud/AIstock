from __future__ import annotations

from pathlib import Path

import yaml

from backend.services.validation.file_ownership import FileOwnershipCatalog


REPO_ROOT = Path(__file__).resolve().parents[3]
LAUNCHER_PATH = REPO_ROOT / "start_all_ai_stock.bat"
RUNTIME_CATALOG_PATH = REPO_ROOT / "docs" / "standards" / "aistock_runtime_targets_v1.yaml"


def _launcher_text() -> str:
    return LAUNCHER_PATH.read_text(encoding="utf-8")


def test_backend_launcher_uses_the_aistock_interpreter_in_both_branches() -> None:
    text = _launcher_text()
    backend_commands = [line.strip() for line in text.splitlines() if "backend.main:app" in line]

    assert backend_commands == [
        (
            '; new-tab --title "AIstock Backend" cmd /k "chcp 65001>nul & cd /d %AIROOT% && '
            "call %AISTOCK_CONDA_BAT% activate AIstock && %AISTOCK_BACKEND_PYTHON% -m uvicorn "
            'backend.main:app --host 0.0.0.0 --port 8001" ^'
        ),
        'start "AIstock Backend" cmd /k "chcp 65001>nul & cd /d %AIROOT% && '
        "call %AISTOCK_CONDA_BAT% activate AIstock && %AISTOCK_BACKEND_PYTHON% -m uvicorn "
        'backend.main:app --host 0.0.0.0 --port 8001"',
    ]
    assert "call conda activate AIstock & uvicorn backend.main:app" not in text
    assert " & uvicorn backend.main:app" not in text


def test_backend_launcher_fails_visibly_when_the_interpreter_is_missing() -> None:
    text = _launcher_text()

    assert 'set "AISTOCK_CONDA_BAT=C:\\Users\\lc999\\miniconda3\\condabin\\conda.bat"' in text
    assert 'set "AISTOCK_BACKEND_PYTHON=C:\\Users\\lc999\\miniconda3\\envs\\AIstock\\python.exe"' in text
    assert (
        'if not exist "%AISTOCK_CONDA_BAT%" (\n'
        "  echo ERROR: Conda activation script not found: %AISTOCK_CONDA_BAT%\n"
        "  pause\n"
        "  exit /b 1\n"
        ")"
    ) in text
    assert (
        'if not exist "%AISTOCK_BACKEND_PYTHON%" (\n'
        "  echo ERROR: AIstock backend Python not found: %AISTOCK_BACKEND_PYTHON%\n"
        "  pause\n"
        "  exit /b 1\n"
        ")"
    ) in text
    assert text.index('if not exist "%AISTOCK_CONDA_BAT%"') < text.index("where wt")
    assert text.index('if not exist "%AISTOCK_BACKEND_PYTHON%"') < text.index("where wt")


def test_launcher_is_owned_and_classified_as_backend_main_runtime_source() -> None:
    ownership = FileOwnershipCatalog().match_path("start_all_ai_stock.bat")
    runtime_catalog = yaml.safe_load(RUNTIME_CATALOG_PATH.read_text(encoding="utf-8"))

    assert ownership.ownership_status == "mapped"
    assert ownership.primary_module == "platform.config"
    assert "platform.api" in ownership.impact_modules
    assert "start_all_ai_stock.bat" in runtime_catalog["targets"]["backend-main"]["source_globs"]
