import importlib.util
import sys
from pathlib import Path

import pytest


_SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "aistock_data_quality_smoke.py"
_SPEC = importlib.util.spec_from_file_location("aistock_data_quality_smoke", _SCRIPT_PATH)
assert _SPEC is not None and _SPEC.loader is not None
smoke = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = smoke
_SPEC.loader.exec_module(smoke)


def test_explicit_env_file_is_loaded_and_overrides_empty_env(monkeypatch, tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("TDX_DB_HOST=127.0.0.1\nTDX_DB_PASSWORD=secret-for-test\n", encoding="utf-8")
    monkeypatch.setenv("TDX_DB_HOST", "")
    monkeypatch.delenv("TDX_DB_PASSWORD", raising=False)

    loaded = smoke._load_dotenv(env_file)

    assert loaded == env_file
    assert smoke.os.environ["TDX_DB_HOST"] == "127.0.0.1"
    assert smoke.os.environ["TDX_DB_PASSWORD"] == "secret-for-test"


def test_missing_explicit_env_file_fails_fast(tmp_path):
    with pytest.raises(smoke.SmokeFailure, match="explicit env file does not exist"):
        smoke._discover_env_file(tmp_path / "missing.env")


def test_use_dev_db_maps_only_safe_local_dev_target(monkeypatch):
    for key, value in {
        "TDX_DB_DEV_HOST": "127.0.0.1",
        "TDX_DB_DEV_PORT": "5433",
        "TDX_DB_DEV_NAME": "aistock_dev",
        "TDX_DB_DEV_USER": "postgres",
        "TDX_DB_DEV_PASSWORD": "dev-password-for-test",
    }.items():
        monkeypatch.setenv(key, value)
    for runtime_key in smoke.DEV_DB_KEYS.values():
        monkeypatch.delenv(runtime_key, raising=False)

    target = smoke._apply_dev_db_env()

    assert target["source"] == "tdx_db_dev"
    assert target["host"] == "127.0.0.1"
    assert target["port"] == "5433"
    assert target["dbname"] == "aistock_dev"
    assert target["password_configured"] is True
    assert smoke.os.environ["TDX_DB_PASSWORD"] == "dev-password-for-test"


def test_use_dev_db_refuses_non_dev_target(monkeypatch):
    for key, value in {
        "TDX_DB_DEV_HOST": "127.0.0.1",
        "TDX_DB_DEV_PORT": "5432",
        "TDX_DB_DEV_NAME": "aistock",
        "TDX_DB_DEV_USER": "postgres",
        "TDX_DB_DEV_PASSWORD": "dev-password-for-test",
    }.items():
        monkeypatch.setenv(key, value)

    with pytest.raises(smoke.SmokeFailure, match="not the local dev DB"):
        smoke._apply_dev_db_env()
