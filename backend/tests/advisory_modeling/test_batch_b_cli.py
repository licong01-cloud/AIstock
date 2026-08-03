from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.advisory_short_rebound_batch_b import (
    _database_config,
    _existing_directory,
    _load_environment,
    build_parser,
)
from backend.services.advisory_modeling.batch_b import (
    BATCH_B_CANDIDATE_PREFETCH_PER_PROGRAM,
)


def test_batch_b_cli_requires_every_explicit_root_and_env_source(tmp_path: Path) -> None:
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args([])

    relative = Path("relative-root")
    with pytest.raises(ValueError, match="explicit absolute"):
        _existing_directory(relative, field_name="artifact_root")

    env_file = tmp_path / ".env"
    env_file.write_text("TDX_DB_HOST=localhost\n", encoding="utf-8")
    with pytest.raises(ValueError, match="missing required Batch B configuration"):
        _load_environment(env_file)


def test_database_config_uses_only_loaded_env_values_without_logging_secrets() -> None:
    values = {
        "TDX_DB_HOST": "db.example",
        "TDX_DB_PORT": "5432",
        "TDX_DB_NAME": "aistock",
        "TDX_DB_USER": "research",
        "TDX_DB_PASSWORD": "secret",
    }

    config = _database_config(values)

    assert config == {
        "host": "db.example",
        "port": 5432,
        "dbname": "aistock",
        "user": "research",
        "password": "secret",
    }
    assert "secret" not in json.dumps({"keys": sorted(config)})


def test_batch_b_uses_the_frozen_bounded_candidate_prefetch_width() -> None:
    assert BATCH_B_CANDIDATE_PREFETCH_PER_PROGRAM == 8
