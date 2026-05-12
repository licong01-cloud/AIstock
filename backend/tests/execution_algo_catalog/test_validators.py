from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.db.utils.execution_algo_catalog_validation import run_execution_algo_catalog_migration_with_validation
from backend.services.strategy_package.model_asset_resolver import ModelAssetResolver
from backend.services.execution_algo_catalog.validators import (
    ExecutionAlgoArtifactMissingError,
    validate_enabled_default_config_model_paths,
    validate_enabled_default_config_model_paths_from_db,
)
from backend.services.trading_core.errors import DataUnavailableError


class RecordingResolver:
    def __init__(self, *, fail_key: str | None = None) -> None:
        self.fail_key = fail_key
        self.calls: list[tuple[str, str, bool]] = []

    def resolve_runtime_asset(self, *, manifest, config_key: str, copy_missing: bool = True):
        algo_code = manifest.minute_execution_policy.algo_code
        self.calls.append((algo_code, config_key, copy_missing))
        if config_key == self.fail_key:
            raise DataUnavailableError(
                "resolver could not find asset",
                context={"attempted_paths": ["/missing/model.pt"]},
            )
        return SimpleNamespace(
            original_path=manifest.minute_execution_policy.algo_config[config_key],
            resolved_path=Path("/cache") / algo_code / Path(manifest.minute_execution_policy.algo_config[config_key]).name,
        )


def test_validates_enabled_runtime_asset_keys_with_injected_resolver() -> None:
    resolver = RecordingResolver()

    results = validate_enabled_default_config_model_paths(
        [
            {
                "algo_code": "V25_TWO_STAGE",
                "is_enabled": True,
                "default_config": {
                    "early_model_path": "/home/lc999/data/rl_models/v25/early.pt",
                    "late_model_path": "/home/lc999/data/rl_models/v25/late.pt",
                    "device": "cuda",
                },
            }
        ],
        resolver=resolver,
    )

    assert resolver.calls == [
        ("V25_TWO_STAGE", "early_model_path", False),
        ("V25_TWO_STAGE", "late_model_path", False),
    ]
    assert [(item.algo_code, item.config_key, item.original_path) for item in results] == [
        ("V25_TWO_STAGE", "early_model_path", "/home/lc999/data/rl_models/v25/early.pt"),
        ("V25_TWO_STAGE", "late_model_path", "/home/lc999/data/rl_models/v25/late.pt"),
    ]


def test_wraps_resolver_failure_as_execution_algo_artifact_missing() -> None:
    resolver = RecordingResolver(fail_key="late_model_path")

    with pytest.raises(ExecutionAlgoArtifactMissingError) as exc_info:
        validate_enabled_default_config_model_paths(
            [
                {
                    "algo_code": "V25_1_SMALL_CAP",
                    "default_config": {
                        "early_model_path": "/home/lc999/data/rl_models/v25/early.pt",
                        "late_model_path": "/home/lc999/data/rl_models/v25/missing.pt",
                    },
                }
            ],
            resolver=resolver,
        )

    exc = exc_info.value
    assert exc.error_code == "EXECUTION_ALGO_ARTIFACT_MISSING"
    assert "EXECUTION_ALGO_ARTIFACT_MISSING" in exc.message
    assert "V25_1_SMALL_CAP default_config.late_model_path" in exc.message
    assert exc.context["algo_code"] == "V25_1_SMALL_CAP"
    assert exc.context["config_key"] == "late_model_path"
    assert exc.context["resolver_error_code"] == "DATA_UNAVAILABLE"
    assert exc.context["resolver_context"] == {"attempted_paths": ["/missing/model.pt"]}


def test_missing_required_path_fails_before_resolver_call() -> None:
    resolver = RecordingResolver()

    with pytest.raises(ExecutionAlgoArtifactMissingError) as exc_info:
        validate_enabled_default_config_model_paths(
            [
                {
                    "algo_code": "V24_PLAN",
                    "default_config": {"device": "cpu"},
                }
            ],
            resolver=resolver,
        )

    assert resolver.calls == []
    assert "V24_PLAN default_config.model_path is required" in exc_info.value.message
    assert exc_info.value.context["config_key"] == "model_path"


def test_skips_disabled_algos_and_algos_without_runtime_assets() -> None:
    resolver = RecordingResolver()

    results = validate_enabled_default_config_model_paths(
        [
            {
                "algo_code": "V25_TWO_STAGE",
                "is_enabled": False,
                "default_config": {
                    "early_model_path": "missing-early.pt",
                    "late_model_path": "missing-late.pt",
                },
            },
            {
                "algo_code": "TWAP",
                "is_enabled": True,
                "default_config": {},
            },
        ],
        resolver=resolver,
    )

    assert results == []
    assert resolver.calls == []


def test_falls_back_to_model_path_keys_when_capability_metadata_lags() -> None:
    resolver = RecordingResolver()

    validate_enabled_default_config_model_paths(
        [
            {
                "algo_code": "NEW_MODEL_ALGO",
                "default_config": {
                    "model_path": "/home/lc999/data/rl_models/new/model.pt",
                },
            }
        ],
        resolver=resolver,
    )

    assert resolver.calls == [("NEW_MODEL_ALGO", "model_path", False)]


def test_accepts_json_default_config_and_copy_missing_flag() -> None:
    resolver = RecordingResolver()

    validate_enabled_default_config_model_paths(
        [
            {
                "algo_code": "v24_plan",
                "default_config": '{"model_path": "/home/lc999/data/rl_models/v24/model.pt"}',
            }
        ],
        resolver=resolver,
        copy_missing=True,
    )

    assert resolver.calls == [("V24_PLAN", "model_path", True)]


def test_row_asset_namespace_overrides_default_config_for_validation() -> None:
    resolver = RecordingResolver()

    validate_enabled_default_config_model_paths(
        [
            {
                "algo_code": "V25_1_SMALL_CAP",
                "asset_namespace": "V25_TWO_STAGE",
                "default_config": {
                    "early_model_path": "/home/lc999/data/rl_models/v25/early.pt",
                    "late_model_path": "/home/lc999/data/rl_models/v25/late.pt",
                },
            }
        ],
        resolver=resolver,
    )

    assert resolver.calls == [
        ("V25_1_SMALL_CAP", "early_model_path", False),
        ("V25_1_SMALL_CAP", "late_model_path", False),
    ]


def test_real_resolver_validates_hashed_cache_with_top_level_asset_namespace(tmp_path: Path) -> None:
    cache_root = tmp_path / "cache"
    resolver = ModelAssetResolver(cache_root=cache_root)
    original_early = "/home/lc999/data/rl_models/v25/v25_early_net_joint_fixed.pt"
    original_late = "/home/lc999/data/rl_models/v25/v25_late_net_joint_fixed.pt"
    for original_path, payload in [(original_early, b"early"), (original_late, b"late")]:
        destination = resolver._cache_destination("V25_TWO_STAGE", original_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(payload)
        resolver._sidecar_path(destination).write_text(
            json.dumps(
                {
                    "algo_code": "V25_TWO_STAGE",
                    "asset_namespace": "V25_TWO_STAGE",
                    "original_path": original_path,
                    "cached_size": len(payload),
                }
            ),
            encoding="utf-8",
        )

    results = validate_enabled_default_config_model_paths(
        [
            {
                "algo_code": "V25_1_SMALL_CAP",
                "asset_namespace": "V25_TWO_STAGE",
                "default_config": {
                    "early_model_path": original_early,
                    "late_model_path": original_late,
                },
            }
        ],
        resolver=resolver,
    )

    assert [result.config_key for result in results] == ["early_model_path", "late_model_path"]
    assert all("V25_TWO_STAGE" in str(result.resolved_path) for result in results)


def test_db_helper_loads_enabled_rows_from_db_api_connection() -> None:
    class Cursor:
        description = [("algo_code",), ("default_config",), ("is_enabled",), ("asset_namespace",)]

        def __init__(self) -> None:
            self.sql = ""

        def execute(self, sql: str) -> None:
            self.sql = sql

        def fetchone(self):
            return (True,)

        def fetchall(self):
            return [
                (
                    "V24_PLAN",
                    {"model_path": "/home/lc999/data/rl_models/v24/model.pt"},
                    True,
                    None,
                )
            ]

    class Connection:
        def __init__(self) -> None:
            self.cursor_obj = Cursor()

        def cursor(self) -> Cursor:
            return self.cursor_obj

    resolver = RecordingResolver()
    connection = Connection()

    validate_enabled_default_config_model_paths_from_db(connection, resolver=resolver)

    assert "execution_algorithm_catalog" in connection.cursor_obj.sql
    assert resolver.calls == [("V24_PLAN", "model_path", False)]


def test_catalog_migration_helper_rolls_back_before_commit_on_missing_asset() -> None:
    class Cursor:
        description = [("algo_code",), ("default_config",), ("is_enabled",), ("asset_namespace",)]

        def __init__(self, connection) -> None:
            self.connection = connection

        def execute(self, sql: str) -> None:
            self.connection.executed.append(sql)

        def fetchone(self):
            return (True,)

        def fetchall(self):
            return [
                (
                    "V24_PLAN",
                    {"model_path": "/home/lc999/data/rl_models/v24/missing.pt"},
                    True,
                    None,
                )
            ]

    class Connection:
        def __init__(self) -> None:
            self.executed: list[str] = []
            self.committed = False
            self.rolled_back = False

        def cursor(self) -> Cursor:
            return Cursor(self)

        def commit(self) -> None:
            self.committed = True

        def rollback(self) -> None:
            self.rolled_back = True

    connection = Connection()
    resolver = RecordingResolver(fail_key="model_path")

    with pytest.raises(ExecutionAlgoArtifactMissingError):
        run_execution_algo_catalog_migration_with_validation(
            connection,
            migration_sql="INSERT INTO public.execution_algorithm_catalog VALUES ('x')",
            resolver=resolver,
        )

    assert connection.executed[0].startswith("INSERT INTO public.execution_algorithm_catalog")
    assert connection.committed is False
    assert connection.rolled_back is True
