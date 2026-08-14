from __future__ import annotations

from backend.services.advisory_historical_range.outcome_policy_catalog import (
    R4_DEFAULT_HORIZONS,
    R4_LONG_TREND_HORIZONS,
    load_historical_range_outcome_policy_catalog,
)
from backend.services.advisory_historical_range import composition
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import (
    Alpha158SchemaAsset,
    AlphaCombinationPolicy,
    AlphaComponent,
    AlphaLineage,
    AlphaMode,
    BacktestSummary,
    FactorAsset,
    ModelAsset,
    RuntimeAssetManifest,
    SourceType,
    StrategyPackageManifest,
    StrategyPackageSource,
)


def test_options_use_exact_r4_catalog_without_new_policy() -> None:
    catalog = load_historical_range_outcome_policy_catalog()
    assert catalog.default_horizons == R4_DEFAULT_HORIZONS
    assert catalog.long_trend_horizons == R4_LONG_TREND_HORIZONS
    assert len(catalog.catalog_content_hash) == 64


class _Cursor:
    def __init__(self, rows):
        self.rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, statement, params):
        assert "LIMIT" not in statement.upper()
        assert "manifest_json -> 'alpha_components'" in statement
        assert "manifest_json" in statement
        assert "AS alpha_count" in statement
        assert len(params[0]) == 4

    def fetchall(self):
        return self.rows


class _Connection:
    def __init__(self, rows):
        self.rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def set_session(self, **kwargs):
        assert kwargs == {"isolation_level": "REPEATABLE READ", "readonly": True, "autocommit": False}

    def cursor(self, **_kwargs):
        return _Cursor(self.rows)

    def rollback(self):
        pass


def test_options_projection_does_not_truncate_more_than_500_admitted_packages(monkeypatch) -> None:
    rows = [
        {
            "package_id": f"pkg_{index:04d}",
            "package_name": f"Package {index:04d}",
            "alpha_mode": "single_alpha",
            "alpha_count": 1,
            "manifest_sha256": f"{index:064x}"[-64:],
            "package_version": "1",
            "package_status": "SELECTION_ENABLED",
            "manifest_json": {},
        }
        for index in range(501)
    ]
    monkeypatch.setattr(
        composition,
        "AdvisoryProgramPGRepository",
        lambda **_kwargs: type("Programs", (), {"list_programs": lambda *_args, **_kwargs: []})(),
    )
    connection = _Connection(rows)
    monkeypatch.setattr(
        composition,
        "historical_read_only_connection_factory",
        lambda _factory: lambda: connection,
    )
    monkeypatch.setattr(composition, "_is_historical_range_package_ready", lambda _record: True)
    result = composition._project_historical_range_options(lambda: connection)
    projected = result["data"]["admitted_packages"]
    assert len(projected) == 501
    assert [item["package_id"] for item in projected] == sorted(item["package_id"] for item in projected)


def test_options_readiness_reuses_historical_input_projection_contract(monkeypatch) -> None:
    component = AlphaComponent(
        alpha_id="alpha_001",
        alpha_name="alpha_001",
        component_weight=1.0,
        factor_ids=["factor_001"],
        model_id="model_001",
        holding_period="5day",
        rebalance_frequency="1day",
        score_direction="higher_better",
        lineage=AlphaLineage(factor_artifact_refs=["factor_001"]),
    )
    manifest = freeze_manifest(
        StrategyPackageManifest(
            package_id="pkg_ready",
            package_name="Ready package",
            package_version="1",
            source=StrategyPackageSource(
                source_type=SourceType.CANDIDATE_STRATEGY_PACKAGE,
                source_id="unit",
            ),
            alpha_mode=AlphaMode.SINGLE_ALPHA,
            alpha_components=[component],
            alpha_combination_policy=AlphaCombinationPolicy(
                method="identity",
                weights={"alpha_001": 1.0},
            ),
            factor_set=[FactorAsset(factor_id="factor_001", factor_name="Momentum_20D")],
            model_asset=[ModelAsset(model_id="model_001")],
            runtime_assets=RuntimeAssetManifest(
                alpha158=Alpha158SchemaAsset(enabled=True, aliases=["ROC20"], alias_count=1)
            ),
            backtest_summary=BacktestSummary(ic=0.01),
        )
    )

    def record(value: StrategyPackageManifest) -> dict:
        return {
            "package_id": value.package_id,
            "package_version": value.package_version,
            "manifest_sha256": value.manifest_sha256,
            "manifest_json": value.model_dump(mode="json"),
        }

    assert composition._is_historical_range_package_ready(record(manifest)) is True
    tampered_manifest = manifest.model_copy(update={"package_name": "Tampered but old hash"})
    assert composition._is_historical_range_package_ready(record(tampered_manifest)) is False

    runtime_missing = freeze_manifest(
        manifest.model_copy(update={"runtime_assets": None, "manifest_sha256": None})
    )
    assert composition._is_historical_range_package_ready(
        record(runtime_missing)
    ) is False

    second_component = component.model_copy(
        update={
            "alpha_id": "alpha_002",
            "alpha_name": "alpha_002",
            "model_id": "model_002",
        }
    )
    incomplete_multi_alpha = freeze_manifest(
        manifest.model_copy(
            update={
                "alpha_mode": AlphaMode.MULTI_ALPHA,
                "alpha_components": [component, second_component],
                "alpha_combination_policy": AlphaCombinationPolicy(
                    method="weighted_sum",
                    weights={"alpha_001": 1.0},
                ),
                "manifest_sha256": None,
            }
        )
    )
    monkeypatch.setattr(composition, "project_historical_range_inputs", lambda _manifest: object())
    assert composition._is_historical_range_package_ready(record(incomplete_multi_alpha)) is False
