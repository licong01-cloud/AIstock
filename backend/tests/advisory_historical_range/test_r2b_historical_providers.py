from __future__ import annotations

from datetime import UTC, date, datetime

from backend.services.advisory_historical_range.catalog_postgres import _PostgresRequirementResolver
from backend.services.advisory_historical_range.models import (
    HistoricalRangeCatalogPhase,
    HistoricalRangeRequirementPurpose,
    HistoricalRangeRevisionAdmissibility,
    HistoricalRangeSourceRequirementV1,
    HistoricalRangeSourceRevisionMemberV1,
)
from backend.tests.advisory_historical_range.conftest import digest
from backend.services.selection_center.runtime_profile import RuntimeRiskPolicyProfile
from backend.services.strategy_package.historical_selection_providers import (
    build_historical_range_read_only_providers,
)


class _Cursor:
    def __init__(self, calls) -> None:  # noqa: ANN001
        self.calls = calls

    def __enter__(self):  # noqa: ANN204
        return self

    def __exit__(self, *_args):  # noqa: ANN002, ANN204
        return False

    def execute(self, query, params):  # noqa: ANN001, ANN201
        self.calls.append((" ".join(str(query).split()), params))

    def fetchall(self):  # noqa: ANN201
        return [("000001.SZ", date(2020, 1, 1), date(2030, 1, 1), None, None, "v1", {})]


class _Connection:
    def __init__(self, calls) -> None:  # noqa: ANN001
        self.calls = calls

    def __enter__(self):  # noqa: ANN204
        return self

    def __exit__(self, *_args):  # noqa: ANN002, ANN204
        return False

    def cursor(self):  # noqa: ANN201
        return _Cursor(self.calls)

    def set_session(self, **kwargs):  # noqa: ANN003, ANN201
        self.calls.append(("SET_SESSION", kwargs))


def test_historical_st_provider_uses_explicit_connection_without_readiness_query() -> None:
    calls = []
    providers = build_historical_range_read_only_providers(conn_factory=lambda: _Connection(calls))
    profile = RuntimeRiskPolicyProfile(
        enabled=True,
        providers=("st_pit",),
        strict_data_ready=True,
    )

    decisions = providers.risk_policy.evaluate(
        symbols=["000001.SZ"],
        trade_date=date(2026, 6, 2),
        profile=profile,
    )

    assert decisions["000001.SZ"].can_buy is True
    assert calls[0] == (
        "SET_SESSION",
        {"isolation_level": "REPEATABLE READ", "readonly": True, "autocommit": False},
    )
    assert len(calls) == 2
    assert "stock_universe_pit_spans" in calls[1][0]
    assert "stock_universe_pit_state" not in calls[1][0]


def test_catalog_resolver_binds_exact_dependency_revisions_into_member_identity() -> None:
    dependency = HistoricalRangeSourceRevisionMemberV1(
        requirement_id="universe",
        source_role="pit_universe",
        dataset_id="market.stock_universe_pit",
        partition_ref="universe:2026-06-02",
        decision_trade_date=date(2026, 6, 2),
        query_template_id="historical_pit_universe_existing_readonly",
        query_template_version="v1",
        query_template_hash=digest("universe-query"),
        bound_parameters={"trade_date": "2026-06-02"},
        parameter_hash=digest({"trade_date": "2026-06-02"}),
        row_count=1,
        content_hash=digest(["000001.SZ"]),
        admissibility=HistoricalRangeRevisionAdmissibility.RETROSPECTIVE_DB_CONTENT_HASH,
        observed_at=datetime(2026, 7, 20, tzinfo=UTC),
    )
    requirement = HistoricalRangeSourceRequirementV1(
        requirement_id="package-assets",
        source_role="package_runtime_assets",
        dataset_id="strategy_pkg.package_manifest_assets",
        query_template_id="frozen_artifact_identity",
        query_template_version="v1",
        query_template_hash=digest("asset-query"),
        parameter_template={"content_hash": digest("assets"), "row_count": 1},
        partition_ref_template="package:pkg-test",
        depends_on_requirement_ids=("universe",),
        required_for=HistoricalRangeRequirementPurpose.REQUEST_SEAL,
        missing_reason_code="ADVISORY_HR_PACKAGE_RUNTIME_ASSET_UNAVAILABLE",
    )
    resolver = _PostgresRequirementResolver(cur=object(), observed_at=datetime(2026, 7, 20, tzinfo=UTC))

    member = resolver.resolve(
        requirement=requirement,
        dependency_members={"universe": dependency},
        phase=HistoricalRangeCatalogPhase.DISCOVER,
        expected_member=None,
    )

    assert member.bound_parameters["dependency_revision_refs"] == [
        {
            "requirement_id": "universe",
            "revision_id": dependency.revision_id,
            "revision_hash": dependency.revision_hash,
        }
    ]
    assert member.partition_ref.endswith(f"|deps:{digest(member.bound_parameters['dependency_revision_refs'])[:24]}")
