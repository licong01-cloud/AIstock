from __future__ import annotations

import pytest

from backend.services.simulation_runtime.daily_limit_authority import (
    DailyLimitAuthorityContractError,
    allowed_daily_limit_authorities,
    assert_daily_limit_authorities_for_broker,
    required_daily_limit_resolver,
)
from backend.services.simulation_data.daily_context import (
    DailyLimitAuthorityV2,
    DailyLimitResolverV2,
    SimulationBrokerBackend,
)


def test_localsim_broker_matrix_allows_only_stk_limit_tdx_and_explicit_states() -> None:
    allowed = allowed_daily_limit_authorities(SimulationBrokerBackend.LOCAL_SIM)

    assert allowed == frozenset(
        {
            DailyLimitAuthorityV2.TUSHARE_STK_LIMIT,
            DailyLimitAuthorityV2.TDX_REFERENCE_DERIVED_V1,
            DailyLimitAuthorityV2.NO_DAILY_LIMIT,
            DailyLimitAuthorityV2.UNAVAILABLE,
        }
    )
    assert required_daily_limit_resolver("local_sim") is DailyLimitResolverV2.LOCALSIM_STK_LIMIT_TDX_V1
    assert_daily_limit_authorities_for_broker(
        broker_backend="local_sim",
        authorities={
            DailyLimitAuthorityV2.TUSHARE_STK_LIMIT,
            DailyLimitAuthorityV2.TDX_REFERENCE_DERIVED_V1,
        },
    )
    with pytest.raises(DailyLimitAuthorityContractError, match="not permitted for local_sim"):
        assert_daily_limit_authorities_for_broker(
            broker_backend="local_sim",
            authorities={DailyLimitAuthorityV2.MINIQMT_INSTRUMENT_DETAIL_V1},
        )


def test_miniqmt_broker_matrix_allows_only_direct_instrument_and_explicit_states() -> None:
    allowed = allowed_daily_limit_authorities(SimulationBrokerBackend.MINIQMT_SIM)

    assert allowed == frozenset(
        {
            DailyLimitAuthorityV2.MINIQMT_INSTRUMENT_DETAIL_V1,
            DailyLimitAuthorityV2.NO_DAILY_LIMIT,
            DailyLimitAuthorityV2.UNAVAILABLE,
        }
    )
    assert required_daily_limit_resolver("minqmt_sim") is DailyLimitResolverV2.MINIQMT_INSTRUMENT_DETAIL_V1
    assert_daily_limit_authorities_for_broker(
        broker_backend="minqmt_sim",
        authorities={DailyLimitAuthorityV2.MINIQMT_INSTRUMENT_DETAIL_V1},
    )
    for forbidden in (
        DailyLimitAuthorityV2.TUSHARE_STK_LIMIT,
        DailyLimitAuthorityV2.TDX_REFERENCE_DERIVED_V1,
    ):
        with pytest.raises(DailyLimitAuthorityContractError, match="not permitted for minqmt_sim"):
            assert_daily_limit_authorities_for_broker(
                broker_backend="minqmt_sim",
                authorities={forbidden},
            )


@pytest.mark.parametrize("backend", ["", "qmt", "MINIQMT_SIM", None])
def test_broker_matrix_rejects_unknown_or_coerced_backend(backend: object) -> None:
    with pytest.raises(DailyLimitAuthorityContractError, match="broker backend is invalid"):
        allowed_daily_limit_authorities(backend)  # type: ignore[arg-type]


def test_broker_matrix_rejects_empty_and_unknown_authority_sets() -> None:
    with pytest.raises(DailyLimitAuthorityContractError, match="must not be empty"):
        assert_daily_limit_authorities_for_broker(
            broker_backend="local_sim",
            authorities=set(),
        )
    with pytest.raises(DailyLimitAuthorityContractError, match="unknown value"):
        assert_daily_limit_authorities_for_broker(
            broker_backend="local_sim",
            authorities={"UNREGISTERED_AUTHORITY"},
        )
