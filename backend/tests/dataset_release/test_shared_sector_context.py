from __future__ import annotations

import datetime as dt
import json
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.dataset_release.canonical import digest_named_fields
from backend.services.dataset_release.shared_sector_context import (
    RELEASE_SW_L2_CODE_MAP_SCHEMA,
    RELEASE_SW_L2_MEMBER_BACKED_SCHEMA,
    build_release_sw_l2_code_map_payload,
    validate_market_context_frame,
    validate_membership_frame,
    validate_release_sw_l2_code_map,
)
from scripts.build_shared_sector_context_component import (
    _build_authority_membership_spans,
    _derive_release_code_map,
    build_component,
)


def _authority_inputs(tmp_path, monkeypatch):
    authority_path = tmp_path / "industry_pit_authority.json"
    authority_path.write_text("{}", encoding="utf-8")
    universe_path = tmp_path / "stock_universe.txt"
    universe_path.write_text(
        "000001.SZ\t2024-07-01\t2024-07-03\n000002.SZ\t2024-07-01\t2024-07-03\n",
        encoding="utf-8",
    )
    calendar_path = tmp_path / "day.txt"
    calendar_path.write_text(
        "2024-07-01\n2024-07-02\n2024-07-03\n",
        encoding="utf-8",
    )
    adapter = SimpleNamespace(
        classification_resolver=SimpleNamespace(transition_dates=lambda _symbol: ()),
        resolve=lambda symbol, _day: SimpleNamespace(
            status="resolved",
            l2_code="801000.SI" if symbol == "000001.SZ" else "801001.SI",
            reason_code=None,
        ),
    )
    monkeypatch.setattr(
        "scripts.build_shared_sector_context_component._load_industry_adapter",
        lambda _path: (adapter, {"identity": {"schema_version": "fixture"}}),
    )
    return authority_path, universe_path, calendar_path


def _pool_sidecars(universe_path):
    return {
        pool_id: universe_path
        for pool_id in ("csi300", "csi500", "csi1000", "star50", "star100")
    }


def _code_map() -> dict[str, object]:
    authority = {"authority_id": "fixture", "authority_sha256": "a" * 64}
    entries = [{"l2_code_id": index * 2 + 1, "canonical_l2_code": f"801{index:03d}.SI"} for index in range(131)]
    codes = sorted(row["canonical_l2_code"] for row in entries)
    return {
        "schema_version": RELEASE_SW_L2_CODE_MAP_SCHEMA,
        "mapping_authority": authority,
        "entries": entries,
        "member_backed_codes": codes,
        "code_map_digest": digest_named_fields(
            RELEASE_SW_L2_CODE_MAP_SCHEMA,
            {"mapping_authority": authority, "entries": entries},
        ),
        "member_backed_digest": digest_named_fields(
            RELEASE_SW_L2_MEMBER_BACKED_SCHEMA,
            {"mapping_authority": authority, "member_backed_codes": codes},
        ),
    }


def test_release_code_map_preserves_sparse_shared_ids() -> None:
    result = validate_release_sw_l2_code_map(_code_map())
    assert len(result.member_backed_codes) == 131
    assert result.id_to_code[261] == "801130.SI"
    assert result.code_to_id["801130.SI"] == 261


def test_release_code_map_builder_preserves_producer_ids() -> None:
    code_to_id = {f"801{index:03d}.SI": index * 2 + 1 for index in range(131)}

    payload = build_release_sw_l2_code_map_payload(
        code_to_id=code_to_id,
        member_backed_codes=list(code_to_id),
        authority_id="producer-snapshot",
        authority_sha256="a" * 64,
    )

    assert payload["entries"][130] == {
        "l2_code_id": 261,
        "canonical_l2_code": "801130.SI",
    }
    assert validate_release_sw_l2_code_map(payload).id_to_code[261] == "801130.SI"


def test_release_code_map_derivation_closes_unique_sparse_positions() -> None:
    codes = [f"801{index:03d}.SI" for index in range(131)]
    available_ids = [value for value in range(134) if value not in {83, 111, 117}]
    expected = dict(zip(codes, available_ids))
    missing_ranks = {0, 3, 6, 43, 66, 75, 76, 79, 92, 96, 102, 104}
    rows = []
    code_by_symbol = {}
    for rank, code in enumerate(codes):
        if rank in missing_ranks:
            continue
        symbol = f"{rank + 1:06d}.SZ"
        code_by_symbol[symbol] = code
        rows.append(
            {
                "datetime": pd.Timestamp("2026-08-28"),
                "instrument": symbol,
                "l2_code_id": expected[code],
                "sw2_vol": 1.0,
            }
        )
    adapter = SimpleNamespace(
        resolve=lambda symbol, _day: SimpleNamespace(
            status="resolved",
            l2_code=code_by_symbol[symbol],
            reason_code=None,
        )
    )
    envelope = {
        "l2_projection": {
            "canonical_hash": "c" * 64,
            "rows": [{"canonical_l2_code": code} for code in codes],
        }
    }

    payload, derivation = _derive_release_code_map(
        frame=pd.DataFrame(rows),
        adapter=adapter,
        envelope=envelope,
        start=dt.date(2024, 7, 1),
        end=dt.date(2026, 8, 28),
        sector_data_sha256="a" * 64,
        authority_envelope_sha256="b" * 64,
    )

    result = validate_release_sw_l2_code_map(payload)
    assert result.code_to_id == expected
    assert derivation["observed_count"] == 119
    assert derivation["derived_count"] == 12
    assert derivation["ambiguity_count"] == 0


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda value: value["entries"].__setitem__(1, dict(value["entries"][0])), "duplicated l2_code_id"),
        (
            lambda value: value["entries"][1].__setitem__(
                "canonical_l2_code", value["entries"][0]["canonical_l2_code"]
            ),
            "duplicated canonical_l2_code",
        ),
        (lambda value: value["member_backed_codes"].pop(), "exactly 131"),
    ],
)
def test_release_code_map_rejects_ambiguous_or_incomplete_values(mutation, message) -> None:
    payload = _code_map()
    mutation(payload)
    with pytest.raises(ValueError, match=message):
        validate_release_sw_l2_code_map(payload)


def test_market_context_requires_unique_positive_rows_and_window() -> None:
    frame = pd.DataFrame(
        {
            "trade_date": [dt.date(2024, 7, 1), dt.date(2024, 7, 2)],
            "sw_daily_total_vol": [1.0, 2.0],
        }
    )
    assert validate_market_context_frame(
        frame,
        required_start=dt.date(2024, 7, 1),
        required_end=dt.date(2024, 7, 2),
    ) == (dt.date(2024, 7, 1), dt.date(2024, 7, 2), 2)
    frame.loc[1, "sw_daily_total_vol"] = 0.0
    with pytest.raises(ValueError, match="non-positive"):
        validate_market_context_frame(frame)


def test_component_market_context_uses_sql_sum_null_semantics() -> None:
    from scripts.build_shared_sector_context_component import _build_market_context

    frame = pd.DataFrame(
        {
            "datetime": [pd.Timestamp("2024-07-01"), pd.Timestamp("2024-07-01")],
            "l2_code_id": [1, 3],
            "sw2_vol": [None, 10.0],
        }
    )

    result = _build_market_context(frame)

    assert result.to_dict("records") == [{"trade_date": dt.date(2024, 7, 1), "sw_daily_total_vol": 10.0}]


def test_membership_accepts_sparse_ids_and_rejects_overlap() -> None:
    frame = pd.DataFrame(
        {
            "instrument": ["000001.SZ", "000001.SZ", "600000.SH"],
            "start_date": [dt.date(2024, 7, 1), dt.date(2024, 7, 3), dt.date(2024, 7, 1)],
            "end_date": [dt.date(2024, 7, 2), dt.date(2024, 7, 4), dt.date(2024, 7, 4)],
            "l2_code_id": [1, 261, 261],
        }
    )
    assert validate_membership_frame(
        frame,
        id_to_code={1: "801000.SI", 261: "801130.SI"},
        required_start=dt.date(2024, 7, 1),
        required_end=dt.date(2024, 7, 4),
    ) == (dt.date(2024, 7, 1), dt.date(2024, 7, 4), 3, 2)
    frame.loc[1, "start_date"] = dt.date(2024, 7, 2)
    with pytest.raises(ValueError, match="overlap"):
        validate_membership_frame(frame, id_to_code={1: "801000.SI", 261: "801130.SI"})


def test_membership_resolves_historical_security_code_alias() -> None:
    from backend.services.hmm_risk.security_identity import SecuritySourceIdentityManifest
    from backend.services.hmm_risk.security_identity import SecuritySourceResolution

    adapter = SimpleNamespace(
        classification_resolver=SimpleNamespace(
            transition_dates=lambda symbol: (
                (dt.date(2021, 7, 30),) if symbol == "300114.SZ" else (dt.date(2025, 2, 17),)
            )
        ),
        resolve=lambda symbol, _day: SimpleNamespace(
            status="resolved",
            l2_code="801000.SI" if symbol == "300114.SZ" else "801001.SI",
            reason_code=None,
        ),
    )
    manifest = SecuritySourceIdentityManifest(
        manifest_version="security-v1",
        default_resolution="canonical_same_code",
        rows=(
            SecuritySourceResolution(
                security_identity_id="szse_300114_302132",
                canonical_ts_code="302132.SZ",
                source_dataset="market.moneyflow_ts",
                source_ts_code="300114.SZ",
                effective_start=dt.date(2010, 8, 27),
                effective_end=dt.date(2025, 2, 16),
                authority_ref="official",
                authority_hash="a" * 64,
                row_hash="b" * 64,
                resolution_kind="explicit_effective_alias",
            ),
        ),
        manifest_sha256="c" * 64,
        rows_sha256="d" * 64,
    )

    frame = _build_authority_membership_spans(
        adapter=adapter,
        universe_spans=[("302132.SZ", dt.date(2024, 7, 1), dt.date(2025, 2, 18))],
        calendar=[dt.date(2024, 7, 1), dt.date(2025, 2, 14), dt.date(2025, 2, 17), dt.date(2025, 2, 18)],
        code_to_id={"801000.SI": 1, "801001.SI": 3},
        start=dt.date(2024, 7, 1),
        end=dt.date(2025, 2, 18),
        security_identity=manifest,
    )

    assert frame.to_dict("records") == [
        {
            "instrument": "302132.SZ",
            "start_date": dt.date(2024, 7, 1),
            "end_date": dt.date(2025, 2, 14),
            "l2_code_id": 1,
        },
        {
            "instrument": "302132.SZ",
            "start_date": dt.date(2025, 2, 17),
            "end_date": dt.date(2025, 2, 18),
            "l2_code_id": 3,
        },
    ]


def test_component_builder_is_create_exclusive_and_uses_only_frozen_files(tmp_path, monkeypatch) -> None:
    authority_path, universe_path, calendar_path = _authority_inputs(tmp_path, monkeypatch)
    code_map_path = tmp_path / "sector_code_map.json"
    code_map_path.write_text(json.dumps(_code_map(), sort_keys=True), encoding="utf-8")
    index = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2024-07-01"), "000001.SZ"),
            (pd.Timestamp("2024-07-01"), "000002.SZ"),
            (pd.Timestamp("2024-07-02"), "000001.SZ"),
            (pd.Timestamp("2024-07-02"), "000002.SZ"),
        ],
        names=["datetime", "instrument"],
    )
    sector = pd.DataFrame(
        {
            "l2_code_id": [1, 3, 1, 3],
            "sw2_vol": [100.0, 200.0, 110.0, 210.0],
        },
        index=index,
    )
    sector_path = tmp_path / "sector_data.h5"
    sector.to_hdf(sector_path, key="data", format="table", data_columns=True)
    output_root = tmp_path / "component"

    result = build_component(
        sector_data_h5=sector_path,
        code_map_json=code_map_path,
        industry_pit_authority_envelope=authority_path,
        stock_universe_sidecar=universe_path,
        pool_sidecars=_pool_sidecars(universe_path),
        calendar_path=calendar_path,
        output_root=output_root,
        source_dataset_manifest_sha256="b" * 64,
        membership_start=dt.date(2024, 7, 1),
        membership_end=dt.date(2024, 7, 2),
    )

    assert result["database_read"] is False
    assert result["market_context"]["row_count"] == 2
    assert result["membership"]["span_count"] == 2
    assert result["membership"]["symbol_count"] == 2
    assert all(
        stats["trading_day_gap_count"] == 0
        for stats in result["membership"]["coverage"].values()
    )
    assert result["sector_code_map"]["path"] == "sector_code_map.json"
    assert result["sector_code_map"]["byte_size"] > 0
    assert result["market_context"]["schema_version"] == "aistock_market_context_v1"
    assert result["membership"]["schema_version"] == "aistock_sector_membership_spans_v1"
    with pytest.raises(FileExistsError, match="create-exclusive"):
        build_component(
            sector_data_h5=sector_path,
            code_map_json=code_map_path,
            industry_pit_authority_envelope=authority_path,
            stock_universe_sidecar=universe_path,
            pool_sidecars=_pool_sidecars(universe_path),
            calendar_path=calendar_path,
            output_root=output_root,
            source_dataset_manifest_sha256="b" * 64,
            membership_start=dt.date(2024, 7, 1),
            membership_end=dt.date(2024, 7, 2),
        )


def test_component_builder_uses_authority_across_h5_row_gaps(tmp_path, monkeypatch) -> None:
    authority_path, universe_path, calendar_path = _authority_inputs(tmp_path, monkeypatch)
    universe_path.write_text(
        universe_path.read_text(encoding="utf-8")
        + "000003.SZ\t2024-07-01\t2024-07-03\n",
        encoding="utf-8",
    )
    code_map_path = tmp_path / "sector_code_map.json"
    code_map_path.write_text(json.dumps(_code_map(), sort_keys=True), encoding="utf-8")
    index = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2024-07-01"), "000001.SZ"),
            (pd.Timestamp("2024-07-02"), "000002.SZ"),
            (pd.Timestamp("2024-07-03"), "000001.SZ"),
        ],
        names=["datetime", "instrument"],
    )
    sector = pd.DataFrame(
        {"l2_code_id": [1, 3, 1], "sw2_vol": [100.0, 200.0, 110.0]},
        index=index,
    )
    sector_path = tmp_path / "sector_data.h5"
    sector.to_hdf(sector_path, key="data", format="table", data_columns=True)

    result = build_component(
        sector_data_h5=sector_path,
        code_map_json=code_map_path,
        industry_pit_authority_envelope=authority_path,
        stock_universe_sidecar=universe_path,
        pool_sidecars=_pool_sidecars(universe_path),
        calendar_path=calendar_path,
        output_root=tmp_path / "component",
        source_dataset_manifest_sha256="b" * 64,
        membership_start=dt.date(2024, 7, 1),
        membership_end=dt.date(2024, 7, 3),
    )

    assert result["membership"]["symbol_count"] == 3
    assert result["membership"]["span_count"] == 3
    assert result["membership"]["coverage"]["stock_universe"]["eligible_symbol_count"] == 3
    assert result["membership"]["coverage"]["stock_universe"]["missing_symbol_count"] == 0
    assert result["membership"]["coverage"]["stock_universe"]["trading_day_gap_count"] == 0
