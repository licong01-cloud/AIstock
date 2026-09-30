from __future__ import annotations

from datetime import date
from contextlib import nullcontext
import json

import pandas as pd
import pytest

from backend.data_service.security_source_identity import (
    MONEYFLOW_DATASET,
    SecuritySourceIdentityError,
    canonical_sha256,
    load_default_security_source_identity_manifest,
    load_security_source_identity_manifest,
)
from backend.data_service import qe_data_service
from backend.data_service.moneyflow_contract import (
    TUSHARE_MONEYFLOW_AMOUNT_COLUMNS,
    TUSHARE_MONEYFLOW_VOLUME_COLUMNS,
)
from backend.services.dataset_release.direct_monthly import _filter_moneyflow_source_to_pit


def test_default_authority_resolves_historical_moneyflow_code_and_boundary() -> None:
    manifest = load_default_security_source_identity_manifest()

    before = manifest.resolve("302132.SZ", date(2025, 2, 16), MONEYFLOW_DATASET)
    after = manifest.resolve("302132.SZ", date(2025, 2, 17), MONEYFLOW_DATASET)

    assert before.source_ts_code == "300114.SZ"
    assert before.resolution_kind == "explicit_effective_alias"
    assert after.source_ts_code == "302132.SZ"
    assert after.resolution_kind == "canonical_same_code"
    assert manifest.query_source_codes(
        ["302132.SZ"], date(2024, 8, 13), date(2025, 2, 17), MONEYFLOW_DATASET
    ) == ["300114.SZ", "302132.SZ"]


def test_source_rows_are_remapped_to_canonical_without_changing_values() -> None:
    manifest = load_default_security_source_identity_manifest()
    source = pd.DataFrame(
        {
            "trade_date": [date(2025, 2, 14), date(2025, 2, 17)],
            "ts_code": ["300114.SZ", "302132.SZ"],
            "net_mf_amount": [12.5, 13.5],
        }
    )

    actual = manifest.remap_source_rows(
        source,
        canonical_codes=["302132.SZ"],
        source_dataset=MONEYFLOW_DATASET,
    )

    assert actual["ts_code"].tolist() == ["302132.SZ", "302132.SZ"]
    assert actual["net_mf_amount"].tolist() == [12.5, 13.5]


def test_load_moneyflow_queries_historical_source_and_returns_canonical_units(monkeypatch) -> None:
    captured: dict[str, object] = {}
    raw = {
        "trade_date": [date(2025, 2, 14)],
        "ts_code": ["300114.SZ"],
        **{column: [2.0] for column in TUSHARE_MONEYFLOW_VOLUME_COLUMNS},
        **{column: [3.0] for column in TUSHARE_MONEYFLOW_AMOUNT_COLUMNS},
    }

    def fake_read_sql(sql, _conn, params):
        captured["sql"] = sql
        captured["params"] = params
        return pd.DataFrame(raw)

    qe_data_service.clear_data_cache()
    monkeypatch.setattr(qe_data_service, "get_conn", lambda: nullcontext(object()))
    monkeypatch.setattr(qe_data_service.pd, "read_sql", fake_read_sql)

    result = qe_data_service.load_moneyflow(
        ["302132.SZ"],
        date(2025, 2, 14),
        date(2025, 2, 14),
    )

    assert captured["params"][:2] == ["300114.SZ", "302132.SZ"]
    assert result.index.tolist() == [(pd.Timestamp("2025-02-14"), "302132.SZ")]
    assert result["mf_net_vol"].iat[0] == 200.0
    assert result["mf_net_amt"].iat[0] == 30_000.0


def test_load_moneyflow_can_preserve_source_index_for_frozen_h5(monkeypatch) -> None:
    raw = {
        "trade_date": [date(2025, 2, 14)],
        "ts_code": ["300114.SZ"],
        **{column: [2.0] for column in TUSHARE_MONEYFLOW_VOLUME_COLUMNS},
        **{column: [3.0] for column in TUSHARE_MONEYFLOW_AMOUNT_COLUMNS},
    }
    qe_data_service.clear_data_cache()
    monkeypatch.setattr(qe_data_service, "get_conn", lambda: nullcontext(object()))
    monkeypatch.setattr(qe_data_service.pd, "read_sql", lambda *_args, **_kwargs: pd.DataFrame(raw))

    source = qe_data_service.load_moneyflow(
        ["302132.SZ"],
        date(2025, 2, 14),
        date(2025, 2, 14),
        preserve_source_codes=True,
    )
    canonical = qe_data_service.canonicalize_moneyflow_source_frame(source, ["302132.SZ"])

    assert source.index.tolist() == [(pd.Timestamp("2025-02-14"), "300114.SZ")]
    assert canonical.index.tolist() == [(pd.Timestamp("2025-02-14"), "302132.SZ")]
    pd.testing.assert_series_equal(source.iloc[0], canonical.iloc[0], check_names=False)


def test_direct_builder_applies_canonical_pit_span_without_losing_source_code() -> None:
    index = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2024-08-12"), "300114.SZ"),
            (pd.Timestamp("2024-08-13"), "300114.SZ"),
        ],
        names=["datetime", "instrument"],
    )
    source = pd.DataFrame({"mf_net_amt": [1.0, 2.0]}, index=index)
    spans = pd.DataFrame(
        {
            "ts_code": ["302132.SZ"],
            "eligible_start": [pd.Timestamp("2024-08-13")],
            "eligible_end": [pd.Timestamp("2025-02-16")],
        }
    )

    actual = _filter_moneyflow_source_to_pit(
        source,
        spans=spans,
        start=date(2024, 8, 1),
        end=date(2024, 8, 31),
        security_identity=load_default_security_source_identity_manifest(),
        canonical_codes=["302132.SZ"],
    )

    assert actual.index.tolist() == [(pd.Timestamp("2024-08-13"), "300114.SZ")]
    assert actual["mf_net_amt"].tolist() == [2.0]


def test_overlapping_alias_authority_fails_closed(tmp_path) -> None:
    source = load_default_security_source_identity_manifest().source_path
    payload = json.loads(source.read_text(encoding="utf-8"))
    duplicate = dict(payload["rows"][0])
    duplicate["security_identity_id"] = "duplicate"
    body = {key: duplicate[key] for key in sorted(set(duplicate) - {"row_hash"})}
    duplicate["row_hash"] = canonical_sha256(body)
    payload["rows"].append(duplicate)
    path = tmp_path / "identity.json"
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    with pytest.raises(SecuritySourceIdentityError, match="uniquely ordered|overlap"):
        load_security_source_identity_manifest(path)
