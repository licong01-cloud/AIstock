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


@pytest.mark.parametrize("dataset", ["market.cyq_perf", "market.bak_basic"])
def test_dense_stock_panels_share_identity_without_inheriting_other_source_aliases(dataset):
    manifest = load_default_security_source_identity_manifest()
    resolution = manifest.resolve("302132.SZ", date(2025, 1, 2), dataset)
    assert resolution.source_ts_code == "302132.SZ"
    assert resolution.resolution_kind == "canonical_same_code"
    assert manifest.resolve("302132.SZ", date(2025, 1, 2), MONEYFLOW_DATASET).source_ts_code == "300114.SZ"


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


def test_margin_detail_uses_explicit_shared_alias_and_exact_change_boundary():
    manifest = load_default_security_source_identity_manifest()
    for day in (date(2022, 10, 24), date(2024, 8, 13), date(2025, 2, 14), date(2025, 2, 16)):
        resolution = manifest.resolve('302132.SZ', day, 'market.margin_detail')
        assert resolution.source_ts_code == '300114.SZ'
        assert resolution.resolution_kind == 'explicit_effective_alias'
    assert manifest.resolve('302132.SZ', date(2025, 2, 17), 'market.margin_detail').source_ts_code == '302132.SZ'
    assert manifest.query_source_codes(['302132.SZ'], date(2022, 1, 1), date(2026, 10, 30),
        'market.margin_detail') == ['300114.SZ', '302132.SZ']
    assert manifest.resolve('000001.SZ', date(2024, 1, 2), 'market.margin_detail').source_ts_code == '000001.SZ'


def test_margin_alias_keeps_original_facts_units_and_unknown_values():
    manifest = load_default_security_source_identity_manifest()
    facts = pd.DataFrame(dict(ts_code=['300114.SZ', '302132.SZ'],
        trade_date=[date(2025, 2, 14), date(2025, 2, 17)],
        md_rzye=[123.0, 234.0], md_rqyl=[100, 0], md_rqche=[None, None]))
    before = facts.copy(deep=True)
    annotated = manifest.annotate_source_rows(facts, canonical_codes=['302132.SZ'],
        source_dataset='market.margin_detail')
    assert annotated['_canonical_ts_code'].tolist() == ['302132.SZ', '302132.SZ']
    pd.testing.assert_frame_equal(annotated.drop(columns='_canonical_ts_code'), facts)
    pd.testing.assert_frame_equal(before, facts)


def test_margin_alias_rejects_duplicate_canonical_facts_without_silent_precedence():
    manifest = load_default_security_source_identity_manifest()
    rows = pd.DataFrame(dict(ts_code=['300114.SZ', '300114.SZ'],
        trade_date=[date(2025, 2, 14)]*2, md_rzye=[1.0, 2.0]))
    with pytest.raises(SecuritySourceIdentityError, match='duplicate canonical source facts'):
        manifest.annotate_source_rows(rows, canonical_codes=['302132.SZ'], source_dataset='market.margin_detail')


def test_versioned_margin_authority_does_not_mutate_old_moneyflow_authority():
    from pathlib import Path
    from backend.data_service import security_source_identity as module
    old_path = Path(module.__file__).parent/'manifests/security_source_identity_v1.json'
    old = load_security_source_identity_manifest(old_path)
    new = load_default_security_source_identity_manifest()
    assert new.source_path != old.source_path
    assert new.manifest_version == 'aistock_security_source_identity_20261112_v2'
    assert [r for r in new.rows if r.source_dataset == MONEYFLOW_DATASET] == list(old.rows)
    assert old.resolve('302132.SZ', date(2025, 2, 14), 'market.margin_detail').source_ts_code == '302132.SZ'
    assert new.resolve('302132.SZ', date(2025, 2, 14), 'market.daily_basic').source_ts_code == '302132.SZ'


def test_margin_audit_reports_wrong_boundary_and_value_drift_without_repair():
    from backend.data_service.margin_detail_source_identity import compare_alias_rows
    identity = load_default_security_source_identity_manifest()
    frozen = pd.DataFrame(dict(ts_code=['300114.SZ', '302132.SZ'],
        trade_date=[date(2025, 2, 14), date(2025, 2, 17)], md_rzye=[123.0, 234.0]))
    source = frozen.copy()
    rows = compare_alias_rows(identity, '302132.SZ', frozen, source, ['md_rzye'])
    assert [r['classification'] for r in rows] == ['MATCHED', 'MATCHED']
    source.loc[0, 'md_rzye'] = 125
    rows = compare_alias_rows(identity, '302132.SZ', frozen, source, ['md_rzye'])
    assert rows[0]['classification'] == 'VALUE_MISMATCH'
    frozen.loc[0, 'ts_code'] = '302132.SZ'
    rows = compare_alias_rows(identity, '302132.SZ', frozen, source, ['md_rzye'])
    assert any(r['classification'] == 'SOURCE_IDENTITY_CONFLICT' for r in rows)


@pytest.mark.parametrize('canonical,source,last,next_day', [
    ('001872.SZ','000022.SZ',date(2018,12,25),date(2018,12,26)),
    ('001914.SZ','000043.SZ',date(2019,12,15),date(2019,12,16)),
    ('302132.SZ','300114.SZ',date(2025,2,16),date(2025,2,17)),
])
def test_all_margin_aliases_are_explicit_and_do_not_leak_to_other_sources(canonical,source,last,next_day):
    manifest = load_default_security_source_identity_manifest()
    assert manifest.resolve(canonical,last,'market.margin_detail').source_ts_code == source
    assert manifest.resolve(canonical,next_day,'market.margin_detail').source_ts_code == canonical
    for dataset in ('market.bak_basic','market.daily_basic','market.stk_limit'):
        assert manifest.resolve(canonical,last,dataset).source_ts_code == canonical
    assert manifest.resolve('689009.SH',last,'market.margin_detail').source_ts_code == '689009.SH'
    assert manifest.resolve('688009.SH',last,'market.margin_detail').source_ts_code == '688009.SH'


def test_margin_audit_preserves_source_nulls_and_exact_float32_representation():
    import numpy as np
    from backend.data_service.margin_detail_source_identity import compare_alias_rows
    identity = load_default_security_source_identity_manifest()
    source = pd.DataFrame(dict(ts_code=['300114.SZ'],trade_date=[date(2025,2,14)],
        md_rzye=[138819776.12],md_rqyl=[None]))
    frozen = source.copy()
    frozen['md_rzye'] = frozen['md_rzye'].astype('float32')
    frozen['md_rqyl'] = np.nan
    result = compare_alias_rows(identity,'302132.SZ',frozen,source,['md_rzye','md_rqyl'])
    assert result[0]['classification'] == 'MATCHED'
    assert result[0]['source_null_fields'] == ['md_rqyl']
    source['md_rqyl'] = 0
    assert compare_alias_rows(identity,'302132.SZ',frozen,source,['md_rzye','md_rqyl'])[0]['classification'] == 'VALUE_MISMATCH'


def test_margin_audit_does_not_silently_fill_missing_source_or_frozen_records():
    from backend.data_service.margin_detail_source_identity import compare_alias_rows
    identity = load_default_security_source_identity_manifest()
    frame = pd.DataFrame(dict(ts_code=['300114.SZ'],trade_date=[date(2025,2,14)],md_rzye=[1.0]))
    assert compare_alias_rows(identity,'302132.SZ',frame,frame.iloc[:0],['md_rzye'])[0]['classification'] == 'SOURCE_UNVERIFIED'
    assert compare_alias_rows(identity,'302132.SZ',frame.iloc[:0],frame,['md_rzye'])[0]['classification'] == 'EXPORT_MISSING'
    assert compare_alias_rows(identity,'302132.SZ',frame.iloc[:0],frame.iloc[:0],['md_rzye']) == []
    with pytest.raises(ValueError,match='duplicate financing source identity'):
        compare_alias_rows(identity,'302132.SZ',pd.concat([frame,frame]),frame,['md_rzye'])


def test_margin_package_cannot_overwrite_existing_immutable_output(tmp_path,monkeypatch):
    from backend.data_service.margin_detail_source_identity import main
    monkeypatch.setattr('sys.argv',['audit','--candidate-root','unused',
        '--identity-manifest','unused','--expected-manifest','unused','--output-root',str(tmp_path)])
    with pytest.raises(ValueError,match='immutable output already exists'):
        main()


def test_margin_package_cannot_write_inside_frozen_candidate(tmp_path,monkeypatch):
    from backend.data_service.margin_detail_source_identity import main
    monkeypatch.setattr('sys.argv',['audit','--candidate-root',str(tmp_path),
        '--identity-manifest','unused','--expected-manifest','unused','--output-root',str(tmp_path/'new-sidecar')])
    with pytest.raises(ValueError,match='outside the frozen candidate'):
        main()
