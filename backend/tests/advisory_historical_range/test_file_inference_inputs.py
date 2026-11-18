from datetime import date

import pytest

from backend.data_service.frozen_dataset import freeze_dataset_binding, extend_static_projection_binding
from backend.services.advisory_historical_range.inference_inputs import file_input_session
from backend.tests.data_service.test_frozen_dataset import DAYS, SYMBOL, make_dataset, add_static_projection


def test_native_file_price_view_has_causal_factor_and_consumer_owned_cache(tmp_path, monkeypatch):
    import psycopg2
    monkeypatch.setattr(psycopg2, "connect", lambda *_, **__: pytest.fail("file inference opened SQL"))
    binding = freeze_dataset_binding(make_dataset(tmp_path))
    first = file_input_session(consumer_scope="LOCALSIM", run_id="local-1", binding=binding, max_bytes=10**7)
    other = file_input_session(consumer_scope="ADVISORY", run_id="adv-1", binding=binding, max_bytes=10**7)
    early = first.price_reader([SYMBOL], DAYS[0], DAYS[1])
    assert early.close.tolist() == [10.5, 11.5]
    assert early.factor.tolist() == [1., 1.]
    assert early.volume.tolist() == [1000., 1000.]
    later = first.price_reader([SYMBOL], DAYS[0], DAYS[2])
    assert later.factor.tolist() == [.5, .5, 1.]
    model_later = first.model_price_reader([SYMBOL], DAYS[0], DAYS[2])
    assert model_later.close.tolist() == [5.25, 5.75, 999.]
    assert model_later.volume.tolist() == [2000., 2000., 2000.]
    # Frozen model inputs retain the release basis used by Qlib, even when
    # reading an earlier prefix; execution/raw prices still use that cutoff.
    assert first.model_price_reader([SYMBOL], DAYS[0], DAYS[1]).close.tolist() == [5.25, 5.75]
    again = first.price_reader([SYMBOL], DAYS[0], DAYS[1])
    assert again.equals(early) and other.retained_bytes == 0
    assert other.price_reader([SYMBOL], DAYS[0], DAYS[1]).equals(early)
    again.iloc[0, 0] = -1
    assert first.price_reader([SYMBOL], DAYS[0], DAYS[1]).iloc[0, 0] > 0
    assert first.universe_reader(DAYS[2], "TEST_PIT") == [SYMBOL]
    first.close()
    other.close()


def test_file_static_builder_is_original_code_with_private_readers(tmp_path):
    from backend.data_service.qe_data_service import build_static_factors
    binding = freeze_dataset_binding(make_dataset(tmp_path))
    session = file_input_session(consumer_scope="LOCALSIM", run_id="static", binding=binding, max_bytes=10**7)
    assert session.static_builder.__code__ is build_static_factors.__code__
    output = session.static_builder([SYMBOL], DAYS[0], DAYS[1])
    assert len(output) == 2 and "PriceStrength_10D" in output
    assert output.index.get_level_values("datetime").max().date() == DAYS[1]
    with pytest.raises(ValueError):
        session.price_reader([SYMBOL], DAYS[0], date(2026, 1, 22))
    session.close()


def test_frozen_model_static_view_preserves_source_na_before_reference_mask(tmp_path):
    import numpy as np
    import pandas as pd
    profile = make_dataset(tmp_path)
    source = tmp_path / "factors/cyq_perf.h5"
    values = pd.read_hdf(source, key="data")
    values["cp_his_high"] = [10., 10., np.nan, np.nan, 12., 12.]
    values.to_hdf(source, key="data", mode="w", format="table", data_columns=True)
    projection = values[["cp_his_high"]].copy()
    profile = add_static_projection(tmp_path, projection)
    binding = extend_static_projection_binding(freeze_dataset_binding(profile))
    session = file_input_session(consumer_scope="ADVISORY", run_id="raw-reference", binding=binding, max_bytes=10**7)
    raw = session.model_static_builder([SYMBOL], DAYS[0], DAYS[1])
    live = session.static_builder([SYMBOL], DAYS[0], DAYS[1], asof_fill_slow_static=True)
    assert raw["cp_his_high"].tolist()[0] == 10.
    assert pd.isna(raw["cp_his_high"].iloc[1])
    assert live["cp_his_high"].tolist() == [10., 10.]
    session.close()


def test_original_model_computation_domain_does_not_expand_decision_pit_pool(tmp_path):
    from backend.tests.data_service.test_frozen_dataset import HALT
    binding = freeze_dataset_binding(make_dataset(tmp_path))
    session = file_input_session(consumer_scope="ADVISORY", run_id="original-domain", binding=binding, max_bytes=10**7)
    assert session.universe_reader(DAYS[2], "TEST_PIT") == [SYMBOL]
    assert session.model_universe_reader(DAYS[0], DAYS[2]) == sorted([SYMBOL, HALT])
    assert session.universe_reader(DAYS[2], "TEST_PIT") == [SYMBOL]
    assert session.file_dataset.read_receipts[-1]["read_purpose"] == "factor_computation_domain_only"
    session.close()


def test_frozen_model_static_domain_keeps_real_industry_rows_without_filling_basic(tmp_path):
    import pandas as pd
    profile = make_dataset(tmp_path)
    key = (pd.Timestamp(DAYS[1]), SYMBOL)
    basic_path = tmp_path / "factors/daily_basic.h5"
    basic = pd.read_hdf(basic_path, key="data").drop(index=key)
    basic.to_hdf(basic_path, key="data", mode="w", format="table", data_columns=True)
    sector_path = tmp_path / "factors/sector_data.h5"
    sector = pd.read_hdf(sector_path, key="data")
    sector["sw2_close"] = 100.
    sector.to_hdf(sector_path, key="data", mode="w", format="table", data_columns=True)
    projection = pd.DataFrame(index=pd.read_hdf(tmp_path / "factors/daily_pv.h5", key="data").index)
    projection["sw2_close"] = 100.
    projection["value"] = basic["value"]
    profile = add_static_projection(tmp_path, projection)
    binding = extend_static_projection_binding(freeze_dataset_binding(profile))
    session = file_input_session(consumer_scope="ADVISORY", run_id="source-domain", binding=binding, max_bytes=10**7)
    legacy = session.static_builder([SYMBOL], DAYS[0], DAYS[1])
    model = session.model_static_builder([SYMBOL], DAYS[0], DAYS[1])
    assert key not in legacy.index
    assert model.loc[key, "sw2_close"] == 100.
    assert pd.isna(model.loc[key, "value"])
    session.close()


def test_engine_uses_daily_native_pool_and_refuses_a_relabelled_rule(tmp_path, monkeypatch):
    from datetime import datetime
    from types import SimpleNamespace
    from backend.inference_engine import InferenceEngine
    from backend.services.canonical_pit_inference_boundary import InferencePitMode
    from backend.data_service.frozen_dataset import FrozenDatasetError
    binding = freeze_dataset_binding(make_dataset(tmp_path))
    session = file_input_session(consumer_scope="LOCALSIM", run_id="native-pit", binding=binding, max_bytes=10**7)
    engine = object.__new__(InferenceEngine)
    engine.input_session = session
    authority = SimpleNamespace(coverage_start=DAYS[0], coverage_end=DAYS[-1], universe_key="TEST_PIT", rule_version="test-v1")
    pit = SimpleNamespace(mode=InferencePitMode.ROLLING_RUNTIME, binding=authority)
    assert engine._get_default_universe_excluding_st(datetime(2026, 1, 21), pit_identity=pit) == [SYMBOL]
    assert engine._get_latest_available_trade_date(datetime(2026, 1, 20)).date() == DAYS[1]
    authority.rule_version = "different"
    with pytest.raises(FrozenDatasetError, match="rule version") as mismatch:
        engine._get_default_universe_excluding_st(datetime(2026, 1, 20), pit_identity=pit)
    assert mismatch.value.context["reason_code"] == "FROZEN_DATASET_PIT_IDENTITY_MISMATCH"
    session.close()


def test_shared_cli_explicit_file_factory_requires_the_bound_revision(tmp_path):
    import json
    from types import SimpleNamespace
    from scripts.strategy_package_live_inference import _input_session
    binding = freeze_dataset_binding(make_dataset(tmp_path))
    path = tmp_path / "binding.json"
    path.write_text(json.dumps(binding), encoding="utf-8")
    args = SimpleNamespace(dataset_binding=str(path), dataset_root=None, consumer_scope="ADVISORY", input_revision=binding["binding_hash"])
    session = _input_session(args, run_id="advisory-own-file", source_identity=binding["binding_hash"], max_bytes=0)
    assert session.consumer_scope == "ADVISORY" and session.file_dataset.identity == binding["binding_hash"]
    session.close()
    args.input_revision = "f" * 64
    with pytest.raises(ValueError, match="revision"):
        _input_session(args, run_id="wrong", source_identity=args.input_revision, max_bytes=0)


def test_cli_failure_receipt_preserves_the_native_file_identity_reason(tmp_path, monkeypatch):
    import json
    import sys
    from datetime import datetime
    from types import SimpleNamespace
    from backend.data_service.frozen_dataset import FrozenDatasetError
    from backend.inference_engine import InferenceEngine
    from backend.services.canonical_pit_inference_boundary import InferencePitMode
    from backend.tests.advisory_historical_range.test_strategy_package_live_inference import _batch_args
    from scripts import strategy_package_live_inference as runner

    binding = freeze_dataset_binding(make_dataset(tmp_path))
    path = tmp_path / "file-binding.json"
    path.write_text(json.dumps(binding), encoding="utf-8")
    output = _batch_args(tmp_path, monkeypatch, [{"trade_date": "2026-01-20", "cutoff_date": "2026-01-19"}])
    sys.argv[sys.argv.index("--input-revision") + 1] = binding["binding_hash"]
    sys.argv.extend(["--dataset-binding", str(path), "--consumer-scope", "LOCALSIM"])

    class NativePolicyEngine:
        def __init__(self, *, input_session, **_kwargs):
            self.engine = object.__new__(InferenceEngine)
            self.engine.input_session = input_session

        def run_inference(self, **_kwargs):
            authority = SimpleNamespace(coverage_start=DAYS[0], coverage_end=DAYS[-1],
                                        universe_key="TEST_PIT", rule_version="different")
            pit = SimpleNamespace(mode=InferencePitMode.ROLLING_RUNTIME, binding=authority)
            return self.engine._get_default_universe_excluding_st(datetime(2026, 1, 19), pit_identity=pit)

    monkeypatch.setattr(runner, "InferenceEngine", NativePolicyEngine)
    # This engine stops at PIT resolution, before any factor materialization.
    # Keep this failure-propagation test independent of a worker's mounted X.
    monkeypatch.setattr(runner, "FactorInputSession", lambda **_: SimpleNamespace(close=lambda: None))
    with pytest.raises(FrozenDatasetError):
        runner.main()
    receipt = json.loads(output.read_text(encoding="utf-8"))
    assert receipt["reason_code"] == "FROZEN_DATASET_PIT_IDENTITY_MISMATCH"
    assert receipt["status"] == "FAILED" and receipt["ok"] is False and receipt["results"] == []
