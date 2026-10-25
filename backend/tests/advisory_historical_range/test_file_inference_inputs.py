from datetime import date

import pytest

from backend.data_service.frozen_dataset import freeze_dataset_binding
from backend.services.advisory_historical_range.inference_inputs import file_input_session
from backend.tests.data_service.test_frozen_dataset import DAYS, SYMBOL, make_dataset


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
    with pytest.raises(FrozenDatasetError):
        runner.main()
    receipt = json.loads(output.read_text(encoding="utf-8"))
    assert receipt["reason_code"] == "FROZEN_DATASET_PIT_IDENTITY_MISMATCH"
    assert receipt["status"] == "FAILED" and receipt["ok"] is False and receipt["results"] == []
