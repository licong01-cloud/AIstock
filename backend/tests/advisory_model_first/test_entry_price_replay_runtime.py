from types import SimpleNamespace

import pytest

from backend.services.advisory_model_first import entry_price_replay_runtime as runtime
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError


@pytest.mark.parametrize("memory,disk,expected", [
    (2**30, 2**30, "REPLAY_CAPACITY_AVAILABLE"),
    (1, 2**30, "WAITING_RESOURCE"),
    (2**30, 1, "WAITING_RESOURCE"),
])
def test_capacity_uses_local_headroom_not_experiment_status(tmp_path, monkeypatch, memory, disk, expected):
    monkeypatch.setattr(runtime, "_local_capacity", lambda _p: {
        "available_memory_bytes": memory, "free_disk_bytes": disk,
        "process_rss_bytes": 100, "logical_cpus": 32,
    })
    assert runtime.probe_replay_capacity(tmp_path)["status"] == expected


def test_unknown_capacity_and_missing_predictor_are_visible(tmp_path, monkeypatch):
    def unavailable(_p):
        raise OSError("metric unavailable")
    monkeypatch.setattr(runtime, "_local_capacity", unavailable)
    assert runtime.probe_replay_capacity(tmp_path)["reason_code"] == "LOCAL_CAPACITY_UNAVAILABLE"
    monkeypatch.setattr(runtime, "import_module", lambda _name: (_ for _ in ()).throw(ImportError("missing")))
    with pytest.raises(AdvisoryModelFirstError) as error:
        runtime.validate_replay_dependencies()
    assert error.value.reason_code == "ADVISORY_ENTRY_REPLAY_DEPENDENCY_UNAVAILABLE"


def test_replay_loader_bounds_threads_without_changing_model_file(tmp_path, monkeypatch):
    calls = []
    predictions = []
    model = tmp_path / "model.txt"
    model.write_text("frozen asset", encoding="utf-8")
    booster = SimpleNamespace(feature_name=lambda: ["feature"],
                              predict=lambda data, **kwargs: predictions.append((data, kwargs)) or [0.1])
    monkeypatch.setattr(runtime, "import_module", lambda _name: SimpleNamespace(
        Booster=lambda **kwargs: calls.append(kwargs) or booster, __version__="4.6.0"))
    loaded = runtime.load_replay_booster(model)
    assert loaded.feature_name() == ["feature"]
    assert loaded.predict([[1]], num_threads=32) == [0.1]
    assert predictions == [([[1]], {"num_threads": 2})]
    assert calls == [{"model_file": str(model)}]
    assert model.read_text(encoding="utf-8") == "frozen asset"
