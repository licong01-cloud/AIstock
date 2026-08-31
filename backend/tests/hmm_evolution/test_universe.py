from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.hmm_evolution.errors import InvalidSpecError
from backend.services.hmm_evolution.models import EvaluationSpec
from backend.services.hmm_evolution.universe import (
    DATASET_QE_ST_PIT_PREFIX,
    HMM_FORMAL_DATASET_REQUEST_PARAM,
    LEGACY_QE_ST_PIT_UNIVERSE_KEY,
    HMMFormalDatasetBinding,
    QEExecutionUniverseResolver,
    QELoopUniverseRepository,
    SourceLoopRiskPolicySnapshot,
    SourceLoopUniverseContract,
    _canonical_json_sha256,
    _parse_source_risk_policy_snapshot,
)
from backend.services.canonical_equity_pit import (
    CANONICAL_PIT_AUTHORITY_ID,
    CANONICAL_PIT_RULE_VERSION,
    CANONICAL_PIT_SNAPSHOT_PREFIX,
    CANONICAL_PIT_UNIVERSE_KEY,
    PitAuthorityStatus,
    canonical_rule_parameters_digest,
)
from backend.services.dataset_release.cas_store import canonical_json_bytes
from backend.services.dataset_release.pit import (
    DATASET_CANDIDATE_MANIFEST_SCHEMA,
    DATASET_PIT_BINDING_SCHEMA,
    freeze_pit_snapshot,
)
from backend.services.hmm_data_source.legacy_qe_artifact_manifests import (
    LegacyQESTPITCompatibilityReceipt,
)
from backend.services.hmm_evolution import universe as universe_module
from backend.services.quantevolver.qe_dataset_contract import QE_ST_PIT_UNIVERSE_KEY
from backend.services.quantevolver.stock_pool_sync import (
    StockPoolInterval,
    StockPoolSnapshot,
)
from backend.services.stock_universe_pit_service import DEFAULT_ST_PIT_RULE_VERSION


class _LoopRepository:
    def load(self, base_loop_ref: str) -> SourceLoopUniverseContract:
        assert base_loop_ref == "qe_task/Loop8"
        return SourceLoopUniverseContract(
            task_id="qe_task",
            loop_name="Loop8",
            stock_pool="filtered_pool_fixture",
            risk_policy={
                "enabled": True,
                "providers": ["st_pit"],
                "hard_actions": ["block_buy", "force_exit"],
                "policy_version": "stock_event_risk_policy_v1",
                "strict_data_ready": True,
                "st_universe_key": LEGACY_QE_ST_PIT_UNIVERSE_KEY,
                "visible_time_mode": "next_trading_session",
            },
        )


class _CompatibilityLoopRepository:
    def __init__(self, receipt: LegacyQESTPITCompatibilityReceipt) -> None:
        self.receipt = receipt

    def load(self, base_loop_ref: str) -> SourceLoopUniverseContract:
        assert base_loop_ref == "qe_task/Loop8"
        return SourceLoopUniverseContract(
            task_id="qe_task",
            loop_name="Loop8",
            stock_pool="filtered_pool_fixture",
            risk_policy={
                "enabled": True,
                "providers": ["st_pit"],
                "hard_actions": ["block_buy", "force_exit"],
                "policy_version": "stock_event_risk_policy_v1",
                "strict_data_ready": True,
                "st_universe_key": LEGACY_QE_ST_PIT_UNIVERSE_KEY,
                "visible_time_mode": "next_trading_session",
            },
            risk_policy_origin=self.receipt.binding_mode,
            st_pit_compatibility=self.receipt,
        )


class _Cursor:
    def __init__(self, row):
        self.row = row

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, _sql, _params=None):
        return None

    def fetchone(self):
        return self.row


class _Connection:
    def __init__(self, row):
        self.row = row

    def cursor(self, **_kwargs):
        return _Cursor(self.row)


def _conn_factory(row):
    @contextmanager
    def factory(**_kwargs):
        yield _Connection(row)

    return factory


def _spec(*, topk: int = 1) -> EvaluationSpec:
    return EvaluationSpec(
        base_loop_ref="qe_task/Loop8",
        window_start=date(2025, 1, 2),
        window_end=date(2025, 1, 3),
        as_of={"policy": "explicit", "requested_date": "2025-01-03"},
        label_horizon_days=10,
        topk=topk,
        market_forward_return={"mode": "disabled", "horizon_trading_days": 10},
    )


def _pool() -> StockPoolSnapshot:
    return StockPoolSnapshot(
        filename="filtered_pool_fixture.txt",
        instrument_name="filtered_pool_fixture",
        sha256="a" * 64,
        intervals=(
            StockPoolInterval("600000.SH", date(2025, 1, 2), date(2025, 1, 3)),
            StockPoolInterval("000002.SZ", date(2025, 1, 2), date(2025, 1, 3)),
        ),
    )


def _risk_snapshot(
    *,
    universe_key: str = LEGACY_QE_ST_PIT_UNIVERSE_KEY,
    artifact_sha256: str = "b" * 64,
    artifact_size_bytes: int = 123,
    artifact_source_task_id: str = "qe_task",
    artifact_source_loop_name: str = "Loop8",
) -> SourceLoopRiskPolicySnapshot:
    return SourceLoopRiskPolicySnapshot(
        snapshot=StockPoolSnapshot(
            filename="qe_event_risk_policy.json",
            instrument_name=universe_key,
            sha256=artifact_sha256,
            intervals=(
                StockPoolInterval("600000.SH", date(2025, 1, 2), date(2025, 1, 3)),
            ),
        ),
        artifact_sha256=artifact_sha256,
        dataset_contract_id=None,
        universe_key=universe_key,
        binding_mode="legacy_frozen_runtime_artifact_v1",
        rule_version=DEFAULT_ST_PIT_RULE_VERSION,
        scope="st_only_active",
        source_fingerprint_sha256="f" * 64,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 12, 31),
        artifact_size_bytes=artifact_size_bytes,
        artifact_source_task_id=artifact_source_task_id,
        artifact_source_loop_name=artifact_source_loop_name,
    )


def _compatibility_receipt(
    *,
    source_config_sha256: str = "c" * 64,
    stock_pool_sha256: str = "a" * 64,
    artifact_sha256: str = "b" * 64,
) -> LegacyQESTPITCompatibilityReceipt:
    return LegacyQESTPITCompatibilityReceipt(
        artifact_source_task_id="qe_compatibility_task",
        artifact_source_loop_name="Loop10",
        workspace_path="qe_event_risk_policy.json",
        sha256=artifact_sha256,
        size_bytes=123,
        source_config_sha256=source_config_sha256,
        stock_pool_sha256=stock_pool_sha256,
        universe_key=LEGACY_QE_ST_PIT_UNIVERSE_KEY,
        rule_version=DEFAULT_ST_PIT_RULE_VERSION,
        scope="st_only_active",
        source_fingerprint_sha256="f" * 64,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 12, 31),
        span_count=1,
    )


def _formal_dataset_request() -> tuple[dict, HMMFormalDatasetBinding]:
    release_id = "qe-hmm-v2-20260731"
    snapshot = freeze_pit_snapshot(
        [
            {
                "ts_code": "600000.SH",
                "eligible_start": "2025-01-02",
                "eligible_end": "2025-01-03",
                "entry_reason": "fixture",
                "exit_reason": None,
            }
        ],
        universe_key=CANONICAL_PIT_UNIVERSE_KEY,
        rule_version=CANONICAL_PIT_RULE_VERSION,
        scope_start=date(2025, 1, 1),
        cutoff=date(2026, 7, 31),
        state_identity="fixture-state",
        source_fingerprint_sha256="b" * 64,
        parameter_hash=canonical_rule_parameters_digest(),
    )
    manifest = {
        "schema_version": DATASET_CANDIDATE_MANIFEST_SCHEMA,
        "release_id": release_id,
        "cutoff": snapshot.cutoff.isoformat(),
        "scope": "full",
        "artifact_root": "c" * 64,
        "pit_binding": {
            "schema_version": DATASET_PIT_BINDING_SCHEMA,
            "authority_id": CANONICAL_PIT_AUTHORITY_ID,
            "authority_status": PitAuthorityStatus.ACTIVE_CANONICAL.value,
            "scope": "full",
            "rolling_universe_key": CANONICAL_PIT_UNIVERSE_KEY,
            "frozen_universe_key": f"{CANONICAL_PIT_SNAPSHOT_PREFIX}{release_id}",
            "rule_version": CANONICAL_PIT_RULE_VERSION,
            "rule_parameters_digest": canonical_rule_parameters_digest(),
            "cutoff": snapshot.cutoff.isoformat(),
            "rolling_cutoff_spans_sha256": snapshot.spans_sha256,
            "frozen_snapshot_digest": snapshot.spans_sha256,
            "release_id": release_id,
        },
    }
    digest = hashlib.sha256(canonical_json_bytes(manifest)).hexdigest()
    request = {
        "schema_version": "qe_formal_canonical_pit_dataset_request_v1",
        "usage_mode": "formal_prediction",
        "expected_manifest_digest": digest,
        "release_manifest": manifest,
        "runtime_pins": {
            "schema_version": "qe_formal_frozen_runtime_pins_v1",
            "artifact_root": manifest["artifact_root"],
            "qlib_bin_snapshot_id": "qe-hmm-v2-20260731-daily",
            "qlib_instruments_sha256": "f" * 64,
            "qlib_calendar_sha256": "1" * 64,
            "qlib_meta_export_sha256": "2" * 64,
            "suspend_dataset_id": "qe-hmm-v2-20260731-suspend",
            "suspend_parquet_sha256": "3" * 64,
            "suspend_manifest_sha256": "4" * 64,
            "suspend_source_contract": "tushare_suspend_d_shsz_S_v1",
        },
    }
    from backend.services.hmm_evolution.universe import require_hmm_formal_dataset_binding

    return request, require_hmm_formal_dataset_binding(request)


def _risk_policy_payload(
    *,
    universe_key: str = LEGACY_QE_ST_PIT_UNIVERSE_KEY,
    dataset_contract_id: str | None = None,
    physical_universe_key: str | None = None,
) -> dict:
    formal = dataset_contract_id is not None
    state_universe_key = physical_universe_key or universe_key
    rule_version = CANONICAL_PIT_RULE_VERSION if formal else DEFAULT_ST_PIT_RULE_VERSION
    return {
        "enabled": True,
        "contract": "stock_event_risk_policy_v1",
        "providers": ["st_pit"],
        "hard_actions": ["block_buy", "force_exit"],
        "visible_time_mode": "next_trading_session",
        "strict_data_ready": True,
        "dataset_contract_id": dataset_contract_id,
        "st_universe_key": universe_key,
        "start_date": "2025-01-01",
        "end_date": "2025-12-31",
        "span_count": 1,
        "active_spans": [
            {
                "ts_code": "600000.SH",
                "eligible_start": "2025-01-02",
                "eligible_end": "2025-01-03",
                "rule_version": rule_version,
            }
        ],
        "state": {
            "status": "frozen" if formal else "ready",
            "dirty": False,
            "universe_key": state_universe_key,
            "rule_version": rule_version,
            "scope": "frozen_dataset_file" if formal else "st_only_active",
            "source_fingerprint_sha256": "f" * 64,
        },
    }


def _frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    rows = [
        (trade_date, symbol, score)
        for trade_date in (date(2025, 1, 2), date(2025, 1, 3))
        for symbol, score in (
            ("600000.SH", 3.0),
            ("000001.SZ", 2.0),
            ("000002.SZ", 1.0),
        )
    ]
    predictions = pd.DataFrame(rows, columns=["trade_date", "symbol", "score"])
    labels = pd.DataFrame(
        [(trade_date, symbol, 10, 0.1) for trade_date, symbol, _score in rows],
        columns=["trade_date", "symbol", "horizon_days", "future_return"],
    )
    return predictions, labels


def test_loop_repository_reads_stock_pool_and_strict_st_pit_from_persisted_config(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        universe_module,
        "get_conn",
        _conn_factory(
            {
                "config_json": {
                    "stock_pool": "filtered_pool_fixture",
                    "model_params": {
                        "stock_pool": "filtered_pool_fixture",
                        "risk_policy": {
                            "enabled": True,
                            "providers": ["st_pit"],
                            "hard_actions": ["block_buy", "force_exit"],
                            "policy_version": "stock_event_risk_policy_v1",
                            "strict_data_ready": True,
                            "st_universe_key": LEGACY_QE_ST_PIT_UNIVERSE_KEY,
                            "visible_time_mode": "next_trading_session",
                        },
                    },
                }
            }
        ),
    )

    contract = QELoopUniverseRepository().load("qe_task/Loop8")

    assert contract.stock_pool == "filtered_pool_fixture"
    assert contract.risk_policy["strict_data_ready"] is True
    assert contract.risk_policy["st_universe_key"] == LEGACY_QE_ST_PIT_UNIVERSE_KEY


def test_loop_repository_uses_only_allowlisted_legacy_st_pit_compatibility(
    monkeypatch,
) -> None:
    config = {
        "stock_pool": "filtered_pool_fixture",
        "model_params": {"stock_pool": "filtered_pool_fixture"},
    }
    receipt = _compatibility_receipt(
        source_config_sha256=_canonical_json_sha256(config),
    )
    monkeypatch.setattr(universe_module, "get_conn", _conn_factory({"config_json": config}))
    monkeypatch.setattr(
        universe_module,
        "find_legacy_qe_artifact_manifest",
        lambda base_loop_ref: (
            SimpleNamespace(st_pit_compatibility=receipt)
            if base_loop_ref == "qe_legacy/Loop1"
            else None
        ),
    )

    contract = QELoopUniverseRepository().load("qe_legacy/Loop1")

    assert contract.stock_pool == "filtered_pool_fixture"
    assert contract.risk_policy_origin == receipt.binding_mode
    assert contract.st_pit_compatibility == receipt
    assert contract.risk_policy["st_universe_key"] == LEGACY_QE_ST_PIT_UNIVERSE_KEY


def test_loop_repository_rejects_unlisted_loop_without_persisted_st_pit(monkeypatch) -> None:
    config = {"stock_pool": "filtered_pool_fixture"}
    monkeypatch.setattr(universe_module, "get_conn", _conn_factory({"config_json": config}))
    monkeypatch.setattr(
        universe_module,
        "find_legacy_qe_artifact_manifest",
        lambda _base_loop_ref: None,
    )

    with pytest.raises(InvalidSpecError, match="does not declare an ST-PIT risk policy"):
        QELoopUniverseRepository().load("qe_unlisted/Loop1")


def test_loop_repository_rejects_legacy_source_config_drift(monkeypatch) -> None:
    config = {"stock_pool": "filtered_pool_fixture"}
    receipt = _compatibility_receipt(source_config_sha256="d" * 64)
    monkeypatch.setattr(universe_module, "get_conn", _conn_factory({"config_json": config}))
    monkeypatch.setattr(
        universe_module,
        "find_legacy_qe_artifact_manifest",
        lambda _base_loop_ref: SimpleNamespace(st_pit_compatibility=receipt),
    )

    with pytest.raises(InvalidSpecError, match="source config differs"):
        QELoopUniverseRepository().load("qe_legacy/Loop1")


def test_loop_repository_rejects_missing_persisted_universe_key(monkeypatch) -> None:
    monkeypatch.setattr(
        universe_module,
        "get_conn",
        _conn_factory(
            {
                "config_json": {
                    "stock_pool": "filtered_pool_fixture",
                    "model_params": {
                        "risk_policy": {
                            "enabled": True,
                            "providers": ["st_pit"],
                            "strict_data_ready": True,
                        }
                    },
                }
            }
        ),
    )

    with pytest.raises(InvalidSpecError, match="does not persist its ST-PIT universe key"):
        QELoopUniverseRepository().load("qe_task/Loop8")


def test_loop_repository_rejects_conflicting_stock_pool_declarations(monkeypatch) -> None:
    monkeypatch.setattr(
        universe_module,
        "get_conn",
        _conn_factory(
            {
                "config_json": {
                    "stock_pool": "filtered_pool_a",
                    "model_params": {
                        "stock_pool": "filtered_pool_b",
                        "risk_policy": {
                            "enabled": True,
                            "providers": ["st_pit"],
                            "strict_data_ready": True,
                        },
                    },
                }
            }
        ),
    )

    with pytest.raises(InvalidSpecError, match="conflicting stock_pool"):
        QELoopUniverseRepository().load("qe_task/Loop8")


def test_loop_repository_revalidates_formal_dataset_request_without_legacy_fallback(
    monkeypatch,
) -> None:
    request, expected = _formal_dataset_request()
    formal_key = f"{DATASET_QE_ST_PIT_PREFIX}{expected.identity.release_id}"
    config = {
        "stock_pool": "filtered_pool_fixture",
        HMM_FORMAL_DATASET_REQUEST_PARAM: request,
        "risk_policy": {
            "enabled": True,
            "providers": ["st_pit"],
            "hard_actions": ["block_buy", "force_exit"],
            "policy_version": "stock_event_risk_policy_v1",
            "strict_data_ready": True,
            "st_universe_key": formal_key,
            "visible_time_mode": "next_trading_session",
        },
    }
    monkeypatch.setattr(universe_module, "get_conn", _conn_factory({"config_json": config}))
    monkeypatch.setattr(
        universe_module,
        "find_legacy_qe_artifact_manifest",
        lambda _base_loop_ref: pytest.fail("formal source must not use the legacy allowlist"),
    )

    contract = QELoopUniverseRepository().load("qe_task/Loop8")

    assert contract.formal_dataset_binding == expected
    assert contract.risk_policy_origin == "formal_frozen_dataset_v2"
    assert contract.st_pit_compatibility is None


def test_loop_repository_rejects_tampered_formal_dataset_request(monkeypatch) -> None:
    request, expected = _formal_dataset_request()
    tampered = json.loads(json.dumps(request))
    tampered["release_manifest"]["artifact_root"] = "d" * 64
    config = {
        "stock_pool": "filtered_pool_fixture",
        HMM_FORMAL_DATASET_REQUEST_PARAM: tampered,
        "risk_policy": {
            "enabled": True,
            "providers": ["st_pit"],
            "hard_actions": ["block_buy", "force_exit"],
            "policy_version": "stock_event_risk_policy_v1",
            "strict_data_ready": True,
            "st_universe_key": f"{DATASET_QE_ST_PIT_PREFIX}{expected.identity.release_id}",
            "visible_time_mode": "next_trading_session",
        },
    }
    monkeypatch.setattr(universe_module, "get_conn", _conn_factory({"config_json": config}))

    with pytest.raises(InvalidSpecError, match="runtime pins identity|cannot prove a canonical frozen identity"):
        QELoopUniverseRepository().load("qe_task/Loop8")


def test_parser_accepts_exact_frozen_legacy_runtime_artifact() -> None:
    raw = json.dumps(_risk_policy_payload()).encode("utf-8")

    snapshot = _parse_source_risk_policy_snapshot(raw, task_id="qe_task", loop_name="Loop8")

    assert snapshot.universe_key == LEGACY_QE_ST_PIT_UNIVERSE_KEY
    assert snapshot.binding_mode == "legacy_frozen_runtime_artifact_v1"
    assert snapshot.snapshot.intervals == (
        StockPoolInterval("600000.SH", date(2025, 1, 2), date(2025, 1, 3)),
    )


def test_parser_rejects_unknown_legacy_runtime_universe_key() -> None:
    raw = json.dumps(_risk_policy_payload(universe_key="unknown_st_pit_v1")).encode("utf-8")

    with pytest.raises(InvalidSpecError, match="unknown universe key"):
        _parse_source_risk_policy_snapshot(raw, task_id="qe_task", loop_name="Loop8")


def test_parser_accepts_dataset_bound_runtime_artifact() -> None:
    dataset_contract_id = "dataset_contract_v2"
    universe_key = f"shsz_st_pit_qe_dataset_{dataset_contract_id}"
    physical_key = f"{CANONICAL_PIT_SNAPSHOT_PREFIX}{dataset_contract_id}"
    raw = json.dumps(
        _risk_policy_payload(
            universe_key=universe_key,
            dataset_contract_id=dataset_contract_id,
            physical_universe_key=physical_key,
        )
    ).encode("utf-8")

    snapshot = _parse_source_risk_policy_snapshot(raw, task_id="qe_task", loop_name="Loop8")

    assert snapshot.dataset_contract_id == dataset_contract_id
    assert snapshot.universe_key == universe_key
    assert snapshot.physical_universe_key == physical_key
    assert snapshot.binding_mode == "canonical_frozen_dataset_runtime_artifact_v2"


def test_formal_resolver_proves_same_sealed_identity_and_physical_runtime_key() -> None:
    request, formal = _formal_dataset_request()
    del request
    formal_key = f"{DATASET_QE_ST_PIT_PREFIX}{formal.identity.release_id}"
    runtime_snapshot = _parse_source_risk_policy_snapshot(
        json.dumps(
            _risk_policy_payload(
                universe_key=formal_key,
                dataset_contract_id=formal.identity.release_id,
                physical_universe_key=formal.frozen_universe_key,
            )
        ).encode("utf-8"),
        task_id="qe_task",
        loop_name="Loop8",
    )

    class _FormalLoopRepository:
        def load(self, base_loop_ref: str) -> SourceLoopUniverseContract:
            assert base_loop_ref == "qe_task/Loop8"
            return SourceLoopUniverseContract(
                task_id="qe_task",
                loop_name="Loop8",
                stock_pool="filtered_pool_fixture",
                risk_policy={
                    "enabled": True,
                    "providers": ["st_pit"],
                    "hard_actions": ["block_buy", "force_exit"],
                    "policy_version": "stock_event_risk_policy_v1",
                    "strict_data_ready": True,
                    "st_universe_key": formal_key,
                    "visible_time_mode": "next_trading_session",
                },
                risk_policy_origin="formal_frozen_dataset_v2",
                formal_dataset_binding=formal,
            )

    predictions, labels = _frames()
    resolved = QEExecutionUniverseResolver(
        loop_repository=_FormalLoopRepository(),
        stock_pool_loader=lambda _stock_pool: _pool(),
        risk_policy_loader=lambda _task_id, _loop_name: runtime_snapshot,
    ).resolve(evaluation_spec=_spec(), predictions=predictions, labels=labels)

    assert resolved.evidence["st_pit"]["coverage_semantics"] == "canonical_frozen_dataset_v2"
    assert resolved.evidence["st_pit"]["canonical_pit_dataset_identity"] == formal.as_dict()


def test_formal_resolver_rejects_runtime_physical_key_drift() -> None:
    _request, formal = _formal_dataset_request()
    runtime_snapshot = _parse_source_risk_policy_snapshot(
        json.dumps(
            _risk_policy_payload(
                universe_key=f"{DATASET_QE_ST_PIT_PREFIX}{formal.identity.release_id}",
                dataset_contract_id=formal.identity.release_id,
                physical_universe_key=f"{CANONICAL_PIT_SNAPSHOT_PREFIX}{formal.identity.release_id}",
            )
        ).encode("utf-8"),
        task_id="qe_task",
        loop_name="Loop8",
    )
    drifted = HMMFormalDatasetBinding(
        usage_mode=formal.usage_mode,
        identity=formal.identity,
        frozen_universe_key=f"{CANONICAL_PIT_SNAPSHOT_PREFIX}different-release",
        artifact_root=formal.artifact_root,
        qlib_instruments_sha256=formal.qlib_instruments_sha256,
    )

    with pytest.raises(InvalidSpecError, match="differs from its canonical frozen dataset request"):
        universe_module._verify_formal_dataset_matches_runtime_artifact(
            base_loop_ref="qe_task/Loop8",
            formal=drifted,
            runtime_snapshot=runtime_snapshot,
        )


def test_formal_resolver_rejects_runtime_instruments_fingerprint_drift() -> None:
    _request, formal = _formal_dataset_request()
    payload = _risk_policy_payload(
        universe_key=f"{DATASET_QE_ST_PIT_PREFIX}{formal.identity.release_id}",
        dataset_contract_id=formal.identity.release_id,
        physical_universe_key=formal.frozen_universe_key,
    )
    payload["state"]["source_fingerprint_sha256"] = "e" * 64
    runtime_snapshot = _parse_source_risk_policy_snapshot(
        json.dumps(payload).encode("utf-8"),
        task_id="qe_task",
        loop_name="Loop8",
    )

    with pytest.raises(InvalidSpecError, match="differs from its canonical frozen dataset request"):
        universe_module._verify_formal_dataset_matches_runtime_artifact(
            base_loop_ref="qe_task/Loop8",
            formal=formal,
            runtime_snapshot=runtime_snapshot,
        )


def test_formal_runtime_parser_rejects_noncanonical_fingerprint_text() -> None:
    _request, formal = _formal_dataset_request()
    payload = _risk_policy_payload(
        universe_key=f"{DATASET_QE_ST_PIT_PREFIX}{formal.identity.release_id}",
        dataset_contract_id=formal.identity.release_id,
        physical_universe_key=formal.frozen_universe_key,
    )
    payload["state"]["source_fingerprint_sha256"] = ("f" * 64).upper()

    with pytest.raises(InvalidSpecError, match="state is not immutable"):
        _parse_source_risk_policy_snapshot(
            json.dumps(payload).encode("utf-8"),
            task_id="qe_task",
            loop_name="Loop8",
        )


def test_resolver_intersects_source_pool_with_exact_runtime_st_pit_artifact() -> None:
    predictions, labels = _frames()
    resolver = QEExecutionUniverseResolver(
        loop_repository=_LoopRepository(),
        stock_pool_loader=lambda _stock_pool: _pool(),
        risk_policy_loader=lambda _task_id, _loop_name: _risk_snapshot(),
    )

    resolved = resolver.resolve(
        evaluation_spec=_spec(),
        predictions=predictions,
        labels=labels,
    )

    assert set(resolved.predictions["symbol"]) == {"600000.SH"}
    assert set(resolved.labels["symbol"]) == {"600000.SH"}
    assert resolved.evidence["prediction_row_count_before"] == 6
    assert resolved.evidence["prediction_row_count_after"] == 2
    assert resolved.evidence["excluded_prediction_row_count"] == 4
    assert resolved.evidence["stock_pool"]["sha256"] == "a" * 64
    assert resolved.evidence["st_pit"]["universe_key"] == LEGACY_QE_ST_PIT_UNIVERSE_KEY
    assert resolved.evidence["st_pit"]["artifact_sha256"] == "b" * 64
    assert resolved.evidence["st_pit"]["binding_mode"] == "legacy_frozen_runtime_artifact_v1"
    assert len(str(resolved.evidence["universe_hash"])) == 64


def test_resolver_uses_allowlisted_cross_loop_st_pit_with_truthful_provenance() -> None:
    predictions, labels = _frames()
    receipt = _compatibility_receipt()

    def load_risk(task_id: str, loop_name: str) -> SourceLoopRiskPolicySnapshot:
        assert task_id == receipt.artifact_source_task_id
        assert loop_name == receipt.artifact_source_loop_name
        return _risk_snapshot(
            artifact_source_task_id=task_id,
            artifact_source_loop_name=loop_name,
        )

    resolver = QEExecutionUniverseResolver(
        loop_repository=_CompatibilityLoopRepository(receipt),
        stock_pool_loader=lambda _stock_pool: _pool(),
        risk_policy_loader=load_risk,
    )

    resolved = resolver.resolve(
        evaluation_spec=_spec(),
        predictions=predictions,
        labels=labels,
    )

    st_pit = resolved.evidence["st_pit"]
    assert st_pit["artifact_name"] == "qe_event_risk_policy.json"
    assert st_pit["binding_mode"] == receipt.binding_mode
    assert st_pit["coverage_semantics"] == "allowlisted_cross_loop_immutable_artifact_v1"
    assert st_pit["compatibility_receipt"]["artifact_source"] == {
        "task_id": receipt.artifact_source_task_id,
        "loop_name": receipt.artifact_source_loop_name,
        "artifact_name": "qe_event_risk_policy.json",
    }
    assert st_pit["compatibility_receipt"]["artifact_sha256"] == receipt.sha256


def test_resolver_rejects_allowlisted_cross_loop_st_pit_content_drift() -> None:
    predictions, labels = _frames()
    receipt = _compatibility_receipt()
    resolver = QEExecutionUniverseResolver(
        loop_repository=_CompatibilityLoopRepository(receipt),
        stock_pool_loader=lambda _stock_pool: _pool(),
        risk_policy_loader=lambda task_id, loop_name: _risk_snapshot(
            artifact_sha256="e" * 64,
            artifact_source_task_id=task_id,
            artifact_source_loop_name=loop_name,
        ),
    )

    with pytest.raises(InvalidSpecError, match="differs from its allowlisted receipt"):
        resolver.resolve(
            evaluation_spec=_spec(),
            predictions=predictions,
            labels=labels,
        )


def test_resolver_rejects_allowlisted_legacy_stock_pool_drift() -> None:
    predictions, labels = _frames()
    receipt = _compatibility_receipt(stock_pool_sha256="d" * 64)
    resolver = QEExecutionUniverseResolver(
        loop_repository=_CompatibilityLoopRepository(receipt),
        stock_pool_loader=lambda _stock_pool: _pool(),
        risk_policy_loader=lambda _task_id, _loop_name: _risk_snapshot(),
    )

    with pytest.raises(InvalidSpecError, match="stock_pool differs"):
        resolver.resolve(
            evaluation_spec=_spec(),
            predictions=predictions,
            labels=labels,
        )


def test_resolver_rejects_persisted_policy_runtime_artifact_identity_drift() -> None:
    predictions, labels = _frames()
    resolver = QEExecutionUniverseResolver(
        loop_repository=_LoopRepository(),
        stock_pool_loader=lambda _stock_pool: _pool(),
        risk_policy_loader=lambda _task_id, _loop_name: _risk_snapshot(
            universe_key=QE_ST_PIT_UNIVERSE_KEY
        ),
    )

    with pytest.raises(InvalidSpecError, match="differs from its frozen runtime artifact"):
        resolver.resolve(
            evaluation_spec=_spec(),
            predictions=predictions,
            labels=labels,
        )


def test_resolver_fails_when_any_day_has_fewer_eligible_symbols_than_topk() -> None:
    predictions, labels = _frames()
    resolver = QEExecutionUniverseResolver(
        loop_repository=_LoopRepository(),
        stock_pool_loader=lambda _stock_pool: _pool(),
        risk_policy_loader=lambda _task_id, _loop_name: _risk_snapshot(),
    )

    with pytest.raises(InvalidSpecError, match="smaller than TopK"):
        resolver.resolve(
            evaluation_spec=_spec(topk=2),
            predictions=predictions,
            labels=labels,
        )
