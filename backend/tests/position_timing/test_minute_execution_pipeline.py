from __future__ import annotations

import hashlib
import json
from datetime import date, timedelta
from pathlib import Path

import numpy as np

from backend.services.position_timing import minute_execution_pipeline as pipeline
from backend.services.position_timing.minute_execution_pipeline import (
    MinuteExecutionCardResultV1,
    ProspectiveCardItemV1,
    inspect_l4b1_bundle,
    prepare_l4b1_request,
    run_l4b1_audit,
)


TARGET_DATE = date(2026, 9, 4)
FIELDS = pipeline.REQUIRED_MINUTE_FIELDS


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_bin(path: Path, values: list[float]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    np.asarray([0.0, *values], dtype="<f4").tofile(path)


def _minute_provider(
    root: Path,
    *,
    raw_open: float = 9.0,
    raw_window_price: float = 10.0,
    factor: float = 2.0,
    limit_down: float = 8.0,
    limit_up: float = 12.0,
    limit_down_flags: list[float] | None = None,
    limit_up_flags: list[float] | None = None,
    raw_shares_per_bar: float = 10_000.0,
) -> Path:
    calendar = [f"{TARGET_DATE.isoformat()} 09:{minute:02d}:00" for minute in range(30, 60)]
    (root / "calendars").mkdir(parents=True)
    (root / "instruments").mkdir(parents=True)
    (root / "calendars" / "1min.txt").write_text("\n".join(calendar) + "\n", encoding="utf-8")
    (root / "instruments" / "all.txt").write_text(
        f"000001.SZ\t{TARGET_DATE.isoformat()}\t{TARGET_DATE.isoformat()}\n", encoding="utf-8"
    )
    meta = {
        "snapshot_id": "minute_test_candidate",
        "start": TARGET_DATE.isoformat(),
        "end": TARGET_DATE.isoformat(),
        "required_minute_fields": list(FIELDS),
    }
    (root / "meta_export.json").write_text(json.dumps(meta), encoding="utf-8")
    prices = [raw_window_price] * 30
    adjusted_open = [raw_open * factor, *[raw_window_price * factor] * 29]
    adjusted_high = [max(raw_open, raw_window_price) * factor] * 30
    adjusted_low = [min(raw_open, raw_window_price) * factor] * 30
    raw_shares = [raw_shares_per_bar] * 30
    values = {
        "open": adjusted_open,
        "high": adjusted_high,
        "low": adjusted_low,
        "close": [value * factor for value in prices],
        "volume": [value / factor for value in raw_shares],
        "amount": [price * volume for price, volume in zip(prices, raw_shares, strict=True)],
        "factor": [factor] * 30,
        "up_limit_price": [limit_up] * 30,
        "down_limit_price": [limit_down] * 30,
        "limit_up": limit_up_flags or [0.0] * 30,
        "limit_down": limit_down_flags or [0.0] * 30,
    }
    for field, field_values in values.items():
        _write_bin(root / "features" / "000001.sz" / f"{field}.1min.bin", field_values)
    return root


def _request(
    tmp_path: Path,
    monkeypatch,
    service,
    provider: Path,
):
    repository = tmp_path / "repo"
    repository.mkdir(exist_ok=True)
    monkeypatch.setattr(pipeline, "_repository_dirty_paths", lambda _root: [])
    monkeypatch.setattr(pipeline, "_repository_commit", lambda _root: "a" * 40)
    output_root = service.store.root / "research"
    registry = service.store.root / "research_registry" / "timing_execution_window_registry_v1.jsonl"
    request_path = output_root / "l4b1_requests" / "request.json"
    request = prepare_l4b1_request(
        timing_artifact_root=service.store.root,
        minute_provider_root=provider,
        repository_root=repository,
        output_root=output_root,
        registry_path=registry,
        output_path=request_path,
    )
    return request, request_path, registry


def test_pipeline_uses_raw_vwap_component_cost_and_exact_retry(
    tmp_path: Path, monkeypatch, service_factory
) -> None:
    service = service_factory()
    card_set = service.materialize()["card_set"]
    card = card_set.cards[0]
    assert card.action.value == "EXIT"
    assert card.execution_window.value == "AT_OPEN"
    provider = _minute_provider(tmp_path / "minute")
    card_file = next((service.store.root / "cards").rglob("card_set-*.json"))
    event_file = next((service.store.root / "events").glob("*.jsonl"))
    product_before = {card_file: _sha256(card_file), event_file: _sha256(event_file)}
    n0 = tmp_path / "advisory_n0_registry.jsonl"
    n0.write_text('{"existing":true}\n', encoding="utf-8")
    n0_before = _sha256(n0)

    request, request_path, registry = _request(tmp_path, monkeypatch, service, provider)
    assert request.population_counts == {"ELIGIBLE": 1}
    first = run_l4b1_audit(request_path)
    assert first["status"] == "INSUFFICIENT_DATA"
    assert first["selected_sides"] == []
    assert first["registry_appended"] is True
    bundle = Path(first["bundle_path"])
    result = json.loads((bundle / "card_results.json").read_text(encoding="utf-8"))[0]
    assert result["status"] == "PAIRED"
    assert result["benchmark_price_raw"] == 9.0
    assert result["challenger_price_raw"] == 10.0
    assert result["net_improvement_bps"] > 1_000
    assert result["challenger_cost_cny"] > result["benchmark_cost_cny"]
    assert request.familywise_hypothesis_count == 1
    assert inspect_l4b1_bundle(bundle)["status"] == "BUNDLE_VALID"

    retry = run_l4b1_audit(request_path)
    assert retry["bundle_id"] == first["bundle_id"]
    assert retry["receipt_sha256"] == first["receipt_sha256"]
    assert retry["exact_retry"] is True
    assert retry["registry_appended"] is False
    assert len(registry.read_text(encoding="utf-8").splitlines()) == 1
    assert _sha256(n0) == n0_before
    assert {path: _sha256(path) for path in product_before} == product_before


def test_no_action_card_produces_typed_insufficient_receipt(tmp_path: Path, monkeypatch, service_factory) -> None:
    holdings = [
        {
            "id": 1,
            "code": "000001",
            "name": "平安银行",
            "cost_price": 9.0,
            "quantity": 1000,
            "updated_at": "2026-09-03T14:00:00+08:00",
        }
    ]
    service = service_factory(holdings=holdings)
    service.materialize()
    request, request_path, _registry = _request(
        tmp_path,
        monkeypatch,
        service,
        _minute_provider(tmp_path / "minute"),
    )
    assert request.population_counts == {"NON_ACTION": 1}
    result = run_l4b1_audit(request_path)
    assert result["status"] == "INSUFFICIENT_PROSPECTIVE_ACTION_CARDS"
    receipt = json.loads((Path(result["bundle_path"]) / "receipt.json").read_text(encoding="utf-8"))
    assert receipt["familywise_hypothesis_count"] == 1
    assert [item["side"] for item in receipt["direction_summaries"]] == ["SELL"]
    assert receipt["runtime_policy_written"] is False
    assert receipt["card_or_event_written"] is False
    assert receipt["order_written"] is False
    assert receipt["l1_l1a_gate_applied"] is False


def test_invalid_factor_is_visible_data_error_not_default_price(tmp_path: Path, monkeypatch, service_factory) -> None:
    service = service_factory()
    service.materialize()
    provider = _minute_provider(tmp_path / "minute")
    _write_bin(provider / "features" / "000001.sz" / "factor.1min.bin", [0.0] * 30)
    request, request_path, _registry = _request(tmp_path, monkeypatch, service, provider)
    assert request.population_counts == {"ELIGIBLE": 1}
    result = run_l4b1_audit(request_path)
    row = json.loads((Path(result["bundle_path"]) / "card_results.json").read_text(encoding="utf-8"))[0]
    assert row["status"] == "DATA_ERROR_FACTOR_INVALID"
    assert row["benchmark_price_raw"] is None
    assert result["status"] == "NO_PAIRED_EXECUTIONS"


def test_directional_limit_is_business_no_fill(tmp_path: Path, monkeypatch, service_factory) -> None:
    service = service_factory()
    service.materialize()
    provider = _minute_provider(
        tmp_path / "minute",
        raw_open=8.0,
        raw_window_price=8.0,
        limit_down=8.0,
        limit_down_flags=[1.0] * 30,
    )
    _request_value, request_path, _registry = _request(tmp_path, monkeypatch, service, provider)
    result = run_l4b1_audit(request_path)
    row = json.loads((Path(result["bundle_path"]) / "card_results.json").read_text(encoding="utf-8"))[0]
    assert row["status"] == "BENCHMARK_NO_FILL_LIMIT_DOWN"
    receipt = json.loads((Path(result["bundle_path"]) / "receipt.json").read_text(encoding="utf-8"))
    sell = next(item for item in receipt["direction_summaries"] if item["side"] == "SELL")
    assert sell["market_no_fill_count"] == 1
    assert sell["data_error_count"] == 0


def test_benchmark_and_challenger_preserve_the_same_card_quantity(
    tmp_path: Path, monkeypatch, service_factory
) -> None:
    service = service_factory()
    service.materialize()
    provider = _minute_provider(tmp_path / "minute", raw_shares_per_bar=500.0)
    _request_value, request_path, _registry = _request(tmp_path, monkeypatch, service, provider)
    result = run_l4b1_audit(request_path)
    row = json.loads((Path(result["bundle_path"]) / "card_results.json").read_text(encoding="utf-8"))[0]
    assert row["status"] == "BENCHMARK_NO_FILL_VOLUME_BELOW_QUANTITY"
    assert row["challenger_raw_volume_shares"] == 500.0


def test_sell_is_the_only_reachable_preregistered_hypothesis(tmp_path: Path, monkeypatch, service_factory) -> None:
    service = service_factory()
    service.materialize()
    request, _request_path, _registry = _request(
        tmp_path,
        monkeypatch,
        service,
        _minute_provider(tmp_path / "minute"),
    )
    base = request.population_items[0]
    assert pipeline._directions_for_family(1) == ("SELL",)
    assert pipeline._actions_for_family(1) == ("EXIT",)
    assert pipeline._directions_for_family(2) == ("BUY", "SELL")
    assert pipeline._matches_risk_exit_contract(service.materialize()["card_set"].cards[0]) is True
    rows = [
        MinuteExecutionCardResultV1(
            card_id=f"sell-{ordinal}",
            canonical_symbol=base.canonical_symbol,
            target_trade_date=TARGET_DATE + timedelta(days=ordinal // 2),
            side="SELL",
            quantity=base.quantity,
            status="PAIRED",
            reason_code="PAIRED_EXECUTION_COUNTERFACTUAL_AVAILABLE",
            benchmark_price_raw=9.0,
            challenger_price_raw=10.0,
            benchmark_cost_cny=6.0,
            challenger_cost_cny=6.1,
            benchmark_gross_notional_cny=9_000.0,
            challenger_gross_notional_cny=10_000.0,
            challenger_raw_volume_shares=100_000.0,
            net_improvement_cny=2.0,
            net_improvement_bps=2.0,
        )
        for ordinal in range(40)
    ]
    sell = pipeline._summarize_direction("SELL", request=request, results=rows)
    assert sell.effect_evidence == "SUPPORTED"
    assert sell.adjusted_interval is not None and sell.adjusted_interval.lower_bps > 0
    assert sell.nominal_interval is not None
    assert sell.adjusted_interval == sell.nominal_interval


def test_active_family_rejects_an_at_open_exit_without_the_risk_exit_trigger(service_factory) -> None:
    service = service_factory()
    card = service.materialize()["card_set"].cards[0]
    wrong_trigger = card.triggers[0].model_copy(
        update={"branch": "OTHER_AT_OPEN_EXIT", "conditions": {"sell_reason": "other"}}
    )
    drifted_card = card.model_copy(update={"triggers": (wrong_trigger,)})
    item = pipeline._population_item(
        card=drifted_card,
        event={"card_artifact_sha256": pipeline.canonical_sha256(drifted_card)},
        card_set_ref_role=f"card_set:{drifted_card.card_set_id}",
        coverage_start=drifted_card.target_trade_date,
        coverage_end=drifted_card.target_trade_date,
        instrument_spans={
            drifted_card.canonical_symbol: ((drifted_card.target_trade_date, drifted_card.target_trade_date),)
        },
        included_directions=("SELL",),
        included_actions=("EXIT",),
        require_risk_exit_contract=True,
    )
    assert item.population_status == "ACTION_CONTRACT_OUT_OF_SCOPE"


def test_population_excludes_price_trigger_cards_without_inventing_quantity(
    tmp_path: Path, monkeypatch, service_factory
) -> None:
    service = service_factory(holdings=[])
    service.put_analysis_scope(raw_symbol="600000.SH", analysis_enabled=True)
    service.put_intent(
        raw_symbol="600000.SH",
        planned_full_notional_cny="120000",
        desired_target_exposure="1",
    )
    card = service.materialize()["card_set"].cards[0]
    assert card.execution_window.value == "ON_PRICE_TRIGGER"
    provider = _minute_provider(tmp_path / "minute")
    # The source only needs the control files because NOT_AT_OPEN cards must not
    # cause minute feature reads or synthetic branch selection.
    request, request_path, _registry = _request(tmp_path, monkeypatch, service, provider)
    assert request.population_counts == {"NOT_AT_OPEN": 1}
    assert not any(key.startswith("minute_feature:") for key in request.source_refs)
    assert run_l4b1_audit(request_path)["status"] == "INSUFFICIENT_PROSPECTIVE_ACTION_CARDS"


def test_direct_bin_reader_obeys_instrument_pit_spans(tmp_path: Path, monkeypatch, service_factory) -> None:
    service = service_factory()
    service.materialize()
    provider = _minute_provider(tmp_path / "minute")
    (provider / "instruments" / "all.txt").write_text(
        "000001.SZ\t2026-09-01\t2026-09-03\n",
        encoding="utf-8",
    )
    request, request_path, _registry = _request(tmp_path, monkeypatch, service, provider)
    assert request.population_counts == {"MINUTE_UNIVERSE_EXCLUDED": 1}
    assert not any(key.startswith("minute_feature:") for key in request.source_refs)
    assert run_l4b1_audit(request_path)["status"] == "INSUFFICIENT_PROSPECTIVE_ACTION_CARDS"


def test_request_source_drift_fails_before_a_result_bundle(tmp_path: Path, monkeypatch, service_factory) -> None:
    service = service_factory()
    service.materialize()
    provider = _minute_provider(tmp_path / "minute")
    _request_value, request_path, _registry = _request(tmp_path, monkeypatch, service, provider)
    field = provider / "features" / "000001.sz" / "amount.1min.bin"
    field.write_bytes(field.read_bytes() + b"drift")
    try:
        run_l4b1_audit(request_path)
    except pipeline.PositionTimingL4b1Error as exc:
        assert exc.reason_code == "POSITION_TIMING_L4B1_SOURCE_DRIFT"
    else:  # pragma: no cover - assertion spelling keeps the failure readable
        raise AssertionError("source drift must fail closed")
    assert not (service.store.root / "research" / "l4b1_execution_window_bundles").exists()


def test_output_paths_cannot_target_another_registry(tmp_path: Path, monkeypatch, service_factory) -> None:
    service = service_factory()
    service.materialize()
    provider = _minute_provider(tmp_path / "minute")
    repository = tmp_path / "repo"
    repository.mkdir()
    monkeypatch.setattr(pipeline, "_repository_dirty_paths", lambda _root: [])
    monkeypatch.setattr(pipeline, "_repository_commit", lambda _root: "a" * 40)
    try:
        prepare_l4b1_request(
            timing_artifact_root=service.store.root,
            minute_provider_root=provider,
            repository_root=repository,
            output_root=tmp_path / "foreign-output",
            registry_path=tmp_path / "advisory_n0" / "trial_registry.jsonl",
            output_path=tmp_path / "request.json",
        )
    except pipeline.PositionTimingL4b1Error as exc:
        assert exc.reason_code == "POSITION_TIMING_L4B1_OUTPUT_BOUNDARY_INVALID"
    else:  # pragma: no cover
        raise AssertionError("L4b-1 must not accept a foreign output or registry path")


def test_population_item_contract_rejects_noncanonical_exchange() -> None:
    try:
        ProspectiveCardItemV1(
            card_id="card",
            card_set_id="set",
            card_set_ref_role="ref",
            card_artifact_sha256="a" * 64,
            card_issued_event_sha256="b" * 64,
            canonical_symbol="000001.BJ",
            target_trade_date=TARGET_DATE,
            action="EXIT",
            execution_window="AT_OPEN",
            side="SELL",
            quantity=100,
            cost_policy_sha256="c" * 64,
            population_status="ELIGIBLE",
        )
    except ValueError:
        pass
    else:  # pragma: no cover
        raise AssertionError("L4b-1 minute candidate excludes BJ")
