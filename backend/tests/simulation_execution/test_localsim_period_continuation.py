"""Period authority contracts using real native account/fee/journal fixtures."""

from copy import deepcopy
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from backend.services.position_timing.intraday.contracts import digest
from backend.services.position_timing.intraday.replay import FileRef
from backend.services.position_timing.intraday.store import IntradayError
from backend.services.simulation_execution.localsim import dual_mode_research as dual
from backend.services.simulation_execution.localsim import period_continuation as period
from backend.services.simulation_execution.localsim import timing_campaign as campaign
from backend.services.simulation_execution.localsim.timing_research import ref
from backend.services.simulation_execution.localsim.timing_store import publish, read_json
from backend.services.strategy_package.models import StrategyPackageManifest
from backend.tests.paper_trading_v2.test_day_runner import make_paper_enabled_manifest
from backend.tests.simulation_execution.test_localsim_timing_campaign import price_campaign_fixture


def put(path, value):
    publish(path, value)
    return ref(path)


@pytest.fixture
def continuation(tmp_path, monkeypatch):
    _, preflight_ref, templates_ref, _ = price_campaign_fixture(tmp_path, monkeypatch, two_days=True)
    root = tmp_path / "period_fixture"
    preflight = read_json(Path(preflight_ref.path))
    old_inventory = read_json(Path(preflight["selection_inventory"]["path"]))
    profile = {"hmm": {"enabled": False}, "selection": {"top_k": 2}}
    lease = {
        "universe_key": "FUNCTIONAL_ONLY",
        "rule_version": "v2",
        "coverage_start": "2026-01-01",
        "coverage_end": "2026-12-31",
    }
    config = {
        "runtime_profile": profile,
        "canonical_pit_authority": lease,
        "canonical_pit_authority_profile": {"authority_id": "FUNCTIONAL_ONLY"},
        "strategy_runtime_release": {
            "release_id": "functional",
            "release_hash": "a" * 64,
            "package_id": "FUNCTIONAL_FIXTURE",
            "manifest_sha256": preflight["manifest_sha256"],
        },
    }
    for row in old_inventory["days"]:
        projection = read_json(Path(row["projection"]["path"]))
        projection["runtime_config"].update(config)
        row["projection"] = put(root / row["trade_date"] / "projection.json", projection)
    old_inventory_ref = put(root / "inventory.json", old_inventory)
    preflight["selection_inventory"] = old_inventory_ref
    preflight["inputs"]["profile"] = put(root / "profile.json", {"config": profile})
    package = read_json(Path(preflight["inputs"]["package"]["path"]))
    package["package"]["package_id"] = "FUNCTIONAL_FIXTURE"
    preflight["inputs"]["package"] = put(root / "package.json", package)
    preflight_ref = FileRef.model_validate(put(root / "preflight.json", preflight))
    templates = read_json(Path(templates_ref.path))
    templates["selection_inventory_sha256"] = old_inventory_ref["sha256"]
    templates_ref = FileRef.model_validate(put(root / "templates.json", templates))
    manifest = make_paper_enabled_manifest().model_copy(update={"package_id": "FUNCTIONAL_FIXTURE"})
    factory = dual.native_backend_for
    monkeypatch.setattr(StrategyPackageManifest, "model_validate", lambda value: None)
    monkeypatch.setattr(dual, "CandidateReader", campaign.CandidateReader)
    monkeypatch.setattr(dual, "native_backend_for", lambda **kw: factory(**{**kw, "manifest": manifest}))
    original = campaign.freeze_continuous_campaign(
        preflight_ref=preflight_ref,
        templates_ref=templates_ref,
        output_root=root / "original",
        producer_commit="1" * 40,
    )
    frozen = dual.freeze_study(
        source_request=FileRef.model_validate(original["request"]),
        output_root=root / "parent",
        producer_commit="2" * 40,
        matrix="FULL_13",
    )
    parent_request = FileRef.model_validate(frozen["request"])
    finished = dual.run_study(parent_request)
    results = read_json(Path(finished["results"]["path"]))
    endpoints = []
    held = set()
    for account in results["accounts"]:
        end = read_json(Path(account["days"][-1]["path"]))
        held.update(end["positions"])
        endpoints.append(
            {"account_id": account["account_id"], "day": account["days"][-1], "account_receipt": end["account_receipt"]}
        )
    request = read_json(Path(parent_request.path))
    completion = put(
        root / "completion.json",
        {
            "phase": "VERIFIED",
            "request": parent_request.model_dump(),
            "results": finished["results"],
            "account_count": 13,
            "completed_trade_days": 2,
            "latest_complete_trade_date": "2026-09-22",
        },
    )
    stamp = "2026-09-23"
    projection = read_json(Path(old_inventory["days"][-1]["projection"]["path"]))
    projection["runtime_config"]["point_in_time_context"] = {"effective_trade_date": stamp, "cutoff_date": "2026-09-22"}
    projection_ref = put(root / stamp / "projection.json", projection)
    completion_ref = put(
        root / stamp / "completion.json",
        {
            "consumer_scope": "LOCALSIM",
            "status": "SIGNAL_COMMITTED",
            "clocks": {"decision_date": stamp, "target_trade_date": stamp, "data_cutoff": "2026-09-22"},
        },
    )
    symbols = sorted(t["symbol"] for t in projection["aggregate_results"])
    quality = {
        "path": "quality.json",
        "sha256": digest("quality"),
        "quality_status": "ACCEPTED_WITH_WARNINGS",
        "quality_warning_count": 18,
    }
    pit = {
        "universe_key": "FUNCTIONAL_ONLY",
        "rule_version": "v2",
        "selection_universe": {"path": "pool.txt", "sha256": digest("pool")},
    }
    # Only file loading is synthetic; native account calculation, fees, confirmed
    # feedback, T+1 and immutable journals above use the existing implementation.
    source_reader = campaign.CandidateReader

    class Candidate(source_reader):
        def __init__(self, source):
            super().__init__(source)
            self.root = root
            self.pins = {}
            self.manifest = {"source_quality": quality, "st_pit_manifest": pit}
            self.closed = False

        def calendar(self, frequency):
            return pd.DatetimeIndex(["2026-09-21", "2026-09-22", stamp])

        def prepare_window(self, start, end, symbols):
            assert start == date(2026, 9, 22) and end == date(2026, 9, 23)

        def market(self, symbol, day):
            market = super().market(symbol, date(2026, 9, 22))
            delta = timedelta(days=(day - date(2026, 9, 22)).days)
            return market.model_copy(
                update={
                    "trade_date": day,
                    "day_facts": market.day_facts.model_copy(
                        update={"trade_date": day, "available_at": market.day_facts.available_at + delta}
                    ),
                    "bars": tuple(
                        b.model_copy(update={k: getattr(b, k) + delta for k in ("start", "end", "received_at")})
                        for b in market.bars
                    ),
                }
            )

        @property
        def window_status(self):
            return {"open_mappings": 0}

        def close(self):
            self.closed = True

    monkeypatch.setattr(period, "CandidateReader", Candidate)
    monkeypatch.setattr(
        period, "_source_declarations", lambda reader, h: {"source_quality": quality, "pit_identity": pit}
    )
    handoff = {
        "prior_results": finished["results"],
        "prior_completion": completion,
        "prior_selection_inventory": old_inventory_ref,
        "package": preflight["inputs"]["package"],
        "package_id": request["package_id"],
        "package_manifest_sha256": request["manifest_sha256"],
        "candidate_manifest": preflight["inputs"]["candidate_manifest"],
        "trade_dates": [stamp],
        "runtime_profile_sha256": digest(profile),
        "source_quality_declaration": quality,
        "native_projections": [
            {
                "projection": projection_ref,
                "completion": completion_ref,
                "trade_date": stamp,
                "data_cutoff": "2026-09-22",
                "target_count": len(symbols),
                "symbols": symbols,
                "target_payload_sha256": digest(projection["aggregate_results"]),
            }
        ],
        "prior_endpoints": endpoints,
        "prior_held_symbols": sorted(held),
        "october_target_symbols": symbols,
        "prior_held_symbols_not_october_targets": sorted(held - set(symbols)),
    }
    handoff_ref = FileRef.model_validate(put(root / "handoff.json", handoff))
    return {
        "handoff": handoff,
        "handoff_ref": handoff_ref,
        "request": request,
        "parent_results": results,
        "root": root,
        "kwargs": {
            "handoff_ref": handoff_ref,
            "start": date(2026, 9, 23),
            "end": date(2026, 9, 23),
            "output_root": root / "prepared",
            "producer_commit": "3" * 40,
        },
    }


def test_full_13_opening_authority_is_not_new_money_or_account_replay(continuation, monkeypatch):
    kwargs = continuation["kwargs"]
    source = Path(continuation["handoff"]["prior_results"]["path"]).parent
    before = {p: p.read_bytes() for p in source.rglob("*.json")}
    monkeypatch.setattr(
        dual, "_compute_account_day", lambda *a, **kw: pytest.fail("preparation must not calculate account days")
    )
    result = period.prepare_period(**kwargs)
    assert result["status"] == "WAITING_PRICE_PLANS" and result["account_count"] == 13
    assert result["parent_account_days_recomputed"] == 0 and result["replay_started"] is False
    pf = read_json(Path(result["preflight"]["path"]))
    accounts = [read_json(Path(r["path"])) for r in pf["initial_accounts"]]
    for account in accounts:
        parent = read_json(Path(account["parent_day"]["path"]))
        assert account["new_capital_added_cny"] == "0"
        assert account["cash_cny"] == parent["cash_cny"]
        assert account["opening_nav_cny"] == parent["terminal_nav_cny"]
        assert account["origin_initial_cash_cny"] == "10000000"
        for symbol, lot in account["positions"].items():
            assert lot["portfolio_id"] == account["account_id"]
            assert lot["quantity"] == lot["available_quantity"] == parent["positions"][symbol]["quantity"]
            assert lot["avg_cost"] == parent["positions"][symbol]["avg_cost"]
        assert account["analysis_anchor"]["parent_nav_days_verified"] == 2
    assert period.inspect_preparation(FileRef.model_validate(result["preflight"]))["account_count"] == 13
    assert all(p.read_bytes() == raw for p, raw in before.items())


@pytest.mark.parametrize(
    "fault,reason",
    [
        ("duplicate", "SELECTION_POPULATION"),
        ("cutoff", "DAILY_INPUT_IDENTITY"),
        ("profile", "RUNTIME_PROFILE"),
        ("accounts", "ACCOUNT_POPULATION"),
        ("symbols", "SYMBOL_POPULATION"),
    ],
)
def test_invalid_population_and_identity_publish_nothing(continuation, fault, reason):
    h = deepcopy(continuation["handoff"])
    if fault == "duplicate":
        h["native_projections"].append(h["native_projections"][0])
    if fault == "cutoff":
        h["native_projections"][0]["data_cutoff"] = "2026-09-21"
    if fault == "profile":
        h["runtime_profile_sha256"] = "f" * 64
    if fault == "accounts":
        h["prior_endpoints"][-1] = h["prior_endpoints"][0]
    if fault == "symbols":
        h["prior_held_symbols"] = []
    kwargs = {
        **continuation["kwargs"],
        "handoff_ref": FileRef.model_validate(put(continuation["root"] / ("invalid_" + fault + ".json"), h)),
    }
    with pytest.raises(ValueError, match=reason):
        period.prepare_period(**kwargs)
    assert not kwargs["output_root"].exists()


def test_corrupt_parent_journal_is_rejected(continuation):
    path = Path(continuation["handoff"]["prior_endpoints"][0]["account_receipt"]["path"])
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(IntradayError, match="IDENTITY_MISMATCH"):
        period.prepare_period(**continuation["kwargs"])
    assert not continuation["kwargs"]["output_root"].exists()


def test_market_unavailability_preserves_population_and_blocked_status(continuation, monkeypatch):
    reader = period.CandidateReader
    original = reader.market

    def missing(self, symbol, day):
        if day == date(2026, 9, 23):
            raise IntradayError("RESEARCH_DAILY_PRICE_UNAVAILABLE", 422)
        return original(self, symbol, day)

    monkeypatch.setattr(reader, "market", missing)
    result = period.prepare_period(**continuation["kwargs"])
    assert result["status"] == "BLOCKED_SOURCE_INPUTS" and result["account_count"] == 13
    pf = read_json(Path(result["preflight"]["path"]))
    assert all(b["reason"] == "RESEARCH_DAILY_PRICE_UNAVAILABLE" for b in pf["blockers"])
    assert period.inspect_preparation(FileRef.model_validate(result["preflight"]))["status"] == "BLOCKED_SOURCE_INPUTS"


def test_same_facts_with_new_source_metadata_are_not_false_history_conflicts(continuation, monkeypatch):
    cls = period.CandidateReader
    original = cls.market

    def market(self, symbol, day):
        view = original(self, symbol, day)
        return view.model_copy(
            update={"day_facts": view.day_facts.model_copy(update={"source_sha256": digest(str(id(self)))})}
        )

    monkeypatch.setattr(cls, "market", market)
    assert period.prepare_period(**continuation["kwargs"])["status"] == "WAITING_PRICE_PLANS"


def test_source_change_during_preparation_cannot_publish_success(continuation, monkeypatch):
    original = period._nav_anchor

    def changed(parent, value):
        result = original(parent, value)
        path = Path(continuation["handoff_ref"].path)
        if path.read_bytes()[-1:] != b" ":
            path.write_bytes(path.read_bytes() + b" ")
        return result

    monkeypatch.setattr(period, "_nav_anchor", changed)
    with pytest.raises(IntradayError, match="IDENTITY_MISMATCH"):
        period.prepare_period(**continuation["kwargs"])
    assert not continuation["kwargs"]["output_root"].exists()


def test_selection_explicit_refs_equal_legacy_scan_and_preserve_none(continuation):
    h = continuation["handoff"]
    row = h["native_projections"][0]
    kwargs = dict(
        package_id=h["package_id"], manifest_sha256=h["package_manifest_sha256"], trade_dates=h["trade_dates"]
    )
    pinned = campaign.inspect_selection_history(Path(), **kwargs, committed_artifacts=[row])
    scanned = campaign.inspect_selection_history(continuation["root"].parent, **kwargs)
    assert pinned == scanned
    assert pinned["targets_without_entry_band"] == row["target_count"]
    assert pinned["days"][0]["targets"][0]["suggested_entry_price_band"] is None


def test_uncommitted_preparation_prefix_is_not_consumable(continuation, monkeypatch):
    original = period.publish

    def interrupted(path, value):
        if path.name == "preparation_receipt.json":
            raise OSError("publication interrupted")
        return original(path, value)

    monkeypatch.setattr(period, "publish", interrupted)
    with pytest.raises(OSError, match="interrupted"):
        period.prepare_period(**continuation["kwargs"])
    path = next(continuation["kwargs"]["output_root"].glob("*/preflight.json"))
    with pytest.raises((FileNotFoundError, IntradayError)):
        period.inspect_preparation(FileRef.model_validate(ref(path)))


def test_legal_suspension_does_not_require_fabricated_prices(continuation, monkeypatch):
    cls = period.CandidateReader
    original = cls.market

    def suspended(self, symbol, day):
        market = original(self, symbol, day)
        if day == date(2026, 9, 23):
            return market.model_copy(
                update={
                    "bars": (),
                    "day_facts": market.day_facts.model_copy(
                        update={"is_suspended": True, "reference_price": None, "limit_up": None, "limit_down": None}
                    ),
                }
            )
        return market

    monkeypatch.setattr(cls, "market", suspended)
    result = period.prepare_period(**continuation["kwargs"])
    pf = read_json(Path(result["preflight"]["path"]))
    assert result["status"] == "WAITING_PRICE_PLANS"
    assert pf["market_source_preflight"]["days"][0]["suspended_symbols"] == 2


def test_source_declarations_pin_quality_and_pit_without_reclassifying_warnings(tmp_path):
    quality_ref = put(tmp_path / "quality.json", {"warning": "DECLARED_PRECISION_DIFFERENCE"})
    pool = tmp_path / "pool.txt"
    pool.write_bytes(b"000001.SZ\t2026-01-01\t2026-10-30\n")
    pit = {
        "selection_universe": {"path": "pool.txt", "sha256": ref(pool)["sha256"]},
        "universe_key": "FUNCTIONAL_ONLY",
        "rule_version": "v2",
    }
    put(tmp_path / "stock_pools/st_pit_manifest.json", pit)
    quality = {
        "path": "quality.json",
        "sha256": quality_ref["sha256"],
        "quality_status": "ACCEPTED_WITH_WARNINGS",
        "quality_warning_count": 18,
    }

    class Reader:
        manifest = {"source_quality": quality, "st_pit_manifest": pit}

        def file(self, relative):
            return tmp_path / relative

    result = period._source_declarations(Reader(), {"source_quality_declaration": quality})
    assert result["source_quality"] == quality
    with pytest.raises(ValueError, match="QUALITY_DECLARATION_CONFLICT"):
        period._source_declarations(Reader(), {"source_quality_declaration": {**quality, "quality_warning_count": 0}})


def test_historical_real_price_change_is_a_blocker(continuation, monkeypatch):
    cls = period.CandidateReader
    original = cls.market
    instances = []

    def market(self, symbol, day):
        if self not in instances:
            instances.append(self)
        view = original(self, symbol, day)
        if self is instances[0] and day == date(2026, 9, 22):
            return view.model_copy(
                update={"bars": tuple(b.model_copy(update={"close": b.close + 1}) for b in view.bars)}
            )
        return view

    monkeypatch.setattr(cls, "market", market)
    result = period.prepare_period(**continuation["kwargs"])
    pf = read_json(Path(result["preflight"]["path"]))
    assert result["status"] == "BLOCKED_SOURCE_INPUTS"
    assert {b["reason"] for b in pf["blockers"]} == {"PARENT_OPENING_VALUATION_INPUT_CHANGED"}


def test_parent_endpoint_cash_and_nav_cannot_be_invented(continuation):
    parent = period._parent(continuation["handoff"])
    endpoint = continuation["handoff"]["prior_endpoints"][0]
    profile = next(
        p
        for p in dual.profiles_for("FULL_13")
        if "lsdm_" + digest([parent["request"]["identity"], p])[:40] == endpoint["account_id"]
    )
    for field, value in (("cash_cny", "10000000.00"), ("terminal_nav_cny", "999999999.99")):
        day = read_json(Path(endpoint["day"]["path"]))
        day[field] = value
        changed_ref = put(continuation["root"] / ("invented_" + field + ".json"), day)
        changed_endpoint = {**endpoint, "day": changed_ref}
        changed = deepcopy(parent)
        account = next(a for a in changed["results"]["accounts"] if a["account_id"] == endpoint["account_id"])
        account["days"][-1] = changed_ref
        with pytest.raises(ValueError, match="PARENT_(CHECKPOINT|ENDPOINT)_CONFLICT"):
            period._endpoint(changed, changed_endpoint, profile, "2026-09-22")


def test_opening_input_scope_preserves_volume_disclosure_and_future_isolation():
    from backend.tests.simulation_execution.test_localsim_timing_campaign import pair, at

    _, markets = pair()
    old = next(iter(markets.values()))
    terminal = at("14:57:00")
    new = old.model_copy(
        update={"bars": tuple(b.model_copy(update={"volume_shares": b.volume_shares + 1}) for b in old.bars)}
    )
    audited = period._compare_opening_markets(old, new, terminal)
    assert audited["required_inputs_equal"] is True
    assert audited["visible_prefix_equal"] is False
    assert audited["non_valuation_differences"] == {"volume_shares": sum(b.received_at <= terminal for b in old.bars)}
    assert audited["full_historical_replay_input_equivalence_claimed"] is False
    late = old.bars[-1].model_copy(
        update={
            "start": at("14:59:00"),
            "end": at("15:00:00"),
            "received_at": at("15:00:05"),
            "close": old.bars[-1].close + 100,
        }
    )
    assert (
        period._compare_opening_markets(old, old.model_copy(update={"bars": old.bars + (late,)}), terminal)[
            "visible_prefix_equal"
        ]
        is True
    )
    changed = old.model_copy(
        update={"bars": (old.bars[0].model_copy(update={"close": old.bars[0].close + 1}), *old.bars[1:])}
    )
    assert period._compare_opening_markets(old, changed, terminal)["required_inputs_equal"] is False
