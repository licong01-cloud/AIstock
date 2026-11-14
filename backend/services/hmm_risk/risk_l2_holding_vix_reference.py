"""Approved L2-HELD-VIX reference; frozen signals, not an account simulator."""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime, time
import hashlib
import inspect
import json
import math
from pathlib import Path
import re
import subprocess
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from backend.services.hmm_risk import risk_l2_member_loss as member

VERSION = "hmm_l2_held_vix_reference_v1"
CONTRACT = "L2-HELD-VIX-D1-D6"
MANIFEST = "772112c58f959748787b939e33ec874fb0766ca921d88eddbd49c131c3b3c43d"
PINS = {
    "collection": "5d41be51fee66df0f58d47cda24f2eb07004427e163222288b8d18c749020ea7",
    "accounts": "007ee44a7aa2622533881fddb47250efbbb593781c56a5fc882b51c83701ba42",
    "previous": "979a2e652f4b8a450a9fcf1bd1fc7206a164258ab7c031cc6665b05cb65045ba",
    "original": "f21087884e9b617e8372fc7b4ae46a5a2187d05b1baa4757b48250b07ac7ffb9",
}
FEATURE_PIN = "78472f692dac2cdfea27553b7d51e4ff053f046f969bfd24c88974be78a5c5ee"
VIX_PIN = "5cf59b8af771588baf9f81115a532991ef18b339ae20842fd97adf968d299bf2"
SEEDS = (42, 2024, 2026)
COST_BPS = (0, 5, 10, 20)
LIMITATIONS = [
    "REFERENCE_NOT_QE_ACCOUNT_NAV",
    "X_MATCHES_OPENING_AMOUNT_NOT_0915_EXECUTABLE_POLICY_OR_CONSTANT_EXPOSURE",
    "VIX_PUBLICATION_UNKNOWN_NON_VINTAGE_THEORETICAL_1615_NEW_YORK_PROXY",
    "RETROSPECTIVE_CLASSIFICATION_CONSUMED_HISTORY_NOT_UNTOUCHED_HOLDOUT",
    "FROZEN_10D_MODEL_APPLIED_TO_FIVE_SESSIONS_PLUS_RESTORE_OVERNIGHT",
    "FRACTIONAL_ENCODED_SHARES_NO_FILL_OR_LOT_OR_REAL_COST_CLAIM",
]


def require(ok, message):
    if not ok:
        raise ValueError(f"{VERSION}: {message}")


def number(value, *, positive=False):
    require(type(value) in (float, int) and math.isfinite(value), "invalid numeric fact")
    require(not positive or value > 0, "nonpositive fact")
    return float(value)


def vix_context(frame, decisions):
    """Actual Cboe release dates; close time is a proxy, never publication proof."""
    frame = frame.sort_values("trade_date").reset_index(drop=True)
    dates = [str(d) for d in frame.trade_date]
    require(dates == sorted(set(dates)), "duplicate VIX dates")
    values = [number(float(x), positive=True) for x in frame.close]
    ends = [datetime.combine(date.fromisoformat(d), time(16, 15), ZoneInfo("America/New_York")) for d in dates]
    result = {}
    for day in decisions:
        clock = datetime.combine(date.fromisoformat(day), time(9, 15), ZoneInfo("Asia/Shanghai"))
        indices = [i for i, end in enumerate(ends) if end <= clock]
        require(len(indices) >= 2, f"insufficient VIX context: {day}")
        i = indices[-1]
        result[day] = dict(
            us_date=dates[i],
            previous_us_date=dates[i - 1],
            close=values[i],
            previous_close=values[i - 1],
            change=values[i] / values[i - 1] - 1,
            age_hours=(clock - ends[i]).total_seconds() / 3600,
            publication_time_quality="UNKNOWN",
            historical_as_known=False,
        )
    return result


def episode_schedule(calendar, decisions, contexts):
    require(calendar == sorted(set(calendar)) and decisions == sorted(set(decisions)), "unordered calendar")
    positions = {d: i for i, d in enumerate(calendar)}
    require(all(d in positions and positions[d] > 0 for d in decisions), "decision outside calendar")
    used, active, episodes, audit = set(), None, [], []
    for day in decisions:
        i, v = positions[day], contexts[day]
        reason = "VIX_NONPOSITIVE"
        if active is not None:
            reason = "RESTORE" if i == active["restore_index"] else "ACTIVE_EPISODE"
            require(i <= active["restore_index"], "episode clock skipped restore")
            episode_id = active["start"]
            if reason == "RESTORE":
                active = None
        elif v["change"] > 0 and v["us_date"] not in used:
            active = dict(start=day, restore_index=i + 5, restore=calendar[i + 5] if i + 5 < len(calendar) else None)
            episodes.append(active.copy())
            used.add(v["us_date"])
            reason, episode_id = "START", day
        else:
            reason = "REUSED_VIX_OBSERVATION" if v["change"] > 0 else reason
            episode_id = None
        audit.append(dict(day=day, reason=reason, episode=episode_id, vix=v))
    return episodes, audit


def signal_ranks(collection, calendar, catalog):
    require(
        collection["schema_version"] == "hmm_member_prediction_transport_collection_v1"
        and collection["arm"] == "C"
        and collection["catalog"] == catalog,
        "C collection identity differs",
    )
    positions = {d: i for i, d in enumerate(calendar)}
    vintages = {q["model_vintage"]["id"]: q for q in collection["quarters"]}
    require(len(vintages) == len(collection["quarters"]), "duplicate model vintage")
    groups = {}
    for row in collection["predictions"]:
        day, code = row["trade_date"], row["sector_code"]
        require(
            day in positions and positions[day] > 0 and row["as_of_date"] == calendar[positions[day] - 1],
            "prediction as-of differs",
        )
        q = vintages.get(row["vintage_id"])
        require(
            q is not None
            and q["decision_start"] <= day <= q["decision_end"]
            and row["model_sha256"] == q["model_parameter_sha256"],
            "prediction model/vintage differs",
        )
        group = groups.setdefault(day, {})
        require(code in catalog and code not in group, "unknown/duplicate L2 prediction")
        value = row["expected_member_loss"]
        if row["availability"] == "available":
            require(row["reason_code"] is None and 0 <= number(value) <= 1, "invalid available C")
        else:
            require(
                row["availability"] == "unavailable" and value is None and bool(row["reason_code"]),
                "unexplained C availability",
            )
        group[code] = row
    result = {}
    for day, group in groups.items():
        require(set(group) == set(catalog), "incomplete L2 denominator")
        available = {k: x["expected_member_loss"] for k, x in group.items() if x["availability"] == "available"}
        ranks = (
            (pd.Series(available, dtype=float).rank(method="average") - 1) / (len(available) - 1)
            if len(available) >= 2
            else pd.Series(dtype=float)
        )
        result[day] = {
            k: dict(
                rank=float(ranks[k]) if k in ranks else None,
                reason=("NO_IDENTIFIABLE_L2_RANK" if len(available) < 2 else x["reason_code"]),
            )
            for k, x in group.items()
        }
    return result


def evaluate_basket(stocks, path_dates):
    """Fixed encoded-share basket, cash-funded restore; never compounds episodes."""
    stocks = sorted(stocks, key=lambda s: s["symbol"])
    require(len({s["symbol"] for s in stocks}) == len(stocks), "duplicate held security")
    require(all(len(s["closes"]) == len(path_dates) for s in stocks), "daily path shape differs")
    if not stocks:
        return dict(status="NO_PRIOR_HOLDING", stocks=[], metrics=None)
    missing = [dict(symbol=s["symbol"], missing=s["missing"]) for s in stocks if s["missing"]]
    if missing:
        return dict(status="PAIRED_VALUATION_UNAVAILABLE", stocks=stocks, missing=missing, metrics=None)
    quantities = np.array([number(s["quantity"], positive=True) for s in stocks])
    opening = np.array([number(s["open"], positive=True) for s in stocks])
    restore = np.array([number(s["restore_open"], positive=True) for s in stocks])
    cuts = np.array([0.0 if s["rank"] is None else 0.1 * number(s["rank"]) for s in stocks])
    require(np.all((cuts >= 0) & (cuts <= 0.1)), "reduction exceeds contract")
    eligible = np.array([s["rank"] is not None for s in stocks])
    budget = float(np.sum(quantities * opening))
    eligible_budget = float(np.sum(quantities[eligible] * opening[eligible]))
    removed = float(np.sum(quantities * opening * cuts))
    if eligible_budget == 0 or removed == 0:
        return dict(status="NO_ELIGIBLE_REDUCTION", stocks=stocks, metrics=None, budget=budget)
    matched = np.where(eligible, removed / eligible_budget, 0.0)
    require(math.isclose(float(np.sum(quantities * opening * matched)), removed, rel_tol=1e-12), "X amount differs")
    b = float(np.sum(quantities * (restore - opening)) / budget)
    r = -float(np.sum(quantities * cuts * (restore - opening)) / budget)
    x = -float(np.sum(quantities * matched * (restore - opening)) / budget)
    sensitivity = {}
    for bp in COST_BPS:
        fee = bp / 10000 * removed / budget
        sensitivity[str(bp)] = dict(R_minus_B=r - fee, X_minus_B=x - fee, R_minus_X=r - x, cost=fee)
    paths = []
    for i, day in enumerate(path_dates):
        closes = np.array([number(s["closes"][i], positive=True) for s in stocks])
        arms = {}
        for arm, cut in (("B_hold", np.zeros(len(stocks))), ("R_hold", cuts), ("X_hold", matched)):
            equity = float(np.sum(quantities * (1 - cut) * closes))
            cash = 0.0 if arm == "B_hold" else removed
            arms[arm] = dict(
                stock_value=equity,
                cash=cash,
                gross_budget_return=(equity + cash) / budget - 1,
                stock_exposure=equity / (equity + cash),
            )
        paths.append(dict(day=day, arms=arms))
    restoration = []
    for s, q, p, end, rcut, xcut in zip(stocks, quantities, opening, restore, cuts, matched, strict=True):
        arms = {}
        for name, cut in (("R_hold", rcut), ("X_hold", xcut)):
            cash = float(q * p * cut)
            restored = cash * (1 - 10 / 10000) / end
            arms[name] = dict(
                sold_encoded_shares=float(q * cut),
                released_cash=cash,
                roundtrip_cost_10bp=cash * 0.001,
                restored_encoded_shares=float(restored),
                share_gap=float(restored - q * cut),
                external_cash=0.0,
            )
        restoration.append(dict(symbol=s["symbol"], arms=arms))
    return dict(
        status="VALUED_REFERENCE",
        stocks=stocks,
        budget=budget,
        eligible_budget=eligible_budget,
        removed_amount=removed,
        removed_budget_fraction=removed / budget,
        x_fraction=removed / eligible_budget,
        metrics=dict(
            B_hold_gross=b,
            R_minus_B_gross=r,
            X_minus_B_gross=x,
            R_minus_X=r - x,
            avoided_negative_contribution=-float(np.sum(quantities * cuts * np.minimum(restore - opening, 0))) / budget,
            forgone_positive_contribution=float(np.sum(quantities * cuts * np.maximum(restore - opening, 0))) / budget,
        ),
        sensitivity=sensitivity,
        daily_path=paths,
        restoration_10bp=restoration,
        execution_status="NOT_SIMULATED",
        denominator="prior_holding_budget_return",
    )


def summarize(records, episodes):
    pairs = []
    for episode in episodes:
        rows = [r for r in records if r["start"] == episode["start"]]
        require(sorted(r["seed"] for r in rows) == list(SEEDS), "missing seed episode")
        complete = all(r["status"] == "VALUED_REFERENCE" for r in rows)
        row = dict(start=episode["start"], statuses={str(r["seed"]): r["status"] for r in rows}, paired=complete)
        if complete:
            row["sensitivity"] = {
                str(bp): {
                    k: math.fsum(r["sensitivity"][str(bp)][k] for r in rows) / len(SEEDS)
                    for k in ("R_minus_B", "X_minus_B", "R_minus_X", "cost")
                }
                for bp in COST_BPS
            }
        pairs.append(row)
    complete = [r for r in pairs if r["paired"]]

    def means(rows):
        return (
            {
                str(bp): {
                    k: math.fsum(r["sensitivity"][str(bp)][k] for r in rows) / len(rows)
                    for k in ("R_minus_B", "X_minus_B", "R_minus_X", "cost")
                }
                for bp in COST_BPS
            }
            if rows
            else None
        )

    worst = max(complete, key=lambda r: abs(r["sensitivity"]["10"]["R_minus_X"])) if complete else None
    return dict(
        common_paired_episodes=len(complete),
        episode_count=len(episodes),
        paired_mask_and_contributions=pairs,
        means=means(complete),
        segments={
            name: means([r for r in complete if start <= r["start"] <= end])
            for name, start, end in (
                ("Q2", "2026-04-01", "2026-06-30"),
                ("Q3", "2026-07-01", "2026-09-30"),
                ("October", "2026-10-01", "2026-10-30"),
            )
        },
        remove_largest_absolute_R_minus_X=dict(
            removed_start=worst["start"] if worst else None, means=means([r for r in complete if r is not worst])
        ),
        states=dict(Counter(r["status"] for r in records)),
        economic_adoption=False,
        research_judgment="REQUIRES_RESULT_REVIEW_NOT_AUTOMATIC_PROMOTION",
    )


def _native_extract(pin, expected, manifest_identity):
    """Run only in the existing Qlib Python; pinned trusted pickle inputs, stdout only."""
    import hashlib
    import json
    import math
    from pathlib import Path

    import pandas as pd

    def check(ok, message):
        if not ok:
            raise ValueError(message)

    def sha(path):
        check(path.is_file() and not any(p.is_symlink() for p in (path, *path.parents)), "nonordinary native file")
        return hashlib.sha256(path.read_bytes()).hexdigest()

    root = Path(pin["workspace"])
    mp = root / "qe_l2_member_loss_manifest.json"
    check(sha(mp) == pin["manifest_sha256"], "native manifest drift")
    meta = json.loads(mp.read_bytes())
    check(
        meta["arm"] == "B0" and meta["risk_binding"]["release_binding"]["manifest_sha256"] == manifest_identity,
        "native arm/release differs",
    )
    recorder_path = root / "qe_current_recorder.json"
    rec = json.loads(recorder_path.read_bytes())
    check(rec == pin["recorder"], "native recorder changed")
    ap = root / "mlruns" / str(rec["experiment_id"]) / rec["recorder_id"] / "artifacts" / "portfolio_analysis"
    paths = {
        k: ap / name
        for k, name in dict(
            positions="positions_normal_1day.pkl",
            report="report_normal_1day.pkl",
            indicators="indicators_normal_1day_obj.pkl",
            aggregate="indicators_normal_1day.pkl",
        ).items()
    }
    paths.update(actions=root / "qe_l2_member_loss_actions.jsonl", mapping=root / meta["assets"]["mapping"]["path"])
    check({k: sha(p) for k, p in paths.items()} == expected, "native file hash differs")
    # Only these known original artifacts may be deserialized. No new account is run.
    positions, report = pd.read_pickle(paths["positions"]), pd.read_pickle(paths["report"])
    actions = [json.loads(line) for line in paths["actions"].read_text().splitlines()]
    dates = pin["dates"]
    check(
        [str(t.date()) for t in sorted(positions)] == dates == [str(t.date()) for t in report.index],
        "native complete dates differ",
    )
    check([r["trade_date"] for r in actions] == dates and len(dates) == 141, "action dates differ")
    previous, result = {}, []
    for row in actions:
        check(row["arm"] == "B0" and not row["risk_applied"] and not row["actual_fill_claimed"], "not original B0")
        check(set(previous) == set(row["held_before"]), "held-before population differs")
        result.append(
            dict(
                day=row["trade_date"],
                as_of=row["as_of_date"],
                holdings=[dict(symbol=s, quantity=q, prior_close=p) for s, (q, p) in sorted(previous.items())],
            )
        )
        position = positions[pd.Timestamp(row["trade_date"])]
        previous = {
            s: (float(position.get_stock_amount(s)), float(position.get_stock_price(s)))
            for s in position.get_stock_list()
        }
        check(
            all(math.isfinite(q) and math.isfinite(p) and q > 0 and p > 0 for q, p in previous.values()),
            "invalid native holdings",
        )
    check(
        {k: sha(p) for k, p in paths.items()} == expected
        and sha(mp) == pin["manifest_sha256"]
        and json.loads(recorder_path.read_bytes()) == rec,
        "native inputs changed during read",
    )
    return dict(seed=pin["seed"], loop=pin["loop"], pins=expected, days=result)


def read_accounts(receipt, previous, native_access):
    originals = sorted((r for r in receipt["rows"] if r["arm"] == "B0"), key=lambda r: r["seed"])
    require([r["seed"] for r in originals] == list(SEEDS), "original B0 seeds differ")
    accounts = []
    for pin in originals:
        evidence = next(r for r in previous["native"] if r["seed"] == pin["seed"])
        require(evidence["workspace"] == pin["workspace"] and evidence["loop"] == pin["loop"], "workspace differs")
        require(pin["node"] in ("rdagent-node1", "wsl2-5080"), "unknown native owner")
        access = native_access[pin["node"]]
        python = access["python"]
        require(re.fullmatch(r"/[A-Za-z0-9_./-]+/python", python) is not None, "explicit native Python required")
        if pin["node"] == "rdagent-node1":
            require(
                re.fullmatch(r"[A-Za-z0-9_]+@[A-Za-z0-9_.-]+", access["ssh_target"]) is not None,
                "invalid read-only SSH target",
            )
        else:
            require(re.fullmatch(r"[A-Za-z0-9_.-]+", access["distro"]) is not None, "invalid WSL distribution")
        command = (
            [
                "ssh",
                "-F",
                "NUL",
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=10",
                access["ssh_target"],
                python,
                "-B",
                "-",
            ]
            if pin["node"] == "rdagent-node1"
            else ["wsl", "--distribution", access["distro"], "--exec", python, "-B", "-"]
        )
        code = inspect.getsource(_native_extract) + "\nimport json\nprint('HELD_NATIVE '+json.dumps(_native_extract("
        code += repr(pin) + "," + repr(evidence["pins"]) + "," + repr(MANIFEST) + "),allow_nan=False))\n"
        child = subprocess.run(command, input=code, encoding="utf-8", capture_output=True, timeout=180)
        require(child.returncode == 0, "native file read failed: " + child.stderr[-1500:])
        lines = [s.removeprefix("HELD_NATIVE ") for s in child.stdout.splitlines() if s.startswith("HELD_NATIVE ")]
        require(len(lines) == 1, "native output missing/ambiguous")
        accounts.append(json.loads(lines[0]))
    return accounts


class ReferencePrices:
    """Read the existing encoded QE price basis, never raw prices with encoded shares."""

    def __init__(self, source):
        self.source, self.opening = source, {}

    def open(self, symbol, day):
        source = self.source
        start, closes, factors = source._arrays(symbol)
        if symbol not in self.opening:
            path = source.asset(f"components/daily_bin_candidate/features/{symbol.lower()}/open.day.bin")
            arr = np.fromfile(path, dtype="<f4")
            require(len(arr) == len(closes) and arr[0] == start, "opening bin/calendar differ")
            self.opening[symbol] = arr
        at = 1 + source.positions[day] - start
        value = float(self.opening[symbol][at]) if 1 <= at < len(closes) else float("nan")
        if not math.isfinite(value):
            _, reason = source.price(symbol, day, True)
            require(reason in member.LEGAL_ENDPOINT_NA, f"unexplained open: {symbol}/{day}")
            return None, reason
        require(math.isfinite(float(factors[at])) and factors[at] > 0, f"invalid open factor: {symbol}/{day}")
        return number(value, positive=True), None

    def stocks(self, holdings, day, restore, ranks):
        source = self.source
        as_of = source.calendar[source.positions[day] - 1]
        path = source.calendar[source.positions[day] : source.positions[restore]]
        result = []
        for held in sorted(holdings, key=lambda h: h["symbol"]):
            symbol, missing = held["symbol"], []
            require(symbol in source.spans, f"unknown held symbol: {symbol}")
            projection = source.projection.resolve(symbol, date.fromisoformat(as_of))
            if projection.status == "resolved":
                require(projection.l2_code in source.catalog, "held L2 escapes catalog")
                code, rank = projection.l2_code, ranks[projection.l2_code]["rank"]
                reason = ranks[code]["reason"]
            else:
                require(
                    projection.status == "unavailable" and projection.reason_code is not None,
                    "unexplained/ambiguous held classification",
                )
                code, rank, reason = None, None, "HELD_CLASSIFICATION_UNAVAILABLE"
            prior, why = source.price(symbol, as_of, True)
            if prior is None:
                missing.append(dict(day=as_of, reason=why))
            else:
                bound = abs(float(np.spacing(np.float32(prior)))) + abs(
                    float(np.spacing(np.float32(held["prior_close"])))
                )
                if abs(prior - held["prior_close"]) > bound:
                    missing.append(
                        dict(day=as_of, reason="VALUATION_BASIS_UNRESOLVED", native=held["prior_close"], daily=prior)
                    )
            opening, why = self.open(symbol, day)
            if why:
                missing.append(dict(day=day, reason=why))
            last, why = self.open(symbol, restore)
            if why:
                missing.append(dict(day=restore, reason=why))
            closes = []
            for dt in path:
                price, why = source.price(symbol, dt, True)
                closes.append(price)
                if why:
                    missing.append(dict(day=dt, reason=why))
            suspended = [dt for dt in [*path, restore] if (date.fromisoformat(dt), symbol) in source.suspended]
            result.append(
                dict(
                    symbol=symbol,
                    quantity=held["quantity"],
                    prior_close=held["prior_close"],
                    sector_code=code,
                    rank=rank,
                    no_overlay_reason=reason,
                    classification_reason=projection.reason_code,
                    classification_receipt_hash=projection.classification_receipt_hash,
                    open=opening,
                    restore_open=last,
                    closes=closes,
                    missing=missing,
                    suspension_dates=suspended,
                    limit_fill_status="UNKNOWN_NOT_SIMULATED",
                )
            )
        return result, path


def execute(request):
    from backend.data_service.shared_vix_inputs import read_corrected_bundle

    require(
        set(request)
        == {
            "schema_version",
            "contract",
            *PINS,
            "features",
            "features_file_sha256",
            "provider_absence_manifest",
            "vix_root",
            "native_access",
        },
        "unexpected request parameters",
    )
    require(request["schema_version"] == VERSION + "_request" and request["contract"] == CONTRACT, "request differs")
    objects = {name: member.pinned_json(request[name], pin) for name, pin in PINS.items()}
    features = member.pinned_json(request["features"], request["features_file_sha256"])
    member.core.verify_receipt(features)
    require(features["receipt_sha256"] == FEATURE_PIN, "frozen feature source differs")
    collection = objects["collection"]
    member.core.verify_receipt(collection)
    require(collection["release_binding"]["manifest_sha256"] == MANIFEST, "prediction release differs")
    source = member.FrozenMemberSource(
        features,
        dict(source_request_path=request["original"], source_request_sha256=PINS["original"]),
        provider_absence_manifest=Path(request["provider_absence_manifest"]),
    )
    require(
        source.source["release_binding"]["manifest_sha256"] == MANIFEST and len(source.catalog) == 131,
        "formal source identity differs",
    )
    calendar = source.calendar
    prediction_dates = [d for d in calendar if "2026-04-01" <= d <= "2026-10-30"]
    decisions = [d for d in prediction_dates if d <= "2026-10-29"]
    require(len(prediction_dates) == 142 and len(decisions) == 141, "frozen calendar window differs")
    ranks = signal_ranks(collection, calendar, source.catalog)
    require(sorted(ranks) == prediction_dates, "prediction window differs")
    vix = read_corrected_bundle(Path(request["vix_root"]), expected_manifest_sha256=VIX_PIN)
    contexts = vix_context(vix.data, decisions)
    episodes, audit = episode_schedule(calendar, decisions, contexts)
    accounts = read_accounts(objects["accounts"], objects["previous"], request["native_access"])
    prices, records = ReferencePrices(source), []
    for account in accounts:
        require([r["day"] for r in account["days"]] == decisions, "account dates differ")
        days = {r["day"]: r for r in account["days"]}
        for r in days.values():
            require(r["as_of"] == calendar[source.positions[r["day"]] - 1], "native as-of differs")
        for episode in episodes:
            day, restore = episode["start"], episode["restore"]
            if restore is None or restore > "2026-10-30":
                basket = dict(status="RESTORE_ENDPOINT_NOT_MATURE", metrics=None, holdings=days[day]["holdings"])
            else:
                stocks, path = prices.stocks(days[day]["holdings"], day, restore, ranks[day])
                basket = evaluate_basket(stocks, path)
            records.append(dict(seed=account["seed"], start=day, restore=restore, **basket))
    identity = source.identity()
    # Inputs remain immutable throughout remote extraction and local arithmetic.
    for name, pin in PINS.items():
        member.pinned_json(request[name], pin)
    require(
        hashlib.sha256(Path(request["features"]).read_bytes()).hexdigest() == request["features_file_sha256"],
        "features changed during reference",
    )
    read_corrected_bundle(Path(request["vix_root"]), expected_manifest_sha256=VIX_PIN)
    return dict(
        schema_version=VERSION,
        contract=CONTRACT,
        source_identity=identity,
        original_pins=PINS,
        vix_manifest_sha256=VIX_PIN,
        features_canonical_sha256=FEATURE_PIN,
        model_identities=[
            {k: q[k] for k in ("model_parameter_sha256", "model_receipt_sha256", "model_source_commit")}
            for q in collection["quarters"]
        ],
        native_pins=[{k: a[k] for k in ("seed", "loop", "pins")} for a in accounts],
        coverage=dict(decision_days=141, signal_days=142, sectors=131, prediction_rows=18602),
        decision_audit=audit,
        records=records,
        summary=summarize(records, episodes),
        limitations=LIMITATIONS,
        fits=0,
        new_inference_calls=0,
        qe_tasks=0,
        database_access=False,
        dataset_write=False,
        active_profile_write=False,
        service_control=False,
        historical_as_known=False,
        independent_market_histories=1,
        remote_existing_file_read=True,
        execution_status="NOT_SIMULATED",
    )
