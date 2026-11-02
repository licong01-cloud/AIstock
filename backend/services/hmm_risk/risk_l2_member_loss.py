"""Paired fixed-cohort/member losses; immutable research files, no policy."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date
from functools import lru_cache
import hashlib
import json
import math
from pathlib import Path
import re

import numpy as np
import pandas as pd

from backend.services.hmm_risk import risk_l2_loss_severity as loss
from backend.services.hmm_risk.contracts import ALL_CORE_FEATURES, canonical_json_bytes, canonical_sha256
from backend.services.hmm_risk.formal_state_executor import read_json, validate_output_location, write_once
from backend.services.hmm_risk.formal_state_model import receipt
from backend.services.hmm_risk.monthly_release_input import _Files, _manifest_pins, _unique_object
from backend.services.hmm_risk.provider_absence import load_provider_absence_manifest
from backend.services.hmm_risk.security_identity import load_security_source_identity_manifest

core, rolling, require = loss.core, loss.rolling, loss.require
reader = core.reader
VERSION = "hmm_risk_l2_member_loss_study_v1"
ARMS = {
    "B": ("hmm_risk_l2_fixed_cohort_portfolio_loss_ridge_v1", "expected_fixed_cohort_portfolio_loss"),
    "C": ("hmm_risk_l2_member_terminal_loss_ridge_v1", "expected_member_loss"),
}
TARGET = dict(
    as_of="previous_open_session",
    anchor="decision_close",
    horizon=10,
    members="PIT_at_asof_excluding_asof_suspensions",
    weights="strict_prior_circ_mv_fixed_at_asof",
    price_basis="C010_unadjusted_integer_li",
    B="max(0,-sum(w*R))",
    C="sum(w*max(0,-R))",
)
CONTRACT = dict(
    version=VERSION,
    target=TARGET,
    parameters=loss.PARAMS,
    feature_names=list(ALL_CORE_FEATURES),
    vintages=list(rolling.VINTAGES),
    planned_fits=8,
    numeric_versions=loss.EXPECTED_VERSIONS,
    historical_as_known=False,
    untouched_holdout=False,
    policy_applied=False,
)
LEGAL_ENDPOINT_NA = {"LEGAL_SUSPENSION_ENDPOINT_NA", "PIT_EXIT_OR_ENTRY_ENDPOINT_NA"}
OLD_A_PIN = "75250a22ea24aef564fc40c265744855122c9f95d244d9a03416e8025e9dd267"
COHORT_PIN = "4da038f25d37e87f407ab21bec1768eccba202164f517093782bcedb3b53da02"
EXPECTED_PREDICTION_DAYS = 125


def _number(value, *, positive=False):
    require(type(value) in (float, int) and math.isfinite(value), "nonfinite/nonnumeric member fact", "source_invalid")
    require(not positive or value > 0, "nonpositive member weight", "source_invalid")
    return float(value)


def pair_target(members):
    """Never remove a future-unpriceable member or renormalize its peers."""
    require(len({m["symbol"] for m in members}) == len(members), "duplicate fixed member", "identity_mismatch")
    if not members:
        return dict(
            status="UNAVAILABLE",
            reason="NO_ASOF_MEMBER",
            B=None,
            C=None,
            upside=None,
            members=0,
            missing_members=[],
            missing_weight_fraction=None,
        )
    weights = [_number(m["weight"], positive=True) for m in members]
    denominator = math.fsum(weights)
    require(math.isfinite(denominator) and denominator > 0, "invalid fixed weight denominator", "source_invalid")
    missing = []
    values = []
    for member in members:
        if member["value"] is None:
            require(
                member["reason"] in LEGAL_ENDPOINT_NA, f"unknown endpoint absence: {member['symbol']}", "source_invalid"
            )
            missing.append(member["symbol"])
            values.append(None)
        else:
            require(member["reason"] is None, "finite return with unavailable reason", "source_invalid")
            value = _number(member["value"])
            require(value > -1, "member return must be greater than -1", "source_invalid")
            values.append(value)
    identity = canonical_sha256([[m["symbol"], w] for m, w in zip(members, weights, strict=True)])
    if missing:
        return dict(
            status="UNAVAILABLE",
            reason="FIXED_COHORT_ENDPOINT_NA",
            B=None,
            C=None,
            upside=None,
            members=len(members),
            member_weight_sha256=identity,
            missing_members=missing,
            missing_weight_fraction=math.fsum(w for w, v in zip(weights, values, strict=True) if v is None)
            / denominator,
        )
    weighted = [w / denominator for w in weights]
    portfolio = math.fsum(w * v for w, v in zip(weighted, values, strict=True))
    member_loss = math.fsum(w * max(0.0, -v) for w, v in zip(weighted, values, strict=True))
    upside = math.fsum(w * max(0.0, v) for w, v in zip(weighted, values, strict=True))
    require(all(math.isfinite(v) for v in (portfolio, member_loss, upside)), "nonfinite target", "source_invalid")
    return dict(
        status="AVAILABLE",
        reason=None,
        B=max(0.0, -portfolio),
        C=member_loss,
        upside=upside,
        members=len(members),
        member_weight_sha256=identity,
        missing_members=[],
        missing_weight_fraction=0.0,
    )


def prior_values(series, as_of_dates):
    require(series.index.is_unique and series.index.is_monotonic_increasing, "cap event keys differ", "source_invalid")
    # reindex(method=...) carries a real previous event, including an explicit
    # invalid event. Series.ffill() would incorrectly erase invalid latest facts.
    return series.reindex(pd.DatetimeIndex(as_of_dates), method="ffill").to_numpy(dtype=float)


def target_dates(calendar, decisions, maturity_end):
    require(
        calendar == sorted(set(calendar)) and decisions == sorted(set(decisions)), "calendar/decision order differs"
    )
    positions = {d: i for i, d in enumerate(calendar)}
    require(
        maturity_end in positions and all(d in positions and positions[d] > 0 for d in decisions),
        "target boundary absent",
    )
    mature = [d for d in decisions if positions[d] + 10 <= positions[maturity_end]]
    return dict(
        mature=mature,
        immature=[d for d in decisions if d not in set(mature)],
        last_outcome=calendar[positions[mature[-1]] + 10] if mature else None,
    )


def pinned_json(path, expected):
    path = Path(path)
    reader._require_sha256(expected, "input byte SHA256")
    require(
        path.is_absolute() and not any(reader._is_indirect_path(p) for p in (path, *path.parents)),
        "indirect/relative input",
    )
    before = core.inputs._stamp(path)
    raw = path.read_bytes()
    require(hashlib.sha256(raw).hexdigest() == expected, "input byte identity differs", "identity_mismatch")
    value = json.loads(
        raw,
        object_pairs_hook=_unique_object,
        parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite JSON")),
    )
    canonical_sha256(value)
    require(core.inputs._stamp(path) == before, "input changed during read", "identity_mismatch")
    return value


def provider_path(path, expected):
    path = Path(path)
    require(
        path.is_absolute() and not any(reader._is_indirect_path(p) for p in (path, *path.parents)),
        "explicit absolute provider authority required",
    )
    before = core.inputs._stamp(path)
    authority = load_provider_absence_manifest(path, expected_sha256=expected)
    require(authority.manifest_sha256 == expected and core.inputs._stamp(path) == before, "provider authority differs")
    return path


class FrozenMemberSource:
    """Current formal file reader plus bounded scalar member observations."""

    def __init__(self, features, original, *, provider_absence_manifest):
        self.source = features["source"]
        provider = provider_path(provider_absence_manifest, self.source["provider_absence_sha256"])
        self.assets = reader.load_rotation_l1_direct_v2_source_assets(
            Path(self.source["candidate_root"]),
            security_identity_manifest=Path(self.source["security_identity_manifest"]),
            provider_absence_manifest=provider,
            data_window_end=date(2026, 9, 30),
            frozen_release_binding=self.source["release_binding"],
        )
        self.files = _Files(self.assets["release_root"])
        manifest = self.files.read(self.files.path("qe_dataset_manifest.json"))
        self.pins = _manifest_pins(manifest)
        require(
            manifest["dataset_manifest_sha256"] == loss.MANIFEST_PIN,
            "member source release changed",
            "identity_mismatch",
        )
        self.calendar = features["calendar"]
        self.positions = {d: i for i, d in enumerate(self.calendar)}
        self.dates = [date.fromisoformat(d) for d in self.calendar]
        calendar_path = self.asset("components/daily_bin_candidate/calendars/day.txt")
        require(calendar_path.read_text(encoding="utf-8").splitlines() == self.calendar, "source calendar differs")
        self.spans = reader._parse_instrument_spans(self.assets["instrument_universe_path"])
        self.catalog = features["catalog"]
        request = pinned_json(original["source_request_path"], original["source_request_sha256"])
        core.verify_receipt(request)
        frozen, _, _ = core.model_source(request)
        self.adapter = reader._industry_adapter(self.source["industry_authority"], forbidden_roots=())
        require(tuple(reader._canonical_sector_codes(self.adapter)[1]) == tuple(self.catalog), "member catalog changed")
        self.projection = reader._IndustryProjectionIndex(self.adapter, calendar=self.dates)
        security_path = self.assets["files"]["security_identity"]
        self.security = reader._SecurityResolutionIndex(
            load_security_source_identity_manifest(
                security_path, expected_sha256=canonical_sha256(read_json(security_path))
            )
        )
        require(
            self.security.evidence()["manifest_sha256"] == frozen["security_identity_sha256"],
            "security identity changed",
        )
        self.suspended = reader._load_suspend_keys(
            self.assets["files"]["suspend_data"],
            self.assets["files"]["suspend_manifest"],
            calendar=self.dates,
            expected_release_cutoff=self.assets["release_cutoff"],
            expected_universe_key=self.assets["universe_key"],
            bounded=True,
        )
        self.encoding = reader._QlibPriceEncoding(self.assets["qlib_root"], self.dates)
        self.as_of = sorted(set(features["as_of"].values()))
        basic_path = self.asset("components/factor_h5_static_candidate_v2/daily_basic.h5")
        with pd.HDFStore(basic_path, mode="r") as store:
            if store.get_storer("data").is_table:
                basic = store.select(
                    "data",
                    where=[
                        f"datetime >= Timestamp('{reader.SOURCE_START}')",
                        f"datetime <= Timestamp('{self.as_of[-1]}')",
                    ],
                    columns=["db_circ_mv"],
                )
            else:
                basic = reader._load_fixed_h5_window(
                    basic_path,
                    expected_columns=reader._DAILY_BASIC_COLUMNS,
                    expected_dtype="<f4",
                    start=reader.SOURCE_START,
                    end=date.fromisoformat(self.as_of[-1]),
                )[["db_circ_mv"]]
        require(not basic.index.has_duplicates, "duplicate cap events", "source_invalid")
        self.cap = {
            str(code): prior_values(group.droplevel("instrument")["db_circ_mv"].sort_index(), self.as_of)
            for code, group in basic.groupby(level="instrument", sort=False)
        }
        self.as_of_pos = {d: i for i, d in enumerate(self.as_of)}
        self.quote_arrays = {}
        self.source_aliases = Counter()

    def asset(self, relative):
        require(relative in self.pins, f"unpinned member input: {relative}", "identity_mismatch")
        return self.files.verify(self.pins[relative])

    def _arrays(self, symbol):
        if symbol not in self.quote_arrays:
            base = f"components/daily_bin_candidate/features/{symbol.lower()}"
            arrays = [np.fromfile(self.asset(f"{base}/{field}.day.bin"), dtype="<f4") for field in ("close", "factor")]
            close, factor = arrays
            require(
                len(close) == len(factor) > 1
                and close[0] == factor[0]
                and np.isfinite(close[0])
                and close[0] >= 0
                and int(close[0]) == close[0],
                "bin source layout differs",
            )
            self.quote_arrays[symbol] = (int(close[0]), close, factor)
        return self.quote_arrays[symbol]

    @lru_cache(maxsize=800_000)
    def price(self, symbol, day, adjusted=False):
        start, close, factors = self._arrays(symbol)
        at = 1 + self.positions[day] - start
        value = float(close[at]) if 1 <= at < len(close) else float("nan")
        if not math.isfinite(value):
            dt = date.fromisoformat(day)
            if (dt, symbol) in self.suspended:
                return None, "LEGAL_SUSPENSION_ENDPOINT_NA"
            if not any(a <= dt <= b for a, b in self.spans[symbol]):
                return None, "PIT_EXIT_OR_ENTRY_ENDPOINT_NA"
            raise rolling.risk.fail(f"unexplained member quote: {symbol}/{day}", "source_invalid")
        require(
            value > 0 and math.isfinite(float(factors[at])) and factors[at] > 0, f"invalid quote/factor: {symbol}/{day}"
        )
        if adjusted:
            return value, None
        factor = float(factors[at])
        scale = self.encoding.scale(symbol, date.fromisoformat(day))
        value = (
            reader._raw_qlib_price(value, reader._float32_source_interval(factor))
            if scale == 1.0
            else reader._raw_rebased_qlib_price(value, factor, scale)
        )
        return value, None

    def members(self, day):
        require(day in self.positions and self.positions[day] > 0, "decision date absent")
        as_of = self.calendar[self.positions[day] - 1]
        dt = date.fromisoformat(as_of)
        groups = {c: [] for c in self.catalog}
        for symbol, spans in self.spans.items():
            if not any(a <= dt <= b for a, b in spans) or (dt, symbol) in self.suspended:
                continue
            projection = self.projection.resolve(symbol, dt)
            # The formal source's classification partition excludes unresolved
            # identities; index publication status does not remove membership.
            if projection.status != "resolved":
                require(projection.reason_code is not None, "unexplained classification absence", "identity_mismatch")
                continue
            require(projection.l2_code in groups, "classification escapes frozen catalog", "identity_mismatch")
            resolved = self.security.resolve(symbol, dt, "market.daily_basic")
            source = str(resolved.source_ts_code)
            if source != symbol:
                self.source_aliases[f"{symbol}->{source}"] += 1
            require(source in self.cap, f"missing cap source: {symbol}/{as_of}", "source_invalid")
            weight = _number(float(self.cap[source][self.as_of_pos[as_of]]), positive=True)
            groups[projection.l2_code].append((symbol, weight))
        return groups

    def targets(self, decisions, maturity_end):
        plan = target_dates(self.calendar, decisions, maturity_end)
        output = {}
        for day in plan["mature"]:
            end = self.calendar[self.positions[day] + 10]
            groups = self.members(day)
            output[day] = {}
            for code in self.catalog:
                members = []
                for symbol, weight in groups[code]:
                    first, r1 = self.price(symbol, day)
                    last, r2 = self.price(symbol, end)
                    members.append(
                        dict(
                            symbol=symbol,
                            weight=weight,
                            value=None if first is None or last is None else last / first - 1.0,
                            reason=r1 or r2,
                        )
                    )
                output[day][code] = pair_target(members)
        for day in plan["immature"]:
            output[day] = {
                c: dict(status="OUTCOME_NOT_MATURE", reason="OUTCOME_NOT_MATURE", B=None, C=None, upside=None)
                for c in self.catalog
            }
        return {day: output[day] for day in decisions}

    def unchanged(self):
        self.encoding.unchanged()
        self.files.unchanged()

    def identity(self):
        self.unchanged()
        return dict(
            release_identity=self.assets["release_identity"],
            calendar_sha256=self.source["calendar_sha256"],
            industry_authority_sha256=canonical_sha256(self.source["industry_authority"]),
            security_identity_sha256=self.source["security_identity_sha256"],
            provider_absence_sha256=self.source["provider_absence_sha256"],
            source_inventory_sha256=canonical_sha256(self.assets["inventory"]),
            read_file_count=len(self.files.hashes),
            read_inventory_sha256=canonical_sha256(
                sorted(
                    [str(p.relative_to(self.files.root)).replace("\\", "/"), h] for p, h in self.files.hashes.items()
                )
            ),
            target_contract=TARGET,
        )


def validate_old_a(old_a, features):
    core.verify_receipt(old_a)
    expected = [
        (d, c)
        for v in rolling.VINTAGES
        for d in rolling.schedule(features["calendar"], v)["predict"]
        for c in features["catalog"]
    ]
    require(
        old_a["contract"] == loss.CONTRACT
        and [(r["trade_date"], r["sector_code"]) for r in old_a["predictions"]] == expected,
        "old A full grid/contract differs",
        "identity_mismatch",
    )


def cohort_facts(entries, source):
    require(isinstance(entries, list) and bool(entries), "missing audited entry cohort")
    output, seen = [], set()
    prediction_days = {d for v in rolling.VINTAGES for d in rolling.schedule(source.calendar, v)["predict"]}
    for entry in entries:
        key = (entry["seed"], entry["date"], entry["stock"])
        require(
            key not in seen and key[1] in prediction_days and entry["code"] in source.catalog,
            "entry cohort key differs",
        )
        seen.add(key)
        day, symbol = key[1:]
        require(symbol in source.spans, "entry stock outside frozen universe", "identity_mismatch")
        as_of = source.calendar[source.positions[day] - 1]
        projection = source.projection.resolve(symbol, date.fromisoformat(as_of))
        require(
            projection.status == "resolved" and projection.l2_code == entry["code"],
            "entry mapping differs",
            "identity_mismatch",
        )
        future = source.positions[day] + 10
        value, reason = None, "OUTCOME_NOT_MATURE"
        if future < len(source.calendar) and source.calendar[future] <= "2026-09-30":
            first, r1 = source.price(symbol, day, True)
            last, r2 = source.price(symbol, source.calendar[future], True)
            reason = r1 or r2
            if reason is None:
                value = last / first - 1.0
        closed = entry["closed"]
        require(type(closed) is bool, "entry completion flag differs")
        episode_loss = entry["episode_loss"]
        require(
            (not closed and episode_loss is None) or (closed and _number(episode_loss) >= 0), "episode loss differs"
        )
        output.append(
            dict(
                seed=entry["seed"],
                date=day,
                stock=symbol,
                code=entry["code"],
                stock_return=value,
                loss=max(0.0, -value) if value is not None else None,
                reason=reason,
                episode_loss=episode_loss,
                closed=closed,
            )
        )
    return output


def prepare(study_path, cohort_path, cohort_sha256, old_a_path, output, source_commit, *, provider_absence_manifest):
    original, features = loss.load_study(study_path)
    validate_output_location(output, dataset_root=Path(features["source"]["candidate_root"]))
    require(re.fullmatch(r"[0-9a-f]{40}", source_commit) is not None, "invalid source commit")
    require(not output.exists(), "prepare output exists")
    require(cohort_sha256 == COHORT_PIN, "audited cohort source differs", "identity_mismatch")
    entries = pinned_json(cohort_path, cohort_sha256)
    old_a = pinned_json(old_a_path, OLD_A_PIN)
    validate_old_a(old_a, features)
    source = FrozenMemberSource(features, original, provider_absence_manifest=provider_absence_manifest)
    train_locators = {}
    for vintage in rolling.VINTAGES:
        plan = rolling.schedule(features["calendar"], vintage)
        targets = source.targets(plan["train"], vintage["train_end"])
        old = original["training_labels"][vintage["id"]]
        old_labels = core.inputs._asset(old["path"], old["sha256"])
        require(old_labels["vintage"] == vintage, "original training vintage differs")
        body = receipt(
            dict(
                schema_version=VERSION + "_train",
                contract=CONTRACT,
                vintage=vintage,
                feature_sha256=features["receipt_sha256"],
                original_label_sha256=old["sha256"],
                labels=targets,
                original_status={
                    d: {c: old_labels["labels"][d][c]["status"] for c in source.catalog} for d in plan["train"]
                },
            )
        )
        path = output / f"train_{vintage['id']}.json"
        write_once(path, body)
        train_locators[vintage["id"]] = dict(path=str(path), sha256=body["receipt_sha256"])
    prediction_days = [d for v in rolling.VINTAGES for d in rolling.schedule(features["calendar"], v)["predict"]]
    evaluation = receipt(
        dict(
            schema_version=VERSION + "_outcomes",
            feature_sha256=features["receipt_sha256"],
            targets=source.targets(prediction_days, "2026-09-30"),
            cohort=cohort_facts(entries, source),
            old_a_sha256=OLD_A_PIN,
        )
    )
    eval_path = output / "outcomes.json"
    write_once(eval_path, evaluation)
    source_identity = source.identity()
    result = receipt(
        dict(
            schema_version=VERSION + "_prepared",
            contract=CONTRACT,
            source_commit=source_commit,
            original_prepared_path=str(study_path),
            original_prepared_sha256=loss.PREPARED_PIN,
            feature_sha256=features["receipt_sha256"],
            training_labels=train_locators,
            evaluation_path=str(eval_path),
            evaluation_sha256=evaluation["receipt_sha256"],
            old_a_path=str(old_a_path),
            old_a_sha256=OLD_A_PIN,
            cohort_source_path=str(cohort_path),
            cohort_source_sha256=cohort_sha256,
            release_binding=features["source"]["release_binding"],
            member_source_identity=source_identity,
            provider_absence_manifest=str(provider_absence_manifest),
            source_aliases=dict(source.source_aliases),
            fits=0,
            database_access=False,
            dataset_write=False,
        )
    )
    write_once(output / "prepared.json", result)
    return result


def load_prepared(path, expected):
    prepared = core.inputs._asset(str(path), expected)
    require(
        prepared["schema_version"] == VERSION + "_prepared"
        and prepared["contract"] == CONTRACT
        and prepared["original_prepared_sha256"] == loss.PREPARED_PIN
        and prepared["old_a_sha256"] == OLD_A_PIN
        and prepared["release_binding"]["manifest_sha256"] == loss.MANIFEST_PIN,
        "paired prepared identity differs",
        "identity_mismatch",
    )
    _, features = loss.load_study(prepared["original_prepared_path"])
    require(prepared["feature_sha256"] == features["receipt_sha256"], "paired feature identity differs")
    return prepared, features


def fit_pair(rows, on_fit_start):
    require(bool(rows) and all(len(r) == 5 and len(r[2]) == 20 for r in rows), "empty/malformed paired rows")
    require(len({(r[0], r[1]) for r in rows}) == len(rows), "duplicate paired training key")
    x = np.asarray([r[2] for r in rows], dtype=float)
    require(np.isfinite(x).all() and np.any(x != x[0]), "invalid/constant paired features")
    for row in rows:
        require(
            all(type(y) in (float, int) and math.isfinite(y) and 0 <= y < 1 for y in row[3:]), "invalid paired target"
        )
    params, stats = {}, {}
    for arm, column in (("B", 3), ("C", 4)):
        on_fit_start(arm)
        params[arm], stats[arm] = loss.fit_parameters([(r[0], r[1], r[2], r[column]) for r in rows])
    require(all(params["B"][k] == params["C"][k] for k in ("mean", "scale", "active")), "paired scalers differ")
    return params, stats


def validate_model(model, expected=None):
    core.verify_receipt(model, expected)
    arm = model["arm"]
    require(
        arm in ARMS
        and model["schema_version"] == ARMS[arm][0] + "_model"
        and model["contract"] == CONTRACT
        and model["vintage"] in rolling.VINTAGES
        and model["feature_sha256"] == loss.FEATURE_PIN
        and model["release_binding"]["manifest_sha256"] == loss.MANIFEST_PIN
        and model["model_sha256"] == canonical_sha256(model["parameters"])
        and model["catalog"] == sorted(set(model["catalog"]))
        and len(model["catalog"]) == 131,
        "member model contract/identity differs",
        "identity_mismatch",
    )
    for key in ("training_label_sha256", "training_mask_sha256", "feature_definition_sha256"):
        reader._require_sha256(model[key], key)
    source = model["member_source_identity"]
    require(
        source["release_identity"]["dataset_manifest_sha256"] == loss.MANIFEST_PIN
        and source["target_contract"] == TARGET,
        "member model source identity differs",
        "identity_mismatch",
    )
    for key in ("read_inventory_sha256", "source_inventory_sha256", "industry_authority_sha256"):
        reader._require_sha256(source[key], key)
    require(
        type(model["fit_rows"]) is int
        and type(model["fit_dates"]) is int
        and 0 < model["fit_dates"] <= model["fit_rows"] <= 131 * model["fit_dates"]
        and math.isfinite(model["sample_weight_sum"])
        and abs(model["sample_weight_sum"] - model["fit_rows"]) < 1e-6,
        "member model sample statistics differ",
        "identity_mismatch",
    )
    require(re.fullmatch(r"[0-9a-f]{40}", model["source_commit"]) is not None, "model source commit differs")
    loss.parameter_arrays(model["parameters"])


def predict(model, rows, days, as_of, calendar):
    validate_model(model)
    require(days and days == sorted(set(days)) and calendar == sorted(set(calendar)), "prediction dates differ")
    positions = {d: i for i, d in enumerate(calendar)}
    vintage = model["vintage"]
    require(
        all(
            d in positions
            and positions[d] > 0
            and as_of[d] == calendar[positions[d] - 1]
            and vintage["train_end"] < d
            and vintage["effective_start"] <= d <= vintage["effective_end"]
            for d in days
        ),
        "prediction outside causal/effective interval",
    )
    core.validate_feature_rows({"decision_dates": days}, {"rows": rows, "catalog": model["catalog"]}, as_of)
    mean, scale, active, coef, intercept = loss.parameter_arrays(model["parameters"])
    field = ARMS[model["arm"]][1]
    output, clipped = [], 0
    for day in days:
        future = calendar[positions[day] + 1 : positions[day] + 11]
        for code in model["catalog"]:
            item = rows[day][code]
            raw = value = None
            if item["features"] is not None:
                z = (np.asarray(item["features"], dtype=float)[active] - mean[active]) / scale[active]
                raw = float(z @ coef + intercept)
                require(np.isfinite(z).all() and math.isfinite(raw), "nonfinite member prediction", "prediction_failed")
                value = min(1.0, max(0.0, raw))
                clipped += value != raw
            output.append(
                dict(
                    trade_date=day,
                    as_of_date=as_of[day],
                    sector_code=code,
                    vintage_id=vintage["id"],
                    model_sha256=model["model_sha256"],
                    **{field: value, "raw_" + field: raw},
                    availability="available" if value is not None else "unavailable",
                    reason_code=item["reason_code"],
                    target_first_return_date=future[0] if future else None,
                    target_last_return_date=future[-1] if len(future) == 10 else None,
                )
            )
    return output, dict(clipped_rows=int(clipped))


def fit_vintage(features, training, vintage, source_commit, on_fit_start, *, member_source_identity):
    core.verify_receipt(training)
    plan = rolling.schedule(features["calendar"], vintage)
    require(
        training["schema_version"] == VERSION + "_train"
        and training["contract"] == CONTRACT
        and training["vintage"] == vintage
        and training["feature_sha256"] == features["receipt_sha256"]
        and training["original_label_sha256"] == loss.LABEL_PINS[vintage["id"]]
        and set(training["labels"]) == set(plan["train"]),
        "paired training identity differs",
        "identity_mismatch",
    )
    rows = []
    fit_days = set(plan["fit"])
    for day in plan["train"]:
        require(set(training["labels"][day]) == set(features["catalog"]), "paired target denominator differs")
        for code in features["catalog"]:
            target = training["labels"][day][code]
            if day not in fit_days:
                require(
                    target["status"] == "OUTCOME_NOT_MATURE" and target["B"] is None and target["C"] is None,
                    "unmature target populated",
                )
                continue
            require(target["status"] in {"AVAILABLE", "UNAVAILABLE"}, "unknown paired target status")
            vector = features["rows"][day][code]["features"]
            if (
                target["status"] == "AVAILABLE"
                and vector is not None
                and training["original_status"][day][code] == "AVAILABLE"
            ):
                rows.append((day, code, vector, target["B"], target["C"]))
    parameters, stats = fit_pair(rows, on_fit_start)
    output = {}
    for arm in ARMS:
        model = receipt(
            dict(
                schema_version=ARMS[arm][0] + "_model",
                contract=CONTRACT,
                arm=arm,
                vintage=vintage,
                parameters=parameters[arm],
                model_sha256=canonical_sha256(parameters[arm]),
                feature_sha256=features["receipt_sha256"],
                feature_definition_sha256=features["model_identity"]["feature_definition_sha256"],
                training_label_sha256=training["receipt_sha256"],
                training_mask_sha256=canonical_sha256([[r[0], r[1]] for r in rows]),
                member_source_identity=member_source_identity,
                release_binding=features["source"]["release_binding"],
                catalog=features["catalog"],
                source_commit=source_commit,
                **stats[arm],
            )
        )
        predictions, counts = predict(
            model,
            {d: features["rows"][d] for d in plan["predict"]},
            plan["predict"],
            features["as_of"],
            features["calendar"],
        )
        output[arm] = receipt(dict(model=model, predictions=predictions, fits=1, **counts))
    return output


def cohort_metrics(rows):
    unique = {}
    for r in rows:
        key = (r["date"], r["stock"])
        if key in unique:
            require(
                all(unique[key][k] == r[k] for k in ("code", "loss", "stock_return", "A", "B", "C")),
                "duplicate cohort identities/outcomes differ",
                "identity_mismatch",
            )
        else:
            unique[key] = r
    grouped = defaultdict(list)
    for row in unique.values():
        if row["loss"] is not None and row["B"] is not None and row["C"] is not None:
            grouped[row["date"]].append(row)
    daily = []
    for day, group in sorted(grouped.items()):
        y = [r["loss"] for r in group]
        ranks = {a: loss.rank_correlation([r[a] for r in group], y) for a in ARMS}
        with_a = [r for r in group if r["A"] is not None]
        a = loss.rank_correlation([r["A"] for r in with_a], [r["loss"] for r in with_a])
        delta = ranks["C"] - ranks["B"] if all(v is not None for v in ranks.values()) else None
        undefined = {}
        for arm in ARMS:
            if ranks[arm] is None:
                undefined[arm] = (
                    "INSUFFICIENT_STOCKS"
                    if len(group) < 2
                    else "CONSTANT_OUTCOME"
                    if len(set(y)) < 2
                    else "CONSTANT_PREDICTION"
                )
        daily.append(
            dict(
                date=day,
                rows=len(group),
                B=ranks["B"],
                C=ranks["C"],
                A=a,
                delta=delta,
                A_rows=len(with_a),
                undefined_reasons=undefined,
            )
        )
    values = [r["delta"] for r in daily if r["delta"] is not None]
    delta = float(np.mean(values)) if values else None
    paired = [r for r in daily if r["delta"] is not None]
    triple = [r for r in paired if r["A"] is not None and r["A_rows"] == r["rows"]]
    opportunity = {}
    for arm in ARMS:
        bins = []
        for name, lo, hi in (("lowest_20pct", 0.0, 0.2), ("highest_20pct", 0.8, 1.0000001)):
            part = [
                r
                for r in unique.values()
                if r["loss"] is not None and r.get("rank_" + arm) is not None and lo <= r["rank_" + arm] < hi
            ]
            bins.append(
                dict(
                    name=name,
                    rows=len(part),
                    mean_loss=float(np.mean([r["loss"] for r in part])) if part else None,
                    mean_return=float(np.mean([r["stock_return"] for r in part])) if part else None,
                    positive_return_fraction=float(np.mean([r["stock_return"] > 0 for r in part])) if part else None,
                )
            )
        opportunity[arm] = bins
    episode = None
    if len({r["seed"] for r in rows}) == 1:
        episode = {}
        completed = defaultdict(list)
        for row in unique.values():
            if row.get("episode_loss") is not None:
                completed[row["date"]].append(row)
        for arm in ARMS:
            episode_values = []
            for group in completed.values():
                part = [r for r in group if r[arm] is not None]
                r = loss.rank_correlation([r[arm] for r in part], [r["episode_loss"] for r in part])
                if r is not None:
                    episode_values.append(r)
            episode[arm] = dict(
                defined_dates=len(episode_values),
                mean_daily_rank_ic=float(np.mean(episode_values)) if episode_values else None,
                closed_rows=sum(len(g) for g in completed.values()),
            )
    return dict(
        unique_rows=len(unique),
        mature_paired_rows=sum(r["rows"] for r in daily),
        daily=daily,
        common_defined_days=len(values),
        undefined_dates=[r["date"] for r in daily if r["delta"] is None],
        paired_mean_daily_rank_ic={a: float(np.mean([r[a] for r in paired])) if paired else None for a in ARMS},
        old_A_same_sample=dict(
            defined_dates=len(triple),
            mean_daily_rank_ic={a: float(np.mean([r[a] for r in triple])) if triple else None for a in ("A", "B", "C")},
            delta_C_minus_A=float(np.mean([r["C"] - r["A"] for r in triple])) if triple else None,
        ),
        delta_ic=delta,
        opportunity_reference=opportunity,
        completed_episode_reference=episode,
        episode_reference_scope="SEED_SPECIFIC_ONLY",
        excluded_reasons=dict(
            Counter(
                r.get("reason") or "PREDICTION_UNAVAILABLE"
                for r in unique.values()
                if r["loss"] is None or r["B"] is None or r["C"] is None
            )
        ),
        disposition="ELIGIBLE_FOR_QE_CONSUMER_DISCUSSION"
        if delta is not None and delta > 0
        else "STOP_NO_MEMBER_LOSS_INCREMENT"
        if delta is not None
        else "INSUFFICIENT_DEFINED_COHORT_DATES",
    )


def evaluate(features, sealed, facts, old_a):
    core.verify_receipt(facts)
    require(
        facts["feature_sha256"] == features["receipt_sha256"] and facts["old_a_sha256"] == OLD_A_PIN,
        "outcome identity differs",
    )
    mappings = {a: {(r["trade_date"], r["sector_code"]): r for v in sealed for r in v[a]["predictions"]} for a in ARMS}
    old = {(r["trade_date"], r["sector_code"]): r for r in old_a["predictions"]}
    require(set(mappings["B"]) == set(mappings["C"]) == set(old), "paired/old prediction grid differs")
    full_ranks = {}
    for arm in ARMS:
        groups = defaultdict(list)
        for key, r in mappings[arm].items():
            if r["availability"] == "available":
                groups[key[0]].append((key, r[ARMS[arm][1]]))
        full_ranks[arm] = {}
        for pairs in groups.values():
            ranks = loss.rankdata([v for _, v in pairs])
            for (key, _), rank in zip(pairs, ranks, strict=True):
                full_ranks[arm][key] = float((rank - 1) / (len(pairs) - 1)) if len(pairs) > 1 else None
    cohort = []
    for item in facts["cohort"]:
        key = (item["date"], item["code"])
        require(key in old, "cohort escapes prediction grid")
        row = {**item, "A": old[key]["expected_loss"]}
        for arm in ARMS:
            row[arm] = mappings[arm][key][ARMS[arm][1]]
            row["rank_" + arm] = full_ranks[arm].get(key)
        cohort.append(row)
    segments = {}
    bounds = [("full", "2026-04-01", "2026-09-30")] + [
        (v["id"], v["effective_start"], v["effective_end"]) for v in rolling.VINTAGES
    ]
    for name, start, end in bounds:
        segment = cohort_metrics([r for r in cohort if start <= r["date"] <= end])
        segment["by_seed"] = {
            str(s): cohort_metrics([r for r in cohort if r["seed"] == s and start <= r["date"] <= end])
            for s in sorted({r["seed"] for r in cohort})
        }
        mse = {}
        for arm in ARMS:
            daily = []
            means = {v[arm]["model"]["vintage"]["id"]: v[arm]["model"]["training_loss_mean"] for v in sealed}
            for day in sorted(d for d in facts["targets"] if start <= d <= end):
                pairs = [
                    (r, facts["targets"][day][c][arm])
                    for (d, c), r in mappings[arm].items()
                    if d == day
                    and r["availability"] == "available"
                    and facts["targets"][day][c]["status"] == "AVAILABLE"
                ]
                if pairs:
                    daily.append(
                        dict(
                            date=day,
                            rows=len(pairs),
                            mse=float(np.mean([(r[ARMS[arm][1]] - y) ** 2 for r, y in pairs])),
                            mae=float(np.mean([abs(r[ARMS[arm][1]] - y) for r, y in pairs])),
                            constant=float(np.mean([(means[r["vintage_id"]] - y) ** 2 for r, y in pairs])),
                        )
                    )
            value = float(np.mean([r["mse"] for r in daily])) if daily else None
            constant = float(np.mean([r["constant"] for r in daily])) if daily else None
            mse[arm] = dict(
                days=len(daily),
                rows=sum(r["rows"] for r in daily),
                mse=value,
                mae=float(np.mean([r["mae"] for r in daily])) if daily else None,
                mse_skill=1 - value / constant if constant is not None and constant > 0 else None,
            )
        segment["target_error"] = mse
        segments[name] = segment
    return receipt(
        dict(
            schema_version=VERSION + "_evaluation",
            segments=segments,
            research_disposition=segments["full"]["disposition"],
            target_na_reasons=dict(
                Counter(
                    v["reason"] for ds in facts["targets"].values() for v in ds.values() if v["status"] != "AVAILABLE"
                )
            ),
            economic_value_confirmed=False,
            automatic_promotion=False,
            historical_as_known=False,
            untouched_holdout=False,
        )
    )


def execute_child(prepared_path, prepared_sha256, output, source_commit):
    prepared, features = load_prepared(prepared_path, prepared_sha256)
    sealed = []
    for vintage in rolling.VINTAGES:
        loc = prepared["training_labels"][vintage["id"]]
        training = core.inputs._asset(loc["path"], loc["sha256"])
        pair = fit_vintage(
            features,
            training,
            vintage,
            source_commit,
            lambda arm: write_once(
                output / f"{vintage['id']}_{arm}.attempt.json",
                receipt(dict(vintage=vintage, arm=arm, fit_kernel_attempts=1)),
            ),
            member_source_identity=prepared["member_source_identity"],
        )
        for arm in ARMS:
            write_once(output / f"{vintage['id']}_{arm}.sealed.json", pair[arm])
        sealed.append(pair)
    # Evaluation assets are intentionally opened only after all four fits seal.
    facts = core.inputs._asset(prepared["evaluation_path"], prepared["evaluation_sha256"])
    old_a = pinned_json(prepared["old_a_path"], OLD_A_PIN)
    return receipt(
        dict(
            schema_version=VERSION + "_repeat",
            contract=CONTRACT,
            source_commit=source_commit,
            prepared_sha256=prepared_sha256,
            fits=4,
            sealed=sealed,
            evaluation=evaluate(features, sealed, facts, old_a),
            database_access=False,
            runtime_action=False,
        )
    )


def close_processes(first, second, head, prepared_sha256):
    for result in (first, second):
        core.verify_receipt(result)
        require(
            result["contract"] == CONTRACT
            and result["fits"] == 4
            and result["source_commit"] == head
            and result["prepared_sha256"] == prepared_sha256,
            "paired child identity/budget differs",
        )
        require(
            isinstance(result["sealed"], list) and len(result["sealed"]) == len(rolling.VINTAGES),
            "paired child missing a complete vintage",
        )
        grids = {a: [] for a in ARMS}
        for pair, vintage in zip(result["sealed"], rolling.VINTAGES, strict=True):
            require(set(pair) == set(ARMS), "paired child missing an arm")
            for arm in ARMS:
                core.verify_receipt(pair[arm])
                model = pair[arm]["model"]
                validate_model(model)
                require(
                    model["arm"] == arm
                    and model["vintage"] == vintage
                    and pair[arm]["fits"] == 1
                    and model["source_commit"] == head,
                    "sealed model identity differs",
                )
                grids[arm].extend((r["trade_date"], r["sector_code"]) for r in pair[arm]["predictions"])
            require(
                pair["B"]["model"]["training_mask_sha256"] == pair["C"]["model"]["training_mask_sha256"]
                and all(
                    pair["B"]["model"]["parameters"][k] == pair["C"]["model"]["parameters"][k]
                    for k in ("mean", "scale", "active")
                ),
                "paired fitted sample/scaler differs",
            )
        days = sorted({d for d, _ in grids["B"]})
        expected = [(d, c) for d in days for c in result["sealed"][0]["B"]["model"]["catalog"]]
        require(
            len(days) == EXPECTED_PREDICTION_DAYS and grids["B"] == grids["C"] == expected,
            "sealed prediction grid incomplete",
        )
    require(canonical_json_bytes(first) == canonical_json_bytes(second), "fresh processes differ")
    return receipt(
        dict(
            schema_version=VERSION + "_acceptance",
            contract=CONTRACT,
            source_commit=head,
            prepared_sha256=prepared_sha256,
            completed_fits=8,
            status="COMPLETED",
            process_sha256=first["receipt_sha256"],
            evaluation=first["evaluation"],
            numeric_environment=first["numeric_environment"],
            database_write=False,
            runtime_action=False,
            automatic_promotion=False,
        )
    )


def infer_prepared(model, request, bundle, *, source_mode, expected_model_sha256, expected_bundle_sha256):
    if source_mode == "frozen_file":
        consumer = core
    elif source_mode == "database_daily":
        from backend.services.hmm_risk import risk_l2_database_inference as consumer
    else:
        raise rolling.risk.fail("unknown member-loss source mode", "source_invalid")
    consumer.validate_bundle(request, bundle, expected_bundle_sha256)
    validate_model(model, expected_model_sha256)
    require(
        bundle["catalog"] == model["catalog"]
        and bundle["model_identity"]["feature_definition_sha256"] == model["feature_definition_sha256"],
        "member-loss model/feature identity differs",
        "identity_mismatch",
    )
    if source_mode == "frozen_file":
        core.date_plan(request)
        path = core.inputs.reader._require_file(
            Path(request["source"]["candidate_root"]),
            "components/daily_bin_candidate/calendars/day.txt",
            request["source"]["calendar_sha256"],
        )
        stamp = core.inputs._stamp(path)
        calendar = path.read_text(encoding="utf-8").splitlines()
        require(core.inputs._stamp(path) == stamp, "inference calendar changed during read", "identity_mismatch")
    else:
        calendar = bundle["inference_identity"]["calendar"]
    predictions, counts = predict(model, bundle["rows"], request["decision_dates"], bundle["as_of"], calendar)
    return receipt(
        dict(
            schema_version=ARMS[model["arm"]][0] + "_prediction",
            contract=CONTRACT,
            model_bundle_sha256=model["receipt_sha256"],
            source_mode=source_mode,
            source_features_sha256=bundle["receipt_sha256"],
            inference_identity=bundle["inference_identity"],
            predictions=predictions,
            fits=0,
            labels_read=False,
            outcomes_read=False,
            policy_applied=False,
            historical_as_known=False,
            forward_confirmed=False,
            **counts,
        )
    )
