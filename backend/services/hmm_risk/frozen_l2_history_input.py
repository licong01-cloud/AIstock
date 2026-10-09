"""File-only source binding for the approved P1/P2 history, never active/latest."""

from __future__ import annotations

from datetime import date
import hashlib
from pathlib import Path

import numpy as np
import pandas as pd

from backend.services.dataset_release.shared_sector_context import (
    load_release_sw_l2_code_map,
    load_sector_quote_availability,
    validate_membership_frame,
)
from backend.services.hmm_risk import frozen_l2_history as history
from backend.services.hmm_risk import rotation_l2_input as reader
from backend.services.hmm_risk.contracts import ALL_CORE_FEATURES, canonical_sha256
from backend.services.hmm_risk.formal_state_effect import verify_receipt
from backend.services.hmm_risk.formal_state_executor import load_effect_request
from backend.services.hmm_risk.formal_state_input import _bounded_l2_stock_facts
from backend.services.hmm_risk.formal_state_model import receipt
from backend.services.hmm_risk.provider_absence import load_provider_absence_manifest
from backend.services.hmm_risk.security_identity import load_security_source_identity_manifest
from backend.services.hmm_risk.stock_fact_observation import build_c010_feature_domain_panel

require = history.require
P1_MANIFEST_BYTE = "940e56941ee84d9caf05622ce94d0ad18ef5d2ae9c2dcbe30f7f1eb35d1cba92"
P1_INPUT_HASH = "4ac21b8c54efce631a81718cd4541e9c986ae1a1e2e77cfea16923f1551ceb06"


def _asset(path, expected, field="receipt_sha256"):
    path = Path(path)
    require(path.is_absolute() and path.is_file(), "asset must be an explicit absolute regular file")
    require(not any(reader._is_link(p) for p in (path, *path.parents)), "linked asset forbidden")
    before = path.stat()
    value = reader._json(path)
    require(
        (before.st_size, before.st_mtime_ns) == (path.stat().st_size, path.stat().st_mtime_ns),
        "asset changed while read",
    )
    require(
        value.get(field) == expected and canonical_sha256({k: v for k, v in value.items() if k != field}) == expected,
        "asset canonical identity differs",
    )
    return value


def _stamp(path):
    path = Path(path)
    require(
        path.is_file() and not any(reader._is_link(p) for p in (path, *path.parents)),
        "source asset is absent or indirect",
    )
    stat = path.stat()
    return stat.st_size, stat.st_mtime_ns


def _stable_files(files, stamps):
    require(all(_stamp(path) == stamps[key] for key, path in files.items()), "frozen source changed while read")


def _rotation_source(request):
    original = _asset(request["original_input_path"], P1_INPUT_HASH, "input_hash")
    history.price.validate_input(original)
    references = {}
    for a, api, pins in (
        ("rank", history.price, history.value.RANK_PINS),
        ("return", history.value.return_model, history.value.RETURN_PINS),
    ):
        references[a], _ = history.price._reference(Path(request[a + "_acceptance_path"]), variant=api, pins=pins)
    identity, binding = original["source"]["identity"], original["source"]["evaluation_source_binding"]
    require(references["rank"]["input_identity"] == identity, "original source linkage differs")
    root = Path(binding["root"])
    require(
        root.is_absolute() and root.is_dir() and not any(reader._is_link(p) for p in (root, *root.parents)),
        "frozen root invalid",
    )
    manifest_stamp = _stamp(root / "qe_dataset_manifest.json")
    manifest_path = reader._require_file(root, "qe_dataset_manifest.json", P1_MANIFEST_BYTE)
    manifest = reader._json(manifest_path)
    require(
        manifest["dataset_manifest_sha256"] == identity["manifest_sha256"]
        and manifest["schema_version"] == reader.MANIFEST_SCHEMA
        and manifest["availability_status"] == "CANDIDATE_READY"
        and manifest["cutoff_trade_date"] == "2026-08-31"
        and manifest["release_id"] == identity["release_id"],
        "original release differs",
    )
    components = manifest["components"]
    files, stamps = {"manifest": manifest_path}, {"manifest": manifest_stamp}
    for name in (
        "day_calendar",
        "day_meta_export",
        "day_provider_catalog",
        "sector_data",
        "index_daily",
        "suspend_data",
        "sector_code_map",
        "sector_quote_availability",
        "sector_membership_spans",
        "factor_content_manifest",
    ):
        pin = components[name]
        stamps[name] = _stamp(root / pin["path"])
        files[name] = reader._require_file(root, pin["path"], pin["sha256"])
    original_keys = {"day_calendar": "calendar_hash", "sector_code_map": "mapping_hash"}
    require(
        components["day_calendar"]["sha256"] == history.CALENDAR_SHA == identity[original_keys["day_calendar"]],
        "calendar identity differs",
    )
    expected = identity["source_file_hashes"]
    for name, key in (
        ("day_meta_export", "daily_bin_meta"),
        ("sector_data", "sector_data_h5"),
        ("index_daily", "index_daily_h5"),
        ("suspend_data", "suspend"),
        ("sector_membership_spans", "membership"),
        ("factor_content_manifest", "factor_content_manifest"),
    ):
        require(components[name]["sha256"] == expected[key], "original component identity differs: " + name)
    code_map = load_release_sw_l2_code_map(files["sector_code_map"])
    quote = load_sector_quote_availability(
        files["sector_quote_availability"], code_map=code_map, required_end=history.END
    )
    require(
        code_map.member_backed_digest == identity["mapping_hash"]
        and quote.quote_availability_digest == identity["quote_authority_hash"],
        "mapping/quote authority differs",
    )
    calendar = [d.isoformat() for d in reader._parse_calendar(files["day_calendar"])]
    calendar = [d for d in calendar if d <= history.END.isoformat()]
    history.schedule(calendar)
    inventory = reader._json(files["factor_content_manifest"])
    factor_pins = {r["path"]: r["sha256"] for r in inventory["files"]}
    moneyflow = "components/factor_h5_static_candidate_v2/moneyflow.h5"
    require(factor_pins[moneyflow] == expected["moneyflow_h5"], "moneyflow source identity differs")
    stamps["moneyflow"] = _stamp(root / moneyflow)
    files["moneyflow"] = reader._require_file(root, moneyflow, expected["moneyflow_h5"])
    for key in ("security_identity", "provider_absence"):
        path = Path(request[key + "_path"])
        require(
            path.is_absolute() and not any(reader._is_link(p) for p in (path, *path.parents)),
            "source authority path invalid",
        )
        loader = (
            load_security_source_identity_manifest if key == "security_identity" else load_provider_absence_manifest
        )
        stamps[key] = _stamp(path)
        loader(path, expected_sha256=expected[key])
        files[key] = path
    _stable_files(files, stamps)
    return original, references, root, manifest, files, code_map, quote, calendar, stamps


def _risk_source(request):
    risk = history.risk
    source_path = Path(request["source_request_path"])
    require(
        source_path.is_absolute() and not any(reader._is_link(p) for p in (source_path, *source_path.parents)),
        "risk request path invalid",
    )
    source_stamp = _stamp(source_path)
    require(
        hashlib.sha256(source_path.read_bytes()).hexdigest() == risk.SOURCE_REQUEST_FILE_SHA,
        "risk original request byte pin differs",
    )
    source = load_effect_request(source_path)
    source["source"] = {**source["source"], "work_parent": request["work_parent"]}
    pins = history.risk_value.APPROVED_PINS
    sealed = _asset(request["risk_sealed_path"], pins["sealed_hash"])
    features = _asset(request["risk_features_path"], pins["features_hash"])
    acceptance = _asset(request["risk_acceptance_path"], pins["acceptance_hash"])
    # risk_l2.finalize stores the original sealed metadata under `model`,
    # not a top-level model_sha256 alias. Bind that complete carrier to the
    # pinned sealed payload; never accept a replacement digest alone.
    accepted_model = acceptance.get("model")
    expected_model = {k: v for k, v in sealed.items() if k not in {"predictions", "receipt_sha256"}}
    require(
        isinstance(accepted_model, dict)
        and accepted_model == expected_model
        and acceptance.get("sealed_prediction_sha256") == pins["sealed_hash"]
        and sealed.get("feature_sha256") == features["receipt_sha256"]
        and sealed.get("input_identity") == features.get("input_identity")
        and sealed.get("model_sha256") == pins["model_hash"]
        and canonical_sha256(sealed["parameters"]) == pins["model_hash"]
        and accepted_model.get("model_sha256") == pins["model_hash"],
        "risk frozen model linkage differs",
    )
    require(
        sealed["contract"] == risk.CONTRACT and features["contract"] == risk.CONTRACT, "risk original contract differs"
    )
    require(
        features["input_identity"]["pit_bundle_sha256"]
        == risk.PIT_BUNDLE_SHA
        == source["frozen"]["industry_authority"]["identity"]["bundle_hash"],
        "risk PIT identity differs",
    )
    root = Path(source["source"]["candidate_root"])
    calendar_path = reader._require_file(root, "components/daily_bin_candidate/calendars/day.txt", history.CALENDAR_SHA)
    calendar = [d.isoformat() for d in reader._parse_calendar(calendar_path) if d <= history.END]
    history.schedule(calendar)
    require(_stamp(source_path) == source_stamp, "risk source request changed while read")
    return source, features, sealed["parameters"], calendar


def prepare(request, source_commit):
    verify_receipt(request)
    require(
        request["schema_version"] == history.VERSION + "_request"
        and request["contract"] == history.CONTRACT
        and request["kind"] in ("P1", "P2"),
        "explicit history request contract differs",
    )
    if request["kind"] == "P1":
        original, references, root, manifest, files, mapping, quote, calendar, stamps = _rotation_source(request)
        plan = history.schedule(calendar)
        first = plan["positions"][plan["days"][0]]
        source_days = [date.fromisoformat(d) for d in calendar[first - 25 : -1]]
        codes = list(mapping.member_backed_codes)
        membership = pd.read_parquet(files["sector_membership_spans"])
        validate_membership_frame(
            membership, id_to_code=mapping.id_to_code, required_start=date(2024, 7, 1), required_end=history.END
        )
        membership["instrument"] = membership["instrument"].astype(str).str.strip().str.upper()
        for k in ("start_date", "end_date"):
            membership[k] = pd.to_datetime(membership[k]).dt.date
        expected = original["source"]["identity"]["source_file_hashes"]
        daily, amount_hash = reader._daily_aggregates(
            root=root,
            manifest=manifest,
            membership=membership,
            id_to_code=mapping.id_to_code,
            quote_entries=quote.entries,
            catalog=codes,
            source_days=source_days,
            provider_path=files["day_provider_catalog"],
            provider_catalog_path=files["day_provider_catalog"],
            suspend_path=files["suspend_data"],
            moneyflow_path=files["moneyflow"],
            qlib_root=root / "components/daily_bin_candidate",
            calendar=[date.fromisoformat(d) for d in calendar],
            security_identity=load_security_source_identity_manifest(
                files["security_identity"], expected_sha256=expected["security_identity"]
            ),
            provider_absence=load_provider_absence_manifest(
                files["provider_absence"], expected_sha256=expected["provider_absence"]
            ),
            bounded_suspend=True,
        )
        price_start = date.fromisoformat(calendar[first - 26])
        view = {
            "sector_returns": reader._sector_returns(
                files["sector_data"],
                id_to_code=mapping.id_to_code,
                quote_entries=quote.entries,
                catalog=codes,
                calendar=[date.fromisoformat(d) for d in calendar],
                start=source_days[0],
                end=source_days[-1],
            ),
            "benchmark_close": reader.bounded_benchmark_close(
                files["index_daily"], start=price_start, end=source_days[-1]
            ),
        }
        _stable_files(files, stamps)
        bundle = {
            "schema_version": history.VERSION + "_rotation_features",
            "contract": history.CONTRACT,
            "calendar": calendar,
            "catalog": codes,
            "daily_aggregates": daily,
            "price_features": view,
            "parameters": {a: r["parameters"] for a, r in references.items()},
            "source_identity": original["source"]["identity"],
            "new_window_amount_set_sha256": amount_hash,
        }
    else:
        source, old, parameters, calendar = _risk_source(request)
        plan = history.schedule(calendar)
        start = date.fromisoformat(calendar[plan["positions"][plan["days"][0]] - 260])
        end = date.fromisoformat(plan["as_of"][plan["days"][-1]])
        assets, window, aggregates, identity = _bounded_l2_stock_facts(
            source["frozen"], source["source"], start=start, end=end
        )
        panel, definition, cross = build_c010_feature_domain_panel(
            aggregates,
            trading_dates=window,
            csi300_returns={d: assets["benchmark"][d] for d in window},
            expected_sector_count=131,
            direct_sector_level="L2",
            canonical_sector_codes=source["frozen"]["catalog"],
        )
        require(definition == source["frozen"]["feature_definition"], "risk C-010/A5 feature definition differs")
        require(
            identity["release_identity"] == old["input_identity"]["release_identity"],
            "risk full release identity differs",
        )
        rows = {d: {} for d in plan["days"]}
        for code in source["frozen"]["catalog"]:
            frame = panel.xs(code, level="l1_code")
            for day in rows:
                as_of = plan["as_of"][day]
                values = frame.reindex(pd.to_datetime([as_of]))[list(ALL_CORE_FEATURES)].to_numpy(dtype=np.float64)[0]
                valid = bool(np.isfinite(values).all())
                rows[day][code] = {
                    "as_of_date": as_of,
                    "features": values.tolist() if valid else None,
                    "reason_code": None
                    if valid
                    else identity["domain_reasons"].get((as_of, code), "hmm_risk_c010_observation_unavailable"),
                }
        bundle = {
            "schema_version": history.VERSION + "_risk_features",
            "contract": history.CONTRACT,
            "calendar": calendar,
            "catalog": source["frozen"]["catalog"],
            "feature_names": list(ALL_CORE_FEATURES),
            "rows": rows,
            "parameters": parameters,
            "source_identity": old["input_identity"],
            "new_source_identity": {
                k: v for k, v in identity.items() if k not in ("structural_membership", "domain_reasons")
            },
            "cross_section_sha256": canonical_sha256(cross),
        }
    bundle.update(request_sha256=request["receipt_sha256"], source_commit=source_commit)
    return receipt(bundle)


def outcomes(request, bundle):
    """Called only after two sealed inference readbacks; never supplied to an inference child."""
    require(bundle["request_sha256"] == request["receipt_sha256"], "outcome request linkage differs")
    if request["kind"] == "P1":
        original, _, _, _, files, mapping, quote, calendar, stamps = _rotation_source(request)
        require(original["source"]["identity"] == bundle["source_identity"], "outcome source changed")
        result = {
            "sector_returns": reader._sector_returns(
                files["sector_data"],
                id_to_code=mapping.id_to_code,
                quote_entries=quote.entries,
                catalog=bundle["catalog"],
                calendar=[date.fromisoformat(d) for d in calendar],
                start=history.START,
                end=history.END,
            ),
            "benchmark_close": reader.bounded_benchmark_close(
                files["index_daily"], start=history.START, end=history.END
            ),
        }
        _stable_files(files, stamps)
    else:
        source, old, _, calendar = _risk_source(request)
        require(old["input_identity"] == bundle["source_identity"], "risk outcome source changed")
        require(calendar == bundle["calendar"], "risk outcome calendar changed")
        plan = history.schedule(calendar)
        # The unchanged C-010 stock-fact aggregator requires prev_close_10_yuan.
        # Read real prior context; it is not an extra evaluation date or a fill.
        context_start = date.fromisoformat(calendar[plan["positions"][plan["days"][0]] - 10])
        assets, window, aggregates, identity = _bounded_l2_stock_facts(
            source["frozen"], source["source"], start=context_start, end=history.END
        )
        require(
            identity["release_identity"] == old["input_identity"]["release_identity"], "risk outcome release changed"
        )
        returns = {d: {c: None for c in bundle["catalog"]} for d in plan["days"]}
        for a in aggregates:
            day = a.trade_date.isoformat()
            if day in returns:
                returns[day][a.l1_code] = float(a.l1_return)
        result = {
            "returns": {d: v for d, v in returns.items() if d > history.START.isoformat()},
            "event_returns": returns,
        }
    return receipt(
        {
            "schema_version": history.VERSION + "_outcomes",
            "contract": history.CONTRACT,
            "feature_sha256": bundle["receipt_sha256"],
            **result,
        }
    )
