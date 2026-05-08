"""Step 3 (Stage 2) — vn.py + vnpy_xt install / import smoke.

Run from inside .venv-vnpy-poc (NOT the conda main env).

Validates:
  S1. import vnpy + import vnpy_xt + report versions
  S2. vnpy_xt's xtquant resolution: where does it pick up xtquant?
      - Does it find a pip-installed xtquant in venv site-packages?
      - Does it cooperate with the vendored repo xtquant via PYTHONPATH?
      - What path does vnpy_xt's gateway/datafeed module resolve xtquant to?
  S3. vnpy_xt Gateway can be instantiated with vn.py MainEngine WITHOUT
      connecting to a real account (dry-run, no real network/account auth).
      Validates the class registration + add_gateway path only.

Fail-fast: any import error / unexpected resolution / instantiation crash
raises immediately with the full context.

Usage (run from worktree root with the venv python):
    .venv python: backend/services/paper_trading_v2/poc/.venv-vnpy-poc/Scripts/python.exe
    cmd: <venv-python> -m backend.services.paper_trading_v2.poc.step3_vnpy_smoke
"""

from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path


def _hr(msg: str) -> None:
    print(f"\n=== {msg} ===")


def s1_imports() -> dict:
    _hr("S1: import vnpy + vnpy_xt")
    info: dict = {}
    import vnpy
    info["vnpy.__version__"] = getattr(vnpy, "__version__", "unknown")
    info["vnpy.__file__"] = getattr(vnpy, "__file__", "unknown")
    print(f"[S1] vnpy version = {info['vnpy.__version__']}")
    print(f"[S1] vnpy path    = {info['vnpy.__file__']}")

    import vnpy_xt
    info["vnpy_xt.__version__"] = getattr(vnpy_xt, "__version__", "unknown")
    info["vnpy_xt.__file__"] = getattr(vnpy_xt, "__file__", "unknown")
    print(f"[S1] vnpy_xt version = {info['vnpy_xt.__version__']}")
    print(f"[S1] vnpy_xt path    = {info['vnpy_xt.__file__']}")

    print("[S1] PASS")
    return info


def s2_xtquant_resolution() -> dict:
    _hr("S2: how does vnpy_xt find xtquant?")
    info: dict = {}

    vendored = Path(r"F:\Dev\AIstock\xtquant")
    info["vendored_xtquant_exists"] = vendored.is_dir()
    print(f"[S2] vendored xtquant present: {info['vendored_xtquant_exists']} ({vendored})")

    try:
        spec = importlib.util.find_spec("xtquant")
        if spec is None:
            info["xtquant_resolved_to"] = None
            print("[S2] xtquant NOT found in venv site-packages and not on sys.path")
        else:
            info["xtquant_resolved_to"] = spec.origin
            print(f"[S2] xtquant resolves to: {spec.origin}")
    except Exception as e:
        info["xtquant_resolved_to"] = f"ERROR: {e!r}"
        print(f"[S2] xtquant find_spec error: {e!r}")

    try:
        import xtquant
        info["xtquant_imported_ok"] = True
        info["xtquant_imported_path"] = getattr(xtquant, "__file__", None)
        print(f"[S2] xtquant imported OK from: {info['xtquant_imported_path']}")
    except Exception as e:
        info["xtquant_imported_ok"] = False
        info["xtquant_import_error"] = repr(e)
        print(f"[S2] xtquant import FAILED: {e!r}")

    try:
        spec_gw = importlib.util.find_spec("vnpy_xt.gateway")
        info["vnpy_xt.gateway_path"] = spec_gw.origin if spec_gw else None
        spec_df = importlib.util.find_spec("vnpy_xt.datafeed")
        info["vnpy_xt.datafeed_path"] = spec_df.origin if spec_df else None
        print(f"[S2] vnpy_xt.gateway:  {info['vnpy_xt.gateway_path']}")
        print(f"[S2] vnpy_xt.datafeed: {info['vnpy_xt.datafeed_path']}")
    except Exception as e:
        info["vnpy_xt_module_scan_error"] = repr(e)
        print(f"[S2] vnpy_xt module scan error: {e!r}")

    if info.get("xtquant_imported_ok") and info.get("vendored_xtquant_exists"):
        resolved = info.get("xtquant_imported_path") or ""
        if str(vendored).lower() in resolved.lower():
            info["xtquant_source"] = "vendored"
            print("[S2] xtquant resolution = VENDORED (good, version matches local miniQMT)")
        elif "site-packages" in resolved.lower():
            info["xtquant_source"] = "pip"
            print("[S2] xtquant resolution = PIP (RISK: may not match local miniQMT version)")
        else:
            info["xtquant_source"] = f"other: {resolved}"
            print(f"[S2] xtquant resolution = OTHER: {resolved}")

    print("[S2] DONE (informational, not pass/fail)")
    return info


def s3_gateway_instantiate() -> dict:
    _hr("S3: vnpy_xt Gateway dry-run instantiation")
    info: dict = {}

    from vnpy.event import EventEngine
    from vnpy.trader.engine import MainEngine
    info["MainEngine_imported"] = True
    print("[S3] vnpy MainEngine + EventEngine imported")

    gateway_cls = None
    last_err = None
    for mod_name, cls_name in [
        ("vnpy_xt", "XtGateway"),
        ("vnpy_xt.gateway", "XtGateway"),
        ("vnpy_xt.gateway.xt_gateway", "XtGateway"),
        ("vnpy_xt", "XtquantGateway"),
    ]:
        try:
            mod = importlib.import_module(mod_name)
            if hasattr(mod, cls_name):
                gateway_cls = getattr(mod, cls_name)
                info["gateway_module"] = mod_name
                info["gateway_class"] = cls_name
                info["gateway_module_file"] = getattr(mod, "__file__", None)
                print(f"[S3] gateway found: {mod_name}.{cls_name}")
                break
        except Exception as e:
            last_err = (mod_name, cls_name, repr(e))
            continue

    if gateway_cls is None:
        try:
            import vnpy_xt
            attrs = [a for a in dir(vnpy_xt) if not a.startswith("_")]
            info["vnpy_xt_public_attrs"] = attrs
            print(f"[S3] vnpy_xt public attrs: {attrs}")
        except Exception:
            pass
        raise RuntimeError(
            f"S3 FAIL: could not locate XtGateway class in vnpy_xt. "
            f"last attempt: {last_err}"
        )

    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    print("[S3] MainEngine instantiated (no event loop started)")

    try:
        main_engine.add_gateway(gateway_cls)
        info["add_gateway_ok"] = True
        print(f"[S3] add_gateway({gateway_cls.__name__}) succeeded")
    except Exception as e:
        info["add_gateway_ok"] = False
        info["add_gateway_error"] = repr(e)
        print(f"[S3] add_gateway FAILED: {e!r}")

    try:
        gateways = main_engine.get_all_gateway_names()
        info["registered_gateways"] = list(gateways)
        print(f"[S3] registered gateways: {info['registered_gateways']}")
    except Exception as e:
        info["registered_gateways_error"] = repr(e)
        print(f"[S3] gateway listing error: {e!r}")

    try:
        main_engine.close()
        print("[S3] MainEngine closed cleanly")
    except Exception as e:
        print(f"[S3] close() error (non-fatal): {e!r}")

    if not info.get("add_gateway_ok"):
        raise RuntimeError(
            f"S3 FAIL: add_gateway failed: {info.get('add_gateway_error')}")
    print("[S3] PASS")
    return info


def main() -> int:
    print(f"[step3-vnpy] Python = {sys.version}")
    print(f"[step3-vnpy] sys.executable = {sys.executable}")
    if "venv-vnpy-poc" not in sys.executable:
        print(f"[step3-vnpy] WARN: not running from .venv-vnpy-poc — sys.executable={sys.executable}")

    results: dict = {}
    results["S1"] = s1_imports()
    results["S2"] = s2_xtquant_resolution()
    results["S3"] = s3_gateway_instantiate()

    _hr("SUMMARY")
    for k, v in results.items():
        print(f"[{k}] {v}")
    print("\n[step3-vnpy] PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
