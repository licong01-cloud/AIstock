"""Step 3b — Increment after step3_vnpy_smoke: probe whether PYTHONPATH-injected
vendored xtquant can substitute for the pip-installed xtquant when used by vnpy_xt.

Pre-conditions (must be true before running):
  - venv .venv-vnpy-poc has vnpy + vnpy_xt installed
  - venv has NO xtquant installed (pip uninstall xtquant done before this script)
  - sys.path will be prepended with F:\\Dev\\AIstock so vendored xtquant resolves

Two probes:
  Probe 1 (PYTHONPATH resolution):
    - find_spec('xtquant') => expect: vendored path
    - import xtquant => expect: vendored module
    - import vnpy_xt => expect: succeeds, sees vendored xtquant
    - check XtGateway class still loadable

  Probe 2 (light API touch via vendored, NO miniQMT connect):
    - import xtquant.xtdata, xtquant.xtconstant, xtquant.xttype
    - access constants (no DLL load needed)
    - try one truly-light API: xtdata.get_holidays() / get_trading_dates() if exists
      (purely metadata, no client connection)
    - DO NOT construct XtQuantTrader (would touch SIM session)
    - DO NOT call get_full_tick (would connect xtdc)

Fail-fast: if injection works but vnpy_xt does something incompatible at module
import, report the exact traceback. Do not catch and silently continue.

Run:
    cd .../poc
    PYTHONPATH=F:/Dev/AIstock ./.venv-vnpy-poc/Scripts/python.exe step3b_vendored_pythonpath_probe.py
"""

from __future__ import annotations

import importlib
import importlib.util
import os
import sys
import traceback
from pathlib import Path


def _hr(msg: str) -> None:
    print(f"\n=== {msg} ===")


def assert_clean_state() -> None:
    _hr("PRE: assert no xtquant in venv site-packages")
    venv_sp = Path(sys.executable).parent.parent / "Lib" / "site-packages"
    pip_xt = venv_sp / "xtquant"
    if pip_xt.is_dir():
        raise RuntimeError(
            f"PRE FAIL: xtquant still in venv site-packages: {pip_xt}. "
            f"Run: pip uninstall -y xtquant before this probe.")
    print(f"[PRE] no xtquant in {venv_sp} -- good")


def ensure_pythonpath() -> str:
    _hr("PRE: ensure F:\\Dev\\AIstock on sys.path")
    target = r"F:\Dev\AIstock"
    if target not in sys.path:
        sys.path.insert(0, target)
        print(f"[PRE] prepended {target} to sys.path")
    else:
        print(f"[PRE] {target} already on sys.path")
    print(f"[PRE] sys.path[:3] = {sys.path[:3]}")
    return target


def probe1_pythonpath() -> dict:
    _hr("Probe 1: PYTHONPATH-injected vendored xtquant resolution")
    info: dict = {}

    spec = importlib.util.find_spec("xtquant")
    info["xtquant_spec_origin"] = spec.origin if spec else None
    print(f"[P1] find_spec('xtquant').origin = {info['xtquant_spec_origin']}")
    if spec is None:
        raise RuntimeError(
            "P1 FAIL: xtquant not findable even with sys.path injection. "
            "Either F:\\Dev\\AIstock missing or vendored xtquant moved.")
    if "site-packages" in (spec.origin or "").lower():
        raise RuntimeError(
            f"P1 FAIL: xtquant still resolves to site-packages: {spec.origin}. "
            "uninstall step incomplete?")

    import xtquant
    info["xtquant_imported_path"] = xtquant.__file__
    info["xtquant_version"] = getattr(xtquant, "__version__", "unknown")
    print(f"[P1] xtquant.__file__ = {info['xtquant_imported_path']}")
    print(f"[P1] xtquant.__version__ = {info['xtquant_version']}")
    if r"F:\Dev\AIstock\xtquant" not in info["xtquant_imported_path"]:
        raise RuntimeError(
            f"P1 FAIL: imported xtquant is NOT vendored: {info['xtquant_imported_path']}")
    print("[P1] vendored xtquant resolution = OK")

    print("[P1] importing vnpy_xt with vendored xtquant active ...")
    try:
        import vnpy_xt
        info["vnpy_xt_import_ok"] = True
        info["vnpy_xt_version"] = getattr(vnpy_xt, "__version__", "unknown")
        info["vnpy_xt_path"] = vnpy_xt.__file__
        print(f"[P1] vnpy_xt imported OK: version={info['vnpy_xt_version']}")
    except Exception as e:
        info["vnpy_xt_import_ok"] = False
        info["vnpy_xt_import_error"] = repr(e)
        info["vnpy_xt_traceback"] = traceback.format_exc()
        print(f"[P1] vnpy_xt import FAILED with vendored xtquant:\n{info['vnpy_xt_traceback']}")
        raise RuntimeError(
            "P1 FAIL: vnpy_xt cannot import on top of vendored xtquant. "
            "Mitigation A is not viable.")

    print("[P1] looking for XtGateway class ...")
    if not hasattr(vnpy_xt, "XtGateway"):
        attrs = [a for a in dir(vnpy_xt) if not a.startswith("_")]
        info["XtGateway_present"] = False
        info["vnpy_xt_attrs"] = attrs
        raise RuntimeError(
            f"P1 FAIL: XtGateway not on vnpy_xt module. attrs={attrs}")
    info["XtGateway_present"] = True
    info["XtGateway_repr"] = repr(vnpy_xt.XtGateway)
    print(f"[P1] XtGateway = {vnpy_xt.XtGateway}")

    print("[P1] PASS")
    return info


def probe2_light_api() -> dict:
    _hr("Probe 2: light vendored API touch (NO miniQMT connect)")
    info: dict = {}

    try:
        from xtquant import xtconstant
        info["xtconstant_loaded"] = True
        info["constants"] = {
            "STOCK_BUY": xtconstant.STOCK_BUY,
            "STOCK_SELL": xtconstant.STOCK_SELL,
            "FIX_PRICE": xtconstant.FIX_PRICE,
            "ORDER_REPORTED": xtconstant.ORDER_REPORTED,
            "ORDER_CANCELED": xtconstant.ORDER_CANCELED,
        }
        print(f"[P2] xtconstant OK: {info['constants']}")
    except Exception as e:
        info["xtconstant_error"] = repr(e)
        print(f"[P2] xtconstant FAILED: {e!r}")
        raise

    try:
        from xtquant import xttype
        info["xttype_loaded"] = True
        sa = xttype.StockAccount("0000000000", "STOCK")
        info["stock_account_repr"] = f"account_id={sa.account_id} type={sa.account_type}"
        print(f"[P2] xttype.StockAccount construct OK: {info['stock_account_repr']}")
    except Exception as e:
        info["xttype_error"] = repr(e)
        print(f"[P2] xttype FAILED: {e!r}")
        raise

    try:
        from xtquant import xtdata
        info["xtdata_loaded"] = True
        print(f"[P2] xtdata module imported (no DLL touched yet)")
        info["xtdata_attrs_sample"] = sorted(
            a for a in dir(xtdata) if not a.startswith("_") and "callback" not in a
        )[:15]
        print(f"[P2] xtdata attrs sample: {info['xtdata_attrs_sample']}")
    except Exception as e:
        info["xtdata_import_error"] = repr(e)
        info["xtdata_traceback"] = traceback.format_exc()
        print(f"[P2] xtdata import FAILED:\n{info['xtdata_traceback']}")
        raise

    try:
        from xtquant import xttrader
        info["xttrader_module_loaded"] = True
        info["xttrader_has_class"] = hasattr(xttrader, "XtQuantTrader")
        print(f"[P2] xttrader module imported, XtQuantTrader present: "
              f"{info['xttrader_has_class']}")
    except Exception as e:
        info["xttrader_import_error"] = repr(e)
        info["xttrader_traceback"] = traceback.format_exc()
        print(f"[P2] xttrader import FAILED (likely .pyd load issue):\n"
              f"{info['xttrader_traceback']}")
        raise

    print("[P2] PASS (no XtQuantTrader instance constructed; no SIM session touched)")
    return info


def probe3_vnpy_xt_uses_vendored() -> dict:
    """Probe 3: check vnpy_xt internals to see which xtquant module it actually
    references. If vnpy_xt re-imports xtquant after we set sys.path, we should
    see it bound to the vendored one."""
    _hr("Probe 3: does vnpy_xt internally reference vendored xtquant?")
    info: dict = {}

    import sys as _sys
    xt_in_modules = _sys.modules.get("xtquant")
    info["xtquant_in_sys_modules"] = (
        xt_in_modules.__file__ if xt_in_modules else None
    )
    print(f"[P3] sys.modules['xtquant'].__file__ = {info['xtquant_in_sys_modules']}")

    inspected = {}
    for sub in ["xtquant.xttrader", "xtquant.xtdata", "xtquant.xttype",
                "xtquant.xtconstant"]:
        m = _sys.modules.get(sub)
        inspected[sub] = m.__file__ if m else None
    info["xtquant_submodules_in_sys_modules"] = inspected
    for k, v in inspected.items():
        print(f"[P3] {k}: {v}")

    is_vendored = info["xtquant_in_sys_modules"] and \
        r"F:\Dev\AIstock\xtquant" in info["xtquant_in_sys_modules"]
    info["all_resolve_to_vendored"] = bool(is_vendored)
    if not is_vendored:
        raise RuntimeError(
            f"P3 FAIL: xtquant in sys.modules is NOT vendored: "
            f"{info['xtquant_in_sys_modules']}")
    print("[P3] PASS — vnpy_xt + vendored xtquant share same module instance")
    return info


def main() -> int:
    print(f"[step3b] Python = {sys.version.split()[0]}")
    print(f"[step3b] sys.executable = {sys.executable}")
    if "venv-vnpy-poc" not in sys.executable:
        print("[step3b] WARN: not running from .venv-vnpy-poc")

    assert_clean_state()
    ensure_pythonpath()

    results: dict = {}
    results["P1"] = probe1_pythonpath()
    results["P2"] = probe2_light_api()
    results["P3"] = probe3_vnpy_xt_uses_vendored()

    _hr("SUMMARY")
    for k, v in results.items():
        sample = {kk: vv for kk, vv in v.items() if kk != "vnpy_xt_traceback"}
        print(f"[{k}] {sample}")
    print("\n[step3b] PASS — Mitigation A (PYTHONPATH inject vendored) is VIABLE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
