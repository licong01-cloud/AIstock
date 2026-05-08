"""Step 0 — Environment self-check before any xtquant connection.

Validates (fail-fast, no silent fallback per feedback_no_silent_errors):
  1. .env.poc loadable
  2. MINIQMT_USERDATA_PATH exists and contains live miniQMT shm cache files
  3. MINIQMT_XTQUANT_DIR exists and contains xttrader.py + matching .pyd for current Python
  4. xtquant import works (xttrader, xttype, xtdata, xtconstant)
  5. Reports xtquant version banner

Run from repo root:
    python -m backend.services.paper_trading_v2.poc.step0_env_check
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


def _load_poc_env() -> Path:
    poc_dir = Path(__file__).resolve().parent
    env_poc = poc_dir / ".env.poc"
    if not env_poc.is_file():
        raise FileNotFoundError(f".env.poc not found at {env_poc}")
    try:
        from dotenv import load_dotenv
    except ImportError as e:
        raise RuntimeError(
            "python-dotenv not installed. Run: pip install python-dotenv"
        ) from e
    load_dotenv(env_poc, override=True)
    return env_poc


def _require_env(key: str) -> str:
    val = os.environ.get(key, "").strip()
    if not val:
        raise RuntimeError(f"Required env var missing or empty: {key}")
    return val


def check_userdata_path() -> Path:
    path = Path(_require_env("MINIQMT_USERDATA_PATH"))
    if not path.is_dir():
        raise RuntimeError(f"MINIQMT_USERDATA_PATH does not exist: {path}")
    fingerprints = ["miniqmtShmQuoteCache", "miniqmtShmStockListCacheSH"]
    missing = [f for f in fingerprints if not (path / f).exists()]
    if missing:
        raise RuntimeError(
            f"userdata_path missing miniQMT fingerprint files: {missing}. "
            f"Is miniQMT actually running with this userdata dir?"
        )
    return path


def check_xtquant_dir() -> Path:
    xt_dir = Path(_require_env("MINIQMT_XTQUANT_DIR"))
    if not xt_dir.is_dir():
        raise RuntimeError(f"MINIQMT_XTQUANT_DIR does not exist: {xt_dir}")
    must_have = ["__init__.py", "xttrader.py", "xttype.py", "xtdata.py", "xtconstant.py"]
    missing = [f for f in must_have if not (xt_dir / f).exists()]
    if missing:
        raise RuntimeError(f"xtquant dir missing required files: {missing}")
    py_tag = f"cp{sys.version_info.major}{sys.version_info.minor}-win_amd64.pyd"
    pyd_files = list(xt_dir.glob(f"*.{py_tag}"))
    if not pyd_files:
        raise RuntimeError(
            f"No xtquant .pyd matches current Python {py_tag}. "
            f"xtquant available builds: {[p.name for p in xt_dir.glob('*.pyd')][:5]}..."
        )
    return xt_dir


def check_xtquant_import(xt_dir: Path) -> dict:
    parent = str(xt_dir.parent)
    if parent not in sys.path:
        sys.path.insert(0, parent)
    from xtquant import xttrader, xttype, xtdata, xtconstant  # noqa: F401
    if not hasattr(xttrader, "XtQuantTrader"):
        raise RuntimeError("xtquant.xttrader has no XtQuantTrader class")
    return {
        "xtquant.__version__": getattr(
            __import__("xtquant"), "__version__", "unknown"
        ),
        "xttrader.__file__": xttrader.__file__,
        "xtconstant.STOCK_BUY": xtconstant.STOCK_BUY,
        "xtconstant.FIX_PRICE": xtconstant.FIX_PRICE,
    }


def main() -> int:
    print(f"[step0] Python {sys.version}")
    env_path = _load_poc_env()
    print(f"[step0] loaded {env_path}")

    userdata = check_userdata_path()
    print(f"[step0] userdata OK: {userdata}")

    xt_dir = check_xtquant_dir()
    print(f"[step0] xtquant dir OK: {xt_dir}")

    info = check_xtquant_import(xt_dir)
    print(f"[step0] xtquant import OK:")
    for k, v in info.items():
        print(f"          {k} = {v}")

    enabled = os.environ.get("MINIQMT_ENABLED", "false").lower()
    if enabled != "true":
        raise RuntimeError(f"MINIQMT_ENABLED is {enabled!r}, expected 'true'")
    print(f"[step0] MINIQMT_ENABLED={enabled}, account={os.environ.get('MINIQMT_ACCOUNT_ID')}, "
          f"session={os.environ.get('MINIQMT_SESSION_ID')}")
    print("[step0] PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
