"""Refresh the DB-backed factor_analyst/analyze_factor_v2 system prompt.

- Reads F:\\Dev\\AIstock\\.env for TDX_DB_* vars
- Re-renders the default system prompt from factor_analyst._get_default_v2_system_prompt
- Calls PromptManager.update_prompt (or create_prompt on first run)
- Also verifies new qe_factor_classification columns are present
"""

import os
import sys
from pathlib import Path

REPO_ROOT = Path(r"F:/Dev/AIstock")
ENV_FILE = REPO_ROOT / ".env"


def load_env():
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def main():
    load_env()
    # Ensure repo root is importable (so `backend.*` works and relative imports resolve)
    sys.path.insert(0, str(REPO_ROOT))

    import psycopg2
    from backend.services.quantevolver.factor_analyst import _get_default_v2_system_prompt
    from backend.services.quantevolver.prompt_manager import PromptManager

    # ── Step 1: verify 3 new classification columns are present ──
    conn = psycopg2.connect(
        host=os.environ["TDX_DB_HOST"],
        port=int(os.environ["TDX_DB_PORT"]),
        dbname=os.environ["TDX_DB_NAME"],
        user=os.environ["TDX_DB_USER"],
        password=os.environ["TDX_DB_PASSWORD"],
    )
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name='qe_factor_classification'
              AND column_name = ANY(%s)
            ORDER BY column_name
        """, (["direction", "signal_mechanism", "sector_exposure_corr"],))
        cols = [r[0] for r in cur.fetchall()]
    conn.close()
    missing = sorted({"direction", "signal_mechanism", "sector_exposure_corr"} - set(cols))
    if missing:
        print(f"[ABORT] qe_factor_classification missing cols: {missing}", file=sys.stderr)
        sys.exit(1)
    print(f"[1/2] classification cols verified: {cols}")

    # ── Step 2: update (or create) DB prompt ──
    new_system_prompt = _get_default_v2_system_prompt(official_grade=None)
    pm = PromptManager()

    existing = pm.get_active_prompt_text("factor_analyst", "analyze_factor_v2")
    if existing is None:
        print("[2/2] creating new DB prompt factor_analyst/analyze_factor_v2")
        r = pm.create_prompt(
            agent_type="factor_analyst",
            prompt_key="analyze_factor_v2",
            display_name="Factor Analyst v2 (direction-aware)",
            system_prompt=new_system_prompt,
            user_prompt_template="",
            description="v2.0 规则引擎配套: 输出 direction / signal_mechanism / sector_exposure_corr",
        )
    else:
        print("[2/2] updating DB prompt factor_analyst/analyze_factor_v2")
        r = pm.update_prompt(
            agent_type="factor_analyst",
            prompt_key="analyze_factor_v2",
            system_prompt=new_system_prompt,
            is_active=True,
        )

    if not r.get("ok"):
        print(f"[ABORT] {r.get('error')}", file=sys.stderr)
        sys.exit(2)

    print(f"       {r.get('message')}")
    print(f"       system_prompt length: {len(new_system_prompt)} chars")
    print("\n[DONE] factor_analyst/analyze_factor_v2 prompt refreshed.")


if __name__ == "__main__":
    main()
