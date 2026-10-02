"""Read-only export of the three approved size pools, not a market-data export."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import dotenv_values


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    env = dotenv_values(args.env_file)
    kwargs = {name: env[f"TDX_DB_{key}"] for name, key in [
        ("host", "HOST"), ("port", "PORT"), ("dbname", "NAME"), ("user", "USER"), ("password", "PASSWORD")
    ]}
    with psycopg2.connect(**kwargs, options="-c default_transaction_read_only=on") as conn:
        with conn.cursor() as cur:
            cur.execute("""SELECT pool_id,index_code,ts_code,effective_from,effective_to_exclusive,
                           source_provider,source_reference FROM market.core_index_membership_pit
                           WHERE pool_id=ANY(%s) ORDER BY pool_id,ts_code,effective_from""",
                        (["csi300", "csi500", "csi1000"],))
            frame = pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("xb") as handle:
        frame.to_parquet(handle, index=False)
    print(json.dumps({"membership_intervals": len(frame), "pools": frame.groupby("pool_id").size().to_dict(), "database_writes": 0}))


if __name__ == "__main__":
    main()
