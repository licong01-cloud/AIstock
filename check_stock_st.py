import os
import argparse
from importlib import import_module
from dotenv import load_dotenv


def parse_args():
    parser = argparse.ArgumentParser(description="Check Tushare stock_st for a date")
    parser.add_argument("--date", type=str, default="2016-01-07", help="ann_date YYYY-MM-DD")
    parser.add_argument("--limit", type=int, default=1000, help="page size (<=1000)")
    return parser.parse_args()


def main():
    # Load .env so TUSHARE_TOKEN can come from repo/local env file.
    load_dotenv(override=True)

    args = parse_args()
    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        raise SystemExit("TUSHARE_TOKEN not set")

    ts = import_module("tushare")
    pro = ts.pro_api(token)

    try:
        ymd = args.date.replace("-", "")
        df = pro.stock_st(ann_date=ymd, limit=args.limit, offset=0)
        if df is None or df.empty:
            print(f"{args.date}: empty or None")
        else:
            print(f"{args.date}: {len(df)} rows")
            print(df.head())
    except Exception as exc:
        print(f"{args.date}: error -> {exc}")


if __name__ == "__main__":
    main()
