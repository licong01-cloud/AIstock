from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.qlib_exporter.authoritative_bin_exporter import (  # noqa: E402
    MINUTE_FREQ_QLIB,
    export_stock_daily_csv,
    export_stock_minute_csv,
    export_stock_minute_csv_chunked,
    normalize_stock_export_exchanges,
    rewrite_stock_all_txt_for_ipo_filter,
    validate_daily_bin_against_db,
    validate_minute_bin_against_db,
    write_bin_meta,
)


def parse_date(value: str) -> date:
    return date.fromisoformat(value)


def parse_csv_list(value: str | None) -> list[str] | None:
    if not value:
        return None
    out = [part.strip() for part in value.replace(";", ",").split(",") if part.strip()]
    return out or None


def win_to_wsl_path(path: Path | str) -> str:
    text = str(path).replace("\\", "/")
    if len(text) >= 2 and text[1] == ":":
        drive = text[0].lower()
        rest = text[2:]
        if rest.startswith("/"):
            return f"/mnt/{drive}{rest}"
        return f"/mnt/{drive}/{rest}"
    return text


def run_wsl_dump(
    *,
    csv_dir: Path,
    bin_dir: Path,
    freq: str,
    distro: str,
    conda_sh: str,
    conda_env: str,
    rdagent_root_wsl: str,
    dump_subcmd: str,
    max_workers: int | None,
) -> dict:
    dump_script = f"{rdagent_root_wsl.rstrip('/')}/scripts/dump_bin.py"
    inner = " && ".join(
        [
            f"source {shlex.quote(conda_sh)}",
            f"conda activate {shlex.quote(conda_env)}",
            " ".join(
                [
                    "python",
                    shlex.quote(dump_script),
                    shlex.quote(dump_subcmd),
                    "--data_path",
                    shlex.quote(win_to_wsl_path(csv_dir)),
                    "--qlib_dir",
                    shlex.quote(win_to_wsl_path(bin_dir)),
                    "--freq",
                    shlex.quote(freq),
                    "--date_field_name",
                    "date",
                    "--symbol_field_name",
                    "symbol",
                    "--exclude_fields",
                    "date,symbol",
                ]
                + (["--max_workers", shlex.quote(str(max_workers))] if max_workers else [])
            ),
        ]
    )
    if os.name == "nt":
        cmd = ["wsl", "-d", distro, "bash", "-lc", inner]
    else:
        cmd = ["bash", "-lc", inner]
    completed = subprocess.run(
        cmd,
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        check=False,
    )
    return {
        "ok": completed.returncode == 0,
        "returncode": completed.returncode,
        "cmd": " ".join(shlex.quote(part) for part in cmd),
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Authoritative AIstock DB -> per-stock CSV -> Qlib dump_bin exporter for QE/V25 datasets."
    )
    parser.add_argument("--dataset", choices=["stock_daily", "stock_minute"], required=True)
    parser.add_argument("--stage", choices=["export", "dump", "validate", "all"], default="all")
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--basis-start", default=None, help="qfq denominator window start; defaults to --start")
    parser.add_argument("--basis-end", default=None, help="qfq denominator window end; defaults to --end")
    parser.add_argument("--csv-root", default=str(PROJECT_ROOT / "qlib_csv"))
    parser.add_argument("--bin-root", default=str(PROJECT_ROOT / "qlib_bin"))
    parser.add_argument("--reports-dir", default=str(PROJECT_ROOT / "reports" / "qlib_authoritative_export"))
    parser.add_argument("--exchanges", default="sh,sz", help="comma-separated: sh,sz; bj/BSE is rejected for stock exports")
    parser.add_argument("--codes", default=None, help="optional comma-separated explicit stock codes")
    parser.add_argument("--exclude-st", action=argparse.BooleanOptionalAction, default=True, help="Exclude all ST stocks; default: true")
    parser.add_argument("--exclude-delisted-or-paused", action=argparse.BooleanOptionalAction, default=True, help="Exclude delisted or paused listings; default: true")
    parser.add_argument("--strict-limit", action="store_true", default=True)
    parser.add_argument("--no-strict-limit", action="store_false", dest="strict_limit")
    parser.add_argument("--validate-values", action="store_true", default=True)
    parser.add_argument("--no-validate-values", action="store_false", dest="validate_values")
    parser.add_argument("--validate-abs-tol", type=float, default=1e-4)
    parser.add_argument("--dump-subcmd", choices=["dump_all", "dump_update"], default="dump_all")
    parser.add_argument("--dump-workers", type=int, default=None)
    parser.add_argument("--wsl-distro", default=os.getenv("QLIB_WSL_DISTRO", "Ubuntu"))
    parser.add_argument("--wsl-conda-sh", default=os.getenv("QLIB_WSL_CONDA_SH", "/home/lc999/miniconda3/etc/profile.d/conda.sh"))
    parser.add_argument("--wsl-conda-env", default=os.getenv("QLIB_WSL_CONDA_ENV", "rdagent-gpu"))
    parser.add_argument("--rdagent-root-wsl", default=os.getenv("QLIB_RDAGENT_ROOT_WSL", "/mnt/f/Dev/RD-Agent-main"))
    parser.add_argument("--validate-max-errors", type=int, default=50)
    parser.add_argument("--minute-chunked-export", action="store_true", help="Use chunked SQL/CSV export for large stock_minute datasets.")
    parser.add_argument("--minute-code-batch-size", type=int, default=20)
    parser.add_argument("--minute-chunk-months", type=int, default=3)
    parser.add_argument("--overwrite-csv", action="store_true")
    parser.add_argument("--resume-csv", action="store_true", help="Resume a chunked stock_minute CSV export by appending only rows after each file's last timestamp.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    start = parse_date(args.start)
    end = parse_date(args.end)
    basis_start = parse_date(args.basis_start) if args.basis_start else start
    basis_end = parse_date(args.basis_end) if args.basis_end else end
    csv_root = Path(args.csv_root)
    bin_root = Path(args.bin_root)
    reports_dir = Path(args.reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)
    bin_dir = bin_root / args.snapshot_id
    exchanges = normalize_stock_export_exchanges(parse_csv_list(args.exchanges))
    codes = parse_csv_list(args.codes)

    result: dict = {
        "snapshot_id": args.snapshot_id,
        "dataset": args.dataset,
        "stage": args.stage,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "basis_start": basis_start.isoformat(),
        "basis_end": basis_end.isoformat(),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }

    if args.stage in {"export", "all"}:
        if args.dataset == "stock_minute" and args.minute_chunked_export:
            summary = export_stock_minute_csv_chunked(
                snapshot_id=args.snapshot_id,
                start=start,
                end=end,
                csv_root=csv_root,
                exchanges=exchanges,
                exclude_st=args.exclude_st,
                exclude_delisted_or_paused=args.exclude_delisted_or_paused,
                ts_codes=codes,
                basis_start=basis_start,
                basis_end=basis_end,
                strict_limit=args.strict_limit,
                code_batch_size=args.minute_code_batch_size,
                chunk_months=args.minute_chunk_months,
                overwrite_csv=args.overwrite_csv,
                resume_csv=args.resume_csv,
            )
        elif args.dataset == "stock_minute":
            summary = export_stock_minute_csv(
                snapshot_id=args.snapshot_id,
                start=start,
                end=end,
                csv_root=csv_root,
                exchanges=exchanges,
                exclude_st=args.exclude_st,
                exclude_delisted_or_paused=args.exclude_delisted_or_paused,
                ts_codes=codes,
                basis_start=basis_start,
                basis_end=basis_end,
                strict_limit=args.strict_limit,
                overwrite_csv=args.overwrite_csv,
            )
        else:
            summary = export_stock_daily_csv(
                snapshot_id=args.snapshot_id,
                start=start,
                end=end,
                csv_root=csv_root,
                exchanges=exchanges,
                exclude_st=args.exclude_st,
                exclude_delisted_or_paused=args.exclude_delisted_or_paused,
                ts_codes=codes,
                basis_start=basis_start,
                basis_end=basis_end,
                strict_limit=args.strict_limit,
                overwrite_csv=args.overwrite_csv,
            )
        result["export_summary"] = summary.__dict__
        print(json.dumps({"export_summary": summary.__dict__}, ensure_ascii=False, indent=2))

    if args.dataset == "stock_minute":
        csv_dir = csv_root / args.snapshot_id / "stock_minute_1min"
        dump_freq = MINUTE_FREQ_QLIB
        dataset_key = f"stock_minute_{MINUTE_FREQ_QLIB}"
        freq_types = [MINUTE_FREQ_QLIB]
    else:
        csv_dir = csv_root / args.snapshot_id / "stock_daily"
        dump_freq = "day"
        dataset_key = "stock_daily"
        freq_types = ["daily"]

    if args.stage in {"dump", "all"}:
        dump = run_wsl_dump(
            csv_dir=csv_dir,
            bin_dir=bin_dir,
            freq=dump_freq,
            distro=args.wsl_distro,
            conda_sh=args.wsl_conda_sh,
            conda_env=args.wsl_conda_env,
            rdagent_root_wsl=args.rdagent_root_wsl,
            dump_subcmd=args.dump_subcmd,
            max_workers=args.dump_workers,
        )
        result["dump"] = dump
        print(json.dumps({"dump": {k: v for k, v in dump.items() if k not in {"stdout", "stderr"}}}, ensure_ascii=False, indent=2))
        if not dump["ok"]:
            (reports_dir / f"{args.snapshot_id}_{args.dataset}_failed.json").write_text(
                json.dumps(result, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            print(dump["stdout"])
            print(dump["stderr"], file=sys.stderr)
            return 2
        ipo_all_txt_summary = rewrite_stock_all_txt_for_ipo_filter(bin_dir=bin_dir)
        result["ipo_all_txt_rewrite"] = ipo_all_txt_summary
        write_bin_meta(
            bin_dir=bin_dir,
            snapshot_id=args.snapshot_id,
            start=start,
            end=end,
            exchanges=exchanges,
            exclude_st=args.exclude_st,
            exclude_delisted_or_paused=args.exclude_delisted_or_paused,
            freq_types=freq_types,
            last_end_dates={dataset_key: end.isoformat()},
            extra={
                "basis_start": basis_start.isoformat(),
                "basis_end": basis_end.isoformat(),
                "csv_dir": str(csv_dir),
                "tool": "scripts/qlib_authoritative_bin_export.py",
                "ipo_all_txt_rewrite": ipo_all_txt_summary,
            },
        )

    if args.stage in {"validate", "all"}:
        validator = validate_minute_bin_against_db if args.dataset == "stock_minute" else validate_daily_bin_against_db
        validation = validator(
            qlib_dir=bin_dir,
            start=start,
            end=end,
            exchanges=exchanges,
            exclude_st=args.exclude_st,
            exclude_delisted_or_paused=args.exclude_delisted_or_paused,
            ts_codes=codes,
            basis_start=basis_start,
            basis_end=basis_end,
            strict_limit=args.strict_limit,
            compare_values=args.validate_values,
            abs_tol=args.validate_abs_tol,
            max_errors=args.validate_max_errors,
        )
        result["validation"] = validation
        print(json.dumps({"validation": validation}, ensure_ascii=False, indent=2))
        if not validation["ok"]:
            (reports_dir / f"{args.snapshot_id}_{args.dataset}_failed.json").write_text(
                json.dumps(result, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            return 3

    report_path = reports_dir / f"{args.snapshot_id}_{args.dataset}_{args.stage}.json"
    report_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] wrote report: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
