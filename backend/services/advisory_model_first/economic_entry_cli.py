"""Explicit artifact stages for the Advisory economic-entry study."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from backend.services.advisory_model_first.economic_entry_contracts import EconomicEntryStudyPlanV1
from backend.services.advisory_model_first.economic_entry_pipeline import (
    load_economic_study,
    prepare_economic_study,
    preregister_economic_study,
    read_stage,
    train_economic_study,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Advisory只读经济进入价值研究；不绑定生产、不调用QE")
    parser.add_argument("stage", choices=("preregister", "prepare", "train", "status"))
    parser.add_argument("--plan", required=True, help="preregister用方案文件；其余使用已发布plan.json")
    parser.add_argument("--output-root", required=True, help="本任务独立的F盘持久工件根")
    parser.add_argument("--env-file", help="prepare时显式读取既存环境配置；只读、不记录凭据内容")
    args = parser.parse_args(argv)
    try:
        root = Path(args.output_root).resolve()
        if not Path(args.output_root).is_absolute() or root.drive.upper() != "F:":
            raise AdvisoryModelFirstError("CLI持久工件必须显式在F盘", reason_code="ADVISORY_ECONOMIC_OUTPUT_ROOT_INVALID")
        if args.stage == "preregister":
            plan = EconomicEntryStudyPlanV1.model_validate_json(Path(args.plan).read_text(encoding="utf-8"))
            result = {"plan_path": str(preregister_economic_study(plan=plan, output_root=root)), "stage": "PREREGISTERED"}
        elif args.stage == "prepare":
            if args.env_file:
                from dotenv import dotenv_values
                env_path = Path(args.env_file)
                if not env_path.is_absolute() or not env_path.is_file():
                    raise AdvisoryModelFirstError("环境配置必须是显式既存文件", reason_code="ADVISORY_ECONOMIC_ENV_SOURCE_INVALID")
                # Do not allow an environment file to redirect TEMP/TMP to C.
                for name, value in dotenv_values(env_path).items():
                    if name.startswith("TDX_DB_") and value is not None:
                        os.environ[name] = value
            result = {"artifact_path": str(prepare_economic_study(plan_path=args.plan, output_root=root)), "stage": "PREPARED"}
        elif args.stage == "train":
            result = {"artifact_path": str(train_economic_study(plan_path=args.plan, output_root=root)), "stage": "TRAINED"}
        else:
            plan, study = load_economic_study(args.plan, output_root=root)
            stage, parent = "NOT_PREPARED", None
            for name in ("preregistered", "prepared", "trained", "evaluated"):
                if not (study / name).exists():
                    break
                receipt = read_stage(study / name, stage=name, plan_sha256=plan.plan_sha256, parent_sha256=parent)
                parent, stage = receipt["stage_sha256"], name.upper()
            result = {"experiment_id": plan.experiment_id, "stage": stage, "live_process": "NOT_INFERRED_FROM_FILES"}
        print(json.dumps({"status": "ok", **result, "database_written": False, "binding_activated": False,
                          "qe_experiment_submitted": False, "sealed_holdout_accessed": False}, ensure_ascii=False))
        return 0
    except AdvisoryModelFirstError as exc:
        print(json.dumps(exc.as_dict(), ensure_ascii=False))
        return 2
    except Exception as exc:
        # Do not expose raw connection strings or backend credential values.
        print(json.dumps({"status": "failed", "reason_code": "ADVISORY_ECONOMIC_UNEXPECTED_ERROR",
                          "error_type": type(exc).__name__}, ensure_ascii=False))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
