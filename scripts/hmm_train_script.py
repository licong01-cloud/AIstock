#!/usr/bin/env python
"""HMM 训练脚本，由后端通过 WSL subprocess 调用。

使用 RD-Agent 的 7 维训练流程（train_sector_hmm.py），支持:
  - 7 维观测矩阵（日收益率/超额收益/成交量占比/涨停占比/波动率/净资金流/超大单）
  - 可配置 n_states, covariance_type, rolling_window, zscore
  - 训练 + 验证 + 保存到指定路径

用法:
    python hmm_train_script.py \
        --config-file /path/to/config.json \
        --output-path /path/to/models.json \
        --db-password xxx

成功时输出 JSON 摘要到 stdout:
    {"sector_count": N, "status": "ok", "metrics": {...}}

失败时输出错误信息到 stdout 并以非零退出码退出:
    {"status": "error", "message": "..."}
"""

from __future__ import annotations

import argparse
import json
import sys
import os

# Ensure project roots on sys.path
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)  # AIstock/
_RDAGENT_ROOT = os.path.join(os.path.dirname(_PROJECT_ROOT), "RD-Agent-main")
for p in (_RDAGENT_ROOT, _PROJECT_ROOT):
    if p not in sys.path:
        sys.path.insert(0, p)


def main() -> None:
    parser = argparse.ArgumentParser(description="HMM sector model training script (7-dim)")
    parser.add_argument("--config-json", default=None)
    parser.add_argument("--config-file", default=None)
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--db-password", default=None)
    args = parser.parse_args()

    if not args.config_json and not args.config_file:
        print(json.dumps({"status": "error", "message": "--config-json or --config-file required"}))
        sys.exit(1)

    try:
        # 读取配置
        if args.config_file:
            with open(args.config_file, "r", encoding="utf-8") as f:
                config_dict = json.load(f)
        else:
            config_dict = json.loads(args.config_json)

        # 导入 RD-Agent 7 维训练模块
        from model_training.hmm.config import HMMTrainConfig
        from model_training.hmm.train_sector_hmm import train_all_sectors, validate_model

        # 映射配置字段到 HMMTrainConfig
        cfg = HMMTrainConfig(
            n_states=config_dict.get("n_states", 3),
            covariance_type=config_dict.get("covariance_type", "diag"),
            n_iter=config_dict.get("n_iter", 300),
            rolling_window=config_dict.get("rolling_window", 5),
            zscore=config_dict.get("zscore", False),
            use_limit_down=config_dict.get("use_limit_down", False),
            db_password=args.db_password or "",
        )

        print(f"配置: n_states={cfg.n_states}, cov={cfg.covariance_type}, "
              f"rw={cfg.rolling_window}, zscore={cfg.zscore}", file=sys.stderr)

        # Step 1: 训练
        print("[Step 1/2] 训练模型", file=sys.stderr)
        models, cached = train_all_sectors(cfg)
        if not models:
            raise RuntimeError("没有训练出任何行业模型")
        print(f"  训练完成: {len(models)} 个行业", file=sys.stderr)

        # Step 2: 验证
        print("[Step 2/2] 验证模型", file=sys.stderr)
        metrics = validate_model(models, cfg, cached)

        # 保存到指定路径
        output_dir = os.path.dirname(args.output_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        with open(args.output_path, "w", encoding="utf-8") as f:
            json.dump(models, f, ensure_ascii=False, indent=2)
        print(f"  模型已保存: {args.output_path}", file=sys.stderr)

        # 输出摘要到 stdout
        summary = {
            "sector_count": len(models),
            "status": "ok",
            "metrics": metrics,
        }
        print(json.dumps(summary))

    except Exception as exc:
        import traceback
        traceback.print_exc(file=sys.stderr)
        print(json.dumps({"status": "error", "message": str(exc)}))
        sys.exit(1)


if __name__ == "__main__":
    main()
