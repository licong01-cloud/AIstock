"""RL 模型月度滚动训练调度器。

流程:
  1. 检查是否到达滚动时间 (月度)
  2. 增量更新 pkl 数据
  3. 生成新的 train/valid/test 订单
  4. 微调模型 (finetune, 少量 epoch)
  5. 验证集评估
  6. 如果不退化 → 注册新版本; 如果退化 > rollback_threshold → 回滚
"""
from __future__ import annotations

import logging
import os
import shlex
import subprocess
import time
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from dateutil.relativedelta import relativedelta

from .model_registry import model_registry

logger = logging.getLogger("aistock.rl_execution.scheduler")

_AISTOCK_PROJECT_ROOT = Path(__file__).resolve().parents[3]


def _win_to_wsl_guess(path: Path) -> str:
    text = str(path).replace("\\", "/")
    if len(text) >= 2 and text[1] == ":":
        return f"/mnt/{text[0].lower()}{text[2:]}"
    return text


_AISTOCK_ROOT_WSL = os.getenv("RL_AISTOCK_ROOT_WSL", _win_to_wsl_guess(_AISTOCK_PROJECT_ROOT))
_RL_DATA_ROOT_WSL = os.getenv("RL_DATA_ROOT_WSL", f"{_AISTOCK_ROOT_WSL}/rl_data")

# 默认配置
DEFAULT_CONFIG = {
    "slide_months": 1,
    "train_months": 18,
    "valid_months": 3,
    "test_months": 6,
    "finetune_epochs": 10,
    "finetune_lr": 1e-4,
    "rollback_threshold": 0.10,
    "wsl_distro": "Ubuntu",
    "conda_env": "rdagent-gpu",
    "conda_profile": os.getenv("RL_CONDA_PROFILE_WSL", "").strip(),
    "aistock_root": _AISTOCK_ROOT_WSL,
    "config_path": os.getenv("RL_TRAIN_CONFIG_WSL", f"{_AISTOCK_ROOT_WSL}/rl_execution/config/train_ppo.yaml"),
    "pickle_dir": os.getenv("RL_PICKLE_DIR_WSL", f"{_RL_DATA_ROOT_WSL}/backtest"),
    "order_dir": os.getenv("RL_ORDER_DIR_WSL", f"{_RL_DATA_ROOT_WSL}/orders"),
}


def compute_rolling_dates(
    reference_date: date,
    train_months: int = 18,
    valid_months: int = 3,
    test_months: int = 6,
) -> dict:
    """根据参考日期计算滚动训练的时间窗口。"""
    test_end = reference_date
    test_start = test_end - relativedelta(months=test_months)
    valid_end = test_start - relativedelta(days=1)
    valid_start = valid_end - relativedelta(months=valid_months) + relativedelta(days=1)
    train_end = valid_start - relativedelta(days=1)
    train_start = train_end - relativedelta(months=train_months) + relativedelta(days=1)

    return {
        "train_start": train_start,
        "train_end": train_end,
        "valid_start": valid_start,
        "valid_end": valid_end,
        "test_start": test_start,
        "test_end": test_end,
    }


def trigger_rolling_train(
    dev_version: str,
    reference_date: Optional[date] = None,
    parent_model_path: Optional[str] = None,
    cfg: Optional[dict] = None,
) -> dict:
    """触发一次滚动训练。

    Parameters
    ----------
    dev_version : str
        开发版本号 (e.g. "v1")
    reference_date : date
        滚动参考日期 (默认今天)
    parent_model_path : str
        基础模型路径 (用于微调)
    cfg : dict
        覆盖默认配置

    Returns
    -------
    dict
        {"status": "success"/"failed", "version_tag": "v1-r202503", ...}
    """
    config = {**DEFAULT_CONFIG, **(cfg or {})}

    if reference_date is None:
        reference_date = date.today()

    dates = compute_rolling_dates(
        reference_date,
        train_months=config["train_months"],
        valid_months=config["valid_months"],
        test_months=config["test_months"],
    )

    roll_tag = f"r{dates['train_end'].strftime('%Y%m')}"
    version_tag = f"{dev_version}-{roll_tag}"

    logger.info(
        f"Rolling train: {version_tag}, "
        f"train={dates['train_start']}~{dates['train_end']}, "
        f"valid={dates['valid_start']}~{dates['valid_end']}"
    )

    conda_init = (
        f"source {shlex.quote(config['conda_profile'])}"
        if config.get("conda_profile")
        else 'source "$(conda info --base)/etc/profile.d/conda.sh"'
    )

    # 构建 WSL 训练命令
    wsl_cmd = (
        f"{conda_init} && "
        f"conda activate {shlex.quote(config['conda_env'])} && "
        f"cd {shlex.quote(config['aistock_root'])} && "
        f"python scripts/rl_execution/train.py "
        f"--config {shlex.quote(config['config_path'])} "
        f"--dev-version {shlex.quote(dev_version)} "
        f"--roll-tag {shlex.quote(roll_tag)} "
        f"--dev-description 'Rolling train {roll_tag}' "
    )

    t0 = time.time()
    try:
        result = subprocess.run(
            ["wsl", "-d", config["wsl_distro"], "--", "bash", "-c", wsl_cmd],
            capture_output=True,
            text=True,
            timeout=7200,  # 2 小时超时
        )

        duration = int(time.time() - t0)

        if result.returncode == 0:
            logger.info(f"Rolling train completed: {version_tag} ({duration}s)")

            # 检查是否需要回滚
            latest = model_registry.get_latest_active(dev_version=dev_version)
            if latest and latest.get("eval_pa_bps") is not None:
                new_versions = model_registry.list_versions(dev_version=dev_version)
                new_model = next((v for v in new_versions if v["roll_tag"] == roll_tag), None)
                if new_model and new_model.get("eval_pa_bps") is not None:
                    old_pa = latest["eval_pa_bps"]
                    new_pa = new_model["eval_pa_bps"]
                    if old_pa > 0 and (old_pa - new_pa) / abs(old_pa) > config["rollback_threshold"]:
                        model_registry.deactivate(dev_version, roll_tag)
                        logger.warning(
                            f"Rollback {version_tag}: PA {new_pa:.2f} vs {old_pa:.2f} "
                            f"(degradation {(old_pa - new_pa)/abs(old_pa):.1%})"
                        )
                        return {
                            "status": "rollback",
                            "version_tag": version_tag,
                            "reason": f"PA degradation {(old_pa - new_pa)/abs(old_pa):.1%}",
                            "duration_sec": duration,
                        }

            return {
                "status": "success",
                "version_tag": version_tag,
                "duration_sec": duration,
                "dates": {k: v.isoformat() for k, v in dates.items()},
            }
        else:
            logger.error(f"Rolling train failed: {result.stderr[-500:]}")
            return {
                "status": "failed",
                "version_tag": version_tag,
                "error": result.stderr[-500:],
                "duration_sec": duration,
            }

    except subprocess.TimeoutExpired:
        logger.error(f"Rolling train timeout: {version_tag}")
        return {"status": "timeout", "version_tag": version_tag}

    except Exception as e:
        logger.error(f"Rolling train error: {e}")
        return {"status": "error", "version_tag": version_tag, "error": str(e)}
