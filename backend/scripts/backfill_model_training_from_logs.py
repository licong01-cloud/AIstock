"""C3-fix: 从 RDAgent 训练日志 pkl 直接提取训练诊断数据

原 C3 脚本调用 QE workspace enhanced-metrics 端点，但历史模型在原始 RDAgent
log 目录中，enhanced-metrics 端点找不到。本脚本直接解析训练日志 pkl。

使用方式（必须在 WSL conda rdagent-gpu 中执行）:
    python backfill_model_training_from_logs.py [--dry-run]

注意: pkl 由 WSL Python 序列化，必须在 WSL 中执行。
"""
import argparse
import json
import logging
import os
import re
import pickle
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

LOG_ROOT_ENV_KEYS = ("RDAGENT_LOG_ROOT", "QLIB_RDAGENT_LOG_ROOT")


def _load_project_env() -> None:
    try:
        from dotenv import load_dotenv
    except Exception:
        return
    repo_root = Path(__file__).resolve().parents[2]
    candidates = []
    if os.environ.get("AISTOCK_ENV_FILE"):
        candidates.append(Path(os.environ["AISTOCK_ENV_FILE"]))
    candidates.append(repo_root / ".env")
    dotgit = repo_root / ".git"
    if dotgit.is_file():
        text = dotgit.read_text(encoding="utf-8", errors="ignore").strip()
        if text.lower().startswith("gitdir:"):
            gitdir = Path(text.split(":", 1)[1].strip())
            if not gitdir.is_absolute():
                gitdir = (repo_root / gitdir).resolve()
            parts = list(gitdir.parts)
            if ".git" in parts:
                candidates.append(Path(*parts[: parts.index(".git") + 1]).parent / ".env")
    for env_file in candidates:
        if env_file.exists():
            override = env_file == candidates[0] and os.environ.get("AISTOCK_ENV_FILE")
            load_dotenv(env_file, override=bool(override))
            return


def resolve_log_root(explicit: str | None = None) -> Path:
    if explicit:
        return Path(explicit).expanduser()
    _load_project_env()
    for name in LOG_ROOT_ENV_KEYS:
        value = (os.environ.get(name) or "").strip()
        if value:
            return Path(value).expanduser()
    root = (os.environ.get("QLIB_RDAGENT_ROOT_WSL") or "").strip()
    if root:
        return Path(root.rstrip("/")) / "log"
    raise RuntimeError(
        "RD-Agent log root is not configured; set RDAGENT_LOG_ROOT, "
        "QLIB_RDAGENT_LOG_ROOT, or QLIB_RDAGENT_ROOT_WSL."
    )

# PyTorch 模型: Epoch{N}: train {loss}, valid {loss}
RE_EPOCH_PT = re.compile(
    r"Epoch(\d+):\s+train\s+([\d.]+),?\s+valid\s+([\d.]+)"
)
# best score: {score} @ {epoch} epoch
RE_BEST_PT = re.compile(r"best score:\s+([\d.]+)\s*@\s*(\d+)\s*epoch")
# LGBModel: [N] train's l2: {loss}   valid's l2: {loss}
RE_EPOCH_LGB = re.compile(
    r"\[(\d+)\]\s+train's l2:\s+([\d.]+)\s+valid's l2:\s+([\d.]+)"
)
# n_epochs config
RE_N_EPOCHS = re.compile(r"n_epochs\s*:\s*(\d+)")
RE_TRAIN_SAMPLES = re.compile(r"Train samples:\s*([\d,]+)")
RE_VALID_SAMPLES = re.compile(r"Valid samples:\s*([\d,]+)")


def parse_training_log(log_text: str) -> dict:
    """解析训练日志文本，提取训练诊断数据。"""
    lines = log_text.strip().split("\n")

    epochs = []
    best_epoch = None
    _best_score = None
    _n_epochs_config = None
    is_lgb = False

    for line in lines:
        # Check n_epochs config
        m = RE_N_EPOCHS.search(line)
        if m:
            _n_epochs_config = int(m.group(1))

        # PyTorch epoch
        m = RE_EPOCH_PT.search(line)
        if m:
            epochs.append({
                "epoch": int(m.group(1)),
                "train_loss": float(m.group(2)),
                "val_loss": float(m.group(3)),
            })
            continue

        # LGBModel epoch
        m = RE_EPOCH_LGB.search(line)
        if m:
            is_lgb = True
            epochs.append({
                "epoch": int(m.group(1)),
                "train_loss": float(m.group(2)),
                "val_loss": float(m.group(3)),
            })
            continue

        # best score (PyTorch)
        m = RE_BEST_PT.search(line)
        if m:
            _best_score = float(m.group(1))
            best_epoch = int(m.group(2))

    if not epochs:
        return {}

    total_epochs = len(epochs)

    # For LGBModel, best_epoch is the epoch with lowest val_loss
    if is_lgb and best_epoch is None and epochs:
        min_val = min(epochs, key=lambda e: e["val_loss"])
        best_epoch = min_val["epoch"]
        _best_score = min_val["val_loss"]

    # For PyTorch, if best_epoch not found from log, find min val_loss
    if best_epoch is None and epochs:
        min_val = min(epochs, key=lambda e: e["val_loss"])
        best_epoch = min_val["epoch"]

    final_train_loss = epochs[-1]["train_loss"]
    final_val_loss = epochs[-1]["val_loss"]
    initial_train_loss = epochs[0]["train_loss"]
    initial_val_loss = epochs[0]["val_loss"]

    # Convergence ratio: best_epoch / total_epochs
    convergence_ratio = best_epoch / total_epochs if total_epochs > 0 else 0

    # Overfit ratio: how much train improved vs val improved
    train_improve = initial_train_loss - final_train_loss
    val_improve = initial_val_loss - final_val_loss
    overfit_ratio = 0.0
    if train_improve > 0:
        overfit_ratio = max(0, (train_improve - val_improve) / train_improve)

    # Training failed = never improved from epoch 0
    training_failed = (best_epoch == 0 and total_epochs > 1)

    # Training curves
    train_loss_curve = [e["train_loss"] for e in epochs]
    val_loss_curve = [e["val_loss"] for e in epochs]

    return {
        "best_epoch": best_epoch,
        "total_epochs": total_epochs,
        "convergence_ratio": round(convergence_ratio, 4),
        "overfit_ratio": round(overfit_ratio, 4),
        "training_failed": training_failed,
        "final_train_loss": final_train_loss,
        "final_val_loss": final_val_loss,
        "train_loss_curve": train_loss_curve,
        "val_loss_curve": val_loss_curve,
    }


def find_training_log(task_id: str, loop_id: int, log_root: Path | None = None) -> str:
    """在 RDAgent log 目录中查找训练日志 pkl。"""
    root = log_root or resolve_log_root()
    loop_dir = root / task_id / f"Loop_{loop_id}" / "running" / "Qlib_execute_log"
    if not loop_dir.exists():
        return ""

    for pid_dir in loop_dir.iterdir():
        if pid_dir.is_dir():
            for pkl_file in pid_dir.glob("*.pkl"):
                try:
                    with open(pkl_file, "rb") as f:
                        data = pickle.load(f)
                    if isinstance(data, str):
                        return data
                except Exception as e:
                    logger.debug(f"  pkl 解析失败 {pkl_file}: {e}")
    return ""


def main():
    parser = argparse.ArgumentParser(description="从 RDAgent 训练日志提取诊断数据")
    parser.add_argument("--dry-run", action="store_true", help="仅显示结果，不写入 DB")
    parser.add_argument("--db-url", default=None, help="PostgreSQL URL")
    parser.add_argument(
        "--log-root",
        default=None,
        help="RD-Agent log root; defaults to env or QLIB_RDAGENT_ROOT_WSL/log",
    )
    args = parser.parse_args()
    log_root = resolve_log_root(args.log_root)

    # 从 JSON 文件读取需要回填的模型列表（由 Windows 侧生成）
    models_file = Path(__file__).parent / "models_needing_backfill.json"
    if not models_file.exists():
        logger.error(f"请先在 Windows 侧生成 {models_file}")
        sys.exit(1)

    with open(models_file) as f:
        models = json.load(f)

    logger.info(f"共 {len(models)} 个模型需要回填训练诊断数据")

    results = []
    for m in models:
        model_id = m["model_id"]
        task_id = m["task_run_id"]
        loop_id = m["loop_id"]

        logger.info(f"  处理 {model_id} (task={task_id}, loop={loop_id})")
        log_text = find_training_log(task_id, loop_id, log_root)
        if not log_text:
            logger.warning("    无训练日志")
            results.append({"model_id": model_id, "status": "no_log"})
            continue

        diag = parse_training_log(log_text)
        if not diag:
            logger.warning("    训练日志无 epoch 数据")
            results.append({"model_id": model_id, "status": "no_epochs"})
            continue

        logger.info(
            f"    best_epoch={diag['best_epoch']}/{diag['total_epochs']}, "
            f"convergence={diag['convergence_ratio']}, "
            f"overfit={diag['overfit_ratio']}, "
            f"train_loss={diag['final_train_loss']:.6f}, "
            f"val_loss={diag['final_val_loss']:.6f}"
        )
        results.append({
            "model_id": model_id,
            "status": "ok",
            **diag,
        })

    # 输出结果 JSON（供 Windows 侧读取并写入 DB）
    output_file = Path(__file__).parent / "training_diag_results.json"
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2, default=str)
    logger.info(f"结果已写入 {output_file}")

    ok_count = sum(1 for r in results if r["status"] == "ok")
    no_log = sum(1 for r in results if r["status"] == "no_log")
    no_epochs = sum(1 for r in results if r["status"] == "no_epochs")
    logger.info(f"完成: {ok_count} 成功, {no_log} 无日志, {no_epochs} 无 epoch")


if __name__ == "__main__":
    main()
