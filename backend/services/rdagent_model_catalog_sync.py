"""
QuantEvolver Phase 1: 模型数据同步模块

从RDAgent Task的Loop数据中提取Model类型Loop的完整信息，
同步到AIstock的aistock_model_catalog表。

数据来源：RDAgent API /tasks/{task_id}/loops
同步内容：
1. 模型类型（LGBModel/GeneralPTNN/自定义等）
2. 全部回测指标（IC, annualized_return, max_drawdown, information_ratio等）
3. 模型参数配置（hyperparameters, training_hyperparameters）
4. 模型源代码（model.py）→ 保存到资产目录
5. 假设阶段内容和回测评估结论文字
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..db.pg_pool import get_conn
from .rdagent_results_api_client import RDAgentResultsApiClient
from .strategy_package.workspace_policy import ensure_aistock_artifact_path

logger = logging.getLogger("aistock.rdagent_model_catalog_sync")

JsonDict = Dict[str, Any]

# 初始化 API 客户端
_rdagent_client = RDAgentResultsApiClient()


def _rdagent_task_assets_root() -> Path:
    return Path(__file__).resolve().parents[2] / "rdagent_assets" / "rdagent_tasks"


def _ensure_local_task_dir(task_dir: str, *, purpose: str) -> Path:
    return ensure_aistock_artifact_path(
        Path(task_dir),
        purpose=purpose,
        extra_roots=[_rdagent_task_assets_root()],
    )


@dataclass
class ModelSyncResult:
    ok: bool = True
    total_models: int = 0
    inserted: int = 0
    updated: int = 0
    skipped: int = 0
    errors: List[str] = field(default_factory=list)


def _utc_now_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _save_model_code_to_file(
    task_dir: str,
    loop_id: int,
    model_code: str,
) -> Optional[str]:
    """将模型源代码保存到task资产目录下的models子目录。

    返回相对于rdagent_assets的路径。
    """
    task_dir_path = _ensure_local_task_dir(
        task_dir,
        purpose=f"RD-Agent model catalog source-code sync: loop_{loop_id}",
    )
    models_dir = task_dir_path / "models" / f"loop_{loop_id}"
    models_dir.mkdir(parents=True, exist_ok=True)

    model_file = models_dir / "model.py"
    model_file.write_text(model_code, encoding="utf-8")

    # 计算相对于rdagent_assets的路径
    try:
        # task_dir 类似 f:/Dev/AIstock/rdagent_assets/rdagent_tasks/{task_id}
        # 返回 rdagent_tasks/{task_id}/models/loop_{loop_id}/model.py
        assets_root = task_dir_path.parent.parent  # rdagent_assets
        relpath = model_file.relative_to(assets_root)
        return str(relpath).replace("\\", "/")
    except ValueError:
        return str(model_file).replace("\\", "/")


def _generate_model_id(task_id: str, loop_id: int) -> str:
    """生成模型唯一ID: task_id::loop_{loop_id}"""
    return f"{task_id}::loop_{loop_id}"


def sync_models_from_task(
    task_id: str,
    task_dir: str,
    loops_data: Optional[List[Dict]] = None,
) -> ModelSyncResult:
    """从Task中提取SOTA Model Loop数据并同步到aistock_model_catalog。

    只同步进入SOTA的模型（is_sota=True），非SOTA模型不入库。
    严格依赖 API 返回的 loop.is_sota 字段判断，不做任何推测。

    流程：
    1. 使用传入的loops_data或调用API获取所有loop
    2. 过滤出 exp_type 包含 "Model" 且 is_sota=True 的 loop
    3. 对每个 SOTA Model loop:
       a. 提取模型元数据（name, type, config等）
       b. 提取回测指标
       c. 提取或下载模型源代码
       d. 保存源代码文件到 task_dir/models/{loop_id}/model.py
       e. UPSERT 到 aistock_model_catalog

    Args:
        task_id: Task ID
        task_dir: AIstock侧task资产目录路径
        loops_data: 已获取的loops列表（可选，避免重复API调用）
    """
    result = ModelSyncResult()

    # 步骤1: 获取loops数据
    all_loops = loops_data
    if all_loops is None:
        try:
            import requests as _req
            base_url = _rdagent_client.base_url.rstrip("/")
            url = f"{base_url}/tasks/{task_id}/loops"
            resp = _req.get(url, timeout=300.0)
            resp.raise_for_status()
            loops_resp = resp.json()
            all_loops = loops_resp.get("loops", [])
        except Exception as e:
            result.ok = False
            result.errors.append(f"获取loops失败: {e}")
            logger.error(f"[{task_id}] 获取loops失败: {e}")
            return result

    if not all_loops:
        logger.info(f"[{task_id}] 没有loop数据，跳过模型同步")
        return result

    # 步骤2: 过滤出Model类型的loop，并标记SOTA
    model_loops = []
    for lp in all_loops:
        exp_type = str(lp.get("exp_type", ""))
        if "Model" not in exp_type:
            continue

        loop_id = lp.get("loop_id")
        # 严格依赖 API 返回的 is_sota 字段，不做任何推测
        is_sota = bool(lp.get("is_sota", False))
        lp["is_sota"] = is_sota

        if is_sota:
            model_loops.append(lp)
        else:
            result.skipped += 1
            logger.debug(f"[{task_id}] loop {loop_id} 非SOTA模型，跳过")

    if not model_loops:
        logger.info(f"[{task_id}] 没有SOTA Model loop，跳过模型同步")
        return result

    result.total_models = len(model_loops)
    logger.info(f"[{task_id}] 发现 {len(model_loops)} 个SOTA Model loop，开始同步")

    # 步骤3: 逐个处理Model loop
    for lp in model_loops:
        loop_id = lp.get("loop_id")
        if loop_id is None:
            result.skipped += 1
            continue

        try:
            model_id = _generate_model_id(task_id, loop_id)

            # 提取模型元数据
            model_name = lp.get("model_name") or lp.get("hypothesis") or f"model_loop_{loop_id}"
            model_description = lp.get("model_description")
            model_architecture = lp.get("model_architecture")
            model_type_tag = lp.get("model_type_tag")
            model_hyperparameters = lp.get("model_hyperparameters")
            model_training_hyperparameters = lp.get("model_training_hyperparameters")
            model_formulation = lp.get("model_formulation")
            model_variables = lp.get("model_variables")

            # 提取回测指标
            ic = lp.get("valid_score")
            annualized_return = lp.get("annualized_return")
            max_drawdown = lp.get("max_drawdown")
            information_ratio = lp.get("information_ratio")
            is_sota = bool(lp.get("is_sota", False))

            # 构造all_metrics JSON
            all_metrics = {}
            for key in ["valid_score", "annualized_return", "max_drawdown",
                        "information_ratio", "sharpe", "turnover",
                        "multi_score"]:
                val = lp.get(key)
                if val is not None:
                    all_metrics[key] = val

            # 提取假设和反馈文本
            hypothesis_text = lp.get("hypothesis_text") or lp.get("hypothesis")
            feedback_observations = lp.get("feedback_observations")
            feedback_evaluation = lp.get("feedback_hypothesis_evaluation")
            feedback_reason = lp.get("feedback_reason") or lp.get("reason")
            feedback_decision = is_sota

            # 提取或下载模型源代码
            model_code = lp.get("model_code")
            source_code_relpath = None
            code_text = None

            if model_code:
                code_text = model_code
            else:
                # 尝试通过独立API下载
                try:
                    code_resp = _rdagent_client._task_get_json(
                        f"/tasks/{task_id}/loops/{loop_id}/model_code"
                    )
                    if code_resp.get("ok") and code_resp.get("model_code"):
                        code_text = code_resp["model_code"]
                except Exception as e:
                    logger.warning(f"[{task_id}] loop {loop_id} 下载model_code失败: {e}")

            # 保存源代码文件
            if code_text and task_dir:
                try:
                    source_code_relpath = _save_model_code_to_file(
                        task_dir, loop_id, code_text
                    )
                    logger.info(f"[{task_id}] loop {loop_id} 模型源代码已保存: {source_code_relpath}")
                except Exception as e:
                    logger.warning(f"[{task_id}] loop {loop_id} 保存模型源代码失败: {e}")

            # 提取训练诊断数据（从训练日志 pkl 直接解析）
            training_diag = {}
            try:
                diag_resp = _rdagent_client._task_get_json(
                    f"/tasks/{task_id}/loops/{loop_id}/training_diagnostics"
                )
                if diag_resp and diag_resp.get("ok"):
                    training_diag = diag_resp
                    logger.info(
                        f"[{task_id}] loop {loop_id} 训练诊断: "
                        f"best_epoch={diag_resp.get('best_epoch')}/{diag_resp.get('total_epochs')}, "
                        f"convergence={diag_resp.get('convergence_ratio')}, "
                        f"overfit={diag_resp.get('overfit_ratio')}, "
                        f"failed={diag_resp.get('training_failed')}"
                    )
                else:
                    logger.info(f"[{task_id}] loop {loop_id} 训练诊断: 无数据")
            except Exception as e:
                logger.warning(f"[{task_id}] loop {loop_id} 获取训练诊断失败: {e}")

            best_epoch = training_diag.get("best_epoch")
            total_epochs = training_diag.get("total_epochs")
            convergence_ratio = training_diag.get("convergence_ratio")
            overfit_ratio = training_diag.get("overfit_ratio")
            training_failed = training_diag.get("training_failed", False)
            train_loss_final = training_diag.get("final_train_loss")
            val_loss_final = training_diag.get("final_val_loss")
            training_curves = None
            if training_diag.get("train_loss_curve") or training_diag.get("val_loss_curve"):
                training_curves = json.dumps({
                    "train_loss": training_diag.get("train_loss_curve", []),
                    "val_loss": training_diag.get("val_loss_curve", []),
                })

            # UPSERT到数据库
            _upsert_model_catalog(
                model_id=model_id,
                data={
                    "catalog_version": _utc_now_iso(),
                    "generated_at_utc": _utc_now_iso(),
                    "catalog_source": "rdagent_task_sync",
                    "task_run_id": task_id,
                    "loop_id": loop_id,
                    "model_type": model_type_tag or "unknown",
                    "model_name": model_name,
                    "model_description": model_description,
                    "model_architecture": model_architecture,
                    "model_formulation": model_formulation,
                    "model_hyperparameters": json.dumps(model_hyperparameters) if model_hyperparameters else None,
                    "model_training_hyperparameters": json.dumps(model_training_hyperparameters) if model_training_hyperparameters else None,
                    "model_variables": json.dumps(model_variables) if model_variables else None,
                    "ic": ic,
                    "annualized_return": annualized_return,
                    "max_drawdown": max_drawdown,
                    "information_ratio": information_ratio,
                    "all_metrics": json.dumps(all_metrics) if all_metrics else None,
                    "source_code_relpath": source_code_relpath,
                    "code_text": code_text,
                    "hypothesis_text": hypothesis_text,
                    "feedback_observations": feedback_observations,
                    "feedback_evaluation": feedback_evaluation,
                    "feedback_reason": feedback_reason,
                    "feedback_decision": feedback_decision,
                    "is_sota": is_sota,
                    "source_task_id": task_id,
                    "display_name": model_name,
                    # 训练诊断字段
                    "best_epoch": best_epoch,
                    "total_epochs": total_epochs,
                    "convergence_ratio": convergence_ratio,
                    "overfit_ratio": overfit_ratio,
                    "training_failed": training_failed,
                    "train_loss_final": train_loss_final,
                    "val_loss_final": val_loss_final,
                    "training_curves": training_curves,
                },
            )
            result.inserted += 1
            logger.info(
                f"[{task_id}] loop {loop_id} 模型 '{model_name}' "
                f"已同步 (IC={ic}, ann_ret={annualized_return}, sota={is_sota})"
            )

        except Exception as e:
            result.errors.append(f"loop {loop_id}: {e}")
            logger.error(f"[{task_id}] loop {loop_id} 模型同步失败: {e}")

    result.ok = len(result.errors) == 0
    logger.info(
        f"[{task_id}] 模型同步完成: total={result.total_models}, "
        f"inserted={result.inserted}, errors={len(result.errors)}"
    )
    return result


def _upsert_model_catalog(model_id: str, data: JsonDict) -> None:
    """UPSERT模型数据到aistock_model_catalog。"""
    fields = list(data.keys())
    values = []
    for f in fields:
        val = data[f]
        if isinstance(val, dict):
            values.append(json.dumps(val))
        else:
            values.append(val)

    update_parts = [f"{f} = EXCLUDED.{f}" for f in fields]

    placeholders = ", ".join(["%s"] * (len(fields) + 1))  # +1 for model_id
    field_names = ", ".join(["model_id"] + fields)

    sql = f"""
        INSERT INTO aistock_model_catalog ({field_names})
        VALUES ({placeholders})
        ON CONFLICT (model_id) DO UPDATE SET
            {", ".join(update_parts)}
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, [model_id] + values)
