"""
OptunaHyperparamOptimizer — Optuna TPE 贝叶斯超参优化器

为 QE 演进系统的 param_tune 方向提供基于 Optuna TPE 的超参数建议，
替代纯 LLM 随机猜测，提升搜索效率。

Features:
- 持久化 Study（JournalFileStorage），路径 {QE_SOTA_ASSETS_DIR}/optuna_studies/{task_id}_{model_type}.db
- 历史 trial 注入：从 qe_evolution_loops 提取同 task 的 param_tune 记录作为先验
- 跨 task trial 迁移：同 model_type 不同 task 的最优 trial 注入（上限 20 条）
- 优雅降级：optuna 未安装或 Study 加载失败时不中断流程
"""

import json
import logging
import os
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

# Optuna 可能未安装，延迟导入并优雅降级
try:
    import optuna
    from optuna.distributions import FloatDistribution, IntDistribution
    from optuna.storages.journal import JournalFileBackend, JournalStorage
    from optuna.trial import create_trial, TrialState

    OPTUNA_AVAILABLE = True
except ImportError:
    OPTUNA_AVAILABLE = False
    logger.warning("optuna 未安装，OptunaHyperparamOptimizer 将不可用，回退到纯 LLM 模式")

SOTA_ASSETS_DIR = os.environ.get(
    "QE_SOTA_ASSETS_DIR", "f:/Dev/AIstock/rdagent_assets/qe_sota_assets"
)

# 各模型类型的超参数合法范围（与 EvolutionModelAgent.HYPERPARAM_RANGES 保持一致）
HYPERPARAM_RANGES = {
    "LGB": {
        "learning_rate": (0.01, 0.3),
        "max_depth": (4, 12),
        "num_leaves": (31, 512),
        "lambda_l1": (0, 500),
        "lambda_l2": (0, 1000),
        "colsample_bytree": (0.5, 1.0),
        "subsample": (0.5, 1.0),
    },
    "XGB": {
        "n_estimators": (100, 2000),
        "max_depth": (3, 12),
        "learning_rate": (0.01, 0.3),
        "subsample": (0.5, 1.0),
        "colsample_bytree": (0.5, 1.0),
        "reg_alpha": (0, 10),
        "reg_lambda": (0, 10),
    },
    "CATBOOST": {
        "iterations": (100, 2000),
        "depth": (3, 10),
        "learning_rate": (0.01, 0.3),
        "l2_leaf_reg": (1, 10),
        "subsample": (0.5, 1.0),
    },
    "LINEAR": {
        "alpha": (0.001, 1.0),
    },
    "PTNN": {
        "n_epochs": (100, 300),
        "lr": (1e-5, 1e-2),
        "batch_size": (2048, 16384),
        "early_stop": (10, 30),
        "weight_decay": (1e-6, 1e-2),
    },
}

# 整数类型超参数集合
INTEGER_PARAMS = {
    "max_depth", "num_leaves", "n_epochs", "batch_size",
    "early_stop", "n_estimators", "iterations", "depth",
}

# 对数空间搜索的浮点参数集合（学习率类）
LOG_SCALE_PARAMS = {"learning_rate", "lr", "weight_decay"}


class OptunaHyperparamOptimizer:
    """Optuna TPE 超参数优化器，为 QE 演进的 param_tune 方向提供贝叶斯搜索建议。"""

    def __init__(self, task_id: str, model_type: str):
        """
        初始化优化器。

        Args:
            task_id: 演进任务 ID
            model_type: 模型类型（LGB, XGB, CATBOOST, LINEAR, PTNN）
        """
        self.task_id = task_id
        self.model_type = model_type.upper()
        self._study: Optional[Any] = None  # optuna.Study or None

        # Study 持久化路径
        studies_dir = os.path.join(SOTA_ASSETS_DIR, "optuna_studies")
        self.storage_path = os.path.join(
            studies_dir, f"{task_id}_{self.model_type}.db"
        )

        if not OPTUNA_AVAILABLE:
            logger.warning(
                f"optuna 未安装，OptunaHyperparamOptimizer({task_id}, {model_type}) 不可用"
            )

    def get_or_create_study(self) -> Optional[Any]:
        """
        创建或加载持久化 Optuna Study（TPE 采样器）。

        首次创建时注入历史 trial 和跨 task trial 作为先验知识。
        加载失败时创建新 Study。

        Returns:
            optuna.Study 或 None（optuna 不可用时）
        """
        if not OPTUNA_AVAILABLE:
            return None

        if self._study is not None:
            return self._study

        study_name = f"{self.task_id}_{self.model_type}"
        is_new_study = not os.path.exists(self.storage_path)

        try:
            # 确保目录存在
            os.makedirs(os.path.dirname(self.storage_path), exist_ok=True)

            backend = JournalFileBackend(self.storage_path)
            storage = JournalStorage(backend)

            self._study = optuna.create_study(
                study_name=study_name,
                storage=storage,
                sampler=optuna.samplers.TPESampler(),
                direction="maximize",  # 最大化 IC
                load_if_exists=True,
            )

            # 首次创建时注入历史 trial
            if is_new_study:
                logger.info(
                    f"新建 Optuna Study: {study_name}, 注入历史 trial..."
                )
                self._inject_historical_trials(self._study)
                self._inject_cross_task_trials(self._study)
            else:
                logger.info(
                    f"加载已有 Optuna Study: {study_name}, "
                    f"已有 {len(self._study.trials)} 条 trial"
                )

        except Exception as e:
            logger.error(
                f"Study 加载/创建失败 ({study_name}): {e}, 尝试重建"
            )
            try:
                # 删除损坏的文件，重新创建
                if os.path.exists(self.storage_path):
                    os.remove(self.storage_path)
                backend = JournalFileBackend(self.storage_path)
                storage = JournalStorage(backend)
                self._study = optuna.create_study(
                    study_name=study_name,
                    storage=storage,
                    sampler=optuna.samplers.TPESampler(),
                    direction="maximize",
                )
                self._inject_historical_trials(self._study)
                self._inject_cross_task_trials(self._study)
            except Exception as e2:
                logger.warning(
                    f"文件存储重建失败: {e2}, 回退到内存 Study"
                )
                try:
                    self._study = optuna.create_study(
                        study_name=study_name,
                        sampler=optuna.samplers.TPESampler(),
                        direction="maximize",
                    )
                    self._inject_historical_trials(self._study)
                    self._inject_cross_task_trials(self._study)
                except Exception as e3:
                    logger.error(f"内存 Study 也失败: {e3}")
                    self._study = None

        return self._study

    def _inject_historical_trials(self, study: Any) -> None:
        """
        从 qe_evolution_loops 注入同 task_id 的历史 param_tune trial。

        查询条件：task_id 相同, action_type='param_tune', status='completed'
        注入方式：study.add_trial() 作为已完成 trial，目标值为 IC
        """
        try:
            from ...db.pg_pool import get_conn
            from psycopg2.extras import RealDictCursor

            with get_conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT config_json, metrics_json
                        FROM qe_evolution_loops
                        WHERE task_id = %s
                          AND action_type = 'param_tune'
                          AND status = 'completed'
                        ORDER BY loop_index ASC
                        """,
                        (self.task_id,),
                    )
                    rows = cur.fetchall()

            if not rows:
                logger.info(
                    f"task {self.task_id} 无历史 param_tune trial 可注入"
                )
                return

            injected = 0
            for row in rows:
                try:
                    config = row.get("config_json") or {}
                    if isinstance(config, str):
                        config = json.loads(config)
                    metrics = row.get("metrics_json") or {}
                    if isinstance(metrics, str):
                        metrics = json.loads(metrics)

                    model_params = config.get("model_params", {})
                    ic_value = metrics.get("IC")
                    if ic_value is None or not model_params:
                        continue

                    # 构建 distributions 和 params
                    distributions, params = self._build_trial_params(model_params)
                    if not params:
                        continue

                    trial = optuna.trial.create_trial(
                        params=params,
                        distributions=distributions,
                        values=[float(ic_value)],
                        state=TrialState.COMPLETE,
                    )
                    study.add_trial(trial)
                    injected += 1
                except Exception as e:
                    logger.warning(f"注入历史 trial 失败: {e}")
                    continue

            logger.info(
                f"task {self.task_id} 注入 {injected}/{len(rows)} 条历史 trial"
            )

        except Exception as e:
            logger.error(f"查询历史 trial 失败: {e}")

    def _inject_cross_task_trials(self, study: Any) -> None:
        """
        注入同 model_type 跨 task 的最优 trial（IC 最高前 20 条）。

        当可用 trial > 50 条时仅注入前 20 条，避免低质量先验污染。
        每条 trial 的 user_attrs 中标记 source_task_id。
        """
        try:
            from ...db.pg_pool import get_conn
            from psycopg2.extras import RealDictCursor

            with get_conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # 先查询总数
                    cur.execute(
                        """
                        SELECT COUNT(*) as cnt
                        FROM qe_evolution_loops l
                        WHERE l.task_id != %s
                          AND l.action_type = 'param_tune'
                          AND l.status = 'completed'
                          AND l.config_json->>'model_id' IS NOT NULL
                          AND UPPER(
                              COALESCE(
                                  l.config_json->>'model_type',
                                  SPLIT_PART(l.config_json->>'model_id', '_', 1)
                              )
                          ) = %s
                        """,
                        (self.task_id, self.model_type),
                    )
                    total_count = cur.fetchone()["cnt"]

                    # 超过 50 条时仅取前 20，否则全部取
                    limit = 20 if total_count > 50 else total_count

                    if limit == 0:
                        logger.info(
                            f"model_type={self.model_type} 无跨 task trial 可注入"
                        )
                        return

                    cur.execute(
                        """
                        SELECT l.task_id as source_task_id,
                               l.config_json, l.metrics_json
                        FROM qe_evolution_loops l
                        WHERE l.task_id != %s
                          AND l.action_type = 'param_tune'
                          AND l.status = 'completed'
                          AND l.config_json->>'model_id' IS NOT NULL
                          AND UPPER(
                              COALESCE(
                                  l.config_json->>'model_type',
                                  SPLIT_PART(l.config_json->>'model_id', '_', 1)
                              )
                          ) = %s
                        ORDER BY (l.metrics_json->>'IC')::float DESC NULLS LAST
                        LIMIT %s
                        """,
                        (self.task_id, self.model_type, limit),
                    )
                    rows = cur.fetchall()

            injected = 0
            for row in rows:
                try:
                    config = row.get("config_json") or {}
                    if isinstance(config, str):
                        config = json.loads(config)
                    metrics = row.get("metrics_json") or {}
                    if isinstance(metrics, str):
                        metrics = json.loads(metrics)

                    model_params = config.get("model_params", {})
                    ic_value = metrics.get("IC")
                    if ic_value is None or not model_params:
                        continue

                    distributions, params = self._build_trial_params(model_params)
                    if not params:
                        continue

                    trial = optuna.trial.create_trial(
                        params=params,
                        distributions=distributions,
                        values=[float(ic_value)],
                        state=TrialState.COMPLETE,
                        user_attrs={
                            "source_task_id": row["source_task_id"],
                        },
                    )
                    study.add_trial(trial)
                    injected += 1
                except Exception as e:
                    logger.warning(f"注入跨 task trial 失败: {e}")
                    continue

            logger.info(
                f"model_type={self.model_type} 注入 {injected} 条跨 task trial "
                f"(总可用 {total_count}, limit={limit})"
            )

        except Exception as e:
            logger.error(f"查询跨 task trial 失败: {e}")

    def _build_trial_params(
        self, model_params: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        从历史 model_params 构建 Optuna trial 的 distributions 和 params。

        仅包含 HYPERPARAM_RANGES 中定义的参数，忽略未知参数。

        Returns:
            (distributions_dict, params_dict)
        """
        ranges = HYPERPARAM_RANGES.get(self.model_type, {})
        if not ranges:
            return {}, {}

        distributions = {}
        params = {}

        for param_name, (lo, hi) in ranges.items():
            if param_name not in model_params:
                continue

            value = model_params[param_name]
            try:
                if param_name in INTEGER_PARAMS:
                    int_val = int(value)
                    int_val = max(int(lo), min(int(hi), int_val))
                    distributions[param_name] = IntDistribution(int(lo), int(hi))
                    params[param_name] = int_val
                elif param_name in LOG_SCALE_PARAMS:
                    float_val = float(value)
                    # log scale 需要 lo > 0
                    actual_lo = max(lo, 1e-10)
                    float_val = max(actual_lo, min(float(hi), float_val))
                    distributions[param_name] = FloatDistribution(
                        actual_lo, float(hi), log=True
                    )
                    params[param_name] = float_val
                else:
                    float_val = float(value)
                    float_val = max(float(lo), min(float(hi), float_val))
                    distributions[param_name] = FloatDistribution(
                        float(lo), float(hi)
                    )
                    params[param_name] = float_val
            except (ValueError, TypeError) as e:
                logger.warning(
                    f"参数 {param_name}={value} 转换失败: {e}, 跳过"
                )
                continue

        return distributions, params

    def _define_search_space(self, trial: Any) -> Dict[str, Any]:
        """
        根据 HYPERPARAM_RANGES 为当前 model_type 定义搜索空间。

        整数参数使用 suggest_int，浮点参数使用 suggest_float。
        学习率类参数使用 log=True 进行对数空间搜索。

        Args:
            trial: optuna.Trial 对象

        Returns:
            建议的超参数字典
        """
        ranges = HYPERPARAM_RANGES.get(self.model_type, {})
        params = {}

        for param_name, (lo, hi) in ranges.items():
            if param_name in INTEGER_PARAMS:
                params[param_name] = trial.suggest_int(
                    param_name, int(lo), int(hi)
                )
            elif param_name in LOG_SCALE_PARAMS:
                # log scale 需要 lo > 0
                actual_lo = max(lo, 1e-10)
                params[param_name] = trial.suggest_float(
                    param_name, actual_lo, float(hi), log=True
                )
            else:
                params[param_name] = trial.suggest_float(
                    param_name, float(lo), float(hi)
                )

        return params

    def ask(self) -> Optional[Tuple[Any, Dict[str, Any]]]:
        """
        使用 Optuna study.ask() 生成一组候选超参数。

        Returns:
            (trial, suggested_params) 或 None（不可用时）
        """
        if not OPTUNA_AVAILABLE:
            logger.warning("optuna 未安装，ask() 不可用")
            return None

        study = self.get_or_create_study()
        if study is None:
            return None

        try:
            trial = study.ask()
            suggested_params = self._define_search_space(trial)

            logger.info(
                f"Optuna ask(): task={self.task_id}, model={self.model_type}, "
                f"trial_number={trial.number}, params={suggested_params}, "
                f"total_trials={len(study.trials)}"
            )

            return trial, suggested_params

        except Exception as e:
            logger.error(f"Optuna ask() 失败: {e}")
            return None

    def tell(self, trial: Any, ic_value: float) -> bool:
        """
        将演进结果反馈给 Optuna。

        Args:
            trial: ask() 返回的 trial 对象
            ic_value: 本轮演进的 IC 值

        Returns:
            True 反馈成功, False 反馈失败
        """
        if not OPTUNA_AVAILABLE:
            logger.warning("optuna 未安装，tell() 不可用")
            return False

        study = self.get_or_create_study()
        if study is None:
            return False

        try:
            study.tell(trial, float(ic_value))
            logger.info(
                f"Optuna tell(): task={self.task_id}, model={self.model_type}, "
                f"trial_number={trial.number}, ic_value={ic_value}"
            )
            return True

        except Exception as e:
            logger.error(f"Optuna tell() 失败: {e}")
            return False
