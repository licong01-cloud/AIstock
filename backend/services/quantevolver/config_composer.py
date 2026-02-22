"""
QuantEvolver Phase 2: ConfigComposer（配置组装器）

功能：
1. 生成QLib conf.yaml配置文件
2. 组装因子代码文件（factor.py）
3. 组装策略代码文件（如自定义策略）
4. 生成WSL执行命令
5. 管理实验目录和结果同步
"""
from __future__ import annotations

import json
import logging
import os
import re
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from ...db.pg_pool import get_conn

logger = logging.getLogger("aistock.quantevolver.config_composer")

# ── RDAgent 侧 QE 专用实验工作区（WSL 可直接访问） ──
# Windows路径，用于文件写入
QE_WORKSPACE_WIN = Path(os.getenv(
    "QE_WORKSPACE_WIN",
    "F:/Dev/RD-Agent-main/qe_workspace"
))
# WSL路径，用于生成执行命令
QE_WORKSPACE_WSL = os.getenv(
    "QE_WORKSPACE_WSL",
    "/mnt/f/Dev/RD-Agent-main/qe_workspace"
)

# 旧的AIstock侧实验目录（保留兼容）
QE_EXPERIMENTS_ROOT = Path(os.getenv(
    "QE_EXPERIMENTS_ROOT",
    "f:/Dev/AIstock/rdagent_assets/qe_experiments"
))

# ── RDAgent 因子数据目录（包含 daily_pv.h5, static_factors.parquet 等） ──
RDAGENT_FACTOR_DATA_WSL = os.getenv(
    "RDAGENT_FACTOR_DATA_WSL",
    "/mnt/f/Dev/RD-Agent-main/git_ignore_folder/factor_implementation_source_data"
)

# ── QLib 数据路径（与 RDAgent conf_baseline.yaml 一致） ──
QLIB_DATA_PATH_WSL = os.getenv(
    "QLIB_DATA_PATH_WSL",
    "/mnt/f/Dev/AIstock/qlib_bin/qlib_bin_20251209"
)

# ── RDAgent 默认数据时间段（与 conf_baseline.yaml 一致，基于 2018-08-01~2025-12-01 数据集） ──
RDAGENT_DEFAULT_DATA_SPLIT = {
    "train_start": "2018-08-01",
    "train_end": "2022-12-31",
    "valid_start": "2023-01-01",
    "valid_end": "2024-06-30",
    "test_start": "2024-07-01",
    "test_end": "2025-12-01",
    # 回测截止日比数据截止日提前几天，避免 Qlib calendar_index+1 越界
    "backtest_end": "2025-11-28",
}

# ── RDAgent 默认 LGBModel 超参数（与 conf_baseline.yaml 一致） ──
RDAGENT_DEFAULT_LGB_KWARGS = {
    "loss": "mse",
    "device_type": "cpu",
    "max_bin": 63,
    "colsample_bytree": 0.8879,
    "learning_rate": 0.2,
    "subsample": 0.8789,
    "lambda_l1": 205.6999,
    "lambda_l2": 580.9768,
    "max_depth": 8,
    "num_leaves": 210,
    "num_threads": 20,
}


class ConfigComposer:
    """配置组装器。"""

    def compose_experiment(
        self,
        factor_names: List[str],
        factor_sources: Optional[Dict[str, str]] = None,
        model_id: Optional[str] = None,
        strategy_id: Optional[str] = None,
        data_split: Optional[Dict[str, str]] = None,
        custom_params: Optional[Dict[str, Any]] = None,
        experiment_name: Optional[str] = None,
        # ── 演进支持：生成目标 ──
        evolution_goal: Optional[str] = None,
        # evolution_goal: 本轮实验的目标描述
        # - AI生成时：用户输入的目标
        # - 手工配置时：可为空，由LLM根据上一轮结果自主发挥
        # ── 演进支持：LLM假设参数 ──
        llm_hypothesis: Optional[Dict[str, Any]] = None,
        # llm_hypothesis 结构:
        # {
        #   "hypothesis": "假设描述",
        #   "source": "llm_generate" | "manual",
        #   "reasoning_process": "LLM分析过程（可选）"
        # }
    ) -> Dict[str, Any]:
        """组装完整实验配置。

        实验文件直接写入 RDAgent 侧的 qe_workspace 目录，
        WSL 环境可直接 cd 到该目录执行 qrun。

        Returns:
            包含conf.yaml内容、factor.py内容、WSL命令、实验目录等信息
        """
        experiment_id = str(uuid.uuid4())[:8]
        if not experiment_name:
            experiment_name = f"qe_exp_{experiment_id}"

        # 创建实验目录（在 AIstock 侧本地保存一份）
        exp_dir = QE_EXPERIMENTS_ROOT / experiment_name
        exp_dir.mkdir(parents=True, exist_ok=True)

        # 通过API在RDAgent侧创建实验工作区并链接数据文件
        self._api_setup_experiment_workspace(experiment_name)

        # 默认数据划分（与 RDAgent conf_baseline.yaml 一致）
        if not data_split:
            data_split = dict(RDAGENT_DEFAULT_DATA_SPLIT)

        # 获取因子信息
        factors_info = self._get_factors_info(factor_names, factor_sources)

        # 获取模型信息
        model_info = self._get_model_info(model_id) if model_id else None

        # 获取策略信息
        strategy_info = self._get_strategy_info(strategy_id) if strategy_id else None

        # 判断因子类型：是否包含RDAgent自定义因子
        has_custom_factors = any(
            f.get("source") == "rdagent_task_sync" for f in factors_info
        )
        has_alpha158 = any(
            f.get("source") == "alpha158" for f in factors_info
        )
        has_alpha360 = any(
            f.get("source") == "alpha360" for f in factors_info
        )

        # 判断模型类型（用于WSL命令环境变量）
        model_type_tag = None
        if model_info and model_info.get("code_text"):
            model_type_raw = model_info.get("model_type") or ""
            if model_type_raw in ("TimeSeries", "timeseries"):
                model_type_tag = "TimeSeries"
            elif model_type_raw in ("Tabular", "tabular"):
                model_type_tag = "Tabular"
            else:
                name_lower = (model_info.get("model_name") or "").lower()
                if any(k in name_lower for k in ["transformer", "lstm", "gru", "rnn", "timeseries", "temporal"]):
                    model_type_tag = "TimeSeries"
                else:
                    model_type_tag = "Tabular"

        # 从custom_params提取disable_alpha158和quick_train参数
        disable_alpha158 = False
        quick_train = False  # 快速训练模式：训练时间缩短到20%
        if custom_params:
            disable_alpha158 = custom_params.get("disable_alpha158", False)
            quick_train = custom_params.get("quick_train", False)

        # 生成conf.yaml
        conf_yaml = self._compose_conf_yaml(
            factors_info=factors_info,
            model_info=model_info,
            strategy_info=strategy_info,
            data_split=data_split,
            custom_params=custom_params,
            has_custom_factors=has_custom_factors,
            has_alpha158=has_alpha158,
            has_alpha360=has_alpha360,
            disable_alpha158=disable_alpha158,
            quick_train=quick_train,
        )

        # 生成因子文件和预处理脚本（如果有自定义因子）
        has_factor_files = False
        prepare_factors_py = None
        if has_custom_factors:
            factor_marker = self._compose_factor_file(factors_info)
            has_factor_files = factor_marker is not None
            prepare_factors_py = self._compose_prepare_factors(factors_info)

        # 保存文件
        conf_path = exp_dir / "conf.yaml"
        conf_path.write_text(conf_yaml, encoding="utf-8")

        # 写入因子原始代码到 factors/ 子目录（保持原始格式不变）
        if has_factor_files:
            self._write_factor_files(exp_dir, factors_info)

        if prepare_factors_py:
            prepare_path = exp_dir / "prepare_factors.py"
            prepare_path.write_text(prepare_factors_py, encoding="utf-8")

        # 如果使用 DynamicFactorsOnlyLoader，复制 QE 独立的 loader 文件到实验目录
        if has_custom_factors and disable_alpha158:
            self._copy_qe_custom_loaders(exp_dir)

        # 复制read_exp_res.py模板
        self._copy_read_exp_res(exp_dir)

        # 如果模型使用自定义源码，写入实验目录（model.py + model_cls导出）
        if model_info and model_info.get("code_text"):
            self._write_custom_model(exp_dir, model_info)

        # 如果策略使用自定义源码，复制到实验目录
        if strategy_info and strategy_info.get("source_code"):
            self._write_custom_strategy(exp_dir, strategy_info)

        # 通过API同步文件到RDAgent侧（支持独立部署场景）
        self._api_sync_experiment_files(experiment_name, exp_dir)

        # 生成WSL命令（直接使用 qe_workspace WSL路径）
        wsl_path = f"{QE_WORKSPACE_WSL}/{experiment_name}"
        wsl_command = self._generate_wsl_command(
            wsl_path, has_custom_factors=has_custom_factors,
            use_custom_model=has_custom_factors and model_info and bool(model_info.get("code_text")),
            model_type_tag=model_type_tag if model_info and model_info.get("code_text") else None,
        )

        # 保存实验记录到数据库
        self._save_experiment_record(
            experiment_id=experiment_id,
            experiment_name=experiment_name,
            exp_dir=str(exp_dir),
            factor_names=factor_names,
            model_id=model_id,
            strategy_id=strategy_id,
            data_split=data_split,
            custom_params=custom_params,
            evolution_goal=evolution_goal,
            llm_hypothesis=llm_hypothesis,
        )

        # 初始化轨迹文件（存储LLM假设）
        self._init_trace_file(
            exp_dir=exp_dir,
            experiment_id=experiment_id,
            experiment_name=experiment_name,
            factor_names=factor_names,
            model_id=model_id,
            strategy_id=strategy_id,
            custom_params=custom_params,
            llm_hypothesis=llm_hypothesis,
        )

        return {
            "ok": True,
            "experiment_id": experiment_id,
            "experiment_name": experiment_name,
            "experiment_dir": str(exp_dir),
            "conf_yaml_path": str(conf_path),
            "factors_dir": str(exp_dir / "factors") if has_factor_files else None,
            "prepare_factors_py_path": str(exp_dir / "prepare_factors.py") if prepare_factors_py else None,
            "wsl_path": wsl_path,
            "wsl_command": wsl_command,
            "conf_yaml_preview": conf_yaml[:2000],
            "factor_count": len(factor_names),
            "has_custom_factors": has_custom_factors,
            "data_split": data_split,
            "llm_hypothesis": llm_hypothesis,
        }

    def sync_experiment_results(self, experiment_id: str) -> Dict[str, Any]:
        """同步实验结果。

        从实验目录读取QLib执行结果文件。
        """
        # 获取实验记录
        exp_record = self._get_experiment_record(experiment_id)
        if not exp_record:
            return {"ok": False, "error": f"实验 {experiment_id} 不存在"}

        exp_dir = Path(exp_record.get("experiment_dir", ""))
        if not exp_dir.exists():
            return {"ok": False, "error": f"实验目录不存在: {exp_dir}"}

        results = {}

        # 读取qlib_res.csv
        qlib_res_path = exp_dir / "qlib_res.csv"
        if qlib_res_path.exists():
            try:
                import csv
                with open(qlib_res_path, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    results["qlib_res"] = list(reader)
            except Exception as e:
                results["qlib_res_error"] = str(e)

        # 读取回测指标
        metrics = self._extract_metrics_from_results(exp_dir)
        results["metrics"] = metrics

        # 更新数据库
        if metrics:
            self._update_experiment_metrics(experiment_id, metrics)
            # 为每个参与因子保存实验表现指标
            try:
                self._save_factor_experiment_metrics(experiment_id, metrics)
            except Exception as e:
                logger.error(f"保存因子实验指标失败: {e}")
                results["factor_metrics_error"] = str(e)

        results["ok"] = True
        results["experiment_id"] = experiment_id
        return results

    def list_experiments(
        self,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """获取实验列表。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM qe_experiments")
                total = cur.fetchone()[0]

                cur.execute("""
                    SELECT experiment_id, experiment_name, status,
                           factor_names, model_id, strategy_id,
                           workspace_path, wsl_command,
                           result_metrics, created_at, updated_at
                    FROM qe_experiments
                    ORDER BY created_at DESC
                    LIMIT %s OFFSET %s
                """, (limit, offset))
                cols = [desc[0] for desc in cur.description]
                rows = [dict(zip(cols, row)) for row in cur.fetchall()]

        return {"ok": True, "total": total, "items": rows}

    def get_experiment_detail(self, experiment_id: str) -> Dict[str, Any]:
        """获取实验详情。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM qe_experiments WHERE experiment_id = %s
                """, (experiment_id,))
                row = cur.fetchone()
                if not row:
                    return {"ok": False, "error": "实验不存在"}
                cols = [desc[0] for desc in cur.description]
                return {"ok": True, "experiment": dict(zip(cols, row))}

    def regenerate_experiment(self, experiment_id: str) -> Dict[str, Any]:
        """重新生成实验脚本（复用同一实验ID和名称）。

        从数据库读取实验的因子、模型、策略等信息，
        重新生成所有实验文件（conf.yaml, prepare_factors.py, read_exp_res.py等），
        但保留原有的实验ID和名称。
        """
        # 获取实验记录
        exp_record = self._get_experiment_record(experiment_id)
        if not exp_record:
            return {"ok": False, "error": f"实验 {experiment_id} 不存在"}

        factor_names = exp_record.get("factor_names") or []
        model_id = exp_record.get("model_id")
        strategy_id = exp_record.get("strategy_id")
        data_split = exp_record.get("data_split")
        custom_params = exp_record.get("custom_params")
        experiment_name = exp_record.get("experiment_name")

        if not factor_names:
            return {"ok": False, "error": "实验没有关联因子"}

        # 解析data_split和custom_params（可能是JSON字符串）
        if isinstance(data_split, str):
            try:
                import json as _json
                data_split = _json.loads(data_split)
            except Exception:
                data_split = None
        if isinstance(custom_params, str):
            try:
                import json as _json
                custom_params = _json.loads(custom_params)
            except Exception:
                custom_params = None

        # 创建实验目录 (本地)
        exp_dir = QE_EXPERIMENTS_ROOT / experiment_name
        exp_dir.mkdir(parents=True, exist_ok=True)

        # 默认数据划分
        if not data_split:
            data_split = dict(RDAGENT_DEFAULT_DATA_SPLIT)

        # 获取因子信息
        factors_info = self._get_factors_info(factor_names)

        # 获取模型信息
        model_info = self._get_model_info(model_id) if model_id else None

        # 获取策略信息
        strategy_info = self._get_strategy_info(strategy_id) if strategy_id else None

        # 判断因子类型
        has_custom_factors = any(f.get("source") == "rdagent_task_sync" for f in factors_info)
        has_alpha158 = any(f.get("source") == "alpha158" for f in factors_info)
        has_alpha360 = any(f.get("source") == "alpha360" for f in factors_info)

        # 判断模型类型
        model_type_tag = None
        if model_info and model_info.get("code_text"):
            model_type_raw = model_info.get("model_type") or ""
            if model_type_raw in ("TimeSeries", "timeseries"):
                model_type_tag = "TimeSeries"
            elif model_type_raw in ("Tabular", "tabular"):
                model_type_tag = "Tabular"
            else:
                name_lower = (model_info.get("model_name") or "").lower()
                if any(k in name_lower for k in ["transformer", "lstm", "gru", "rnn", "timeseries", "temporal"]):
                    model_type_tag = "TimeSeries"
                else:
                    model_type_tag = "Tabular"

        # 从custom_params提取disable_alpha158和quick_train参数
        disable_alpha158 = False
        quick_train = False  # 快速训练模式：训练时间缩短到20%
        if custom_params:
            disable_alpha158 = custom_params.get("disable_alpha158", False)
            quick_train = custom_params.get("quick_train", False)

        # 生成conf.yaml
        conf_yaml = self._compose_conf_yaml(
            factors_info=factors_info,
            model_info=model_info,
            strategy_info=strategy_info,
            data_split=data_split,
            custom_params=custom_params,
            has_custom_factors=has_custom_factors,
            has_alpha158=has_alpha158,
            has_alpha360=has_alpha360,
            disable_alpha158=disable_alpha158,
            quick_train=quick_train,
        )

        # 生成因子文件和预处理脚本
        has_factor_files = False
        prepare_factors_py = None
        if has_custom_factors:
            factor_marker = self._compose_factor_file(factors_info)
            has_factor_files = factor_marker is not None
            prepare_factors_py = self._compose_prepare_factors(factors_info)

        # 保存文件
        conf_path = exp_dir / "conf.yaml"
        conf_path.write_text(conf_yaml, encoding="utf-8")

        if has_factor_files:
            self._write_factor_files(exp_dir, factors_info)

        if prepare_factors_py:
            prepare_path = exp_dir / "prepare_factors.py"
            prepare_path.write_text(prepare_factors_py, encoding="utf-8")

        # 复制read_exp_res.py模板
        self._copy_read_exp_res(exp_dir)

        # 如果模型使用自定义源码
        if model_info and model_info.get("code_text"):
            self._write_custom_model(exp_dir, model_info)

        # 如果策略使用自定义源码
        if strategy_info and strategy_info.get("source_code"):
            self._write_custom_strategy(exp_dir, strategy_info)

        # 同步文件到RDAgent侧
        self._api_sync_experiment_files(experiment_name, exp_dir)

        # 生成WSL命令
        wsl_path = f"{QE_WORKSPACE_WSL}/{experiment_name}"
        wsl_command = self._generate_wsl_command(
            wsl_path, has_custom_factors=has_custom_factors,
            use_custom_model=has_custom_factors and model_info and bool(model_info.get("code_text")),
            model_type_tag=model_type_tag if model_info and model_info.get("code_text") else None,
        )

        # 更新数据库中的WSL命令和状态
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE qe_experiments
                    SET wsl_command = %s, status = 'created', updated_at = NOW()
                    WHERE experiment_id = %s
                """, (wsl_command, experiment_id))
            conn.commit()

        return {
            "ok": True,
            "experiment_id": experiment_id,
            "experiment_name": experiment_name,
            "experiment_dir": str(exp_dir),
            "wsl_command": wsl_command,
            "factor_count": len(factor_names),
            "has_custom_factors": has_custom_factors,
            "message": "实验脚本已重新生成",
        }

    # ---- conf.yaml 生成 ----

    def _compose_conf_yaml(
        self,
        factors_info: List[Dict],
        model_info: Optional[Dict],
        strategy_info: Optional[Dict],
        data_split: Dict[str, str],
        custom_params: Optional[Dict],
        has_custom_factors: bool,
        has_alpha158: bool,
        has_alpha360: bool = False,
        disable_alpha158: bool = False,
        quick_train: bool = False,  # 快速训练模式：训练时间缩短到20%
    ) -> str:
        """生成QLib conf.yaml内容。

        结构与 RDAgent conf_baseline.yaml 完全一致，确保在 WSL 环境中
        使用相同的数据集、模型超参数、策略配置执行实验。
        """
        # ── 模型配置 ──
        # 与 RDAgent model_runner.py 完全一致的模型选择逻辑：
        # - 有源代码的自定义模型 → GeneralPTNN + pt_model_uri: "model.model_cls"
        # - LGB/LGBM/GBDT → LGBModel
        # - 无源代码的NN → GeneralPTNN（需要用户提供pt_model_uri）
        model_class = "LGBModel"
        model_module = "qlib.contrib.model.gbdt"
        model_kwargs = dict(RDAGENT_DEFAULT_LGB_KWARGS)  # 默认使用RDAgent超参
        use_custom_model = False  # 是否使用自定义模型源代码（GeneralPTNN + model.model_cls）
        model_type_tag = None  # "TimeSeries" | "Tabular" | None

        if model_info:
            model_name = model_info.get("model_name", "")
            model_type = (model_info.get("model_type") or "").upper()
            code_text = model_info.get("code_text")

            if code_text:
                # RDAgent自定义模型：使用 GeneralPTNN + pt_model_uri: "model.model_cls"
                # 与 RDAgent conf_sota_factors_model.yaml 完全一致
                use_custom_model = True
                model_class = "GeneralPTNN"
                model_module = "qlib.contrib.model.pytorch_general_nn"

                # 训练超参数
                thp = model_info.get("model_training_hyperparameters") or {}
                if isinstance(thp, str):
                    thp = json.loads(thp)

                model_kwargs = {
                    "n_epochs": thp.get("n_epochs", 30),
                    "lr": thp.get("lr", "1e-3"),
                    "early_stop": thp.get("early_stop", 5),
                    "batch_size": thp.get("batch_size", 256),
                    "weight_decay": thp.get("weight_decay", "1e-4"),
                    "metric": "loss",
                    "loss": "mse",
                    "n_jobs": 20,
                    "GPU": 0,
                    "pt_model_uri": '"model.model_cls"',
                }

                # 快速训练模式：训练时间缩短到20%
                if quick_train:
                    n_epochs_val = int(model_kwargs["n_epochs"]) if isinstance(model_kwargs["n_epochs"], str) else model_kwargs["n_epochs"]
                    early_stop_val = int(model_kwargs["early_stop"]) if isinstance(model_kwargs["early_stop"], str) else model_kwargs["early_stop"]
                    batch_size_val = int(model_kwargs["batch_size"]) if isinstance(model_kwargs["batch_size"], str) else model_kwargs["batch_size"]
                    model_kwargs["n_epochs"] = max(3, n_epochs_val // 5)
                    model_kwargs["early_stop"] = max(1, early_stop_val // 3)
                    model_kwargs["batch_size"] = batch_size_val * 2  # 增大batch加速

                # 判断模型类型（TimeSeries vs Tabular）
                model_type_raw = model_info.get("model_type") or ""
                if model_type_raw in ("TimeSeries", "timeseries"):
                    model_type_tag = "TimeSeries"
                elif model_type_raw in ("Tabular", "tabular"):
                    model_type_tag = "Tabular"
                else:
                    # 从模型名称推断
                    name_lower = model_name.lower()
                    if any(k in name_lower for k in ["transformer", "lstm", "gru", "rnn", "timeseries", "temporal"]):
                        model_type_tag = "TimeSeries"
                    else:
                        model_type_tag = "Tabular"

            elif "LGB" in model_type or "LGBM" in model_type or "GBDT" in model_type:
                model_class = "LGBModel"
                model_module = "qlib.contrib.model.gbdt"
                hp = model_info.get("model_hyperparameters")
                if hp:
                    if isinstance(hp, str):
                        hp = json.loads(hp)
                    model_kwargs.update(hp)
            elif "PTNN" in model_type or "NN" in model_type:
                model_class = "GeneralPTNN"
                model_module = "qlib.contrib.model.pytorch_general_nn"
                model_kwargs = {}
                hp = model_info.get("model_hyperparameters")
                if hp:
                    if isinstance(hp, str):
                        hp = json.loads(hp)
                    model_kwargs.update(hp)
            else:
                # 未知模型类型但无源代码，回退LGBModel
                logger.warning(f"未知模型类型 '{model_type}'，无源代码，回退到LGBModel")

        # ── 策略配置 ──
        # 默认值：仅在用户未选择策略时使用 qlib 内置 TopkDropoutStrategy
        strategy_class = "TopkDropoutStrategy"
        strategy_module = "qlib.contrib.strategy.signal_strategy"
        strategy_kwargs = {
            "signal": "<PRED>",
            "topk": 50,
            "n_drop": 5,
            "method_buy": "top",
            "hold_thresh": 1,
            "method_sell": "bottom",
            "risk_degree": 0.95,
            "only_tradable": False,
            "forbid_all_trade_at_limit": True,
        }
        if strategy_info:
            # 用户选择了策略 → 必须使用该策略的源代码
            source_code = strategy_info.get("source_code")
            if not source_code:
                raise ValueError(
                    f"策略 '{strategy_info.get('strategy_id', '?')}' 没有源代码，"
                    f"无法生成实验配置。请先在策略管理页面编辑并保存策略源代码。"
                )
            # 有源代码 → module_path 指向本地 custom_strategy
            strategy_module = "custom_strategy"

            # 从源代码中提取策略类名（确保与实际定义匹配）
            import re
            class_match = re.search(r'class\s+(\w+)\s*\(', source_code)
            if class_match:
                extracted_class = class_match.group(1)
                logger.info(f"从源代码提取策略类名: {extracted_class}")

            pc = strategy_info.get("portfolio_config")
            if pc:
                if isinstance(pc, str):
                    pc = json.loads(pc)
                # 优先使用portfolio_config中的class，否则使用从源码提取的类名
                config_class = pc.get("class")
                if config_class:
                    strategy_class = config_class
                elif class_match:
                    strategy_class = extracted_class
                sk = pc.get("kwargs", {})
                strategy_kwargs.update(sk)
            elif class_match:
                # 没有portfolio_config，使用源码提取的类名
                strategy_class = extracted_class

            # 使用 default_kwargs 覆盖（UI 编辑的参数）
            dk = strategy_info.get("default_kwargs")
            if dk:
                if isinstance(dk, str):
                    dk = json.loads(dk)
                for k, v in dk.items():
                    strategy_kwargs[k] = v

        if custom_params:
            # 过滤掉非策略参数（如 disable_alpha158 是数据加载器配置，不是策略参数）
            _NON_STRATEGY_PARAMS = {
                "disable_alpha158", "disable_alpha360", "use_custom_model",
                "model_type", "dataset_cls", "step_len", "num_timesteps", "num_features",
                "quick_train",  # 快速训练模式：控制模型训练参数
            }
            filtered_params = {k: v for k, v in custom_params.items() if k not in _NON_STRATEGY_PARAMS}
            if set(custom_params.keys()) - set(filtered_params.keys()):
                logger.info(f"策略参数过滤: 移除非策略参数 {set(custom_params.keys()) - set(filtered_params.keys())}")
            strategy_kwargs.update(filtered_params)

        # 确保 signal 始终为 <PRED>
        strategy_kwargs["signal"] = "<PRED>"

        # 安全过滤：只保留策略支持的参数
        # 避免不支持的参数通过 **kwargs 传递到 BaseStrategy 导致 TypeError
        # TopkDropoutStrategy 支持的参数
        _TOPK_DROPOUT_ALLOWED_KEYS = {
            "signal", "topk", "n_drop", "method_sell", "method_buy",
            "hold_thresh", "only_tradable", "forbid_all_trade_at_limit",
            "risk_degree",
        }
        # EnhancedTopkDropoutStrategy 支持的额外参数
        _ENHANCED_TOPK_ALLOWED_KEYS = _TOPK_DROPOUT_ALLOWED_KEYS | {
            "min_score", "max_position_ratio", "stop_loss", "max_market_cap",
        }
        
        if strategy_class == "TopkDropoutStrategy":
            _removed = {k for k in strategy_kwargs if k not in _TOPK_DROPOUT_ALLOWED_KEYS}
            if _removed:
                logger.warning(f"策略参数安全过滤: 移除不支持的参数 {_removed}")
                for k in _removed:
                    del strategy_kwargs[k]
        elif strategy_class == "EnhancedTopkDropoutStrategy":
            _removed = {k for k in strategy_kwargs if k not in _ENHANCED_TOPK_ALLOWED_KEYS}
            if _removed:
                logger.warning(f"策略参数安全过滤: 移除不支持的参数 {_removed}")
                for k in _removed:
                    del strategy_kwargs[k]
        else:
            # 其他自定义策略：过滤掉已知的非策略参数（更宽松的过滤）
            _removed = {k for k in strategy_kwargs if k in _NON_STRATEGY_PARAMS}
            if _removed:
                logger.warning(f"策略参数安全过滤: 移除非策略参数 {_removed}")
                for k in _removed:
                    del strategy_kwargs[k]

        # 回测截止日（避免越界）
        backtest_end = data_split.get("backtest_end", "2025-11-28")

        # ── 生成 YAML（与 RDAgent conf_baseline.yaml 结构一致） ──
        lines = []
        lines.append("qlib_init:")
        lines.append(f'    provider_uri: "{QLIB_DATA_PATH_WSL}"')
        lines.append("    region: cn")
        lines.append("")
        lines.append("market: &market all")
        lines.append("benchmark: &benchmark 000300.SH")
        lines.append("")

        # data_handler_config
        lines.append("data_handler_config: &data_handler_config")
        lines.append(f"    start_time: {data_split['train_start']}")
        lines.append(f"    end_time: {data_split['test_end']}")
        # fit_start_time/fit_end_time 只有 Alpha158 接受，DataHandlerLP 不接受
        if not has_custom_factors:
            lines.append(f"    fit_start_time: {data_split['train_start']}")
            lines.append(f"    fit_end_time: {data_split['valid_end']}")
        lines.append("    instruments: *market")

        if has_custom_factors and not disable_alpha158:
            # 自定义因子：使用 NestedDataLoader（Alpha158DL + StaticDataLoader）
            # 与 RDAgent conf_sota_factors_model.yaml 完全一致
            # 需要先执行 prepare_factors.py 生成 combined_factors_df.parquet
            lines.append("    data_loader:")
            lines.append("        class: NestedDataLoader")
            lines.append("        kwargs:")
            lines.append("            dataloader_l:")
            lines.append("                - class: qlib.contrib.data.loader.Alpha158DL")
            lines.append("                  kwargs:")
            lines.append("                    config:")
            lines.append("                        label: ")
            lines.append('                            - ["Ref($close, -2)/Ref($close, -1) - 1"]')
            lines.append('                            - ["LABEL0"]')
            lines.append("                        feature:")
            lines.append('                            - ["Resi($close, 5)/$close", "Std(Abs($close/Ref($close, 1)-1)*$volume, 5)/(Mean(Abs($close/Ref($close, 1)-1)*$volume, 5)+1e-12)",')
            lines.append('                               "Rsquare($close, 5)", "($high-$low)/$open", "Rsquare($close, 10)", "Corr($close, Log($volume+1), 5)",')
            lines.append('                               "Corr($close/Ref($close,1), Log($volume/Ref($volume, 1)+1), 5)", "Corr($close, Log($volume+1), 10)",')
            lines.append('                               "Ref($close, 60)/$close", "Resi($close, 10)/$close", "Std($volume, 5)/($volume+1e-12)",')
            lines.append('                               "Rsquare($close, 60)", "Corr($close, Log($volume+1), 60)", "Std(Abs($close/Ref($close, 1)-1)*$volume, 60)/(Mean(Abs($close/Ref($close, 1)-1)*$volume, 60)+1e-12)",')
            lines.append('                               "Std($close, 5)/$close", "Rsquare($close, 20)", "Corr($close/Ref($close,1), Log($volume/Ref($volume, 1)+1), 60)",')
            lines.append('                               "Corr($close/Ref($close,1), Log($volume/Ref($volume, 1)+1), 10)", "Corr($close, Log($volume+1), 20)",')
            lines.append('                               "(Less($open, $close)-$low)/$open"]')
            lines.append('                            - ["RESI5", "WVMA5", "RSQR5", "KLEN", "RSQR10", "CORR5", "CORD5", "CORR10", ')
            lines.append('                               "ROC60", "RESI10", "VSTD5", "RSQR60", "CORR60", "WVMA60", "STD5", ')
            lines.append('                               "RSQR20", "CORD60", "CORD10", "CORR20", "KLOW"]')
            lines.append("                - class: qlib.data.dataset.loader.StaticDataLoader")
            lines.append("                  kwargs:")
            lines.append('                    config: "combined_factors_df.parquet"')
            lines.append("    infer_processors:")
            lines.append("        - class: RobustZScoreNorm")
            lines.append("          kwargs:")
            lines.append("              fields_group: feature")
            lines.append("              clip_outlier: true")
            lines.append(f"              fit_start_time: {data_split['train_start']}")
            lines.append(f"              fit_end_time: {data_split['valid_end']}")
            lines.append("        - class: Fillna")
            lines.append("          kwargs:")
            lines.append("              fields_group: feature")
            lines.append("    learn_processors:")
            lines.append("        - class: DropnaLabel")
            lines.append("        - class: CSZScoreNorm")
            lines.append("          kwargs:")
            lines.append("              fields_group: label")
        elif has_custom_factors and disable_alpha158:
            # 自定义因子 + 禁用Alpha158基线：使用 DynamicFactorsOnlyLoader
            # 注意：使用实验目录中的 qe_custom_loaders（QE独立版本），不影响RDAgent
            # DynamicFactorsOnlyLoader 会忽略 instruments 参数，直接加载 parquet 中所有数据
            # 同时从 QLib provider 加载 label 数据，确保包含 feature 和 label 列
            lines.append("    data_loader:")
            lines.append("        class: DynamicFactorsOnlyLoader")
            lines.append("        module_path: qe_custom_loaders")
            lines.append("        kwargs:")
            lines.append('            dynamic_path: "combined_factors_df.parquet"')
            lines.append("    infer_processors:")
            lines.append("        - class: RobustZScoreNorm")
            lines.append("          kwargs:")
            lines.append("              fields_group: feature")
            lines.append("              clip_outlier: true")
            lines.append(f"              fit_start_time: {data_split['train_start']}")
            lines.append(f"              fit_end_time: {data_split['valid_end']}")
            lines.append("        - class: Fillna")
            lines.append("          kwargs:")
            lines.append("              fields_group: feature")
            lines.append("    learn_processors:")
            lines.append("        - class: DropnaLabel")
            lines.append("        - class: CSZScoreNorm")
            lines.append("          kwargs:")
            lines.append("              fields_group: label")
        else:
            # Alpha158 标准处理器（与 RDAgent conf_baseline.yaml 一致）
            lines.append("    infer_processors:")
            lines.append("        - class: FilterCol")
            lines.append("          kwargs:")
            lines.append("              fields_group: feature")
            lines.append('              col_list: ["RESI5", "WVMA5", "RSQR5", "KLEN", "RSQR10", "CORR5", "CORD5", "CORR10",')
            lines.append('                            "ROC60", "RESI10", "VSTD5", "RSQR60", "CORR60", "WVMA60", "STD5",')
            lines.append('                            "RSQR20", "CORD60", "CORD10", "CORR20", "KLOW"]')
            lines.append("        - class: RobustZScoreNorm")
            lines.append("          kwargs:")
            lines.append("              fields_group: feature")
            lines.append("              clip_outlier: true")
            lines.append("        - class: Fillna")
            lines.append("          kwargs:")
            lines.append("              fields_group: feature")
            lines.append("    learn_processors:")
            lines.append("        - class: DropnaLabel")
            lines.append("        - class: CSRankNorm")
            lines.append("          kwargs:")
            lines.append("              fields_group: label")
            lines.append('    label: ["Ref($close, -2) / Ref($close, -1) - 1"]')

        lines.append("")

        # port_analysis_config
        lines.append("port_analysis_config: &port_analysis_config")
        lines.append("    strategy:")
        lines.append(f"        class: {strategy_class}")
        lines.append(f"        module_path: {strategy_module}")
        lines.append("        kwargs:")
        for k, v in strategy_kwargs.items():
            if k == "signal":
                lines.append(f"            {k}: <PRED>")
            elif isinstance(v, bool):
                lines.append(f"            {k}: {'true' if v else 'false'}")
            elif isinstance(v, str):
                lines.append(f"            {k}: {v}")
            else:
                lines.append(f"            {k}: {v}")
        lines.append("    backtest:")
        lines.append(f"        start_time: {data_split['test_start']}")
        lines.append(f"        end_time: {backtest_end}")
        lines.append("        account: 100000000")
        lines.append("        benchmark: *benchmark")
        lines.append("        exchange_kwargs:")
        lines.append("            limit_threshold: 0.095")
        lines.append("            deal_price: close")
        lines.append("            open_cost: 0.0005")
        lines.append("            close_cost: 0.0015")
        lines.append("            min_cost: 5")

        # task（模型 + 数据集）
        lines.append("task:")
        lines.append("    model:")
        lines.append(f"        class: {model_class}")
        lines.append(f"        module_path: {model_module}")
        if model_kwargs:
            lines.append("        kwargs:")
            for k, v in model_kwargs.items():
                # pt_model_kwargs 使用Jinja2模板变量，与RDAgent一致
                if k == "pt_model_uri":
                    lines.append(f"            {k}: {v}")
                else:
                    lines.append(f"            {k}: {v}")
            # 自定义模型需要 pt_model_kwargs（num_features 等）
            if use_custom_model:
                lines.append('            pt_model_kwargs: {')
                lines.append('                "num_features": {{ num_features }}')
                if model_type_tag == "TimeSeries":
                    lines.append('                {% if num_timesteps %}, "num_timesteps": {{ num_timesteps }}{% endif %}')
                lines.append('            }')

        # 数据集配置
        if use_custom_model:
            # 自定义模型：dataset_cls 通过Jinja2模板变量控制（TSDatasetH/DatasetH）
            # 与 RDAgent conf_sota_factors_model.yaml 完全一致
            lines.append("    dataset:")
            lines.append('        class: {{ dataset_cls | default("DatasetH") }}')
            lines.append("        module_path: qlib.data.dataset")
        else:
            lines.append("    dataset:")
            lines.append("        class: DatasetH")
            lines.append("        module_path: qlib.data.dataset")
        lines.append("        kwargs:")
        lines.append("            handler:")
        if has_custom_factors:
            lines.append("                class: DataHandlerLP")
            lines.append("                module_path: qlib.contrib.data.handler")
            lines.append("                kwargs: *data_handler_config")
        else:
            lines.append("                class: Alpha158")
            lines.append("                module_path: qlib.contrib.data.handler")
            lines.append("                kwargs: *data_handler_config")
        lines.append("            segments:")
        lines.append(f"                train: [{data_split['train_start']}, {data_split['train_end']}]")
        lines.append(f"                valid: [{data_split['valid_start']}, {data_split['valid_end']}]")
        lines.append(f"                test: [{data_split['test_start']}, {data_split['test_end']}]")
        if use_custom_model:
            lines.append("            {% if step_len %}step_len: {{ step_len }}{% endif %}")

        # record
        lines.append("    record:")
        lines.append("        - class: SignalRecord")
        lines.append("          module_path: qlib.workflow.record_temp")
        lines.append("          kwargs:")
        lines.append("            model: <MODEL>")
        lines.append("            dataset: <DATASET>")
        lines.append("        - class: SigAnaRecord")
        lines.append("          module_path: qlib.workflow.record_temp")
        lines.append("          kwargs:")
        lines.append("            ana_long_short: False")
        lines.append("            ann_scaler: 252")
        lines.append("        - class: PortAnaRecord")
        lines.append("          module_path: qlib.workflow.record_temp")
        lines.append("          kwargs:")
        lines.append("            config: *port_analysis_config")
        lines.append("")

        return "\n".join(lines)

    def _compose_factor_file(self, factors_info: List[Dict]) -> Optional[str]:
        """保存因子原始代码到 factors/ 子目录。

        与 RDAgent 完全一致：因子代码保持原始格式不做任何修改，
        每个因子作为独立的 factor.py 脚本直接执行。
        因子代码自己负责读取数据（daily_pv.h5, static_factors.parquet 等）
        并将结果写入 result.h5。

        Returns:
            返回一个标记字符串表示有因子文件（实际文件在compose_experiment中写入）
        """
        custom_factors = [f for f in factors_info if f.get("source") == "rdagent_task_sync"]
        if not custom_factors:
            return None

        # 返回标记，实际文件写入在 compose_experiment 中通过 _write_factor_files 完成
        return "__FACTOR_FILES_WRITTEN__"

    def _write_factor_files(self, exp_dir: Path, factors_info: List[Dict]) -> None:
        """将每个因子的原始代码写入 factors/ 子目录。

        与 RDAgent 完全一致：因子代码保持原始格式不做任何修改。
        prepare_factors.py 会从 factors/ 目录读取每个因子代码，
        在独立的临时目录中执行（python factor.py），读取 result.h5。
        """
        custom_factors = [f for f in factors_info if f.get("source") == "rdagent_task_sync"]
        if not custom_factors:
            return

        factors_dir = exp_dir / "factors"
        factors_dir.mkdir(parents=True, exist_ok=True)

        for f in custom_factors:
            code = f.get("code_text")
            if not code:
                continue
            fname = f["factor_name"]
            factor_path = factors_dir / f"{fname}.py"
            factor_path.write_text(code, encoding="utf-8")
            logger.info(f"写入因子文件: {factor_path}")

    def _compose_prepare_factors(self, factors_info: List[Dict]) -> Optional[str]:
        """生成 prepare_factors.py 预处理脚本。

        与 RDAgent FactorFBWorkspace.execute() 完全一致的因子执行方式：
        1. 从 RDAgent 数据目录链接所有数据文件到实验目录（daily_pv.h5, static_factors.parquet,
           moneyflow.h5, daily_basic.h5, bak_basic.h5, cyq_perf.h5, README.md, schemas/ 等）
        2. 每个因子作为独立脚本直接执行（python factor.py），因子代码自己读取数据
        3. 每个因子输出 result.h5，与 RDAgent 因子执行方式完全一致
        4. 合并所有因子结果为 combined_factors_df.parquet（NestedDataLoader StaticDataLoader 所需格式）
        """
        custom_factors = [f for f in factors_info if f.get("source") == "rdagent_task_sync"]
        if not custom_factors:
            return None

        factor_names = [f["factor_name"] for f in custom_factors if f.get("code_text")]

        lines: list[str] = []
        lines.append('"""')
        lines.append("QuantEvolver 因子预处理脚本")
        lines.append("与 RDAgent FactorFBWorkspace.execute() 完全一致的因子执行方式")
        lines.append("在 qrun 之前执行，生成 combined_factors_df.parquet")
        lines.append('"""')
        lines.append("import os")
        lines.append("import sys")
        lines.append("import shutil")
        lines.append("import logging")
        lines.append("import subprocess")
        lines.append("import tempfile")
        lines.append("import textwrap")
        lines.append("import re")
        lines.append("import numpy as np")
        lines.append("import pandas as pd")
        lines.append("")
        lines.append("logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')")
        lines.append("logger = logging.getLogger('prepare_factors')")
        lines.append("")
        lines.append(f"FACTOR_DATA_DIR = os.environ.get('RDAGENT_FACTOR_DATA_DIR', '{RDAGENT_FACTOR_DATA_WSL}')")
        lines.append("")
        lines.append("")
        lines.append("def link_all_files_to_dir(src_dir, dst_dir):")
        lines.append('    """与 RDAgent core/experiment.py link_all_files_in_folder_to_workspace 完全一致。')
        lines.append('    将数据目录中所有文件链接到目标目录，确保因子代码可以读取所有数据。"""')
        lines.append("    src_dir = os.path.abspath(src_dir)")
        lines.append("    for item in os.listdir(src_dir):")
        lines.append("        if item in ('result.h5', 'execution.lock'):")
        lines.append("            continue")
        lines.append("        src_path = os.path.join(src_dir, item)")
        lines.append("        dst_path = os.path.join(dst_dir, item)")
        lines.append("        if not os.path.isfile(src_path):")
        lines.append("            continue")
        lines.append("        if os.path.exists(dst_path) or os.path.islink(dst_path):")
        lines.append("            os.remove(dst_path)")
        lines.append("        try:")
        lines.append("            os.link(src_path, dst_path)")
        lines.append("            continue")
        lines.append("        except OSError:")
        lines.append("            pass")
        lines.append("        try:")
        lines.append("            os.symlink(src_path, dst_path)")
        lines.append("            continue")
        lines.append("        except OSError:")
        lines.append("            pass")
        lines.append("        # 检查文件大小")
        lines.append("        file_size = os.path.getsize(src_path)")
        lines.append("        if file_size >= 50 * 1024 * 1024:  # >= 50MB 大文件")
        lines.append("            # 大文件：创建路径映射文件，绝不复制")
        lines.append("            mapping_file = dst_path + '.pathmap'")
        lines.append("            with open(mapping_file, 'w') as f:")
        lines.append("                f.write(src_path)")
        lines.append("            logger.info(f'Large file {item} ({file_size/1024/1024:.0f}MB): created pathmap -> {src_path}')")
        lines.append("        else:")
        lines.append("            # 小文件：最后手段复制")
        lines.append("            shutil.copy2(src_path, dst_path)")
        lines.append("")
        lines.append("")
        lines.append("def execute_factor(factor_name, factor_code, work_dir):")
        lines.append('    """与 RDAgent FactorFBWorkspace.execute() 完全一致的因子执行方式。')
        lines.append('    ')
        lines.append('    每个因子在独立的临时目录中执行：')
        lines.append('    1. 创建临时目录')
        lines.append('    2. 链接所有数据文件')
        lines.append('    3. 写入 factor.py（直接使用原始源码，不做任何修改）')
        lines.append('    4. 执行 python factor.py')
        lines.append('    5. 读取 result.h5')
        lines.append('    """')
        lines.append("    factor_dir = os.path.join(work_dir, f'_factor_{factor_name}')")
        lines.append("    os.makedirs(factor_dir, exist_ok=True)")
        lines.append("")
        lines.append("    # 链接所有数据文件到因子执行目录")
        lines.append("    link_all_files_to_dir(FACTOR_DATA_DIR, factor_dir)")
        lines.append("")
        lines.append("    # 直接使用原始因子源码，不做任何修改")
        lines.append("    # 因子源码从 RDAgent API 获取，已保持原始完整格式")
        lines.append("    factor_py_path = os.path.join(factor_dir, 'factor.py')")
        lines.append("    with open(factor_py_path, 'w', encoding='utf-8') as f:")
        lines.append("        f.write(factor_code)")
        lines.append("")
        lines.append("    # 执行因子代码")
        lines.append("    try:")
        lines.append("        output = subprocess.check_output(")
        lines.append("            [sys.executable, 'factor.py'],")
        lines.append("            cwd=factor_dir,")
        lines.append("            stderr=subprocess.STDOUT,")
        lines.append("            timeout=300,")
        lines.append("        )")
        lines.append("        logger.info(f'  {factor_name}: execution succeeded')")
        lines.append("    except subprocess.CalledProcessError as e:")
        lines.append("        err_msg = e.output.decode('utf-8', errors='replace')[-500:]")
        lines.append("        logger.error(f'  {factor_name}: execution failed: {err_msg}')")
        lines.append("        return None")
        lines.append("    except subprocess.TimeoutExpired:")
        lines.append("        logger.error(f'  {factor_name}: execution timeout (300s)')")
        lines.append("        return None")
        lines.append("")
        lines.append("    # 读取结果")
        lines.append("    result_path = os.path.join(factor_dir, 'result.h5')")
        lines.append("    if not os.path.exists(result_path):")
        lines.append("        logger.error(f'  {factor_name}: result.h5 not found')")
        lines.append("        return None")
        lines.append("")
        lines.append("    try:")
        lines.append("        df = pd.read_hdf(result_path)")
        lines.append("        if df is None or (hasattr(df, 'empty') and df.empty):")
        lines.append("            logger.warning(f'  {factor_name}: empty result')")
        lines.append("            return None")
        lines.append("        # 确保是 DataFrame 或 Series")
        lines.append("        if isinstance(df, pd.Series):")
        lines.append("            df = df.to_frame(name=factor_name)")
        lines.append("        logger.info(f'  {factor_name}: {df.shape[0]} rows, {df.shape[1]} cols')")
        lines.append("        return df")
        lines.append("    except Exception as e:")
        lines.append("        logger.error(f'  {factor_name}: failed to read result.h5: {e}')")
        lines.append("        return None")
        lines.append("")
        lines.append("")
        lines.append("def main():")
        lines.append("    script_dir = os.path.dirname(os.path.abspath(__file__))")
        lines.append("    os.chdir(script_dir)")
        lines.append("")
        lines.append("    logger.info(f'Factor data dir: {FACTOR_DATA_DIR}')")
        lines.append("    logger.info(f'Work dir: {script_dir}')")
        lines.append("")
        lines.append("    # 同时将数据文件链接到实验根目录（供 qrun 时 StaticDataLoader 使用）")
        lines.append("    link_all_files_to_dir(FACTOR_DATA_DIR, script_dir)")
        lines.append("    logger.info('Linked all data files to experiment dir')")
        lines.append("")

        # 读取因子代码（从 factors/ 子目录中的独立文件）
        lines.append("    # 因子代码定义")
        lines.append("    factor_codes = {}")
        for fname in factor_names:
            lines.append(f"    factor_codes['{fname}'] = open(os.path.join(script_dir, 'factors', '{fname}.py'), 'r', encoding='utf-8').read()")

        lines.append("")
        lines.append("    # 逐个执行因子")
        lines.append("    factor_results = []")
        lines.append("    for factor_name, factor_code in factor_codes.items():")
        lines.append("        logger.info(f'Executing factor: {factor_name}...')")
        lines.append("        result = execute_factor(factor_name, factor_code, script_dir)")
        lines.append("        if result is not None:")
        lines.append("            factor_results.append(result)")
        lines.append("")
        lines.append("    # 合并为 combined_factors_df.parquet")
        lines.append("    if not factor_results:")
        lines.append("        logger.error('No factors computed successfully!')")
        lines.append("        sys.exit(1)")
        lines.append("")
        lines.append("    logger.info(f'Combining {len(factor_results)} factor results...')")
        lines.append("    combined = pd.concat(factor_results, axis=1)")
        lines.append("    combined = combined.sort_index()")
        lines.append("    combined = combined.loc[:, ~combined.columns.duplicated(keep='last')]")
        lines.append("")
        lines.append("    # 确保索引格式正确：MultiIndex(datetime, instrument)")
        lines.append("    # StaticDataLoader 要求索引必须是 MultiIndex(datetime, instrument)")
        lines.append("    if not isinstance(combined.index, pd.MultiIndex):")
        lines.append("        # 检查是否有 datetime 和 instrument 列")
        lines.append("        if 'datetime' in combined.columns and 'instrument' in combined.columns:")
        lines.append("            logger.info('Setting index from datetime/instrument columns...')")
        lines.append("            combined['datetime'] = pd.to_datetime(combined['datetime'], errors='coerce')")
        lines.append("            combined = combined.set_index(['datetime', 'instrument'])")
        lines.append("        else:")
        lines.append("            logger.error('Factor data must have MultiIndex(datetime, instrument) or datetime/instrument columns!')")
        lines.append("            sys.exit(1)")
        lines.append("")
        lines.append("    # 确保索引名称顺序正确：[""datetime"", ""instrument""]")
        lines.append("    if isinstance(combined.index, pd.MultiIndex):")
        lines.append("        names = list(combined.index.names)")
        lines.append("        if set(names) == {'datetime', 'instrument'} and names != ['datetime', 'instrument']:")
        lines.append("            logger.info('Swapping index levels to (datetime, instrument)...')")
        lines.append("            combined = combined.swaplevel('datetime', 'instrument')")
        lines.append("        combined = combined.sort_index()")
        lines.append("")
        lines.append("    # 添加 'feature' 多级列索引（与 RDAgent factor_runner.py 一致）")
        lines.append("    new_columns = pd.MultiIndex.from_product([['feature'], combined.columns])")
        lines.append("    combined.columns = new_columns")
        lines.append("")
        lines.append("    output_path = os.path.join(script_dir, 'combined_factors_df.parquet')")
        lines.append("    combined.to_parquet(output_path, engine='pyarrow')")
        lines.append("    logger.info(f'Saved combined_factors_df.parquet: shape={combined.shape}')")
        lines.append("")
        lines.append("    # 计算 num_features：根据 conf.yaml 中的 data_loader 类型决定")
        lines.append("    # - 如果使用 DynamicFactorsOnlyLoader（disable_alpha158=True）：num_features = 自定义因子数")
        lines.append("    # - 如果使用 NestedDataLoader（disable_alpha158=False）：num_features = Alpha158数 + 自定义因子数")
        lines.append("    import yaml")
        lines.append("    conf_path = os.path.join(script_dir, 'conf.yaml')")
        lines.append("    alpha158_count = 0")
        lines.append("    use_alpha158 = False")
        lines.append("    try:")
        lines.append("        with open(conf_path, 'r', encoding='utf-8') as cf:")
        lines.append("            conf_text = cf.read()")
        lines.append("        # conf.yaml 包含 Jinja2 模板语法，先做简单替换以便 yaml 解析")
        lines.append("        import re as _re")
        lines.append("        conf_text = _re.sub(r'\\{\\{.*?\\}\\}', '0', conf_text)")
        lines.append("        conf_text = _re.sub(r'\\{%.*?%\\}', '', conf_text)")
        lines.append("        conf_obj = yaml.safe_load(conf_text)")
        lines.append("        # 检查 data_loader 类型")
        lines.append("        dhc = conf_obj.get('data_handler_config', {})")
        lines.append("        dl = dhc.get('data_loader', {})")
        lines.append("        dl_cls = dl.get('class', '')")
        lines.append("        # 如果使用 DynamicFactorsOnlyLoader，说明 disable_alpha158=True")
        lines.append("        if 'DynamicFactorsOnlyLoader' in dl_cls:")
        lines.append("            use_alpha158 = False")
        lines.append("            logger.info('检测到 DynamicFactorsOnlyLoader，disable_alpha158=True，只使用自定义因子')")
        lines.append("        else:")
        lines.append("            # 使用 NestedDataLoader，遍历 dataloader_l 找 Alpha158DL")
        lines.append("            use_alpha158 = True")
        lines.append("            dl_list = dl.get('kwargs', {}).get('dataloader_l', [])")
        lines.append("            for dl_item in dl_list:")
        lines.append("                dl_item_cls = dl_item.get('class', '')")
        lines.append("                if 'Alpha158' in dl_item_cls:")
        lines.append("                    feat_cfg = dl_item.get('kwargs', {}).get('config', {}).get('feature', [])")
        lines.append("                    if len(feat_cfg) >= 2:")
        lines.append("                        # feat_cfg[0] = 表达式列表, feat_cfg[1] = 名称列表")
        lines.append("                        alpha158_count = len(feat_cfg[0])")
        lines.append("                        logger.info(f'从 conf.yaml 解析到 Alpha158 特征数: {alpha158_count}')")
        lines.append("                    break")
        lines.append("    except Exception as e:")
        lines.append("        logger.warning(f'解析 conf.yaml 失败: {e}')")
        lines.append("    # 如果使用 Alpha158 但未解析到特征数，使用默认值")
        lines.append("    if use_alpha158 and alpha158_count == 0:")
        lines.append("        alpha158_count = int(os.environ.get('INITIAL_FACTOR_LIBRARY_SIZE', '20'))")
        lines.append("        logger.warning(f'未能从 conf.yaml 解析 Alpha158 特征数，使用默认值: {alpha158_count}')")
        lines.append("    custom_factor_cols = len([c for c in combined.columns if c[0] == 'feature'])")
        lines.append("    if use_alpha158:")
        lines.append("        num_features = alpha158_count + custom_factor_cols")
        lines.append("        logger.info(f'num_features = {alpha158_count} (Alpha158) + {custom_factor_cols} (custom) = {num_features}')")
        lines.append("    else:")
        lines.append("        num_features = custom_factor_cols")
        lines.append("        logger.info(f'num_features = {custom_factor_cols} (custom only, disable_alpha158=True)')")
        lines.append("")
        lines.append("    # 写入环境变量文件，供后续 qrun 使用")
        lines.append("    env_file = os.path.join(script_dir, '.factor_env')")
        lines.append("    with open(env_file, 'w') as f:")
        lines.append("        f.write(f'export num_features={num_features}\\n')")
        lines.append("    logger.info(f'Wrote .factor_env: num_features={num_features}')")
        lines.append("")
        lines.append("    logger.info('Factor preparation completed successfully.')")
        lines.append("")
        lines.append("if __name__ == '__main__':")
        lines.append("    main()")

        return "\n".join(lines)

    # ---- 辅助方法 ----

    def _get_factors_info(self, factor_names: List[str],
                          factor_sources: Optional[Dict[str, str]] = None) -> List[Dict]:
        """获取因子详细信息。
        
        对于 RDAgent 因子（source=rdagent_task_sync），从 RDAgent API 获取原始源码，
        确保源码保持原始格式不做任何修改。
        """
        if not factor_names:
            return []

        with get_conn() as conn:
            with conn.cursor() as cur:
                placeholders = ",".join(["%s"] * len(factor_names))
                cur.execute(f"""
                    SELECT factor_name, source, expression, code_text,
                           ic, sharpe, annualized_return,
                           best_loop_task_run_id
                    FROM aistock_factor_catalog
                    WHERE factor_name IN ({placeholders})
                """, factor_names)
                cols = [desc[0] for desc in cur.description]
                results = [dict(zip(cols, row)) for row in cur.fetchall()]

        # 如果指定了source映射，过滤
        if factor_sources:
            results = [r for r in results if r["factor_name"] not in factor_sources
                       or r["source"] == factor_sources[r["factor_name"]]]

        # 对于 RDAgent 因子，从 API 获取原始源码
        for r in results:
            if r.get("source") == "rdagent_task_sync":
                task_id = r.get("best_loop_task_run_id")
                factor_name = r.get("factor_name")
                if task_id and factor_name:
                    source_code = self._fetch_factor_source_from_api(task_id, factor_name)
                    if source_code:
                        r["code_text"] = source_code
                        logger.info(f"[QE] 从RDAgent API获取因子源码: {factor_name}, task={task_id}")

        # 补充未在数据库中的因子
        found_names = {r["factor_name"] for r in results}
        for fn in factor_names:
            if fn not in found_names:
                results.append({"factor_name": fn, "source": "unknown"})

        return results

    def _fetch_factor_source_from_api(self, task_id: str, factor_name: str) -> Optional[str]:
        """从 RDAgent API 获取因子原始源码。
        
        Args:
            task_id: RDAgent 任务 ID
            factor_name: 因子名称
            
        Returns:
            因子源码字符串，失败时返回 None
        """
        try:
            from .qe_file_sync_client import QEFileSyncClient
            client = QEFileSyncClient()
            
            result = client.get_factor_source_code(task_id, factor_name)
            if result.get("success"):
                return result.get("source_code")
            else:
                logger.warning(
                    f"[QE] 获取因子源码失败: factor={factor_name}, task={task_id}, "
                    f"error={result.get('error', 'unknown')}"
                )
                return None
        except Exception as e:
            logger.error(f"[QE] 获取因子源码异常: factor={factor_name}, task={task_id}, {e}")
            return None

    def _get_model_info(self, model_id: str) -> Optional[Dict]:
        """获取模型信息（含源代码）。"""
        # 内置LGBModel无需数据库查询
        if model_id == "__builtin_lgbmodel__":
            return {
                "model_id": "__builtin_lgbmodel__",
                "model_name": "LGBModel",
                "model_type": "LGB",
                "model_hyperparameters": None,
                "model_training_hyperparameters": None,
                "ic": None,
                "annualized_return": None,
                "is_sota": False,
                "code_text": None,
            }
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT model_id, model_name, model_type,
                           model_hyperparameters, model_training_hyperparameters,
                           ic, annualized_return, is_sota,
                           code_text
                    FROM aistock_model_catalog
                    WHERE model_id = %s
                """, (model_id,))
                row = cur.fetchone()
                if not row:
                    return None
                cols = [desc[0] for desc in cur.description]
                return dict(zip(cols, row))

    def _get_strategy_info(self, strategy_id: str) -> Optional[Dict]:
        """获取策略信息（含源码和参数）。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT strategy_id, display_name, portfolio_config,
                           source_code, default_kwargs, param_schema
                    FROM aistock_strategy_catalog
                    WHERE strategy_id = %s
                """, (strategy_id,))
                row = cur.fetchone()
                if not row:
                    return None
                cols = [desc[0] for desc in cur.description]
                return dict(zip(cols, row))

    def _windows_to_wsl_path(self, win_path: str) -> str:
        """Windows路径转WSL路径。"""
        path = win_path.replace("\\", "/")
        if len(path) >= 2 and path[1] == ":":
            drive = path[0].lower()
            path = f"/mnt/{drive}{path[2:]}"
        return path

    def _generate_wsl_command(self, wsl_path: str,
                              has_custom_factors: bool = False,
                              use_custom_model: bool = False,
                              model_type_tag: Optional[str] = None) -> str:
        """生成WSL执行命令。

        与 RDAgent model_runner.py / factor_runner.py 完全一致的执行方式：
        - 自定义因子：先执行 prepare_factors.py 生成 combined_factors_df.parquet
        - 自定义模型：通过环境变量传递 num_features, dataset_cls, step_len 等
        - PYTHONPATH 设置确保 model.py 可被 GeneralPTNN 加载
        """
        if has_custom_factors:
            # 环境变量设置
            env_lines = []
            env_lines.append(f'export PYTHONPATH="{wsl_path}:/mnt/f/Dev/RD-Agent-main:$PYTHONPATH"')

            if use_custom_model and model_type_tag:
                # 与 RDAgent model_runner.py 一致的环境变量
                if model_type_tag == "TimeSeries":
                    env_lines.append("export dataset_cls=TSDatasetH")
                    env_lines.append("export step_len=20")
                    env_lines.append("export num_timesteps=20")
                else:
                    env_lines.append("export dataset_cls=DatasetH")
                # num_features 在 prepare_factors.py 执行后才能确定
                # 由 conf.yaml 中的 Jinja2 模板变量引用
                env_lines.append("# num_features 将在 qrun 时由 conf.yaml Jinja2 模板自动计算")

            env_block = "\n".join(env_lines)

            return f"""# QuantEvolver 实验执行命令（含自定义因子预处理）
# 请在WSL终端中执行以下命令：

cd {wsl_path}
conda activate rdagent-gpu

# 步骤1: 预计算因子 → 生成 combined_factors_df.parquet
python prepare_factors.py

# 步骤2: 设置环境变量
{env_block}
# 加载因子预处理输出的 num_features（由 prepare_factors.py 自动计算）
source .factor_env

# 步骤3: 运行QLib回测
qrun conf.yaml

# 步骤4: 读取结果
python read_exp_res.py

# 执行完成后，回到AIstock界面点击"同步结果"按钮"""
        else:
            return f"""# QuantEvolver 实验执行命令
# 请在WSL终端中执行以下命令：

cd {wsl_path}
conda activate rdagent-gpu
qrun conf.yaml
python read_exp_res.py

# 执行完成后，回到AIstock界面点击"同步结果"按钮"""

    def _api_sync_experiment_files(self, experiment_name: str, exp_dir: Path) -> None:
        """通过API将实验文件同步到RDAgent侧。
        
        当AIstock和RDAgent在同一台机器上时，文件已通过直接写入共享目录完成同步。
        此方法额外通过HTTP API同步一份，确保独立部署场景下也能正常工作。
        若API同步失败则抛错中断，避免RDAgent侧缺文件导致实验执行失败。
        """
        try:
            from .qe_file_sync_client import QEFileSyncClient
            client = QEFileSyncClient()
            
            # 递归收集实验目录中的所有文本文件（包含 factors/ 子目录）
            # 这样独立部署场景下也能同步 factors/*.py、custom_strategy.py、model.py 等关键文件
            files_to_sync = {}
            allowed_suffixes = {".yaml", ".yml", ".py", ".txt", ".json"}
            for f in exp_dir.rglob("*"):
                try:
                    if f.is_file() and f.suffix in allowed_suffixes:
                        try:
                            rel_path = f.relative_to(exp_dir).as_posix()
                            files_to_sync[rel_path] = f.read_text(encoding="utf-8")
                        except Exception as e:
                            logger.warning(f"读取文件 {f} 失败: {e}")
                except OSError as e:
                    logger.debug(f"检查文件状态失败(可能为无效的软链接或无权限): {f} - {e}")
            
            if not files_to_sync:
                return
            
            result = client.sync_experiment_files(experiment_name, files_to_sync)
            if result.get("success"):
                logger.info(
                    f"[QESync] API同步成功: {experiment_name}, "
                    f"{result.get('success_count', 0)} 个文件"
                )
            else:
                raise RuntimeError(
                    f"[QESync] API同步失败: {experiment_name}, "
                    f"error={result.get('error', 'unknown')}"
                )
        except Exception as e:
            # 独立部署场景必须保证同步成功，否则RDAgent侧可能缺文件导致实验失败
            logger.error(f"[QESync] API同步异常: {e}")
            raise

    def _api_setup_experiment_workspace(self, experiment_name: str) -> None:
        """通过API在RDAgent侧创建实验工作区并链接数据文件。
        
        调用RDAgent的POST /api/qe/experiments/{exp_id}/setup API：
        1. 在WSL环境中创建实验工作区目录
        2. 链接数据文件（daily_pv.h5, static_factors.parquet等）
        
        若API调用失败则抛错中断，确保RDAgent侧工作区和数据链接已就绪。
        """
        try:
            from .qe_file_sync_client import QEFileSyncClient
            client = QEFileSyncClient()
            
            result = client.setup_experiment_workspace(
                exp_id=experiment_name,
                link_data_files=True,
                overwrite=False,
            )
            if result.get("success"):
                logger.info(
                    f"[QESetup] 实验工作区创建成功: {experiment_name}, "
                    f"workspace={result.get('workspace_path')}, "
                    f"linked_files={result.get('linked_files_count', 0)}"
                )
            else:
                raise RuntimeError(
                    f"[QESetup] 实验工作区创建失败: {experiment_name}, "
                    f"error={result.get('error', 'unknown')}"
                )
        except Exception as e:
            # 必须确保RDAgent侧工作区和数据链接创建成功，否则后续实验不可可靠执行
            logger.error(f"[QESetup] API创建实验工作区异常: {e}")
            raise

    def _write_custom_model(self, exp_dir: Path, model_info: Dict) -> None:
        """将RDAgent自定义模型源码写入实验目录的 model.py。

        与 RDAgent 完全一致的方式：
        1. model.py 只包含纯 PyTorch NN 类代码
        2. 文件末尾导出 model_cls 变量指向 NN 类
        3. GeneralPTNN 通过 pt_model_uri: "model.model_cls" 自动加载并包装

        这样 GeneralPTNN 负责所有训练/推理逻辑（分批处理、GPU管理、早停等），
        无需 QE 自行实现 QLib Model 接口，支持所有 QLib 模型类型。
        """
        code_text = model_info.get("code_text", "")
        if not code_text:
            return

        model_name = model_info.get("model_name", "CustomModel")

        # 从源代码中提取NN类名
        class_match = re.search(r'class\s+(\w+)\s*\(', code_text)
        nn_class_name = class_match.group(1) if class_match else model_name

        # 构建 model.py：纯NN类 + model_cls 导出
        # 与 RDAgent 中 model.py 的格式完全一致
        model_py_content = f'''"""
RDAgent SOTA模型: {model_name}
QuantEvolver自动生成 - 由 GeneralPTNN 通过 pt_model_uri: "model.model_cls" 加载
"""
{code_text}

# GeneralPTNN 通过此变量加载 NN 类
model_cls = {nn_class_name}
'''

        (exp_dir / "model.py").write_text(model_py_content, encoding="utf-8")
        logger.info(f"写入模型文件: {exp_dir / 'model.py'} (NN类: {nn_class_name}, 由GeneralPTNN加载)")

    def _write_custom_strategy(self, exp_dir: Path, strategy_info: Dict) -> None:
        """将自定义策略源码写入实验目录的 custom_strategy.py。
        
        写入前会进行编译验证，确保代码无语法错误。
        并自动添加必要的import语句，移除函数内部的重复import。
        """
        source_code = strategy_info.get("source_code", "")
        if not source_code:
            return
        
        # 编译验证：检查语法错误
        validation_result = self._validate_strategy_code(source_code)
        if not validation_result["ok"]:
            raise ValueError(
                f"策略代码编译验证失败:\n{validation_result['error']}\n"
                f"请修复策略代码后再创建实验。"
            )
        
        # 自动添加必要的import语句（如果源码中没有）
        required_imports = [
            "import pandas as pd",
            "import numpy as np",
        ]
        
        # 检查并添加缺失的import
        import_lines = []
        for imp in required_imports:
            # 检查是否已存在该import（检查模块级import）
            if imp not in source_code:
                import_lines.append(imp)
        
        # 如果需要添加import，在文件开头插入
        if import_lines:
            # 找到现有import语句后的位置
            lines = source_code.split("\n")
            insert_pos = 0
            
            # 跳过文件开头的注释和docstring
            in_docstring = False
            for i, line in enumerate(lines):
                stripped = line.strip()
                
                # 跳过空行
                if not stripped:
                    insert_pos = i + 1
                    continue
                
                # 跳过注释
                if stripped.startswith("#"):
                    insert_pos = i + 1
                    continue
                
                # 跳过docstring
                if '"""' in stripped or "'''" in stripped:
                    if in_docstring:
                        in_docstring = False
                        insert_pos = i + 1
                    else:
                        in_docstring = True
                        insert_pos = i + 1
                    continue
                
                if in_docstring:
                    insert_pos = i + 1
                    continue
                
                # 遇到import语句，继续跳过
                if stripped.startswith("import ") or stripped.startswith("from "):
                    insert_pos = i + 1
                    continue
                
                # 遇到其他代码，停止
                break
            
            # 在合适位置插入import
            for imp in import_lines:
                lines.insert(insert_pos, imp)
                insert_pos += 1
            
            source_code = "\n".join(lines)
            logger.info(f"自动添加了缺失的import: {import_lines}")
        
        # 移除函数内部的重复import（避免UnboundLocalError）
        # 检测函数/方法内部的import语句并移除
        import re
        lines = source_code.split("\n")
        cleaned_lines = []
        in_function = False
        indent_level = 0
        
        for i, line in enumerate(lines):
            stripped = line.strip()
            
            # 检测函数/方法定义开始
            if re.match(r'^(def |async def |class )', stripped):
                in_function = True
                indent_level = len(line) - len(line.lstrip())
            # 检测缩进回到函数外部
            elif in_function and line and not line[0].isspace():
                in_function = False
            elif in_function and line:
                current_indent = len(line) - len(line.lstrip())
                if current_indent <= indent_level and stripped and not stripped.startswith('#'):
                    in_function = False
            
            # 如果在函数内部且是import语句，跳过（移除）
            if in_function and (stripped.startswith("import pandas") or 
                                stripped.startswith("import numpy") or
                                stripped.startswith("import pandas as pd") or
                                stripped.startswith("import numpy as np")):
                logger.info(f"移除函数内部的重复import: 第{i+1}行 '{stripped}'")
                continue
            
            cleaned_lines.append(line)
        
        source_code = "\n".join(cleaned_lines)
        
        (exp_dir / "custom_strategy.py").write_text(source_code, encoding="utf-8")
        logger.info(f"写入自定义策略文件: {exp_dir / 'custom_strategy.py'}")
    
    def _validate_strategy_code(self, source_code: str) -> Dict[str, Any]:
        """验证策略代码是否能正确编译。
        
        检查内容：
        1. Python语法是否正确
        2. 是否能正确导入qlib相关模块
        3. 策略类是否继承自正确的基类
        4. __init__方法参数是否能正确处理
        """
        import ast
        
        result = {"ok": True, "error": None, "warnings": []}
        
        # 1. 语法检查
        try:
            ast.parse(source_code)
        except SyntaxError as e:
            result["ok"] = False
            result["error"] = f"语法错误 (行 {e.lineno}): {e.msg}\n{e.text}"
            return result
        
        # 2. 检查类定义
        try:
            tree = ast.parse(source_code)
            class_defs = [node for node in ast.walk(tree) if isinstance(node, ast.ClassDef)]
            
            if not class_defs:
                result["ok"] = False
                result["error"] = "未找到策略类定义（class关键字）"
                return result
            
            # 检查是否继承自BaseSignalStrategy或其子类
            valid_base_classes = {"BaseSignalStrategy", "BaseStrategy", "TopkDropoutStrategy"}
            found_valid_class = False
            
            for class_def in class_defs:
                for base in class_def.bases:
                    base_name = None
                    if isinstance(base, ast.Name):
                        base_name = base.id
                    elif isinstance(base, ast.Attribute):
                        base_name = base.attr
                    
                    if base_name in valid_base_classes:
                        found_valid_class = True
                        break
                
                if found_valid_class:
                    break
            
            if not found_valid_class:
                result["warnings"].append(
                    f"策略类应继承自 BaseSignalStrategy 或 TopkDropoutStrategy，"
                    f"当前继承自: {[ast.unparse(b) for b in class_defs[0].bases]}"
                )
            
            # 3. 检查__init__方法是否正确处理**kwargs
            for class_def in class_defs:
                for item in class_def.body:
                    if isinstance(item, ast.FunctionDef) and item.name == "__init__":
                        # 检查是否有**kwargs参数
                        has_kwargs = any(
                            arg.arg == "kwargs" and arg.arg == "**" 
                            for arg in item.args.kwonlyargs or []
                        )
                        # 更准确的方式检查kwargs
                        if item.args.kwarg:
                            has_kwargs = True
                        
                        if not has_kwargs:
                            result["warnings"].append(
                                f"策略类 {class_def.name} 的 __init__ 方法没有 **kwargs 参数，"
                                f"可能导致无法接收Qlib传递的额外参数。建议添加 **kwargs 参数。"
                            )
                        break
            
        except Exception as e:
            result["warnings"].append(f"代码结构分析警告: {str(e)}")
        
        # 4. 尝试在隔离环境中编译（不执行）
        try:
            compile(source_code, "<strategy>", "exec")
        except Exception as e:
            result["ok"] = False
            result["error"] = f"编译错误: {str(e)}"
            return result
        
        if result["warnings"]:
            logger.warning(f"策略代码验证警告: {result['warnings']}")
        
        return result

    def _copy_qe_custom_loaders(self, exp_dir: Path) -> None:
        """复制QE独立的custom_loaders.py到实验目录。
        
        这样实验目录中有独立的loader文件，不会影响RDAgent的原有custom_loaders.py。
        """
        import shutil
        from pathlib import Path
        
        # QE独立的loader源文件
        qe_loader_source = Path(__file__).parent / "qe_custom_loaders.py"
        
        if not qe_loader_source.exists():
            logger.warning(f"QE custom loaders源文件不存在: {qe_loader_source}")
            return
        
        # 复制到实验目录，命名为qe_custom_loaders.py
        qe_loader_dest = exp_dir / "qe_custom_loaders.py"
        shutil.copy2(qe_loader_source, qe_loader_dest)
        logger.info(f"复制QE独立loader: {qe_loader_dest}")

    def _copy_read_exp_res(self, exp_dir: Path) -> None:
        """复制read_exp_res.py模板到实验目录。"""
        template_content = '''"""
读取QLib实验结果并输出关键指标。
支持mlflow metrics文件格式（timestamp value step）和pkl文件。
输出结构化JSON供后续agent分析，包含交易统计和策略胜率。
"""
import json
import pickle
import glob
import os
import numpy as np
import pandas as pd
from pathlib import Path


def _parse_mlflow_metric(filepath):
    """解析mlflow metric文件，格式: timestamp value step"""
    content = open(filepath).read().strip()
    parts = content.split()
    if len(parts) >= 2:
        return float(parts[1])
    return float(content)


def _calc_daily_win_stats(report_df):
    """从report_normal_1day.pkl计算日/周胜率和连续盈亏统计。

    report_df columns: return, bench, cost, turnover (index=datetime)
    """
    stats = {}
    if report_df is None or report_df.empty:
        return stats

    # 超额收益 = return - bench - cost
    excess = report_df["return"] - report_df["bench"] - report_df["cost"]
    total_days = len(excess)
    stats["total_trading_days"] = total_days

    # 日胜率
    win_days = int((excess > 0).sum())
    stats["daily_win_rate"] = round(win_days / total_days, 4) if total_days > 0 else None

    # 周胜率
    weekly_excess = excess.resample("W").sum()
    weekly_total = len(weekly_excess)
    weekly_wins = int((weekly_excess > 0).sum())
    stats["weekly_win_rate"] = round(weekly_wins / weekly_total, 4) if weekly_total > 0 else None

    # 最大连续盈利/亏损天数
    is_win = (excess > 0).astype(int)
    max_con_win = 0
    max_con_loss = 0
    cur_win = 0
    cur_loss = 0
    for w in is_win:
        if w == 1:
            cur_win += 1
            cur_loss = 0
            max_con_win = max(max_con_win, cur_win)
        else:
            cur_loss += 1
            cur_win = 0
            max_con_loss = max(max_con_loss, cur_loss)
    stats["max_consecutive_win"] = max_con_win
    stats["max_consecutive_loss"] = max_con_loss

    # 平均换手率
    if "turnover" in report_df.columns:
        stats["avg_turnover"] = round(float(report_df["turnover"].mean()), 6)

    # 夏普比率 (年化超额收益 / 年化超额波动率)
    if total_days > 1:
        ann_factor = np.sqrt(252)
        excess_mean = excess.mean()
        excess_std = excess.std()
        if excess_std > 0:
            stats["sharpe_ratio"] = round(float(excess_mean / excess_std * ann_factor), 4)

    return stats


def _calc_stock_trade_stats(positions_dict, stop_loss_threshold=None, take_profit_threshold=None):
    """从positions_normal_1day.pkl计算个股交易统计。

    positions_dict: dict{date_str -> {instrument -> {amount, weight, price}}}
    stop_loss_threshold: 止损阈值（负数，如-0.10表示亏损10%止损）
    take_profit_threshold: 止盈阈值（正数，如0.15表示盈利15%止盈）

    Returns:
        stats: {
            total_trades, winning_trades, losing_trades, stock_win_rate,
            avg_profit_pct, avg_loss_pct, profit_loss_ratio,
            max_single_profit_pct, max_single_loss_pct,
            stop_loss_count: 止损触发次数,
            take_profit_count: 止盈触发次数,
            top_profit_stocks: [{instrument, profit_pct, holding_days, entry_date, exit_date, entry_price, exit_price, exit_reason}],
            top_loss_stocks: [{instrument, loss_pct, holding_days, entry_date, exit_date, entry_price, exit_price, exit_reason}],
            all_trades: [{instrument, profit_pct, holding_days, entry_date, exit_date, exit_reason}]
        }
    """
    stats = {}
    if not positions_dict or not isinstance(positions_dict, dict):
        return stats

    # 构建每只股票的持仓时间序列
    # stock_history[instrument] = [(date, price, amount), ...]
    stock_history = {}
    sorted_dates = sorted(positions_dict.keys())

    for date_key in sorted_dates:
        pos = positions_dict[date_key]
        if not isinstance(pos, dict):
            continue
        for instrument, info in pos.items():
            if instrument == "cash" or not isinstance(info, dict):
                continue
            price = info.get("price", 0)
            amount = info.get("amount", 0)
            if instrument not in stock_history:
                stock_history[instrument] = []
            stock_history[instrument].append((date_key, price, amount))

    # 分析每只股票的交易：找到买入和卖出点
    trade_returns = []  # 每笔交易的收益率
    trade_details = []  # 每笔交易的详细信息

    for instrument, history in stock_history.items():
        if len(history) < 2:
            continue

        # 找到持仓开始和结束的区间
        i = 0
        while i < len(history):
            # 找到持仓开始（amount > 0）
            if history[i][2] <= 0:
                i += 1
                continue

            entry_date = history[i][0]
            entry_price = history[i][1]
            if entry_price <= 0:
                i += 1
                continue

            # 找到持仓结束（amount变为0或到达末尾）
            j = i + 1
            while j < len(history) and history[j][2] > 0:
                j += 1

            # 退出价格和日期：最后一个有持仓的日期的价格
            exit_date = history[j - 1][0]
            exit_price = history[j - 1][1]
            if exit_price > 0 and entry_price > 0:
                ret_pct = (exit_price - entry_price) / entry_price * 100
                ret_ratio = ret_pct / 100  # 转换为比率形式（如 -0.10）
                trade_returns.append(ret_pct)

                # 计算持仓天数
                try:
                    from datetime import datetime
                    entry_dt = datetime.strptime(entry_date, "%Y-%m-%d") if isinstance(entry_date, str) else entry_date
                    exit_dt = datetime.strptime(exit_date, "%Y-%m-%d") if isinstance(exit_date, str) else exit_date
                    holding_days = (exit_dt - entry_dt).days + 1
                except Exception:
                    holding_days = j - i  # 回退到交易日数

                # 推断退出原因
                exit_reason = "signal"  # 默认为信号反转
                if stop_loss_threshold is not None and stop_loss_threshold > -1.0:
                    # 止损阈值是负数，如 -0.10，当 ret_ratio <= stop_loss_threshold 时触发
                    if ret_ratio <= stop_loss_threshold:
                        exit_reason = "stop_loss"
                if take_profit_threshold is not None and take_profit_threshold > 0:
                    # 止盈阈值是正数，如 0.15，当 ret_ratio >= take_profit_threshold 时触发
                    if ret_ratio >= take_profit_threshold:
                        exit_reason = "take_profit"

                trade_details.append({
                    "instrument": instrument,
                    "profit_pct": round(ret_pct, 2),
                    "holding_days": holding_days,
                    "entry_date": entry_date,
                    "exit_date": exit_date,
                    "entry_price": round(entry_price, 4),
                    "exit_price": round(exit_price, 4),
                    "exit_reason": exit_reason,
                })

            i = j + 1 if j < len(history) else j

    if not trade_returns:
        return stats

    trade_returns = np.array(trade_returns)
    total = len(trade_returns)
    winners = trade_returns[trade_returns > 0]
    losers = trade_returns[trade_returns < 0]

    stats["total_trades"] = total
    stats["winning_trades"] = len(winners)
    stats["losing_trades"] = len(losers)
    stats["stock_win_rate"] = round(len(winners) / total, 4) if total > 0 else None

    stats["avg_profit_pct"] = round(float(winners.mean()), 2) if len(winners) > 0 else None
    stats["avg_loss_pct"] = round(float(losers.mean()), 2) if len(losers) > 0 else None

    if stats.get("avg_profit_pct") and stats.get("avg_loss_pct") and stats["avg_loss_pct"] != 0:
        stats["profit_loss_ratio"] = round(abs(stats["avg_profit_pct"] / stats["avg_loss_pct"]), 2)

    stats["max_single_profit_pct"] = round(float(trade_returns.max()), 2) if total > 0 else None
    stats["max_single_loss_pct"] = round(float(trade_returns.min()), 2) if total > 0 else None

    # 统计止损/止盈触发次数
    stop_loss_count = sum(1 for t in trade_details if t.get("exit_reason") == "stop_loss")
    take_profit_count = sum(1 for t in trade_details if t.get("exit_reason") == "take_profit")
    stats["stop_loss_count"] = stop_loss_count
    stats["take_profit_count"] = take_profit_count

    # 新增：按收益率排序的个股交易明细
    # 盈利最多的前10只股票
    profit_trades = [t for t in trade_details if t["profit_pct"] > 0]
    profit_trades.sort(key=lambda x: x["profit_pct"], reverse=True)
    stats["top_profit_stocks"] = profit_trades[:10]

    # 亏损最多的前10只股票
    loss_trades = [t for t in trade_details if t["profit_pct"] < 0]
    loss_trades.sort(key=lambda x: x["profit_pct"])  # 升序，最亏损的在前面
    stats["top_loss_stocks"] = loss_trades[:10]

    # 所有交易记录（按收益率降序）
    trade_details.sort(key=lambda x: x["profit_pct"], reverse=True)
    stats["all_trades"] = trade_details

    return stats


def read_results():
    """读取QLib回测结果。"""
    result_dir = Path(".")
    mlruns = result_dir / "mlruns"
    if not mlruns.exists():
        print("ERROR: 未找到mlruns目录，请确认qrun已执行完成")
        return

    # ── 1. 读取mlflow metrics ──
    metrics = {}
    metrics_dirs = glob.glob(str(mlruns / "*" / "*" / "metrics"))
    for md in metrics_dirs:
        for fname in os.listdir(md):
            fp = os.path.join(md, fname)
            if os.path.isfile(fp):
                try:
                    metrics[fname] = _parse_mlflow_metric(fp)
                except Exception:
                    pass

    # ── 2. 读取portfolio analysis pkl ──
    port_analysis = {}
    pkl_files = glob.glob(str(mlruns / "*" / "*" / "artifacts" / "portfolio_analysis" / "port_analysis_1day.pkl"))
    for pf in pkl_files:
        try:
            with open(pf, "rb") as f:
                df = pickle.load(f)
            if isinstance(df, pd.DataFrame):
                for idx in df.index:
                    if isinstance(idx, tuple):
                        key = ".".join(str(x) for x in idx)
                    else:
                        key = str(idx)
                    for col in df.columns:
                        port_analysis[f"{key}.{col}"] = float(df.loc[idx, col])
        except Exception as e:
            print(f"WARNING: 读取{pf}失败: {e}")

    # ── 3. 读取report_normal_1day.pkl → 日/周胜率统计 ──
    daily_win_stats = {}
    report_pkls = glob.glob(str(mlruns / "*" / "*" / "artifacts" / "portfolio_analysis" / "report_normal_1day.pkl"))
    for rp in report_pkls:
        try:
            with open(rp, "rb") as f:
                report_df = pickle.load(f)
            if isinstance(report_df, pd.DataFrame):
                daily_win_stats = _calc_daily_win_stats(report_df)
                print(f"  report_normal_1day.pkl: {report_df.shape[0]} 天数据")
        except Exception as e:
            print(f"WARNING: 读取{rp}失败: {e}")

    # ── 4. 读取positions_normal_1day.pkl → 个股交易统计 ──
    stock_trade_stats = {}
    pos_pkls = glob.glob(str(mlruns / "*" / "*" / "artifacts" / "portfolio_analysis" / "positions_normal_1day.pkl"))
    
    # 从conf.yaml读取止损/止盈参数
    stop_loss_threshold = None
    take_profit_threshold = None
    conf_yaml_path = result_dir / "conf.yaml"
    if conf_yaml_path.exists():
        try:
            import yaml
            conf = yaml.safe_load(conf_yaml_path.read_text(encoding="utf-8"))
            port_analysis_config = conf.get("port_analysis_config", {})
            strategy_config = port_analysis_config.get("strategy", {})
            kwargs = strategy_config.get("kwargs", {})
            stop_loss_threshold = kwargs.get("stop_loss")  # 如 -0.10
            take_profit_threshold = kwargs.get("take_profit")  # 如 0.15
        except Exception:
            pass
    
    for pp in pos_pkls:
        try:
            with open(pp, "rb") as f:
                positions = pickle.load(f)
            if isinstance(positions, dict):
                stock_trade_stats = _calc_stock_trade_stats(
                    positions, stop_loss_threshold, take_profit_threshold
                )
                print(f"  positions_normal_1day.pkl: {len(positions)} 天持仓数据")
        except Exception as e:
            print(f"WARNING: 读取{pp}失败: {e}")

    # ── 5. 汇总关键指标 ──
    summary = {
        "IC": metrics.get("IC"),
        "ICIR": metrics.get("ICIR"),
        "Rank_IC": metrics.get("Rank IC"),
        "Rank_ICIR": metrics.get("Rank ICIR"),
        "excess_return_without_cost_annualized": metrics.get("1day.excess_return_without_cost.annualized_return"),
        "excess_return_without_cost_IR": metrics.get("1day.excess_return_without_cost.information_ratio"),
        "excess_return_without_cost_max_drawdown": metrics.get("1day.excess_return_without_cost.max_drawdown"),
        "excess_return_with_cost_annualized": metrics.get("1day.excess_return_with_cost.annualized_return"),
        "excess_return_with_cost_IR": metrics.get("1day.excess_return_with_cost.information_ratio"),
        "excess_return_with_cost_max_drawdown": metrics.get("1day.excess_return_with_cost.max_drawdown"),
    }

    # 卡尔玛比率 = 年化收益 / |最大回撤|
    calmar = None
    ar_val = summary.get("excess_return_with_cost_annualized")
    md_val = summary.get("excess_return_with_cost_max_drawdown")
    if ar_val is not None and md_val is not None and md_val != 0:
        calmar = round(ar_val / abs(md_val), 4)

    # ── 6. 输出结果 ──
    print("\\n" + "=" * 60)
    print("QLib 回测结果摘要")
    print("=" * 60)

    print("\\n【信号质量】")
    print(f"  IC:        {summary['IC']:.6f}" if summary['IC'] is not None else "  IC:        N/A")
    print(f"  ICIR:      {summary['ICIR']:.6f}" if summary['ICIR'] is not None else "  ICIR:      N/A")
    print(f"  Rank IC:   {summary['Rank_IC']:.6f}" if summary['Rank_IC'] is not None else "  Rank IC:   N/A")
    print(f"  Rank ICIR: {summary['Rank_ICIR']:.6f}" if summary['Rank_ICIR'] is not None else "  Rank ICIR: N/A")

    print("\\n【超额收益（不含成本）】")
    ar = summary.get('excess_return_without_cost_annualized')
    ir = summary.get('excess_return_without_cost_IR')
    md = summary.get('excess_return_without_cost_max_drawdown')
    print(f"  年化收益:   {ar*100:.2f}%" if ar is not None else "  年化收益:   N/A")
    print(f"  信息比率:   {ir:.4f}" if ir is not None else "  信息比率:   N/A")
    print(f"  最大回撤:   {md*100:.2f}%" if md is not None else "  最大回撤:   N/A")

    print("\\n【超额收益（含成本）】")
    ar2 = summary.get('excess_return_with_cost_annualized')
    ir2 = summary.get('excess_return_with_cost_IR')
    md2 = summary.get('excess_return_with_cost_max_drawdown')
    print(f"  年化收益:   {ar2*100:.2f}%" if ar2 is not None else "  年化收益:   N/A")
    print(f"  信息比率:   {ir2:.4f}" if ir2 is not None else "  信息比率:   N/A")
    print(f"  最大回撤:   {md2*100:.2f}%" if md2 is not None else "  最大回撤:   N/A")

    if daily_win_stats:
        print("\\n【策略胜率】")
        dwr = daily_win_stats.get("daily_win_rate")
        wwr = daily_win_stats.get("weekly_win_rate")
        print(f"  日胜率:     {dwr*100:.1f}%" if dwr is not None else "  日胜率:     N/A")
        print(f"  周胜率:     {wwr*100:.1f}%" if wwr is not None else "  周胜率:     N/A")
        print(f"  最大连赢:   {daily_win_stats.get('max_consecutive_win', 'N/A')} 天")
        print(f"  最大连亏:   {daily_win_stats.get('max_consecutive_loss', 'N/A')} 天")
        sr = daily_win_stats.get("sharpe_ratio")
        print(f"  夏普比率:   {sr:.4f}" if sr is not None else "  夏普比率:   N/A")

    if stock_trade_stats:
        print("\\n【个股交易统计】")
        print(f"  总交易数:   {stock_trade_stats.get('total_trades', 'N/A')}")
        print(f"  盈利笔数:   {stock_trade_stats.get('winning_trades', 'N/A')}")
        print(f"  亏损笔数:   {stock_trade_stats.get('losing_trades', 'N/A')}")
        swr = stock_trade_stats.get("stock_win_rate")
        print(f"  个股胜率:   {swr*100:.1f}%" if swr is not None else "  个股胜率:   N/A")
        print(f"  平均盈利:   +{stock_trade_stats.get('avg_profit_pct', 'N/A')}%")
        print(f"  平均亏损:   {stock_trade_stats.get('avg_loss_pct', 'N/A')}%")
        print(f"  盈亏比:     {stock_trade_stats.get('profit_loss_ratio', 'N/A')}")
        print(f"  最大单笔盈: +{stock_trade_stats.get('max_single_profit_pct', 'N/A')}%")
        print(f"  最大单笔亏: {stock_trade_stats.get('max_single_loss_pct', 'N/A')}%")
        
        # 止损/止盈触发统计
        sl_count = stock_trade_stats.get("stop_loss_count", 0)
        tp_count = stock_trade_stats.get("take_profit_count", 0)
        if sl_count > 0 or tp_count > 0:
            print("\\n【风控触发统计】")
            if sl_count > 0:
                print(f"  止损触发:   {sl_count} 次")
            if tp_count > 0:
                print(f"  止盈触发:   {tp_count} 次")

        # 新增：盈利最多的前10只股票
        top_profit = stock_trade_stats.get("top_profit_stocks", [])
        if top_profit:
            print("\\n【盈利最多的前10只股票】")
            for i, t in enumerate(top_profit, 1):
                print(f"  {i}. {t['instrument']}: +{t['profit_pct']}% (持仓{t['holding_days']}天, {t['entry_date']}→{t['exit_date']})")

        # 新增：亏损最多的前10只股票
        top_loss = stock_trade_stats.get("top_loss_stocks", [])
        if top_loss:
            print("\\n【亏损最多的前10只股票】")
            for i, t in enumerate(top_loss, 1):
                print(f"  {i}. {t['instrument']}: {t['profit_pct']}% (持仓{t['holding_days']}天, {t['entry_date']}→{t['exit_date']})")

    print("\\n" + "=" * 60)

    # ── 7. 保存完整结果 ──
    all_results = {
        "summary": summary,
        "metrics": metrics,
        "port_analysis": port_analysis,
        "daily_win_stats": daily_win_stats,
        "stock_trade_stats": stock_trade_stats,
        "calmar_ratio": calmar,
    }

    # 保存JSON（供后续agent分析）
    with open("qlib_results.json", "w") as f:
        json.dump(all_results, f, indent=2, default=str)
    print("\\n完整结果已保存到 qlib_results.json")

    # 保存CSV（摘要）
    csv_row = {k: v for k, v in summary.items() if v is not None}
    csv_row.update(daily_win_stats)
    csv_row.update(stock_trade_stats)
    if calmar is not None:
        csv_row["calmar_ratio"] = calmar
    pd.DataFrame([csv_row]).to_csv("qlib_res.csv", index=False)
    print("摘要已保存到 qlib_res.csv")

    return all_results


if __name__ == "__main__":
    read_results()
'''
        (exp_dir / "read_exp_res.py").write_text(template_content, encoding="utf-8")

    def _save_experiment_record(self, experiment_id: str, experiment_name: str,
                                exp_dir: str, factor_names: List[str],
                                model_id: Optional[str], strategy_id: Optional[str],
                                data_split: Dict, custom_params: Optional[Dict],
                                evolution_goal: Optional[str] = None,
                                llm_hypothesis: Optional[Dict] = None) -> None:
        """保存实验记录到数据库。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO qe_experiments
                        (experiment_id, experiment_name, status,
                         factor_names, model_id, strategy_id,
                         data_split, custom_params, workspace_path,
                         evolution_goal, llm_hypothesis, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (experiment_id) DO UPDATE SET
                        experiment_name = EXCLUDED.experiment_name,
                        factor_names = EXCLUDED.factor_names,
                        model_id = EXCLUDED.model_id,
                        strategy_id = EXCLUDED.strategy_id,
                        data_split = EXCLUDED.data_split,
                        custom_params = EXCLUDED.custom_params,
                        evolution_goal = EXCLUDED.evolution_goal,
                        llm_hypothesis = EXCLUDED.llm_hypothesis
                """, (
                    experiment_id, experiment_name, "created",
                    json.dumps(factor_names),
                    model_id, strategy_id,
                    json.dumps(data_split),
                    json.dumps(custom_params) if custom_params else None,
                    exp_dir,
                    evolution_goal,
                    json.dumps(llm_hypothesis) if llm_hypothesis else None,
                ))

    def _init_trace_file(
        self,
        exp_dir: Path,
        experiment_id: str,
        experiment_name: str,
        factor_names: List[str],
        model_id: Optional[str],
        strategy_id: Optional[str],
        custom_params: Optional[Dict],
        llm_hypothesis: Optional[Dict],
    ) -> None:
        """初始化轨迹文件，存储LLM假设和配置信息。"""
        from .qe_evolution_models import QETrace, QELoopRecord, QEHypothesis

        trace_path = exp_dir / "qe_trace.json"

        # 构建配置信息
        configuration = {
            "factors": factor_names,
            "model_id": model_id,
            "strategy_id": strategy_id,
            "strategy_params": custom_params.get("strategy_params", {}) if custom_params else {},
            "custom_params": custom_params,
        }

        # 构建假设
        hypothesis = None
        if llm_hypothesis:
            hypothesis = QEHypothesis(
                hypothesis=llm_hypothesis.get("hypothesis", ""),
                source=llm_hypothesis.get("source", "manual"),
                configuration=configuration,
                reasoning_process=llm_hypothesis.get("reasoning_process"),
                loop_id=1,
                experiment_id=experiment_id,
            )

        # 创建初始LOOP记录
        loop_record = QELoopRecord(
            loop_id=1,
            hypothesis=hypothesis,
            configuration=configuration,
            results={},  # 实验运行后填充
            is_new_sota=False,
        )

        # 创建轨迹
        trace = QETrace(
            trace_id=experiment_name,
            loops=[loop_record],
        )

        # 保存轨迹文件
        trace.to_json_file(str(trace_path))
        logger.info(f"轨迹文件已初始化: {trace_path}")

    def _get_experiment_record(self, experiment_id: str) -> Optional[Dict]:
        """获取实验记录。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM qe_experiments WHERE experiment_id = %s",
                    (experiment_id,),
                )
                row = cur.fetchone()
                if not row:
                    return None
                cols = [desc[0] for desc in cur.description]
                record = dict(zip(cols, row))
                # 兼容字段名映射
                if "workspace_path" in record:
                    record["experiment_dir"] = record["workspace_path"]
                return record

    def _extract_metrics_from_results(self, exp_dir: Path) -> Dict[str, Any]:
        """从实验结果目录提取指标。优先读取qlib_results.json（包含完整交易统计）。"""
        metrics = {}

        # 优先读取qlib_results.json（包含summary + daily_win_stats + stock_trade_stats）
        json_path = exp_dir / "qlib_results.json"
        if json_path.exists():
            try:
                with open(json_path, "r", encoding="utf-8") as f:
                    full_results = json.load(f)
                # 合并所有指标到一个扁平dict
                if "summary" in full_results:
                    metrics.update(full_results["summary"])
                if "daily_win_stats" in full_results:
                    metrics.update(full_results["daily_win_stats"])
                if "stock_trade_stats" in full_results:
                    metrics.update(full_results["stock_trade_stats"])
                if "calmar_ratio" in full_results and full_results["calmar_ratio"] is not None:
                    metrics["calmar_ratio"] = full_results["calmar_ratio"]
                # 保留完整原始数据
                metrics["_raw_json"] = full_results
                logger.info(f"从qlib_results.json读取到 {len(metrics)} 个指标")
                return metrics
            except Exception as e:
                logger.warning(f"读取qlib_results.json失败: {e}")

        # 回退：读取qlib_res.csv
        csv_path = exp_dir / "qlib_res.csv"
        if csv_path.exists():
            try:
                import csv
                with open(csv_path, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        metrics.update(row)
                        break  # 只取第一行
            except Exception as e:
                logger.warning(f"读取qlib_res.csv失败: {e}")

        return metrics

    def _update_experiment_metrics(self, experiment_id: str, metrics: Dict) -> None:
        """更新实验指标。"""
        # 保存到qe_experiments表（排除_raw_json避免过大）
        save_metrics = {k: v for k, v in metrics.items() if k != "_raw_json"}
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE qe_experiments
                    SET result_metrics = %s, status = 'completed', completed_at = NOW(), updated_at = NOW()
                    WHERE experiment_id = %s
                """, (json.dumps(save_metrics, default=str), experiment_id))

    def _save_factor_experiment_metrics(self, experiment_id: str, metrics: Dict) -> None:
        """为实验中的每个因子保存实验表现指标到qe_factor_experiment_metrics表。"""
        exp_record = self._get_experiment_record(experiment_id)
        if not exp_record:
            logger.warning(f"保存因子实验指标失败：实验 {experiment_id} 不存在")
            return

        factor_names = exp_record.get("factor_names") or []
        if isinstance(factor_names, str):
            try:
                factor_names = json.loads(factor_names)
            except Exception:
                factor_names = [factor_names]

        if not factor_names:
            logger.info(f"实验 {experiment_id} 没有关联因子，跳过因子指标保存")
            return

        experiment_name = exp_record.get("experiment_name", "")
        model_id = exp_record.get("model_id")
        data_split = exp_record.get("data_split")

        # 提取各类指标
        def _float(val):
            if val is None:
                return None
            try:
                return float(val)
            except (ValueError, TypeError):
                return None

        def _int(val):
            if val is None:
                return None
            try:
                return int(val)
            except (ValueError, TypeError):
                return None

        # 原始完整指标（备份）
        raw_metrics = metrics.get("_raw_json") or {k: v for k, v in metrics.items() if k != "_raw_json"}

        with get_conn() as conn:
            with conn.cursor() as cur:
                for factor_name in factor_names:
                    # 确定因子来源
                    factor_source = self._detect_factor_source(cur, factor_name)
                    other_factors = [f for f in factor_names if f != factor_name]

                    try:
                        cur.execute("""
                            INSERT INTO qe_factor_experiment_metrics (
                                factor_name, factor_source, experiment_id, experiment_name,
                                ic, icir, rank_ic, rank_icir,
                                ann_return_no_cost, info_ratio_no_cost, max_drawdown_no_cost,
                                ann_return_with_cost, info_ratio_with_cost, max_drawdown_with_cost,
                                daily_win_rate, weekly_win_rate, max_consecutive_win, max_consecutive_loss,
                                total_trades, winning_trades, losing_trades, stock_win_rate,
                                avg_profit_pct, avg_loss_pct, profit_loss_ratio,
                                max_single_profit_pct, max_single_loss_pct,
                                sharpe_ratio, calmar_ratio, avg_turnover, total_trading_days,
                                model_id, other_factors, data_split, raw_metrics
                            ) VALUES (
                                %s, %s, %s, %s,
                                %s, %s, %s, %s,
                                %s, %s, %s,
                                %s, %s, %s,
                                %s, %s, %s, %s,
                                %s, %s, %s, %s,
                                %s, %s, %s,
                                %s, %s,
                                %s, %s, %s, %s,
                                %s, %s, %s, %s
                            )
                            ON CONFLICT (factor_name, factor_source, experiment_id)
                            DO UPDATE SET
                                experiment_name = EXCLUDED.experiment_name,
                                ic = EXCLUDED.ic, icir = EXCLUDED.icir,
                                rank_ic = EXCLUDED.rank_ic, rank_icir = EXCLUDED.rank_icir,
                                ann_return_no_cost = EXCLUDED.ann_return_no_cost,
                                info_ratio_no_cost = EXCLUDED.info_ratio_no_cost,
                                max_drawdown_no_cost = EXCLUDED.max_drawdown_no_cost,
                                ann_return_with_cost = EXCLUDED.ann_return_with_cost,
                                info_ratio_with_cost = EXCLUDED.info_ratio_with_cost,
                                max_drawdown_with_cost = EXCLUDED.max_drawdown_with_cost,
                                daily_win_rate = EXCLUDED.daily_win_rate,
                                weekly_win_rate = EXCLUDED.weekly_win_rate,
                                max_consecutive_win = EXCLUDED.max_consecutive_win,
                                max_consecutive_loss = EXCLUDED.max_consecutive_loss,
                                total_trades = EXCLUDED.total_trades,
                                winning_trades = EXCLUDED.winning_trades,
                                losing_trades = EXCLUDED.losing_trades,
                                stock_win_rate = EXCLUDED.stock_win_rate,
                                avg_profit_pct = EXCLUDED.avg_profit_pct,
                                avg_loss_pct = EXCLUDED.avg_loss_pct,
                                profit_loss_ratio = EXCLUDED.profit_loss_ratio,
                                max_single_profit_pct = EXCLUDED.max_single_profit_pct,
                                max_single_loss_pct = EXCLUDED.max_single_loss_pct,
                                sharpe_ratio = EXCLUDED.sharpe_ratio,
                                calmar_ratio = EXCLUDED.calmar_ratio,
                                avg_turnover = EXCLUDED.avg_turnover,
                                total_trading_days = EXCLUDED.total_trading_days,
                                model_id = EXCLUDED.model_id,
                                other_factors = EXCLUDED.other_factors,
                                data_split = EXCLUDED.data_split,
                                raw_metrics = EXCLUDED.raw_metrics,
                                collected_at = NOW()
                        """, (
                            factor_name, factor_source, experiment_id, experiment_name,
                            _float(metrics.get("IC")),
                            _float(metrics.get("ICIR")),
                            _float(metrics.get("Rank_IC")),
                            _float(metrics.get("Rank_ICIR")),
                            _float(metrics.get("excess_return_without_cost_annualized")),
                            _float(metrics.get("excess_return_without_cost_IR")),
                            _float(metrics.get("excess_return_without_cost_max_drawdown")),
                            _float(metrics.get("excess_return_with_cost_annualized")),
                            _float(metrics.get("excess_return_with_cost_IR")),
                            _float(metrics.get("excess_return_with_cost_max_drawdown")),
                            _float(metrics.get("daily_win_rate")),
                            _float(metrics.get("weekly_win_rate")),
                            _int(metrics.get("max_consecutive_win")),
                            _int(metrics.get("max_consecutive_loss")),
                            _int(metrics.get("total_trades")),
                            _int(metrics.get("winning_trades")),
                            _int(metrics.get("losing_trades")),
                            _float(metrics.get("stock_win_rate")),
                            _float(metrics.get("avg_profit_pct")),
                            _float(metrics.get("avg_loss_pct")),
                            _float(metrics.get("profit_loss_ratio")),
                            _float(metrics.get("max_single_profit_pct")),
                            _float(metrics.get("max_single_loss_pct")),
                            _float(metrics.get("sharpe_ratio")),
                            _float(metrics.get("calmar_ratio")),
                            _float(metrics.get("avg_turnover")),
                            _int(metrics.get("total_trading_days")),
                            model_id,
                            json.dumps(other_factors) if other_factors else None,
                            json.dumps(data_split, default=str) if data_split else None,
                            json.dumps(raw_metrics, default=str),
                        ))
                        logger.info(f"保存因子实验指标: {factor_name} @ {experiment_id}")
                    except Exception as e:
                        logger.error(f"保存因子 {factor_name} 实验指标失败: {e}")

    def _detect_factor_source(self, cur, factor_name: str) -> str:
        """检测因子来源。"""
        try:
            cur.execute(
                "SELECT source FROM aistock_factor_catalog WHERE factor_name = %s LIMIT 1",
                (factor_name,),
            )
            row = cur.fetchone()
            if row:
                return row[0]
        except Exception:
            pass
        return "unknown"
