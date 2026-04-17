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

import httpx

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

# ── QE 专用程序目录（模板文件、脚本等，独立于 RD-Agent 核心代码） ──
QE_PROGRAMS_WIN = Path(os.getenv(
    "QE_PROGRAMS_WIN",
    "F:/Dev/RD-Agent-main/qe_programs"
))
QE_PROGRAMS_WSL = os.getenv(
    "QE_PROGRAMS_WSL",
    "/mnt/f/Dev/RD-Agent-main/qe_programs"
)

# 旧的AIstock侧实验目录（保留兼容）
QE_EXPERIMENTS_ROOT = Path(os.getenv(
    "QE_EXPERIMENTS_ROOT",
    "f:/Dev/AIstock/rdagent_assets/qe_experiments"
))

# ── RDAgent 因子数据目录（与 FACTOR_CoSTEER_data_folder 保持一致，包含 sw2 行业数据） ──
RDAGENT_FACTOR_DATA_WSL = os.getenv(
    "RDAGENT_FACTOR_DATA_WSL",
    "/mnt/f/dev/RD-Agent-main/git_ignore_folder/factor_implementation_source_data"
)

# ── QLib 数据路径（与 RDAgent conf_baseline.yaml 一致） ──
QLIB_DATA_PATH_WSL = os.getenv(
    "QLIB_DATA_PATH_WSL",
    "/home/lc999/data/qlib_bin"
)
QLIB_MINUTE_PATH_WSL = os.getenv(
    "QLIB_MINUTE_PATH_WSL",
    "/home/lc999/data/qlib_minute_bin"
)

# ── RDAgent 源码根目录（用于子进程 PYTHONPATH，执行节点 .env 中定义） ──
RDAGENT_CODE_ROOT_WSL = os.getenv(
    "QLIB_RDAGENT_ROOT_WSL",
    "/mnt/f/Dev/RD-Agent-main"
)

# ── RDAgent 默认数据时间段（与 rdagent conf_baseline.yaml 保持一致） ──
RDAGENT_DEFAULT_DATA_SPLIT = {
    "train_start": "2018-08-01",
    "train_end": "2022-12-31",
    "valid_start": "2023-01-01",
    "valid_end": "2024-06-30",
    "test_start": "2024-07-01",
    "test_end": "2026-03-10",
    # backtest_end 不再硬编码，在 compose 时从 test_end 自动计算 -7 天
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
    "num_threads": 24,
}


class ConfigComposer:
    """配置组装器。"""

    _workspace_config_cache: Optional[Dict[str, str]] = None

    @staticmethod
    def _validate_data_split(data_split: Dict[str, str]):
        """校验 data_split 日期合法性。"""
        from datetime import datetime
        required = ["train_start", "train_end", "valid_start", "valid_end", "test_start", "test_end"]
        for key in required:
            if key not in data_split:
                raise ValueError(f"data_split 缺少必填字段: {key}")
            try:
                datetime.strptime(data_split[key], "%Y-%m-%d")
            except ValueError as e:
                raise ValueError(f"data_split[{key}] 日期格式错误: {data_split[key]}，应为 YYYY-MM-DD") from e

        dates = {k: datetime.strptime(data_split[k], "%Y-%m-%d") for k in required}
        if dates["train_end"] > dates["valid_start"]:
            raise ValueError(f"train_end ({data_split['train_end']}) 不能晚于 valid_start ({data_split['valid_start']})")
        if dates["valid_end"] > dates["test_start"]:
            raise ValueError(f"valid_end ({data_split['valid_end']}) 不能晚于 test_start ({data_split['test_start']})")
        if dates["test_end"] > datetime.now():
            raise ValueError(f"test_end ({data_split['test_end']}) 不能超过当前日期")

    @staticmethod
    def _ensure_backtest_end(data_split: Dict[str, str]):
        """从 test_end 自动派生 backtest_end（-7 天），确保回测结束日早于 Qlib 日历最后一条记录。

        Qlib 回测在每步执行 calendar[index+1]，若 backtest_end 是日历最后一天则越界。
        用 -7 自然日（约 5 个交易日）确保安全边距，跨过周末和节假日。
        """
        from datetime import datetime, timedelta
        if "test_end" in data_split:
            test_end_dt = datetime.strptime(data_split["test_end"], "%Y-%m-%d")
            data_split["backtest_end"] = (test_end_dt - timedelta(days=7)).strftime("%Y-%m-%d")

    def _fetch_workspace_config(self, node_id: Optional[str] = None) -> Dict[str, str]:
        """
        获取 QE 工作区路径配置。
        所有路径直接从 AIstock 侧 .env 环境变量读取，不再依赖 RDAgent API。
        node_id: 指定节点时从 infra.compute_nodes 查询路径，None 则使用默认常量。
        """
        if node_id:
            return self._get_node_paths(node_id)

        if ConfigComposer._workspace_config_cache is not None:
            return ConfigComposer._workspace_config_cache

        ConfigComposer._workspace_config_cache = {
            "workspace_base": QE_WORKSPACE_WSL,
            "factor_data_dir": RDAGENT_FACTOR_DATA_WSL,
            "qlib_data_path": QLIB_DATA_PATH_WSL,
            "qlib_minute_path": QLIB_MINUTE_PATH_WSL,
            "qlib_rdagent_root": RDAGENT_CODE_ROOT_WSL,
        }
        return ConfigComposer._workspace_config_cache

    def _get_node_paths(self, node_id: str) -> Dict[str, str]:
        """查询目标节点的路径配置。所有路径字段必须已配置，缺失则报错。"""
        from ...db.pg_pool import get_conn
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT workspace_base, factor_data_dir, qlib_data_path, "
                    "       qlib_minute_path, qlib_rdagent_root "
                    "FROM infra.compute_nodes WHERE node_id = %s",
                    (node_id,),
                )
                row = cur.fetchone()
        if not row or not row[0]:
            raise ValueError(f"节点 {node_id} 未配置 workspace_base，请先在节点管理中设置路径")
        missing = []
        if not row[1]:
            missing.append("factor_data_dir")
        if not row[2]:
            missing.append("qlib_data_path")
        if not row[3]:
            missing.append("qlib_minute_path")
        if not row[4]:
            missing.append("qlib_rdagent_root")
        if missing:
            raise ValueError(f"节点 {node_id} 缺少必要路径配置: {', '.join(missing)}，请在节点管理中补全")
        return {
            "workspace_base": row[0],
            "factor_data_dir": row[1],
            "qlib_data_path": row[2],
            "qlib_minute_path": row[3],
            "qlib_rdagent_root": row[4],
        }

    def _generate_unique_experiment_id(self) -> str:
        """生成基于日期时间的唯一实验ID，格式: qe_YYYYMMDD_HHMMSS"""
        from datetime import datetime
        base_id = f"qe_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM qe_experiments WHERE experiment_id = %s",
                    (base_id,),
                )
                if not cur.fetchone():
                    return base_id
                # 极端冲突：追加序号
                for i in range(2, 100):
                    candidate = f"{base_id}_{i}"
                    cur.execute(
                        "SELECT 1 FROM qe_experiments WHERE experiment_id = %s",
                        (candidate,),
                    )
                    if not cur.fetchone():
                        return candidate
        raise RuntimeError(f"无法生成唯一实验ID: {base_id}")

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
        experiment_id = self._generate_unique_experiment_id()
        experiment_name = experiment_id  # 两者统一

        # 创建实验目录（在 AIstock 侧本地保存一份）
        exp_dir = QE_EXPERIMENTS_ROOT / experiment_name
        exp_dir.mkdir(parents=True, exist_ok=True)

        # 通过API在RDAgent侧创建实验工作区并链接数据文件
        self._api_setup_experiment_workspace(experiment_name)

        # 默认数据划分（与 RDAgent conf_baseline.yaml 一致）
        if not data_split:
            data_split = dict(RDAGENT_DEFAULT_DATA_SPLIT)
        self._validate_data_split(data_split)
        self._ensure_backtest_end(data_split)

        # 获取因子信息
        factors_info = self._get_factors_info(factor_names, factor_sources)

        # 获取模型信息
        model_info = self._get_model_info(model_id) if model_id else None

        # 获取策略信息
        strategy_info = self._get_strategy_info(strategy_id) if strategy_id else None

        # 判断因子类型：是否包含需要执行代码的自定义因子
        has_custom_factors = any(
            f.get("code_text") for f in factors_info
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
                    raise ValueError(
                        f"模型 '{model_info.get('model_name', '?')}' 的 model_type='{model_type_raw}' "
                        f"不在已知类型中 (TimeSeries/Tabular)，请更新模型目录中的 model_type 字段"
                    )

        # 从custom_params提取disable_alpha158和quick_train参数
        disable_alpha158 = False
        quick_train = False  # 快速训练模式：训练时间缩短到20%
        if custom_params:
            disable_alpha158 = custom_params.get("disable_alpha158", False)
            quick_train = custom_params.get("quick_train", False)

        # backtest_freq: "1min"（分钟线，默认）或 "day"（日线回退模式）
        backtest_freq = (custom_params or {}).get("backtest_freq", "1min")
        execution_algo = (custom_params or {}).get("execution_algo")
        execution_algo_params = dict((custom_params or {}).get("execution_algo_params") or {})

        # 尾盘涨停未成交处理：从 custom_params 提取并注入到 execution_algo_params
        # （这些参数是给 inner_strategy TailTWAPWithLimitStrategy 的）
        _cp = custom_params or {}
        if _cp.get("unfilled_handler"):
            execution_algo_params["unfilled_handler"] = _cp["unfilled_handler"]
        if _cp.get("unfilled_trigger_minute"):
            execution_algo_params["unfilled_trigger_minute"] = _cp["unfilled_trigger_minute"]
        if _cp.get("unfilled_backup_depth"):
            execution_algo_params["unfilled_backup_depth"] = _cp["unfilled_backup_depth"]

        # HMM 预计算（必须在 conf.yaml 之前，使 hmm_coefficients_file 写入策略 kwargs）
        hmm_json_content: Optional[str] = None
        if _cp.get("enable_sector_hmm"):
            # 构造 strategy_params 供 _precompute_hmm_coefficients 使用
            _hmm_sp = dict(custom_params or {})
            hmm_json_content = self._precompute_hmm_coefficients(_hmm_sp, data_split)
            custom_params["hmm_coefficients_file"] = "hmm_sector_coefficients.json"

            # 严格验证：策略必须原生支持 HMM
            _hmm_supported_classes = {
                "TopkDropoutWithRiskControlStrategy",
                "ScoreWeightedTopkStrategy",
                "ScoreWeightedTopkStrategyV2",
                "EnhancedTopkDropoutStrategy",
                "SmallCapTopkDropoutStrategy",
            }
            _strategy_class = None
            if strategy_info:
                _pc = strategy_info.get("portfolio_config")
                if isinstance(_pc, str):
                    import json as _json2
                    _pc = _json2.loads(_pc)
                if _pc:
                    _strategy_class = _pc.get("class")
                if not _strategy_class:
                    _strategy_class = strategy_info.get("strategy_name", "")
            if _strategy_class and _strategy_class not in _hmm_supported_classes:
                raise ValueError(
                    f"enable_sector_hmm=True 但策略 '{_strategy_class}' 不支持 HMM。"
                    f"支持的策略: {', '.join(sorted(_hmm_supported_classes))}"
                )

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
            backtest_freq=backtest_freq,
            execution_algo=execution_algo,
            execution_algo_params=execution_algo_params,
        )

        # 生成因子文件和预处理脚本（如果有自定义因子）
        has_factor_files = False
        prepare_factors_py = None
        if has_custom_factors:
            factor_marker = self._compose_factor_file(factors_info)
            has_factor_files = factor_marker is not None
            prepare_factors_py = self._compose_prepare_factors(factors_info, data_split=data_split)

        # 保存文件
        conf_path = exp_dir / "conf.yaml"
        conf_path.write_text(conf_yaml, encoding="utf-8")

        # 写入 HMM 系数文件（如果启用了 HMM）
        if hmm_json_content:
            hmm_path = exp_dir / "hmm_sector_coefficients.json"
            hmm_path.write_text(hmm_json_content, encoding="utf-8")

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

        # 复制 qrun_limit runner（分钟线使用 qrun_limit_minute.py，日线使用 qrun_limit.py）
        scripts_dir = Path(__file__).parent.parent.parent.parent / "scripts"
        import shutil
        if backtest_freq != "day":
            # 分钟线：复制 qrun_limit_minute.py（含内存 patch + benchmark 注入）
            minute_src = scripts_dir / "qrun_limit_minute.py"
            if minute_src.exists():
                shutil.copy2(minute_src, exp_dir / "qrun_limit_minute.py")
            # 复制 TailTWAPWithLimitStrategy（分钟级执行策略）
            twap_src = scripts_dir / "tail_twap_strategy.py"
            if twap_src.exists():
                shutil.copy2(twap_src, exp_dir / "tail_twap_strategy.py")
            # 复制 v24 Plan 执行策略（依赖 tail_twap_strategy.py）
            v24_src = scripts_dir / "tail_twap_v24_strategy.py"
            if v24_src.exists():
                shutil.copy2(v24_src, exp_dir / "tail_twap_v24_strategy.py")
            # benchmark parquet
            bench_src = scripts_dir / "benchmark_sh000300.parquet"
            if bench_src.exists():
                shutil.copy2(bench_src, exp_dir / "benchmark_sh000300.parquet")
                # 同时复制到 qe_workspace（实际执行目录，qrun_limit_minute.py 从此目录加载）
                ws_dir = QE_WORKSPACE_WIN / experiment_name
                if ws_dir.exists():
                    shutil.copy2(bench_src, ws_dir / "benchmark_sh000300.parquet")
        # 始终复制日线版作为 fallback
        qrun_limit_src = scripts_dir / "qrun_limit.py"
        if qrun_limit_src.exists():
            shutil.copy2(qrun_limit_src, exp_dir / "qrun_limit.py")
        # benchmark parquet 也复制到日线实验的 qe_workspace（qrun_limit.py 同样需要）
        bench_src = scripts_dir / "benchmark_sh000300.parquet"
        if bench_src.exists():
            ws_dir = QE_WORKSPACE_WIN / experiment_name
            if ws_dir.exists() and not (ws_dir / "benchmark_sh000300.parquet").exists():
                shutil.copy2(bench_src, ws_dir / "benchmark_sh000300.parquet")

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
            use_custom_model=bool(model_info and model_info.get("code_text")),
            model_type_tag=model_type_tag if model_info and model_info.get("code_text") else None,
            backtest_freq=backtest_freq,
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

    def compose_experiment_in_memory(
        self,
        factor_names: List[str],
        factor_sources: Optional[Dict[str, str]] = None,
        model_id: Optional[str] = None,
        strategy_id: Optional[str] = None,
        data_split: Optional[Dict[str, str]] = None,
        custom_params: Optional[Dict[str, Any]] = None,
        experiment_name: Optional[str] = None,
        evolution_goal: Optional[str] = None,
        llm_hypothesis: Optional[Dict[str, Any]] = None,
        skip_db_save: bool = False,
        execution_algo: Optional[str] = None,
        execution_algo_params: Optional[Dict[str, Any]] = None,
        strategy_params: Optional[Dict[str, Any]] = None,
        node_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """组装实验配置到内存字典，不写入磁盘。

        复用现有生成逻辑，但将所有文件内容收集到 Dict[str, str]，
        供演进循环通过 RDAgent loop API 的 experiment_files 参数直接传递。

        Returns:
            {
                "experiment_files": Dict[str, str],  # 相对路径 -> 文件内容
                "wsl_command": str,
                "experiment_name": str,
                "experiment_id": str,
                "factor_count": int,
                "has_custom_factors": bool,
            }
        """
        import uuid as _uuid

        experiment_id = str(_uuid.uuid4())[:8]
        if not experiment_name:
            experiment_name = f"qe_exp_{experiment_id}"

        if not data_split:
            data_split = dict(RDAGENT_DEFAULT_DATA_SPLIT)
        self._validate_data_split(data_split)
        self._ensure_backtest_end(data_split)

        # ── 获取因子 / 模型 / 策略信息 ──
        factors_info = self._get_factors_info(factor_names, factor_sources)
        model_info = self._get_model_info(model_id) if model_id else None
        strategy_info = self._get_strategy_info(strategy_id) if strategy_id else None

        has_custom_factors = any(f.get("code_text") for f in factors_info)
        has_alpha158 = any(f.get("source") == "alpha158" for f in factors_info)
        has_alpha360 = any(f.get("source") == "alpha360" for f in factors_info)

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
                    raise ValueError(
                        f"模型 '{model_info.get('model_name', '?')}' 的 model_type='{model_type_raw}' "
                        f"不在已知类型中 (TimeSeries/Tabular)，请更新模型目录中的 model_type 字段"
                    )

        disable_alpha158 = False
        quick_train = False
        if custom_params:
            disable_alpha158 = custom_params.get("disable_alpha158", False)
            quick_train = custom_params.get("quick_train", False)

        # backtest_freq: "1min"（分钟线，默认）或 "day"（日线回退模式）
        backtest_freq = (custom_params or {}).get("backtest_freq", "1min")

        # 尾盘涨停未成交处理：从 custom_params 提取并注入到 execution_algo_params
        # （这些参数是给 inner_strategy TailTWAPWithLimitStrategy 的）
        _cp = custom_params or {}
        if execution_algo_params is None:
            execution_algo_params = {}
        else:
            execution_algo_params = dict(execution_algo_params)
        if _cp.get("unfilled_handler"):
            execution_algo_params["unfilled_handler"] = _cp["unfilled_handler"]
        if _cp.get("unfilled_trigger_minute"):
            execution_algo_params["unfilled_trigger_minute"] = _cp["unfilled_trigger_minute"]
        if _cp.get("unfilled_backup_depth"):
            execution_algo_params["unfilled_backup_depth"] = _cp["unfilled_backup_depth"]

        # ── 获取路径配置（支持多节点） ──
        rdagent_cfg = self._fetch_workspace_config(node_id)
        workspace_wsl = rdagent_cfg.get("workspace_base", QE_WORKSPACE_WSL)
        qlib_data_path = rdagent_cfg.get("qlib_data_path", QLIB_DATA_PATH_WSL)
        factor_data_dir = rdagent_cfg.get("factor_data_dir", RDAGENT_FACTOR_DATA_WSL)
        qlib_minute_path = rdagent_cfg.get("qlib_minute_path", QLIB_MINUTE_PATH_WSL)

        # ── 生成各文件内容到 dict ──
        experiment_files: Dict[str, str] = {}

        # 0) HMM 预计算（必须在 conf.yaml 之前，使 hmm_coefficients_file 写入策略 kwargs）
        # 与 compose_experiment() 一致，从 custom_params 检查 enable_sector_hmm
        if _cp.get("enable_sector_hmm"):
            # 构造 strategy_params 供 _precompute_hmm_coefficients 使用
            _hmm_sp = dict(_cp)
            hmm_json = self._precompute_hmm_coefficients(_hmm_sp, data_split)
            experiment_files["hmm_sector_coefficients.json"] = hmm_json
            # 注入到 custom_params 以便 _compose_conf_yaml 写入 strategy kwargs
            if custom_params is None:
                custom_params = {}
            custom_params["hmm_coefficients_file"] = "hmm_sector_coefficients.json"

            # 严格验证：策略必须原生支持 HMM，禁止静默替换策略
            _hmm_supported_classes = {
                "TopkDropoutWithRiskControlStrategy",
                "ScoreWeightedTopkStrategy",
                "ScoreWeightedTopkStrategyV2",
                "EnhancedTopkDropoutStrategy",
                "SmallCapTopkDropoutStrategy",
            }
            _strategy_class = None
            if strategy_info:
                _pc = strategy_info.get("portfolio_config")
                if isinstance(_pc, str):
                    import json as _json
                    _pc = _json.loads(_pc)
                if _pc:
                    _strategy_class = _pc.get("class")
                if not _strategy_class:
                    _sc = strategy_info.get("source_code", "")
                    import re as _re
                    _class_match = _re.search(r'class\s+(\w+)\s*\(', _sc)
                    if _class_match:
                        _strategy_class = _class_match.group(1)

            if _strategy_class and _strategy_class not in _hmm_supported_classes:
                raise ValueError(
                    f"enable_sector_hmm=True 但策略 '{_strategy_class}' 不支持 HMM。"
                    f"已支持 HMM 的策略: {', '.join(sorted(_hmm_supported_classes))}。"
                    f"请切换到支持 HMM 的策略或关闭 HMM。"
                )

        # 1) conf.yaml
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
            qlib_data_path=qlib_data_path,
            qlib_minute_path=qlib_minute_path,
            backtest_freq=backtest_freq,
            execution_algo=execution_algo,
            execution_algo_params=execution_algo_params,
            initial_cash=(strategy_params or {}).get("initial_cash"),
        )
        experiment_files["conf.yaml"] = conf_yaml

        # 2) 因子文件 + prepare_factors.py
        if has_custom_factors:
            # 因子代码文件 → factors/<name>.py
            for f in factors_info:
                code = f.get("code_text")
                if code:
                    experiment_files[f"factors/{f['factor_name']}.py"] = code

            prepare_factors_py = self._compose_prepare_factors(factors_info, factor_data_dir=factor_data_dir, data_split=data_split)
            if prepare_factors_py:
                experiment_files["prepare_factors.py"] = prepare_factors_py

        # 3) qe_custom_loaders.py（仅 disable_alpha158 时需要）
        if has_custom_factors and disable_alpha158:
            qe_loader_source = Path(__file__).parent / "qe_custom_loaders.py"
            if qe_loader_source.exists():
                experiment_files["qe_custom_loaders.py"] = qe_loader_source.read_text(encoding="utf-8")

        # 4) read_exp_res.py
        experiment_files["read_exp_res.py"] = self._get_read_exp_res_content()

        # 4b) qrun_limit runner（分钟线/日线双模板）
        scripts_dir = Path(__file__).parent.parent.parent.parent / "scripts"
        qrun_limit_path = scripts_dir / "qrun_limit.py"
        if qrun_limit_path.exists():
            experiment_files["qrun_limit.py"] = qrun_limit_path.read_text(encoding="utf-8")
        if backtest_freq != "day":
            minute_path = scripts_dir / "qrun_limit_minute.py"
            if minute_path.exists():
                experiment_files["qrun_limit_minute.py"] = minute_path.read_text(encoding="utf-8")
            # TailTWAPWithLimitStrategy（分钟级执行策略）
            twap_path = scripts_dir / "tail_twap_strategy.py"
            if twap_path.exists():
                experiment_files["tail_twap_strategy.py"] = twap_path.read_text(encoding="utf-8")
            # v24 Plan 执行策略（继承 TailTWAPWithLimitStrategy）
            v24_path = scripts_dir / "tail_twap_v24_strategy.py"
            if v24_path.exists():
                experiment_files["tail_twap_v24_strategy.py"] = v24_path.read_text(encoding="utf-8")
            # benchmark parquet 是二进制文件，需要特殊处理
            bench_path = scripts_dir / "benchmark_sh000300.parquet"
            if bench_path.exists():
                import base64
                experiment_files["benchmark_sh000300.parquet.b64"] = base64.b64encode(
                    bench_path.read_bytes()
                ).decode("ascii")

        # 5) model.py（自定义模型）
        if model_info and model_info.get("code_text"):
            experiment_files["model.py"] = self._build_model_py_content(model_info)

        # 6) custom_strategy.py（自定义策略）
        if strategy_info and strategy_info.get("source_code"):
            strategy_content, strategy_deps = self._build_strategy_py_content(strategy_info)
            experiment_files["custom_strategy.py"] = strategy_content
            for dep_name, dep_content in strategy_deps.items():
                experiment_files[dep_name] = dep_content

        # 7) hmm_sector_coefficients.json — 已在步骤 0 提前处理

        # ── 生成 WSL 命令 ──
        wsl_path = f"{workspace_wsl}/{experiment_name}"
        wsl_command = self._generate_wsl_command(
            wsl_path,
            has_custom_factors=has_custom_factors,
            use_custom_model=bool(model_info and model_info.get("code_text")),
            model_type_tag=model_type_tag if model_info and model_info.get("code_text") else None,
            mode="auto",
            backtest_freq=backtest_freq,
        )

        # ── 保存 DB 记录（不写文件） ──
        if not skip_db_save:
            self._save_experiment_record(
                experiment_id=experiment_id,
                experiment_name=experiment_name,
                exp_dir=f"<in_memory>/{experiment_name}",
                factor_names=factor_names,
                model_id=model_id,
                strategy_id=strategy_id,
                data_split=data_split,
                custom_params=custom_params,
                evolution_goal=evolution_goal,
                llm_hypothesis=llm_hypothesis,
            )

        return {
            "experiment_files": experiment_files,
            "wsl_command": wsl_command,
            "experiment_name": experiment_name,
            "experiment_id": experiment_id,
            "factor_count": len(factor_names),
            "has_custom_factors": has_custom_factors,
        }

    # ── 内存生成辅助方法 ──

    def _build_model_py_content(self, model_info: Dict) -> str:
        """生成 model.py 文件内容（不写磁盘）。"""
        code_text = model_info.get("code_text", "")
        model_name = model_info.get("model_name", "CustomModel")
        class_match = re.search(r'class\s+(\w+)\s*\(', code_text)
        nn_class_name = class_match.group(1) if class_match else model_name
        return f'"""\nRDAgent SOTA模型: {model_name}\nQuantEvolver自动生成 - 由 GeneralPTNN 通过 pt_model_uri: "model.model_cls" 加载\n"""\n{code_text}\n\n# GeneralPTNN 通过此变量加载 NN 类\nmodel_cls = {nn_class_name}\n'

    def _build_strategy_py_content(self, strategy_info: Dict) -> tuple:
        """生成 custom_strategy.py 文件内容（不写磁盘）。

        复用 _write_custom_strategy 中的验证和 import 处理逻辑。

        Returns:
            (source_code, deps_dict) — deps_dict: {filename: content} 依赖文件
        """
        source_code = strategy_info.get("source_code", "")
        if not source_code:
            raise ValueError(
                f"策略 '{strategy_info.get('strategy_id', '?')}' 没有源代码 (source_code)，"
                f"无法生成策略文件。请检查策略目录。"
            )

        validation_result = self._validate_strategy_code(source_code)
        if not validation_result["ok"]:
            raise ValueError(
                f"策略代码编译验证失败:\n{validation_result['error']}\n"
                f"请修复策略代码后再创建实验。"
            )

        # 处理相对导入：读取依赖文件，转换为本地导入
        import re as _re
        strategy_pkg_dir = Path(__file__).parent.parent / "rebalance_strategies"
        # factor_template 目录：score_weighted_strategy.py 等策略基类所在位置
        _rdagent_root = QE_WORKSPACE_WIN.parent  # F:/Dev/RD-Agent-main
        factor_template_dir = _rdagent_root / "rdagent" / "scenarios" / "qlib" / "experiment" / "factor_template"
        # 只允许复制策略类文件，避免误复制 qrun_limit.py / read_exp_res.py 等运行时文件
        _STRATEGY_DEP_WHITELIST = {"score_weighted_strategy", "score_weighted_strategy_v2",
                                   "tail_twap_strategy", "tail_twap_v24_strategy"}
        deps_dict: Dict[str, str] = {}

        def _resolve_deps(code: str, collected: set) -> str:
            """递归处理相对导入并收集依赖文件内容."""
            out_lines = []
            for ln in code.split("\n"):
                s = ln.strip()
                # 匹配 from .module import ... (相对导入)
                m = _re.match(r'^(\s*)from\s+\.(\w+)\s+import\s+(.+)$', s)
                if m:
                    indent, mod, imps = m.group(1), m.group(2), m.group(3)
                    # 先查 rebalance_strategies，再查 factor_template（仅白名单模块）
                    dep = strategy_pkg_dir / f"{mod}.py"
                    if not dep.exists() and mod in _STRATEGY_DEP_WHITELIST:
                        dep = factor_template_dir / f"{mod}.py"
                    if dep.exists() and mod not in collected:
                        collected.add(mod)
                        dep_code = dep.read_text(encoding="utf-8")
                        dep_code = _resolve_deps(dep_code, collected)
                        deps_dict[f"{mod}.py"] = dep_code
                    out_lines.append(f"{indent}from {mod} import {imps}")
                    continue
                # 匹配 from module import ... (无点号，检查是否为本地策略包模块)
                m2 = _re.match(r'^(\s*)from\s+(\w+)\s+import\s+(.+)$', s)
                if m2:
                    indent, mod, imps = m2.group(1), m2.group(2), m2.group(3)
                    # 先查 rebalance_strategies，再查 factor_template（仅白名单模块）
                    dep = strategy_pkg_dir / f"{mod}.py"
                    if not dep.exists() and mod in _STRATEGY_DEP_WHITELIST:
                        dep = factor_template_dir / f"{mod}.py"
                    if dep.exists() and mod not in collected:
                        collected.add(mod)
                        dep_code = dep.read_text(encoding="utf-8")
                        dep_code = _resolve_deps(dep_code, collected)
                        deps_dict[f"{mod}.py"] = dep_code
                    out_lines.append(ln)
                    continue
                if s == "@register":
                    continue
                out_lines.append(ln)
            return "\n".join(out_lines)

        collected_mods: set = set()
        source_code = _resolve_deps(source_code, collected_mods)

        # 自动添加缺失的 import
        required_imports = ["import pandas as pd", "import numpy as np"]
        import_lines = [imp for imp in required_imports if imp not in source_code]
        if import_lines:
            lines = source_code.split("\n")
            insert_pos = 0
            in_docstring = False
            for i, line in enumerate(lines):
                stripped = line.strip()
                if not stripped:
                    insert_pos = i + 1; continue
                if stripped.startswith("#"):
                    insert_pos = i + 1; continue
                if '"""' in stripped or "'''" in stripped:
                    in_docstring = not in_docstring
                    insert_pos = i + 1; continue
                if in_docstring:
                    insert_pos = i + 1; continue
                if stripped.startswith("import ") or stripped.startswith("from "):
                    insert_pos = i + 1; continue
                break
            for imp in import_lines:
                lines.insert(insert_pos, imp)
                insert_pos += 1
            source_code = "\n".join(lines)

        # 移除函数内部的重复 import
        lines = source_code.split("\n")
        cleaned_lines = []
        in_function = False
        indent_level = 0
        for i, line in enumerate(lines):
            stripped = line.strip()
            if re.match(r'^(def |async def |class )', stripped):
                in_function = True
                indent_level = len(line) - len(line.lstrip())
            elif in_function and line and not line[0].isspace():
                in_function = False
            elif in_function and line:
                current_indent = len(line) - len(line.lstrip())
                if current_indent <= indent_level and stripped and not stripped.startswith('#'):
                    in_function = False
            if in_function and (stripped.startswith("import pandas") or
                                stripped.startswith("import numpy")):
                continue
            cleaned_lines.append(line)
        return "\n".join(cleaned_lines), deps_dict

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
                raise RuntimeError(f"读取qlib_res.csv失败: {e}") from e

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
                raise RuntimeError(f"保存因子实验指标失败: {e}") from e

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
                           result_metrics, qe_task_id, qe_loop_id,
                           loop_index, parent_experiment_id, is_evolution_loop,
                           ic, icir, rank_ic, rank_icir,
                           annualized_return, max_drawdown, information_ratio,
                           annualized_return_no_cost, max_drawdown_no_cost, information_ratio_no_cost,
                           created_at, updated_at,
                           alpha_mode, multi_alpha_config, parent_multi_alpha_id
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
            except Exception as e:
                raise ValueError(f"data_split JSON 解析失败: {data_split!r}") from e
        if isinstance(custom_params, str):
            try:
                import json as _json
                custom_params = _json.loads(custom_params)
            except Exception as e:
                raise ValueError(f"custom_params JSON 解析失败: {custom_params!r}") from e

        # 创建实验目录 (本地)
        exp_dir = QE_EXPERIMENTS_ROOT / experiment_name
        exp_dir.mkdir(parents=True, exist_ok=True)

        # 默认数据划分
        if not data_split:
            data_split = dict(RDAGENT_DEFAULT_DATA_SPLIT)
        self._validate_data_split(data_split)
        self._ensure_backtest_end(data_split)

        # 获取因子信息
        factors_info = self._get_factors_info(factor_names)

        # 获取模型信息
        model_info = self._get_model_info(model_id) if model_id else None

        # 获取策略信息
        strategy_info = self._get_strategy_info(strategy_id) if strategy_id else None

        # 判断因子类型
        has_custom_factors = any(f.get("code_text") for f in factors_info)
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
                    raise ValueError(
                        f"模型 '{model_info.get('model_name', '?')}' 的 model_type='{model_type_raw}' "
                        f"不在已知类型中 (TimeSeries/Tabular)，请更新模型目录中的 model_type 字段"
                    )

        # 从custom_params提取disable_alpha158和quick_train参数
        disable_alpha158 = False
        quick_train = False  # 快速训练模式：训练时间缩短到20%
        if custom_params:
            disable_alpha158 = custom_params.get("disable_alpha158", False)
            quick_train = custom_params.get("quick_train", False)

        # backtest_freq: "1min"（分钟线，默认）或 "day"（日线回退模式）
        backtest_freq = (custom_params or {}).get("backtest_freq", "1min")

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
            backtest_freq=backtest_freq,
        )

        # 生成因子文件和预处理脚本
        has_factor_files = False
        prepare_factors_py = None
        if has_custom_factors:
            factor_marker = self._compose_factor_file(factors_info)
            has_factor_files = factor_marker is not None
            prepare_factors_py = self._compose_prepare_factors(factors_info, data_split=data_split)

        # 保存文件
        conf_path = exp_dir / "conf.yaml"
        conf_path.write_text(conf_yaml, encoding="utf-8")

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

        # 复制 qrun_limit runner（分钟线使用 qrun_limit_minute.py，日线使用 qrun_limit.py）
        scripts_dir = Path(__file__).parent.parent.parent.parent / "scripts"
        import shutil
        if backtest_freq != "day":
            minute_src = scripts_dir / "qrun_limit_minute.py"
            if minute_src.exists():
                shutil.copy2(minute_src, exp_dir / "qrun_limit_minute.py")
            # TailTWAPWithLimitStrategy（分钟级执行策略）
            twap_src = scripts_dir / "tail_twap_strategy.py"
            if twap_src.exists():
                shutil.copy2(twap_src, exp_dir / "tail_twap_strategy.py")
            # v24 Plan 执行策略（继承 TailTWAPWithLimitStrategy）
            v24_src = scripts_dir / "tail_twap_v24_strategy.py"
            if v24_src.exists():
                shutil.copy2(v24_src, exp_dir / "tail_twap_v24_strategy.py")
            if bench_src.exists():
                shutil.copy2(bench_src, exp_dir / "benchmark_sh000300.parquet")
                # 同时复制到 qe_workspace（实际执行目录）
                ws_dir = QE_WORKSPACE_WIN / experiment_name
                if ws_dir.exists():
                    shutil.copy2(bench_src, ws_dir / "benchmark_sh000300.parquet")
        qrun_limit_src = scripts_dir / "qrun_limit.py"
        if qrun_limit_src.exists():
            shutil.copy2(qrun_limit_src, exp_dir / "qrun_limit.py")
        # benchmark parquet 也复制到日线实验的 qe_workspace
        bench_src = scripts_dir / "benchmark_sh000300.parquet"
        if bench_src.exists():
            ws_dir = QE_WORKSPACE_WIN / experiment_name
            if ws_dir.exists() and not (ws_dir / "benchmark_sh000300.parquet").exists():
                shutil.copy2(bench_src, ws_dir / "benchmark_sh000300.parquet")

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
            use_custom_model=bool(model_info and model_info.get("code_text")),
            model_type_tag=model_type_tag if model_info and model_info.get("code_text") else None,
            backtest_freq=backtest_freq,
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
        qlib_data_path: Optional[str] = None,
        qlib_minute_path: Optional[str] = None,
        backtest_freq: str = "1min",  # "1min" 分钟线回测 | "day" 日线回测
        execution_algo: Optional[str] = None,  # None/"TWAP" → TailTWAPWithLimitStrategy | "CLOSE_PRICE" → CloseExecutionStrategy
        execution_algo_params: Optional[Dict[str, Any]] = None,
        initial_cash: Optional[int] = None,  # 初始资金，None → 100000000
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
        model_dataset_cls = "DatasetH"
        model_step_len: Optional[int] = None

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

                # batch_size 对齐 Qlib 官方 (2000-8000) 和 QE 数据库验证结果:
                # batch=4096 → IC=0.051/AnnRet=89% (TOP1/TOP2 SOTA)
                # batch=16384+ → 梯度过度平滑，弱信号学习困难
                _DEFAULT_BATCH_SIZE = 4096

                raw_bs = thp.get("batch_size")
                if raw_bs is not None:
                    if isinstance(raw_bs, str):
                        raw_bs = int(raw_bs)
                    gpu_batch_size = raw_bs
                else:
                    gpu_batch_size = _DEFAULT_BATCH_SIZE

                model_kwargs = {
                    "n_epochs": thp.get("n_epochs", 200),
                    "lr": float(thp.get("lr", 1e-3)) if isinstance(thp.get("lr", 1e-3), str) else thp.get("lr", 1e-3),
                    "early_stop": thp.get("early_stop", 20),
                    "batch_size": gpu_batch_size,
                    "weight_decay": float(thp.get("weight_decay", 1e-4)) if isinstance(thp.get("weight_decay", 1e-4), str) else thp.get("weight_decay", 1e-4),
                    "metric": "loss",
                    "loss": "mse",
                    "n_jobs": 2,
                    "GPU": 0,
                    "use_amp": False,
                    "gradient_accumulation_steps": 1,
                    "pin_memory": True,
                    "prefetch_factor": 2,
                    "persistent_workers": False,
                    "pt_model_uri": "model.model_cls",
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
                    model_dataset_cls = "TSDatasetH"
                    model_step_len = 20
                elif model_type_raw in ("Tabular", "tabular"):
                    model_type_tag = "Tabular"
                    model_dataset_cls = "DatasetH"
                    model_step_len = None
                else:
                    raise ValueError(
                        f"模型 '{model_info.get('model_name', '?')}' 的 model_type='{model_type_raw}' "
                        f"不在已知类型中 (TimeSeries/Tabular)，请更新模型目录中的 model_type 字段"
                    )

            elif "LGB" in model_type or "LGBM" in model_type or "GBDT" in model_type:
                model_class = "LGBModel"
                model_module = "qlib.contrib.model.gbdt"
                hp = model_info.get("model_hyperparameters")
                if hp:
                    if isinstance(hp, str):
                        hp = json.loads(hp)
                    model_kwargs.update(hp)
            elif "XGB" in model_type or "XGBOOST" in model_type:
                model_class = "XGBModel"
                model_module = "qlib.contrib.model.xgboost"
                model_kwargs = {
                    "n_estimators": 500,
                    "max_depth": 8,
                    "learning_rate": 0.05,
                    "subsample": 0.8,
                    "colsample_bytree": 0.8,
                    "reg_alpha": 0.1,
                    "reg_lambda": 1.0,
                    "n_jobs": -1,
                }
                hp = model_info.get("model_hyperparameters")
                if hp:
                    if isinstance(hp, str):
                        hp = json.loads(hp)
                    model_kwargs.update(hp)
            elif "CATBOOST" in model_type:
                model_class = "CatBoostModel"
                model_module = "qlib.contrib.model.catboost_model"
                model_kwargs = {
                    "iterations": 500,
                    "depth": 8,
                    "learning_rate": 0.05,
                    "l2_leaf_reg": 3.0,
                    "subsample": 0.8,
                    "verbose": 0,
                    "task_type": "CPU",
                }
                hp = model_info.get("model_hyperparameters")
                if hp:
                    if isinstance(hp, str):
                        hp = json.loads(hp)
                    model_kwargs.update(hp)
            elif "LINEAR" in model_type or "RIDGE" in model_type or "LASSO" in model_type:
                model_class = "LinearModel"
                model_module = "qlib.contrib.model.linear"
                model_kwargs = {
                    "estimator": "ridge",
                    "alpha": 0.05,
                }
                hp = model_info.get("model_hyperparameters")
                if hp:
                    if isinstance(hp, str):
                        hp = json.loads(hp)
                    model_kwargs.update(hp)
            elif "PTNN" in model_type or "NN" in model_type:
                # 无源代码的 GeneralPTNN：必须在 model_hyperparameters 中提供 pt_model_uri
                model_class = "GeneralPTNN"
                model_module = "qlib.contrib.model.pytorch_general_nn"

                # 提供完整的默认训练参数（数值类型，不是字符串）
                model_dataset_cls = model_info.get("default_dataset_type") or "DatasetH"
                model_step_len = 20 if model_dataset_cls == "TSDatasetH" else None
                model_kwargs = {
                    "n_epochs": 30,
                    "lr": 1e-3,
                    "early_stop": 5,
                    "batch_size": 4096,
                    "weight_decay": 1e-4,
                    "metric": "loss",
                    "loss": "mse",
                    "n_jobs": 2,
                    "GPU": 0,
                }

                training_hp = model_info.get("model_training_hyperparameters")
                if training_hp:
                    if isinstance(training_hp, str):
                        training_hp = json.loads(training_hp)
                    if "lr" in training_hp and isinstance(training_hp["lr"], str):
                        training_hp["lr"] = float(training_hp["lr"])
                    if "weight_decay" in training_hp and isinstance(training_hp["weight_decay"], str):
                        training_hp["weight_decay"] = float(training_hp["weight_decay"])
                    model_kwargs.update(training_hp)

                hp = model_info.get("model_hyperparameters")
                if hp:
                    if isinstance(hp, str):
                        hp = json.loads(hp)
                    # 确保 lr 和 weight_decay 是数值类型
                    if "lr" in hp and isinstance(hp["lr"], str):
                        hp["lr"] = float(hp["lr"])
                    if "weight_decay" in hp and isinstance(hp["weight_decay"], str):
                        hp["weight_decay"] = float(hp["weight_decay"])

                    pt_model_uri = hp.get("pt_model_uri")
                    pt_model_kwargs = {}
                    for arch_key in ("d_feat", "hidden_size", "num_layers", "dropout"):
                        if arch_key in hp:
                            pt_model_kwargs[arch_key] = hp[arch_key]

                    for key, value in hp.items():
                        if key in {"pt_model_uri", "d_feat", "hidden_size", "num_layers", "dropout"}:
                            continue
                        model_kwargs[key] = value

                    if pt_model_uri:
                        model_kwargs["pt_model_uri"] = pt_model_uri
                    if pt_model_kwargs:
                        model_kwargs["pt_model_kwargs"] = pt_model_kwargs

                # 检查是否提供了 pt_model_uri
                if "pt_model_uri" not in model_kwargs:
                    raise ValueError(
                        f"模型类型 '{model_type}' 无源代码，必须在 model_hyperparameters 中"
                        f"提供 pt_model_uri 参数指定模型类路径"
                    )
            else:
                # 未知模型类型且无源代码，无法生成配置
                raise ValueError(
                    f"未知模型类型 '{model_type}'，无源代码可用，"
                    f"无法生成 Qlib 配置。请检查模型数据完整性。"
                )

        # ── 策略配置 ──
        # 默认值：仅在用户未选择策略时使用 qlib 内置 TopkDropoutStrategy
        strategy_class = "TopkDropoutStrategy"
        strategy_module = "qlib.contrib.strategy.signal_strategy"
        strategy_kwargs = {
            "signal": "<PRED>",
            "topk": 50,
            "n_drop": 5,
            "method_buy": "top",
            "hold_thresh": 2,
            "method_sell": "bottom",
            "risk_degree": 0.95,
            "only_tradable": True,
            "forbid_all_trade_at_limit": False,
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

        # ── 模型超参键白名单（始终可用，供策略安全过滤引用） ──
        _PTNN_HP_KEYS = {
            "n_epochs", "lr", "early_stop", "batch_size", "weight_decay",
            "optimizer",
        }
        # NOTE: hidden_size, num_layers, dropout 是模型架构参数，属于 pt_model_kwargs，
        # 由模型源码 (model.py) 硬编码，不能作为 GeneralPTNN.__init__() 的顶层参数传入。
        _LGB_HP_KEYS = {
            "learning_rate", "max_depth", "num_leaves", "lambda_l1", "lambda_l2",
            "colsample_bytree", "subsample", "n_estimators", "min_child_samples",
        }
        _XGB_HP_KEYS = {
            "n_estimators", "max_depth", "learning_rate", "subsample",
            "colsample_bytree", "reg_alpha", "reg_lambda", "n_jobs",
        }
        _CATBOOST_HP_KEYS = {
            "iterations", "depth", "learning_rate", "l2_leaf_reg",
            "subsample", "verbose", "task_type",
        }
        _LINEAR_HP_KEYS = {
            "estimator", "alpha",
        }
        _NON_STRATEGY_PARAMS = {
            "disable_alpha158", "disable_alpha360", "use_custom_model",
            "model_type", "dataset_cls", "step_len", "num_timesteps", "num_features",
            "quick_train",  # 快速训练模式：控制模型训练参数
            "label_type",   # 训练标签类型：close/open/vwap
            "stock_pool",   # 股票池文件路径
            "backtest_freq",        # 回测频率（已在上层提取）
            "execution_algo",       # 执行算法（已在上层提取到 inner_strategy）
            "execution_algo_params",  # 执行算法参数（已在上层提取到 inner_strategy）
            "unfilled_handler",       # 尾盘涨停处理（已在上层提取到 inner_strategy.kwargs）
            "unfilled_trigger_minute", # 尾盘处理触发分钟（已在上层提取到 inner_strategy.kwargs）
            "unfilled_backup_depth",   # 替补候选深度（已在上层提取到 inner_strategy.kwargs）
            "initial_cash",         # 初始资金（已在上层处理）
            "hmm_model_version_id", # HMM 版本 ID（已在上层处理为 hmm_coefficients_file）
        } | _PTNN_HP_KEYS | _LGB_HP_KEYS | _XGB_HP_KEYS | _CATBOOST_HP_KEYS | _LINEAR_HP_KEYS

        if custom_params:
            # ── 模型超参透传: 从 custom_params 中提取模型超参 → model_kwargs ──
            hp_keys = set()
            if model_class in ("GeneralPTNN",):
                hp_keys = _PTNN_HP_KEYS
            elif model_class in ("LGBModel",):
                hp_keys = _LGB_HP_KEYS
            elif model_class in ("XGBModel",):
                hp_keys = _XGB_HP_KEYS
            elif model_class in ("CatBoostModel",):
                hp_keys = _CATBOOST_HP_KEYS
            elif model_class in ("LinearModel",):
                hp_keys = _LINEAR_HP_KEYS
            # 也包括有自定义代码的 PTNN 模型
            if use_custom_model and model_type_tag in ("TimeSeries", "Tabular"):
                hp_keys = hp_keys | _PTNN_HP_KEYS

            model_hp_overrides = {}
            for key in hp_keys:
                if key in custom_params:
                    val = custom_params[key]
                    # 确保 GeneralPTNN 的 lr 和 weight_decay 是数值类型
                    if model_class == "GeneralPTNN" and key in ("lr", "weight_decay") and isinstance(val, str):
                        val = float(val)
                    model_hp_overrides[key] = val
            if model_hp_overrides:
                logger.info(f"模型超参透传: {list(model_hp_overrides.keys())} → model_kwargs")
                model_kwargs.update(model_hp_overrides)

            # 过滤掉非策略参数（含模型超参、数据加载器配置等）
            filtered_params = {k: v for k, v in custom_params.items() if k not in _NON_STRATEGY_PARAMS}
            if set(custom_params.keys()) - set(filtered_params.keys()):
                logger.info(f"策略参数过滤: 移除非策略参数 {set(custom_params.keys()) - set(filtered_params.keys())}")
            strategy_kwargs.update(filtered_params)

        # 确保 signal 始终为 <PRED>
        strategy_kwargs["signal"] = "<PRED>"

        # 安全过滤：只保留策略支持的参数
        # 避免不支持的参数通过 **kwargs 传递到 BaseStrategy 导致 TypeError
        _HMM_KEYS = {
            "enable_sector_hmm", "sector_hmm_model_path",
            "hmm_signal_preset", "hmm_signal_presets",
            "hmm_coefficients_file",
        }
        _UNFILLED_KEYS = {
            "unfilled_handler", "unfilled_trigger_minute", "unfilled_backup_depth",
        }
        _TOPK_DROPOUT_ALLOWED_KEYS = {
            "signal", "topk", "n_drop", "method_sell", "method_buy",
            "hold_thresh", "only_tradable", "forbid_all_trade_at_limit",
            "risk_degree",
        } | _UNFILLED_KEYS
        # EnhancedTopkDropoutStrategy 支持的额外参数
        _ENHANCED_TOPK_ALLOWED_KEYS = _TOPK_DROPOUT_ALLOWED_KEYS | {
            "min_score", "max_position_ratio", "stop_loss", "max_market_cap",
        } | _HMM_KEYS
        # SmallCapTopkDropoutStrategy 支持的参数
        _SMALLCAP_TOPK_ALLOWED_KEYS = _TOPK_DROPOUT_ALLOWED_KEYS | {
            "max_market_cap",
        } | _HMM_KEYS
        # TopkDropoutWithRiskControlStrategy 支持的参数（含 HMM）
        _RC_TOPK_ALLOWED_KEYS = _TOPK_DROPOUT_ALLOWED_KEYS | {
            "stop_loss_pct", "max_daily_turnover_pct",
            "stock_pool",
        } | _HMM_KEYS
        # ScoreWeightedTopkStrategy 支持的参数（T1.1+T1.3, 2026-04-06）
        _SCORE_WEIGHTED_TOPK_ALLOWED_KEYS = _TOPK_DROPOUT_ALLOWED_KEYS | {
            "weight_method", "temperature", "score_clip_quantile",
            "max_weight", "min_weight", "max_position_ratio",
            "enable_dynamic_ndrop", "max_n_drop", "min_n_drop",
            "threshold_method", "min_improvement", "adaptive_multiplier",
            "threshold_floor", "min_trade_price", "max_trade_price",
            "max_single_order_value", "lot_size",
        } | _UNFILLED_KEYS | _HMM_KEYS

        if strategy_class == "TopkDropoutStrategy":
            _removed = {k for k in strategy_kwargs if k not in _TOPK_DROPOUT_ALLOWED_KEYS}
            if _removed:
                raise ValueError(
                    f"策略 '{strategy_class}' 不支持参数: {sorted(_removed)}。"
                    f"允许的参数: {sorted(_TOPK_DROPOUT_ALLOWED_KEYS)}"
                )
        elif strategy_class == "EnhancedTopkDropoutStrategy":
            _removed = {k for k in strategy_kwargs if k not in _ENHANCED_TOPK_ALLOWED_KEYS}
            if _removed:
                raise ValueError(
                    f"策略 '{strategy_class}' 不支持参数: {sorted(_removed)}。"
                    f"允许的参数: {sorted(_ENHANCED_TOPK_ALLOWED_KEYS)}"
                )
        elif strategy_class == "SmallCapTopkDropoutStrategy":
            _removed = {k for k in strategy_kwargs if k not in _SMALLCAP_TOPK_ALLOWED_KEYS}
            if _removed:
                raise ValueError(
                    f"策略 '{strategy_class}' 不支持参数: {sorted(_removed)}。"
                    f"允许的参数: {sorted(_SMALLCAP_TOPK_ALLOWED_KEYS)}"
                )
        elif strategy_class == "TopkDropoutWithRiskControlStrategy":
            _removed = {k for k in strategy_kwargs if k not in _RC_TOPK_ALLOWED_KEYS}
            if _removed:
                raise ValueError(
                    f"策略 '{strategy_class}' 不支持参数: {sorted(_removed)}。"
                    f"允许的参数: {sorted(_RC_TOPK_ALLOWED_KEYS)}"
                )
        elif strategy_class == "ScoreWeightedTopkStrategy":
            _removed = {k for k in strategy_kwargs if k not in _SCORE_WEIGHTED_TOPK_ALLOWED_KEYS}
            if _removed:
                raise ValueError(
                    f"策略 '{strategy_class}' 不支持参数: {sorted(_removed)}。"
                    f"允许的参数: {sorted(_SCORE_WEIGHTED_TOPK_ALLOWED_KEYS)}"
                )
        elif strategy_class == "ScoreWeightedTopkStrategyV2":
            # V2 与 V1 参数集相同，修复了补仓模式 Bug #1 和幽灵持仓 Bug #2
            _removed = {k for k in strategy_kwargs if k not in _SCORE_WEIGHTED_TOPK_ALLOWED_KEYS}
            if _removed:
                raise ValueError(
                    f"策略 '{strategy_class}' 不支持参数: {sorted(_removed)}。"
                    f"允许的参数: {sorted(_SCORE_WEIGHTED_TOPK_ALLOWED_KEYS)}"
                )
        else:
            # 未知策略类型：只过滤已知的非策略参数（如 backtest_freq, execution_algo 等）
            _removed = {k for k in strategy_kwargs if k in _NON_STRATEGY_PARAMS}
            if _removed:
                logger.info(f"未知策略 '{strategy_class}': 移除非策略参数 {sorted(_removed)}")
                for k in _removed:
                    del strategy_kwargs[k]

        # 回测截止日（必须由 data_split 提供，禁止硬编码兜底）
        backtest_end = data_split.get("backtest_end")
        if not backtest_end:
            raise ValueError("data_split 缺少 backtest_end，无法生成回测配置")

        # ── 生成 YAML（与 RDAgent conf_baseline.yaml 结构一致） ──
        lines = []
        lines.append("qlib_init:")
        _day_uri = qlib_data_path or QLIB_DATA_PATH_WSL
        _min_uri = qlib_minute_path or QLIB_MINUTE_PATH_WSL
        lines.append("    provider_uri:")
        lines.append(f'        day: "{_day_uri}"')
        lines.append(f'        1min: "{_min_uri}"')
        lines.append("    region: cn")
        lines.append("    dataset_cache: null")
        lines.append("    expression_cache: null")
        lines.append("")
        stock_pool = (custom_params or {}).get("stock_pool", "all")
        # 训练标签选择（custom_params.label_type 可选覆盖）
        # close: Ref($close,-2)/Ref($close,-1)-1  — 传统 close-to-close（默认）
        # open:  Ref($open,-2)/Ref($open,-1)-1    — open-to-open（更贴近可执行价）
        # vwap:  Ref($vwap,-2)/Ref($vwap,-1)-1    — vwap-to-vwap
        _LABEL_FORMULAS = {
            "close": "Ref($close, -2) / Ref($close, -1) - 1",
            "open":  "Ref($open, -2) / Ref($open, -1) - 1",
            "vwap":  "Ref($vwap, -2) / Ref($vwap, -1) - 1",
        }
        _label_type = (custom_params or {}).get("label_type", "close")
        if _label_type not in _LABEL_FORMULAS:
            raise ValueError(
                f"label_type='{_label_type}' invalid, must be one of {list(_LABEL_FORMULAS.keys())}"
            )
        _label_formula = _LABEL_FORMULAS[_label_type]
        if _label_type != "close":
            logger.info(f"使用非默认训练标签: label_type={_label_type}, formula={_label_formula}")
        lines.append(f"market: &market {stock_pool}")
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
            lines.append(f'                            - ["{_label_formula}"]')
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
            lines.append(f'    label: ["{_label_formula}"]')

        lines.append("")

        # port_analysis_config — executor 根据 backtest_freq 切换
        lines.append("port_analysis_config: &port_analysis_config")
        lines.append("    executor:")
        if backtest_freq == "day":
            # 日线模式：单层 SimulatorExecutor
            lines.append("        class: SimulatorExecutor")
            lines.append("        module_path: qlib.backtest.executor")
            lines.append("        kwargs:")
            lines.append("            time_per_step: day")
            lines.append("            generate_portfolio_metrics: true")
        else:
            # 分钟线模式：NestedExecutor + 执行算法策略
            lines.append("        class: NestedExecutor")
            lines.append("        module_path: qlib.backtest.executor")
            lines.append("        kwargs:")
            lines.append("            time_per_step: day")
            lines.append("            inner_executor:")
            lines.append("                class: SimulatorExecutor")
            lines.append("                module_path: qlib.backtest.executor")
            lines.append("                kwargs:")
            lines.append("                    time_per_step: 1min")
            lines.append("                    generate_portfolio_metrics: false")
            lines.append("            inner_strategy:")
            _algo_upper = (execution_algo or "").upper()
            if _algo_upper == "CLOSE_PRICE":
                lines.append("                class: CloseExecutionStrategy")
                lines.append("                module_path: close_execution_strategy")
                if execution_algo_params:
                    lines.append("                kwargs:")
                    for k, v in execution_algo_params.items():
                        if isinstance(v, bool):
                            lines.append(f"                    {k}: {'true' if v else 'false'}")
                        elif isinstance(v, str):
                            lines.append(f"                    {k}: {v}")
                        else:
                            lines.append(f"                    {k}: {v}")
            elif _algo_upper == "V24_PLAN":
                # v24 方向感知执行计划 (继承 TailTWAPWithLimitStrategy)
                lines.append("                class: TailTWAPWithV24PlanStrategy")
                lines.append("                module_path: tail_twap_v24_strategy")
                lines.append("                kwargs:")
                # model_path 默认值 (可被 execution_algo_params 覆盖)
                _v24_params = dict(execution_algo_params or {})
                _v24_params.setdefault("model_path", "/home/lc999/data/rl_models/v24/v24_plan_net.pt")
                for k, v in _v24_params.items():
                    if isinstance(v, bool):
                        lines.append(f"                    {k}: {'true' if v else 'false'}")
                    elif isinstance(v, str):
                        lines.append(f"                    {k}: {v}")
                    else:
                        lines.append(f"                    {k}: {v}")
            else:
                # 默认：TailTWAPWithLimitStrategy
                lines.append("                class: TailTWAPWithLimitStrategy")
                lines.append("                module_path: tail_twap_strategy")
                if execution_algo_params:
                    lines.append("                kwargs:")
                    for k, v in execution_algo_params.items():
                        if isinstance(v, bool):
                            lines.append(f"                    {k}: {'true' if v else 'false'}")
                        elif isinstance(v, str):
                            lines.append(f"                    {k}: {v}")
                        else:
                            lines.append(f"                    {k}: {v}")
            lines.append("            generate_portfolio_metrics: true")
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
        _account_cash = int(initial_cash) if initial_cash else 100000000
        lines.append(f"        account: {_account_cash}")
        lines.append("        benchmark: ~")
        lines.append("        exchange_kwargs:")
        if backtest_freq == "day":
            lines.append('            freq: day')
            lines.append('            limit_threshold: ["$limit_up", "$limit_down"]')
        else:
            lines.append('            freq: 1min')
            lines.append('            limit_threshold: ["$limit_up", "$limit_down"]')
        lines.append("            deal_price: close")
        lines.append("            open_cost: 0.000095")
        lines.append("            close_cost: 0.000595")
        lines.append("            min_cost: 5")
        lines.append("            trade_unit: 100")

        # task（模型 + 数据集）
        lines.append("task:")
        lines.append("    model:")
        lines.append(f"        class: {model_class}")
        lines.append(f"        module_path: {model_module}")
        if model_kwargs:
            lines.append("        kwargs:")
            pt_model_kwargs = None
            if model_class == "GeneralPTNN":
                pt_model_kwargs = model_kwargs.get("pt_model_kwargs")
            for k, v in model_kwargs.items():
                if k == "pt_model_kwargs":
                    continue
                # pt_model_kwargs 使用Jinja2模板变量，与RDAgent一致
                if k == "pt_model_uri":
                    lines.append(f"            {k}: {v}")
                else:
                    lines.append(f"            {k}: {v}")
            # 自定义模型和内置 PTNN 模型都需要 pt_model_kwargs
            if model_class == "GeneralPTNN":
                if pt_model_kwargs is None and use_custom_model:
                    pt_model_kwargs = {"num_features": "{{ num_features }}"}
                    if model_type_tag == "TimeSeries":
                        pt_model_kwargs["num_timesteps"] = "{{ num_timesteps }}"
                if pt_model_kwargs is not None:
                    lines.append('            pt_model_kwargs: {')
                    first_item = True
                    for pt_key, pt_value in pt_model_kwargs.items():
                        prefix = "                " if first_item else "                , "
                        if isinstance(pt_value, str) and pt_value.startswith("{{"):
                            rendered_value = pt_value
                        elif isinstance(pt_value, str):
                            rendered_value = f'"{pt_value}"'
                        else:
                            rendered_value = str(pt_value)
                        lines.append(f"{prefix}\"{pt_key}\": {rendered_value}")
                        first_item = False
                    lines.append('            }')

        # 数据集配置
        if model_class == "GeneralPTNN":
            lines.append("    dataset:")
            lines.append(f'        class: {model_dataset_cls}')
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
        if model_class == "GeneralPTNN" and model_step_len:
            lines.append(f"            step_len: {model_step_len}")

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
        custom_factors = [f for f in factors_info if f.get("code_text")]
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
        custom_factors = [f for f in factors_info if f.get("code_text")]
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

    def _compose_prepare_factors(
        self,
        factors_info: List[Dict],
        factor_data_dir: Optional[str] = None,
        data_split: Optional[Dict[str, str]] = None,
    ) -> Optional[str]:
        """生成 prepare_factors.py 预处理脚本（含因子值缓存集成）。

        与 RDAgent FactorFBWorkspace.execute() 完全一致的因子执行方式：
        1. 从 RDAgent 数据目录链接所有数据文件到实验目录
        2. 对每个因子先检查缓存 (source_hash + 日期覆盖) → 命中则读 parquet
        3. 未命中则执行 python factor.py (timeout=1200s) → 成功后回写缓存
        4. 合并所有因子结果为 combined_factors_df.parquet
        """
        # 提取缓存窗口（用于全周期缓存命中判断）
        train_start = "2018-08-01"
        test_end = "2026-04-03"
        if data_split:
            train_start = data_split.get("train_start", train_start)
            test_end = data_split.get("test_end", test_end)
        custom_factors = [f for f in factors_info if f.get("code_text")]
        if not custom_factors:
            return None

        factor_names = [f["factor_name"] for f in custom_factors]

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
        lines.append(f"FACTOR_DATA_DIR = os.environ.get('RDAGENT_FACTOR_DATA_DIR', '{factor_data_dir or RDAGENT_FACTOR_DATA_WSL}')")
        lines.append("")
        lines.append("# ── 因子值缓存 ──────────────────────────────────────────")
        lines.append("import hashlib")
        lines.append("import json as _json")
        lines.append("RAW_FACTOR_CACHE_DIR = os.environ.get('FACTOR_CACHE_DIR', '')")
        lines.append("if RAW_FACTOR_CACHE_DIR:")
        lines.append("    _cache_base = RAW_FACTOR_CACHE_DIR.rstrip('/\\')")
        lines.append("    if os.path.basename(_cache_base) == 'single':")
        lines.append("        FACTOR_CACHE_SINGLE_DIR = _cache_base")
        lines.append("        FACTOR_CACHE_META = os.path.join(os.path.dirname(_cache_base), '_meta.json')")
        lines.append("    else:")
        lines.append("        FACTOR_CACHE_SINGLE_DIR = os.path.join(_cache_base, 'single')")
        lines.append("        FACTOR_CACHE_META = os.path.join(_cache_base, '_meta.json')")
        lines.append("else:")
        lines.append("    FACTOR_CACHE_SINGLE_DIR = ''")
        lines.append("    FACTOR_CACHE_META = ''")
        lines.append(f"TRAIN_START = '{train_start}'")
        lines.append(f"TEST_END = '{test_end}'")
        lines.append("")
        lines.append("")
        lines.append("def _try_cache_hit(factor_name, factor_code):")
        lines.append("    \"\"\"尝试从缓存读取因子值。命中返回 DataFrame，否则返回 None。\"\"\"")
        lines.append("    if not FACTOR_CACHE_SINGLE_DIR:")
        lines.append("        return None")
        lines.append("    cache_path = os.path.join(FACTOR_CACHE_SINGLE_DIR, f'{factor_name}.parquet')")

        lines.append("    if not os.path.exists(cache_path) or not FACTOR_CACHE_META or not os.path.exists(FACTOR_CACHE_META):")
        lines.append("        return None")
        lines.append("    code_hash = hashlib.sha256(factor_code.encode()).hexdigest()[:16]")
        lines.append("    try:")
        lines.append("        meta = _json.load(open(FACTOR_CACHE_META, 'r'))")
        lines.append("    except Exception:")
        lines.append("        return None")
        lines.append("    entry = meta.get('factors', {}).get(factor_name, {})")
        lines.append("    cached_hash = entry.get('source_hash_raw')")
        lines.append("    if cached_hash != code_hash:")
        lines.append("        logger.info(f'  {factor_name}: cache hash mismatch (cached={cached_hash}, current={code_hash})')")
        lines.append("        return None")
        lines.append("    cached_range = entry.get('date_range', '')")
        lines.append("    if '~' not in cached_range:")
        lines.append("        return None")
        lines.append("    c_start, c_end = cached_range.split('~')")
        lines.append("    # 允许时序因子 lookback 缺口：缓存起始日比 train_start 晚 60 日历天内视为正常")
        lines.append("    _LOOKBACK_TOLERANCE_DAYS = 60")
        lines.append("    _ts = _pd.Timestamp(TRAIN_START)")
        lines.append("    _gap_ok = (_pd.Timestamp(c_start) - _ts).days <= _LOOKBACK_TOLERANCE_DAYS if c_start > TRAIN_START else True")
        lines.append("    if (not _gap_ok) or c_end < TEST_END:")
        lines.append("        logger.info(f'  {factor_name}: cache date insufficient ({cached_range} vs {TRAIN_START}~{TEST_END})')")
        lines.append("        return None")
        lines.append("    # 命中")
        lines.append("    df = pd.read_parquet(cache_path)")
        lines.append("    dates = df.index.get_level_values(0)")
        lines.append("    df = df[(dates >= pd.Timestamp(TRAIN_START)) & (dates <= pd.Timestamp(TEST_END))]")
        lines.append("    if 'value' in df.columns:")
        lines.append("        df = df.rename(columns={'value': factor_name})")
        lines.append("    logger.info(f'  {factor_name}: CACHE HIT ({len(df)} rows, {cached_range})')")
        lines.append("    return df")
        lines.append("")
        lines.append("")
        lines.append("def _write_cache(factor_name, factor_code, result_df):")
        lines.append("    \"\"\"执行成功后回写因子值缓存。\"\"\"")
        lines.append("    if not FACTOR_CACHE_SINGLE_DIR:")
        lines.append("        return")
        lines.append("    try:")
        lines.append("        cache_path = os.path.join(FACTOR_CACHE_SINGLE_DIR, f'{factor_name}.parquet')")

        lines.append("        code_hash = hashlib.sha256(factor_code.encode()).hexdigest()[:16]")
        lines.append("        save_df = result_df.copy()")
        lines.append("        if len(save_df.columns) == 1:")
        lines.append("            save_df = save_df.rename(columns={save_df.columns[0]: 'value'})")
        lines.append("        save_df.to_parquet(cache_path, engine='pyarrow', compression='snappy')")
        lines.append("        # 原子更新 _meta.json")
        lines.append("        import tempfile as _tmpf")
        lines.append("        meta = {}")
        lines.append("        if os.path.exists(FACTOR_CACHE_META):")
        lines.append("            try:")
        lines.append("                meta = _json.load(open(FACTOR_CACHE_META, 'r'))")
        lines.append("            except Exception:")
        lines.append("                pass")
        lines.append("        factors = meta.get('factors', {})")
        lines.append("        dates = result_df.index.get_level_values(0)")
        lines.append("        d_min = str(dates.min().date())")
        lines.append("        d_max = str(dates.max().date())")
        lines.append("        factors[factor_name] = {")
        lines.append("            'computed_at': __import__('datetime').datetime.now().isoformat(),")
        lines.append("            'rows': len(result_df),")
        lines.append("            'date_range': f'{d_min}~{d_max}',")
        lines.append("            'as_of_date': d_max,")
        lines.append("            'source_hash_raw': code_hash,")
        lines.append("        }")
        lines.append("        meta['factors'] = factors")
        lines.append("        tmp_fd, tmp_path = _tmpf.mkstemp(dir=os.path.dirname(FACTOR_CACHE_META), suffix='.json')")

        lines.append("        with os.fdopen(tmp_fd, 'w') as f:")
        lines.append("            _json.dump(meta, f, indent=2, ensure_ascii=False)")
        lines.append("        os.replace(tmp_path, FACTOR_CACHE_META)")
        lines.append("        logger.info(f'  {factor_name}: cache WRITTEN ({len(result_df)} rows, {d_min}~{d_max}, window={TRAIN_START}~{TEST_END})')")

        lines.append("    except Exception as e:")
        lines.append("        logger.warning(f'  {factor_name}: cache write failed: {e}')")
        lines.append("")
        lines.append("")
        lines.append("ALLOWED_DATA_FILES = ('daily_pv.h5', 'daily_basic.h5', 'moneyflow.h5', 'bak_basic.h5', 'cyq_perf.h5', 'sector_data.h5', 'margin_detail.h5', 'static_factors.parquet')")
        lines.append("")
        lines.append("")
        lines.append("def link_all_files_to_dir(src_dir, dst_dir):")
        lines.append('    """仅链接 execution layer 允许的历史数据文件，避免混入实时数据。"""')
        lines.append("    src_dir = os.path.abspath(src_dir)")
        lines.append("    for item in ALLOWED_DATA_FILES:")
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
        lines.append('    """因子执行（含缓存读取/回写）。')
        lines.append('    1. 检查缓存：source_hash 匹配 + 日期覆盖 → 读 parquet')
        lines.append('    2. 未命中：执行 factor.py (timeout=1200s)')
        lines.append('    3. 成功后回写缓存')
        lines.append('    """')
        lines.append("    # ── 缓存检查 ──")
        lines.append("    cached = _try_cache_hit(factor_name, factor_code)")
        lines.append("    if cached is not None:")
        lines.append("        return cached")
        lines.append("")
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
        lines.append("    # 执行因子代码 (超时 1800s, 适配720万行 × rolling.cov 等重型算子)")
        lines.append("    _factor_timeout = int(os.environ.get('AISTOCK_FACTOR_TIMEOUT', '1800'))")
        lines.append("    try:")
        lines.append("        output = subprocess.check_output(")
        lines.append("            [sys.executable, 'factor.py'],")
        lines.append("            cwd=factor_dir,")
        lines.append("            stderr=subprocess.STDOUT,")
        lines.append("            timeout=_factor_timeout,")
        lines.append("        )")
        lines.append("        logger.info(f'  {factor_name}: execution succeeded')")
        lines.append("    except subprocess.CalledProcessError as e:")
        lines.append("        err_msg = e.output.decode('utf-8', errors='replace')[-500:]")
        lines.append("        logger.error(f'  {factor_name}: execution failed: {err_msg}')")
        lines.append("        return None")
        lines.append("    except subprocess.TimeoutExpired:")
        lines.append("        logger.error(f'  {factor_name}: execution timeout ({_factor_timeout}s)')")
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
        lines.append("        # 回写缓存")
        lines.append("        _write_cache(factor_name, factor_code, df)")
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
        api_failures: list[str] = []
        for r in results:
            if r.get("source") == "rdagent_task_sync":
                task_id = r.get("best_loop_task_run_id")
                factor_name = r.get("factor_name")
                if task_id and factor_name:
                    try:
                        source_code = self._fetch_factor_source_from_api(task_id, factor_name)
                        if source_code:
                            r["code_text"] = source_code
                            logger.info(f"[QE] 从RDAgent API获取因子源码: {factor_name}, task={task_id}")
                        else:
                            api_failures.append(factor_name)
                    except Exception as e:
                        logger.warning(f"[QE] 获取因子源码失败 {factor_name}: {e}")
                        api_failures.append(factor_name)
        if api_failures:
            logger.warning(
                f"[QE] {len(api_failures)} 个RDAgent因子源码获取失败: {api_failures[:10]}"
            )

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
                raise RuntimeError(
                    f"[QE] 获取因子源码失败: factor={factor_name}, task={task_id}, "
                    f"error={result.get('error', 'unknown')}"
                )
        except Exception as e:
            raise RuntimeError(f"[QE] 获取因子源码异常: factor={factor_name}, task={task_id}, {e}") from e

    # Qlib 内置模型定义（不需要自定义代码，仅超参数）
    #
    # Multi-Alpha 架构新增说明:
    #   - 每个模型可附带 default_hyperparameters（composer 兜底使用）
    #   - default_dataset_type 暗示 Multi-Alpha 分组时的首选 dataset (TSDatasetH / DatasetH)
    #   - 实际 dataset_cls 由 Multi-Alpha engine 在 Jinja 渲染阶段注入，此处仅作为元信息
    _BUILTIN_MODELS = {
        "__builtin_LGBModel__": {
            "model_name": "LGBModel", "model_type": "LGB",
            "default_dataset_type": "DatasetH",
        },
        "__builtin_XGBModel__": {
            "model_name": "XGBModel", "model_type": "XGB",
            "default_dataset_type": "DatasetH",
        },
        "__builtin_CatBoostModel__": {
            "model_name": "CatBoostModel", "model_type": "CATBOOST",
            "default_dataset_type": "DatasetH",
        },
        "__builtin_LinearModel__": {
            "model_name": "LinearModel", "model_type": "LINEAR",
            "default_dataset_type": "DatasetH",
            "default_hyperparameters": {"estimator": "ridge", "alpha": 0.1},
        },
        # 兼容旧数据中的小写 ID
        "__builtin_lgbmodel__": {
            "model_name": "LGBModel", "model_type": "LGB",
            "default_dataset_type": "DatasetH",
        },
        # ── Multi-Alpha 新增: 时序/非线性模型 ──────────────────────
        "__builtin_ALSTM__": {
            "model_name": "ALSTM", "model_type": "PTNN",
            "default_dataset_type": "TSDatasetH",
            "default_hyperparameters": {
                "pt_model_uri": "qlib.contrib.model.pytorch_alstm_ts.ALSTMModel",
                "d_feat": 20, "hidden_size": 64, "num_layers": 1,
                "dropout": 0.0,
            },
            "default_training_hyperparameters": {
                "n_epochs": 200, "lr": 3e-4,
                "early_stop": 20, "batch_size": 4096,
                "weight_decay": 1e-5,
            },
        },
        "__builtin_GRU2__": {
            "model_name": "GRU2", "model_type": "PTNN",
            "default_dataset_type": "TSDatasetH",
            "default_hyperparameters": {
                "pt_model_uri": "qlib.contrib.model.pytorch_gru_ts.GRUModel",
                "d_feat": 20, "hidden_size": 128, "num_layers": 2,
                "dropout": 0.2,
            },
            "default_training_hyperparameters": {
                "n_epochs": 200, "lr": 2e-4,
                "early_stop": 20, "batch_size": 4096,
                "weight_decay": 1e-4,
            },
        },
        "__builtin_Ridge__": {
            "model_name": "Ridge", "model_type": "LINEAR",
            "default_dataset_type": "DatasetH",
            "default_hyperparameters": {"estimator": "ridge", "alpha": 0.1},
        },
    }

    def _get_model_info(self, model_id: str) -> Optional[Dict]:
        """获取模型信息（含源代码）。

        Multi-Alpha 解析优先级:
          1. aistock_model_catalog (用户可见/可管理)
          2. _BUILTIN_MODELS fallback (系统内置兜底)

        Catalog 中 model_hyperparameters 为空时，会自动从 _BUILTIN_MODELS 补齐 defaults。
        """
        # 1. 先查 catalog
        catalog_row = None
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
                if row:
                    cols = [desc[0] for desc in cur.description]
                    catalog_row = dict(zip(cols, row))

        # 2. 如果 catalog 没有，尝试 builtin
        if catalog_row is None:
            builtin = self._BUILTIN_MODELS.get(model_id)
            if not builtin:
                return None
            return {
                "model_id": model_id,
                "model_name": builtin["model_name"],
                "model_type": builtin["model_type"],
                "model_hyperparameters": builtin.get("default_hyperparameters"),
                "model_training_hyperparameters": builtin.get("default_training_hyperparameters"),
                "ic": None,
                "annualized_return": None,
                "is_sota": False,
                "code_text": None,
                "default_dataset_type": builtin.get("default_dataset_type"),
            }

        # 3. catalog 命中：补充 builtin default 作为 fallback（仅当对应字段为空）
        # 通过 model_name 匹配最接近的 builtin
        builtin_match = None
        for bid, bmeta in self._BUILTIN_MODELS.items():
            if bmeta["model_name"] == catalog_row.get("model_name"):
                builtin_match = bmeta
                break
        if builtin_match:
            if not catalog_row.get("model_hyperparameters") and builtin_match.get("default_hyperparameters"):
                catalog_row["model_hyperparameters"] = builtin_match["default_hyperparameters"]
            if not catalog_row.get("model_training_hyperparameters") and builtin_match.get("default_training_hyperparameters"):
                catalog_row["model_training_hyperparameters"] = builtin_match["default_training_hyperparameters"]
            if not catalog_row.get("model_type") and builtin_match.get("model_type"):
                catalog_row["model_type"] = builtin_match["model_type"]
            catalog_row["default_dataset_type"] = builtin_match.get("default_dataset_type")
        return catalog_row

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
                              model_type_tag: Optional[str] = None,
                              mode: str = "manual",
                              backtest_freq: str = "1min") -> str:
        """生成WSL执行命令。

        Args:
            mode: "manual" — 面向用户手动复制执行（含注释、conda activate）
                  "auto"   — 面向子进程自动执行（纯净命令链，用 && 连接）
        """
        env_lines = []
        if has_custom_factors or use_custom_model:
            # 环境变量设置
            env_lines.append(f'export PYTHONPATH="{wsl_path}:${{QLIB_RDAGENT_ROOT_WSL:-.}}:$PYTHONPATH"')
            
        if use_custom_model and model_type_tag:
            # 与 RDAgent model_runner.py 一致的环境变量
            if model_type_tag == "TimeSeries":
                env_lines.append("export dataset_cls=TSDatasetH")
                env_lines.append("export step_len=20")
                env_lines.append("export num_timesteps=20")
            else:
                env_lines.append("export dataset_cls=DatasetH")
        
        if use_custom_model and not has_custom_factors:
            # Alpha158 经 FilterCol 过滤后实际只有 20 个特征（与 RDAgent conf_baseline 一致）
            env_lines.append("export num_features=20")
        elif has_custom_factors:
            # num_features 在 prepare_factors.py 执行后才能确定
            # 供 conf.yaml 中的 Jinja2 模板变量引用
            env_lines.append("# num_features 将在 qrun 时由 conf.yaml Jinja2 模板自动计算")

        # 因子值缓存目录（与 backfill_factor_cache.py 共享同一缓存路径）
        # 环境变量 FACTOR_CACHE_DIR 优先；否则基于本机 AIstock 目录推导 WSL 路径
        _fv_base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # backend/
        _fv_dir = os.path.join(_fv_base, "rdagent_assets", "factor_values")
        _fv_wsl = _fv_dir.replace("\\", "/").replace("F:", "/mnt/f").replace("f:", "/mnt/f")
        env_lines.append(
            f'export FACTOR_CACHE_DIR="${{FACTOR_CACHE_DIR:-{_fv_wsl}}}"'
        )
        env_lines.append('export FACTOR_CACHE_DATA_MODE="backtest_factor_data_dir"')

        env_block = "\n".join(env_lines)

        # 数据文件链接命令（幂等）— 确保策略所需的 h5 文件始终可访问
        # 使用 shell 变量 $RDAGENT_FACTOR_DATA_WSL（从执行节点 .env 继承到子进程环境）
        _link_data_cmd = (
            '_FDD="${RDAGENT_FACTOR_DATA_WSL:-.}" && '
            'for f in daily_basic.h5 daily_pv.h5 moneyflow.h5 bak_basic.h5 cyq_perf.h5 sector_data.h5 static_factors.parquet; do '
            '[ ! -e "$f" ] && [ -e "$_FDD/$f" ] && ln -sf "$_FDD/$f" .; done; true'
        )

        # 分钟线使用 qrun_limit_minute.py（含内存 patch + benchmark），日线使用 qrun_limit.py
        runner = "qrun_limit_minute.py" if backtest_freq != "day" else "qrun_limit.py"

        # ── auto 模式：纯净命令链，供子进程直接执行 ──
        if mode == "auto":
            # 过滤掉注释行，只保留实际命令
            env_cmds = [l for l in env_lines if l and not l.startswith("#")]
            parts = [f"cd {wsl_path}"]
            # conda activate — 确保远端节点子进程能找到 python 和依赖包
            parts.append(
                '. "${QLIB_WSL_CONDA_SH:-$HOME/miniconda3/etc/profile.d/conda.sh}" && '
                'conda activate "${QLIB_WSL_CONDA_ENV:-rdagent-gpu}"'
            )
            # 限制 glibc malloc arena 数量，防止内存碎片膨胀（默认 8×CPU 核数）
            parts.append("export MALLOC_ARENA_MAX=4")
            # 强制禁用 Python stdout 缓冲，确保训练日志实时输出到 pipe
            parts.append("export PYTHONUNBUFFERED=1")
            parts.extend(env_cmds)
            parts.append(_link_data_cmd)
            if has_custom_factors:
                parts.append("python prepare_factors.py")
                parts.append(". ./.factor_env")
            parts.append(f"python {runner} conf.yaml")
            parts.append("python read_exp_res.py")
            return " && ".join(parts)

        # 手动模式的数据链接步骤（可读格式）
        _link_data_manual = f"""# 链接策略所需数据文件到实验目录（幂等）
_FDD="${{RDAGENT_FACTOR_DATA_WSL:-{RDAGENT_FACTOR_DATA_WSL}}}"
for f in daily_basic.h5 daily_pv.h5 moneyflow.h5 bak_basic.h5 cyq_perf.h5 sector_data.h5 static_factors.parquet; do
  [ ! -e "$f" ] && [ -e "$_FDD/$f" ] && ln -sf "$_FDD/$f" .
done"""

        # ── manual 模式：面向用户手动复制执行 ──
        if has_custom_factors:
            return f"""# QuantEvolver 实验执行命令（含自定义因子预处理）
# 请在WSL终端中执行以下命令：

cd {wsl_path}
conda activate rdagent-gpu

{_link_data_manual}

# 步骤1: 预计算因子 -> 生成 combined_factors_df.parquet
python prepare_factors.py

# 步骤2: 设置环境变量
{env_block}
# 加载因子预处理输出的 num_features（由 prepare_factors.py 自动计算）
. .factor_env

# 步骤3: 运行QLib回测
python {runner} conf.yaml

# 步骤4: 读取结果
python read_exp_res.py

# 执行完成后，回到AIstock界面点击"同步结果"按钮"""
        elif use_custom_model:
            return f"""# QuantEvolver 实验执行命令（含自定义模型）
# 请在WSL终端中执行以下命令：

cd {wsl_path}
conda activate rdagent-gpu

# 设置环境变量
{env_block}

{_link_data_manual}

# 运行QLib回测
python {runner} conf.yaml

# 读取结果
python read_exp_res.py

# 执行完成后，回到AIstock界面点击"同步结果"按钮"""
        else:
            return f"""# QuantEvolver 实验执行命令
# 请在WSL终端中执行以下命令：

cd {wsl_path}
conda activate rdagent-gpu

{_link_data_manual}

python {runner} conf.yaml
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
            
            # 递归收集实验目录中的所有文件（包含 factors/ 子目录）
            # 文本文件直接同步，二进制文件（parquet 等）以 base64 编码同步
            import base64 as _b64
            files_to_sync = {}
            text_suffixes = {".yaml", ".yml", ".py", ".txt", ".json"}
            binary_suffixes = {".parquet"}
            for f in exp_dir.rglob("*"):
                try:
                    if f.is_file() and f.suffix in text_suffixes:
                        try:
                            rel_path = f.relative_to(exp_dir).as_posix()
                            files_to_sync[rel_path] = f.read_text(encoding="utf-8")
                        except Exception as e:
                            raise RuntimeError(f"读取文件 {f} 失败: {e}") from e
                    elif f.is_file() and f.suffix in binary_suffixes:
                        try:
                            rel_path = f.relative_to(exp_dir).as_posix()
                            files_to_sync[rel_path + ".b64"] = _b64.b64encode(f.read_bytes()).decode("ascii")
                        except Exception as e:
                            raise RuntimeError(f"读取二进制文件 {f} 失败: {e}") from e
                except OSError as e:
                    raise RuntimeError(f"检查文件状态失败: {f} - {e}") from e
            
            if not files_to_sync:
                return
            
            result = client.sync_experiment_files(experiment_name, files_to_sync)
            if result.get("success"):
                logger.info(
                    f"[QESync] API同步成功: {experiment_name}, "
                    f"{result.get('success_count', 0)} 个文件"
                )
            else:
                logger.error(
                    f"[QESync] API同步失败: {experiment_name}, "
                    f"full_result={result}"
                )
                raise RuntimeError(
                    f"[QESync] API同步失败: {experiment_name}, "
                    f"error={result.get('error', 'unknown')}, "
                    f"failed={result.get('failed', [])}"
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

    def _precompute_hmm_coefficients(
        self, strategy_params: Dict[str, Any], data_split: Dict[str, str],
    ) -> str:
        """获取 HMM 行业热度系数 JSON 字符串.

        优先读取训练时预生成的系数文件（毫秒级），文件不存在时走 WSL 子进程实时计算。
        系数文件命名: coefficients_{preset}_{test_start}_{backtest_end}.json
        存放在 model_path 同级目录。

        Raises ValueError/RuntimeError on any failure — no silent fallback.
        """
        import subprocess

        model_path = strategy_params.get("sector_hmm_model_path")
        if not model_path:
            raise ValueError("enable_sector_hmm=True 但未提供 sector_hmm_model_path")

        test_start = data_split.get("test_start")
        backtest_end = data_split.get("backtest_end")
        if not test_start or not backtest_end:
            raise ValueError(f"data_split 缺少 test_start 或 backtest_end: {data_split}")

        preset_key = strategy_params.get("hmm_signal_preset", "preset_A")
        presets = strategy_params.get("hmm_signal_presets", {})
        preset_coeffs = None
        if preset_key and preset_key in presets:
            day_coeffs = presets[preset_key].get("coefficients", {})
            preset_coeffs = day_coeffs.get("1") or next(iter(day_coeffs.values()), None)
        if not preset_coeffs:
            preset_coeffs = {"trending": 1.05, "neutral": 1.00, "fading": 0.96}

        # Windows 路径自动转 WSL 路径
        if not model_path.startswith("/"):
            mp = model_path.replace("\\", "/")
            if len(mp) >= 2 and mp[1] == ":":
                model_path = f"/mnt/{mp[0].lower()}{mp[2:]}"
            else:
                raise ValueError(f"无法识别的 model_path 格式: {model_path}")
            logger.info(f"HMM model_path 自动转换为 WSL 路径: {model_path}")

        # ── 优先读取训练时预生成的系数文件 ──
        model_dir = model_path.rsplit("/", 1)[0]
        coeff_filename = f"coefficients_{preset_key}_{test_start}_{backtest_end}.json"
        coeff_path = f"{model_dir}/{coeff_filename}"

        read_result = subprocess.run(
            ["wsl", "bash", "-c", f"cat '{coeff_path}'"],
            capture_output=True, text=True, timeout=10,
            encoding="utf-8", errors="replace",
        )
        if read_result.returncode == 0 and read_result.stdout.strip():
            data = json.loads(read_result.stdout)
            if "daily_coefficients" in data and "stock_sector_map" in data:
                logger.info(
                    f"HMM 系数文件命中: {coeff_filename} "
                    f"({data.get('sector_count', '?')} 行业, "
                    f"{len(data['daily_coefficients'])} 天)"
                )
                return read_result.stdout.strip()

        # ── 文件未命中 → WSL 子进程实时预计算 ──
        logger.info(f"HMM 系数文件未命中: {coeff_path}，启动实时预计算")

        # WSL 中访问 Windows 主机 DB
        db_params = {
            "db_host": os.getenv("TDX_DB_HOST", "127.0.0.1"),
            "db_port": int(os.getenv("TDX_DB_PORT", "5432")),
            "db_name": os.getenv("TDX_DB_NAME", "aistock"),
            "db_user": os.getenv("TDX_DB_USER", "postgres"),
            "db_password": os.environ["TDX_DB_PASSWORD"],
        }
        if db_params["db_host"] in ("127.0.0.1", "localhost"):
            try:
                _res = subprocess.run(
                    ["wsl", "bash", "-c",
                     "sed -n 's/^nameserver //p' /etc/resolv.conf | head -1"],
                    capture_output=True, text=True, timeout=5,
                    encoding="utf-8", errors="replace",
                )
                wsl_host_ip = _res.stdout.strip()
                if wsl_host_ip and wsl_host_ip not in ("10.255.255.254", ""):
                    db_params["db_host"] = wsl_host_ip
            except (OSError, subprocess.TimeoutExpired, subprocess.SubprocessError) as e:
                logger.debug("WSL host IP detection failed (non-critical): %s", e)

        stdin_params = {
            "model_path": model_path,
            "test_start": test_start,
            "backtest_end": backtest_end,
            "preset_coeffs": preset_coeffs,
            "preset_key": preset_key,
            **db_params,
        }

        _script_abs = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "..", "scripts", "precompute_hmm_coefficients.py")
        )
        _mp = _script_abs.replace("\\", "/")
        wsl_script = f"/mnt/{_mp[0].lower()}{_mp[2:]}" if len(_mp) >= 2 and _mp[1] == ":" else _mp
        wsl_cmd = (
            '. "${QLIB_WSL_CONDA_SH:-$HOME/miniconda3/etc/profile.d/conda.sh}" && '
            "conda activate rdagent-gpu && "
            f"python {wsl_script} --output-path '{coeff_path}'"
        )

        logger.info(
            f"HMM 预计算: model={model_path}, "
            f"日期={test_start}~{backtest_end}, preset={preset_key}"
        )

        proc = subprocess.run(
            ["wsl", "bash", "-c", wsl_cmd],
            input=json.dumps(stdin_params, ensure_ascii=False),
            capture_output=True, text=True, timeout=300,
            encoding="utf-8", errors="replace",
        )

        if proc.stderr:
            for line in proc.stderr.strip().splitlines():
                logger.info(f"[HMM-WSL] {line}")

        if proc.returncode != 0:
            raise RuntimeError(
                f"WSL HMM 预计算失败 (exit={proc.returncode}):\n{proc.stderr}"
            )

        stdout = proc.stdout.strip()
        if not stdout:
            raise RuntimeError("WSL HMM 预计算返回空结果")

        result = json.loads(stdout)
        if "daily_coefficients" not in result or "stock_sector_map" not in result:
            raise RuntimeError(
                f"WSL HMM 预计算结果缺少必要字段: keys={list(result.keys())}"
            )

        logger.info(
            f"HMM 预计算完成并已缓存到文件: {coeff_filename}, "
            f"{result.get('sector_count', '?')} 行业, "
            f"{len(result['daily_coefficients'])} 天"
        )

        return stdout

    def _write_custom_strategy(self, exp_dir: Path, strategy_info: Dict) -> None:
        """将自定义策略源码写入实验目录的 custom_strategy.py。
        
        写入前会进行编译验证，确保代码无语法错误。
        并自动添加必要的import语句，移除函数内部的重复import。
        """
        source_code = strategy_info.get("source_code", "")
        if not source_code:
            raise ValueError(
                f"策略 '{strategy_info.get('strategy_id', '?')}' 没有源代码 (source_code)，"
                f"无法写入策略文件。请检查策略目录。"
            )
        
        # 编译验证：检查语法错误
        validation_result = self._validate_strategy_code(source_code)
        if not validation_result["ok"]:
            raise ValueError(
                f"策略代码编译验证失败:\n{validation_result['error']}\n"
                f"请修复策略代码后再创建实验。"
            )
        
        # 处理相对导入：递归复制依赖文件到实验目录，将相对导入转为本地导入
        import re as _re
        strategy_pkg_dir = Path(__file__).parent.parent / "rebalance_strategies"
        # factor_template 目录：score_weighted_strategy.py 等策略基类所在位置
        _rdagent_root = QE_WORKSPACE_WIN.parent  # F:/Dev/RD-Agent-main
        factor_template_dir = _rdagent_root / "rdagent" / "scenarios" / "qlib" / "experiment" / "factor_template"
        # 只允许复制策略类文件，避免误复制 qrun_limit.py / read_exp_res.py 等运行时文件
        _STRATEGY_DEP_WHITELIST = {"score_weighted_strategy", "score_weighted_strategy_v2",
                                   "tail_twap_strategy", "tail_twap_v24_strategy"}

        def _copy_deps_recursive(code: str, copied: set) -> str:
            """递归处理相对导入：转换为本地导入并复制依赖文件."""
            out_lines = []
            for ln in code.split("\n"):
                s = ln.strip()
                # 匹配 from .module import ... (相对导入)
                m = _re.match(r'^(\s*)from\s+\.(\w+)\s+import\s+(.+)$', s)
                if m:
                    indent, mod, imps = m.group(1), m.group(2), m.group(3)
                    # 先查 rebalance_strategies，再查 factor_template（仅白名单模块）
                    dep = strategy_pkg_dir / f"{mod}.py"
                    if not dep.exists() and mod in _STRATEGY_DEP_WHITELIST:
                        dep = factor_template_dir / f"{mod}.py"
                    if dep.exists() and mod not in copied:
                        copied.add(mod)
                        dep_code = dep.read_text(encoding="utf-8")
                        dep_code = _copy_deps_recursive(dep_code, copied)
                        (exp_dir / f"{mod}.py").write_text(dep_code, encoding="utf-8")
                        logger.info(f"复制策略依赖文件: {mod}.py")
                    out_lines.append(f"{indent}from {mod} import {imps}")
                    continue
                # 匹配 from module import ... (无点号，检查是否为本地策略包模块)
                m2 = _re.match(r'^(\s*)from\s+(\w+)\s+import\s+(.+)$', s)
                if m2:
                    indent, mod, imps = m2.group(1), m2.group(2), m2.group(3)
                    # 先查 rebalance_strategies，再查 factor_template（仅白名单模块）
                    dep = strategy_pkg_dir / f"{mod}.py"
                    if not dep.exists() and mod in _STRATEGY_DEP_WHITELIST:
                        dep = factor_template_dir / f"{mod}.py"
                    if dep.exists() and mod not in copied:
                        copied.add(mod)
                        dep_code = dep.read_text(encoding="utf-8")
                        dep_code = _copy_deps_recursive(dep_code, copied)
                        (exp_dir / f"{mod}.py").write_text(dep_code, encoding="utf-8")
                        logger.info(f"复制策略依赖文件: {mod}.py")
                    out_lines.append(ln)
                    continue
                if s == "@register":
                    continue
                out_lines.append(ln)
            return "\n".join(out_lines)

        copied_deps: set = set()
        source_code = _copy_deps_recursive(source_code, copied_deps)
        if copied_deps:
            logger.info(f"共复制 {len(copied_deps)} 个策略依赖文件: {copied_deps}")
        
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
            valid_base_classes = {"BaseSignalStrategy", "BaseStrategy", "TopkDropoutStrategy",
                                   "ScoreWeightedTopkStrategy"}
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

        # 5. 检查 HMM 调用顺序：_apply_hmm_adjustment 必须在 None 检查之后
        if "_apply_hmm_adjustment" in source_code:
            lines = source_code.split("\n")
            hmm_line = next((i for i, l in enumerate(lines) if "_apply_hmm_adjustment" in l), None)
            none_check_line = next((i for i, l in enumerate(lines)
                                    if ("is None" in l or "if not" in l) and
                                    ("pred_score" in l or "all_pred_scores" in l or "scores" in l)
                                    and i < (hmm_line or 9999)), None)
            if hmm_line is not None and none_check_line is None:
                result["warnings"].append(
                    f"[HMM顺序警告] _apply_hmm_adjustment (行{hmm_line+1}) 在 None 检查之前调用，"
                    f"当 get_signal 返回 None 时会触发 AttributeError。"
                    f"请确保 None 检查在 HMM 调用之前。"
                )

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

    def _get_read_exp_res_content(self) -> str:
        """从 qe_programs/templates/ 读取 read_exp_res.py 模板内容。

        模板文件维护在 QE 专用程序目录中，不再内嵌于本文件。
        这样 Phase 3 增强的诊断提取功能可以直接生效。
        """
        template_path = QE_PROGRAMS_WIN / "templates" / "read_exp_res.py"
        try:
            return template_path.read_text(encoding="utf-8")
        except FileNotFoundError:
            logger.error(f"QE read_exp_res.py 模板文件不存在: {template_path}")
            raise FileNotFoundError(
                f"QE 模板文件缺失: {template_path}，"
                f"请确认 qe_programs/templates/read_exp_res.py 已部署"
            )

    def _copy_read_exp_res(self, exp_dir: Path) -> None:
        """复制read_exp_res.py模板到实验目录。"""
        template_content = self._get_read_exp_res_content()
        (exp_dir / "read_exp_res.py").write_text(template_content, encoding="utf-8")

    # NOTE: _READ_EXP_RES_TEMPLATE removed, now read from qe_programs/templates/read_exp_res.py


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
                raise RuntimeError(f"读取qlib_results.json失败: {e}") from e

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
                raise RuntimeError(f"读取qlib_res.csv失败: {e}") from e

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
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning("factor_names JSON 解析失败，作为单字符串处理: %s (error: %s)", factor_names[:80], e)
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
                # 解析 model_catalog_id（可选）
                model_catalog_id = None
                qe_task_id = exp_record.get("qe_task_id")
                if qe_task_id:
                    cur.execute("""
                        SELECT mc.id FROM qe_evolution_tasks et
                        JOIN aistock_model_catalog mc ON mc.task_run_id = et.source_task_id
                        WHERE et.task_id = %s LIMIT 1
                    """, (qe_task_id,))
                    mc_row = cur.fetchone()
                    if mc_row:
                        model_catalog_id = mc_row[0]

                for factor_name in factor_names:
                    # 确定因子来源和 catalog_id
                    factor_source, factor_catalog_id = self._detect_factor_source(cur, factor_name)
                    if factor_catalog_id is None:
                        logger.warning(f"因子 {factor_name} 未在 catalog 中找到，跳过指标保存")
                        continue
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
                                model_id, other_factors, data_split, raw_metrics,
                                factor_catalog_id, model_catalog_id
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
                                %s, %s, %s, %s,
                                %s, %s
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
                                factor_catalog_id = EXCLUDED.factor_catalog_id,
                                model_catalog_id = EXCLUDED.model_catalog_id,
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
                            factor_catalog_id,
                            model_catalog_id,
                        ))
                        logger.info(f"保存因子实验指标: {factor_name} @ {experiment_id}")
                    except Exception as e:
                        raise RuntimeError(f"保存因子 {factor_name} 实验指标失败: {e}") from e

    def _detect_factor_source(self, cur, factor_name: str) -> tuple:
        """检测因子来源并返回 catalog_id。Returns (source, catalog_id)."""
        try:
            cur.execute("""
                SELECT source, id FROM aistock_factor_catalog
                WHERE factor_name = %s
                ORDER BY CASE WHEN source = 'rdagent_task_sync' THEN 0 ELSE 1 END, id
                LIMIT 1
            """, (factor_name,))
            row = cur.fetchone()
            if row:
                return row[0], row[1]
            return (None, None)
        except Exception as e:
            raise RuntimeError(f"[QE] 因子来源检测失败: factor={factor_name}, {e}") from e
