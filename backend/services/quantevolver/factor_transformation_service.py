"""
因子代码转换工作流服务

架构原则（文件系统优先）：
- 因子源代码文件是权威数据源，所有改造操作必须以文件系统为基础
- 原始因子源代码文件（asset_path）严禁修改
- 改造后代码必须先写入文件系统（qe_factors/），再更新数据库字段
- 数据库中的 code_text / realtime_code_text 仅用于分析和展示，不作为改造依据

完整的因子代码改造工作流：
1. 从文件系统读取原始因子代码（asset_path，禁止使用数据库 code_text）
2. 规则化转换（AST/正则）
3. 编译测试 → 失败则 LLM 修复
4. 执行测试（改造后因子独立执行 + 原始因子独立执行 + 结果对比）→ 失败则 LLM 修复
5. 审核前将改造后代码写入文件系统（供审核 agent 读取）
6. LLM 最终审核（从文件系统读取完整代码，禁止使用数据库代码）→ 未通过则 LLM 修复后重审
7. 最终成功：正式写入文件系统（覆盖旧文件），更新数据库改造相关字段
"""
from __future__ import annotations

import json
import logging
import traceback
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

from ...db.pg_pool import get_conn
from .factor_code_transformer import FactorCodeTransformer

logger = logging.getLogger("aistock.factor_transformation_service")


def _now_beijing() -> datetime:
    return datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=8)))


def _get_agent_config(agent_type: str) -> tuple[Optional[str], Optional[str]]:
    """从数据库获取agent的model_id和system_prompt，返回(model_id, system_prompt)"""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT model_id, system_prompt FROM qe_agent_model_config WHERE agent_type = %s",
                    (agent_type,)
                )
                row = cur.fetchone()
                if row:
                    return row[0], row[1]
    except Exception:
        pass
    return None, None


def _call_llm(
    system_prompt: str,
    user_prompt: str,
    model_id: Optional[str] = None,
    temperature: float = 0.3,
    max_tokens: int = 4096,
    agent_type: str = "factor_transformer",
    max_retries: int = 2,
) -> str:
    """调用LLM，失败时抛出异常而非返回None。

    量化因子改造场景要求LLM调用必须成功，不允许静默降级。
    """
    import litellm
    from .llm_client import get_llm_kwargs

    # 从数据库读取agent配置
    db_model_id, db_system_prompt = _get_agent_config(agent_type)

    # system_prompt优先级：调用方传值 > 数据库配置
    if db_system_prompt:
        system_prompt = db_system_prompt

    kwargs = get_llm_kwargs(agent_type)
    if model_id:
        kwargs["model"] = model_id

    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            response = litellm.completion(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=temperature,
                max_tokens=max_tokens,
                **kwargs
            )
            content = response.choices[0].message.content
            if not content or not content.strip():
                raise ValueError("LLM返回空内容")
            return content
        except Exception as e:
            last_error = e
            logger.warning(f"LLM调用失败 (尝试 {attempt}/{max_retries}): {e}")
            if attempt < max_retries:
                import time
                time.sleep(2 * attempt)

    raise RuntimeError(f"LLM调用失败，已重试{max_retries}次。最后错误: {last_error}")


class FactorTransformationService:
    """因子代码转换工作流服务"""

    def __init__(self):
        self.transformer = FactorCodeTransformer()

    def transform_factor(
        self,
        factor_name: str,
        factor_source: str,
        max_llm_retries: int = 5,
        llm_model_id: Optional[str] = None,
        test_instruments: Optional[List[str]] = None,
        test_start_date: str = "2022-01-01",
        test_end_date: str = "2026-04-28",
    ) -> Dict[str, Any]:
        """对单个因子执行完整的改造工作流

        流程：
        1. 规则化转换
        2. 编译测试 → 失败则LLM修复（每次修复后重新编译+执行验证）
        3. 执行测试 → 失败则LLM修复（每次修复后重新编译+执行验证）
        4. LLM分析审核 → 未通过则LLM修复（重新编译+执行验证后再审核）
        5. 最终成功
        """
        logger.info(f"开始因子改造: {factor_name} ({factor_source})")
        job_id = self._create_job(factor_name, factor_source, max_llm_retries)

        try:
            # ── 获取原始因子代码（从文件系统，禁止使用数据库 code_text）────────
            original_code_path = self._get_original_code_path(factor_name, factor_source)
            if not original_code_path:
                return self._fail_job(job_id, factor_name, factor_source,
                    "未找到原始因子源代码文件。请确认 aistock_factor_catalog.asset_path 字段已填充，"
                    "且对应文件在文件系统中存在。因子源代码文件是权威数据源，禁止使用数据库 code_text。")

            original_code = self._get_original_code(factor_name, factor_source)
            if not original_code:
                return self._fail_job(job_id, factor_name, factor_source,
                    f"原始因子源代码文件存在但读取失败: {original_code_path}")

            self._update_job(job_id, status="RULE_TRANSFORMING", original_code_text=original_code)

            # ── Step 1: 规则化转换 ────────────────────────────────────────────
            rule_result = self.transformer.transform(original_code, factor_name)
            self._update_job(job_id,
                rule_transform_result={
                    "success": rule_result.success,
                    "changes": rule_result.changes,
                    "warnings": rule_result.warnings,
                    "error": rule_result.error,
                },
                current_transformed_code=rule_result.transformed_code,
            )
            current_code = rule_result.transformed_code if rule_result.success else original_code

            # ── 提前计算有效测试区间，确保全流程（编译修复、执行测试、审核修复）使用一致的日期 ──
            effective_start_date, effective_end_date, window_meta = self._select_effective_test_window(
                test_start_date, test_end_date
            )

            # ── Step 2: 编译测试 ──────────────────────────────────────────────
            self._update_job(job_id, status="COMPILE_TESTING")
            compile_ok, compile_error = self.transformer.compile_test(current_code)
            self._update_job(job_id, compile_test_result={"success": compile_ok, "error": compile_error})

            if not compile_ok:
                current_code, _ = self._llm_repair_loop(
                    job_id, factor_name, current_code, original_code,
                    "compile", compile_error, max_llm_retries, llm_model_id,
                    test_instruments, effective_start_date, effective_end_date)
                if current_code is None:
                    return self._fail_job(job_id, factor_name, factor_source,
                        f"LLM修复后仍无法通过编译测试，已尝试{max_llm_retries}次")

            # ── Step 3: 执行测试（改造后因子独立执行 + 原始因子独立执行 + 对比）─
            self._update_job(job_id, status="EXECUTION_TESTING")

            # 3a: 改造后因子独立执行
            exec_ok, exec_error, exec_result = self.transformer.execution_test(
                current_code, factor_name,
                test_instruments=test_instruments,
                test_start_date=effective_start_date,
                test_end_date=effective_end_date,
            )
            result_sample = self._extract_result_sample(exec_result)

            # 3b: 原始因子独立执行（从文件系统读取原始代码，与改造后因子完全独立）
            orig_result, orig_error = self._run_original_factor_sample(
                factor_name, original_code,
                test_instruments or ["000001.SZ", "600000.SH", "000002.SZ"],
                effective_start_date, effective_end_date,
            )
            orig_sample = self._extract_result_sample(orig_result)
            
            # 如果原始因子计算出来全是 NaN，记录警告但不直接阻断改造流程
            # 由LLM审核综合判断（原始因子可能预热期需求极大）
            import pandas as pd
            if isinstance(orig_result, pd.DataFrame) and not orig_result.empty:
                non_nan_cols = [c for c in orig_result.columns if c != 'instrument' and c != 'datetime']
                if non_nan_cols and orig_result[non_nan_cols].isna().all().all():
                    logger.warning(
                        f"原始因子 {factor_name} 在测试区间 {effective_start_date}~{effective_end_date} "
                        f"结果全NaN，可能预热期不足。将跳过结果对比，由LLM审核判断。"
                    )
                    orig_result = None
                    orig_sample = None
                    orig_error = (
                        f"原始因子在测试区间 {effective_start_date}~{effective_end_date} 内"
                        f"结果全NaN（预热期可能不足），跳过结果对比"
                    )

            self._update_job(job_id, execution_test_result={
                "success": exec_ok, "error": exec_error,
                "result_sample": result_sample,
                "original_sample": orig_sample,
                "original_error": orig_error,
                "effective_test_start_date": effective_start_date,
                "effective_test_end_date": effective_end_date,
                "window_meta": window_meta,
            })

            if not exec_ok:
                current_code, _ = self._llm_repair_loop(
                    job_id, factor_name, current_code, original_code,
                    "execution", exec_error, max_llm_retries, llm_model_id,
                    test_instruments, effective_start_date, effective_end_date)
                if current_code is None:
                    return self._fail_job(job_id, factor_name, factor_source,
                        f"LLM修复后仍无法通过执行测试，已尝试{max_llm_retries}次")
                # LLM修复后重新做执行测试（改造后因子独立执行），更新最终执行结果
                exec_ok, exec_error, exec_result = self.transformer.execution_test(
                    current_code, factor_name,
                    test_instruments=test_instruments,
                    test_start_date=effective_start_date,
                    test_end_date=effective_end_date,
                )
                result_sample = self._extract_result_sample(exec_result)
                self._update_job(job_id, execution_test_result={
                    "success": exec_ok, "error": exec_error,
                    "result_sample": result_sample,
                    "original_sample": orig_sample,
                    "original_error": orig_error,
                    "effective_test_start_date": effective_start_date,
                    "effective_test_end_date": effective_end_date,
                    "window_meta": window_meta,
                })

            # ── Step 4: 审核前先将改造后代码写入文件系统（审核必须读文件）────────
            # 注意：此处写入是为了让审核 agent 能从文件系统读取完整代码，
            # 若审核失败后需要重新改造，会再次覆盖此文件。
            self._update_job(job_id, status="ANALYSIS_REVIEWING")
            try:
                temp_qe_code_path = self._save_realtime_code(
                    factor_name, factor_source, current_code, "REVIEWING"
                )
            except RuntimeError as e:
                return self._fail_job(job_id, factor_name, factor_source,
                    f"审核前写入改造后代码到文件系统失败: {e}")

            # 解析改造后代码的绝对路径（用于审核 agent 读取）
            transformed_code_abs_path: Optional[str] = None
            if temp_qe_code_path:
                transformed_code_abs_path = self._resolve_asset_path(temp_qe_code_path)

            # ── Step 4b: LLM最终审核（从文件系统读取完整代码，禁止使用数据库代码）
            analysis_result = self._llm_analysis_review(
                factor_name, original_code, current_code, llm_model_id,
                compile_ok=True, exec_ok=exec_ok,
                original_code_path=original_code_path,
                transformed_code_path=transformed_code_abs_path,
            )
            self._update_job(job_id, analysis_result=analysis_result)

            if not analysis_result.get("final_decision", analysis_result.get("approved", True)):
                # 审核未通过：将审核问题作为错误信息，继续LLM修复循环
                review_issues = "; ".join(analysis_result.get("issues", []))
                review_reason = analysis_result.get("final_feedback",
                                    analysis_result.get("reason", "审核未通过"))
                review_error = f"审核未通过: {review_reason}\n问题: {review_issues}"
                logger.info(f"LLM审核未通过，继续修复: {factor_name} | {review_error[:200]}")

                current_code, _ = self._llm_repair_loop(
                    job_id, factor_name, current_code, original_code,
                    "review", review_error, max_llm_retries, llm_model_id,
                    test_instruments, effective_start_date, effective_end_date)
                if current_code is None:
                    return self._fail_job(job_id, factor_name, factor_source,
                        f"LLM修复后仍无法通过审核，已尝试{max_llm_retries}次。最后审核结论: {review_reason}")

                # 修复后再次写入文件系统，然后重新审核
                try:
                    temp_qe_code_path = self._save_realtime_code(
                        factor_name, factor_source, current_code, "REVIEWING"
                    )
                    if temp_qe_code_path:
                        transformed_code_abs_path = self._resolve_asset_path(temp_qe_code_path)
                except RuntimeError as e:
                    return self._fail_job(job_id, factor_name, factor_source,
                        f"二次审核前写入改造后代码到文件系统失败: {e}")

                self._update_job(job_id, status="ANALYSIS_REVIEWING")
                analysis_result = self._llm_analysis_review(
                    factor_name, original_code, current_code, llm_model_id,
                    compile_ok=True, exec_ok=exec_ok,
                    original_code_path=original_code_path,
                    transformed_code_path=transformed_code_abs_path,
                )
                self._update_job(job_id, analysis_result=analysis_result)

                if not analysis_result.get("final_decision", analysis_result.get("approved", True)):
                    reason = analysis_result.get("final_feedback",
                                 analysis_result.get("reason", "分析Agent判定改造结果不符合要求"))
                    return self._fail_job(job_id, factor_name, factor_source,
                        f"LLM分析审核未通过: {reason}")

            # ── Step 5: 最终成功：正式写入文件系统（覆盖 REVIEWING 状态的临时文件）
            self._update_job(job_id, status="SUCCESS",
                final_code_text=current_code, completed_at=_now_beijing())
            try:
                final_qe_code_path = self._save_realtime_code(
                    factor_name, factor_source, current_code, "SUCCESS"
                )
            except RuntimeError as e:
                return self._fail_job(job_id, factor_name, factor_source,
                    f"改造成功但写入文件系统失败，改造结果未保存: {e}")

            return {
                "ok": True, "job_id": job_id,
                "factor_name": factor_name, "factor_source": factor_source,
                "status": "SUCCESS", "message": "因子改造成功",
                "qe_code_path": final_qe_code_path,
                "analysis": analysis_result, "transformed_code": current_code,
            }

        except Exception as e:
            tb = traceback.format_exc()
            logger.exception(f"因子改造异常: {factor_name}")
            return self._fail_job(job_id, factor_name, factor_source,
                f"改造过程发生异常: {type(e).__name__}: {e}\n{tb}")

    def _extract_result_sample(self, exec_result: Any) -> Optional[Any]:
        """从执行结果中提取样本数据（优先展示非NaN数据用于日志记录，提供 60 行约 20 个交易日的样本）"""
        if exec_result is None:
            return None
        try:
            import pandas as pd
            if isinstance(exec_result, pd.DataFrame):
                # 优先提取非 NaN 的行进行展示
                non_nan_df = exec_result.dropna(how='all', subset=[c for c in exec_result.columns if c != 'instrument' and c != 'datetime'])
                if not non_nan_df.empty:
                    # 提供更长的样本，以涵盖约 20 个交易日（假设每次 3 只股票，60 行）
                    sample = non_nan_df.head(60).reset_index()
                else:
                    sample = exec_result.head(60).reset_index()
                sample = sample.astype(str)
                return sample.to_dict(orient="records")
        except Exception:
            pass
        return str(exec_result)[:500]

    def _select_effective_test_window(
        self,
        requested_start_date: str,
        requested_end_date: str,
        warmup_trading_days: int = 300,
        compare_trading_days: int = 20,
    ) -> Tuple[str, str, Dict[str, Any]]:
        """基于交易日历选择有效测试区间，避免长假导致测试样本落在预热阶段。"""
        sql = (
            "SELECT cal_date FROM market.trading_calendar "
            "WHERE cal_date BETWEEN %s AND %s AND is_trading = TRUE "
            "ORDER BY cal_date ASC"
        )
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (requested_start_date, requested_end_date))
                rows = cur.fetchall()

        trading_days = [r[0] for r in rows if r and r[0]]
        required = warmup_trading_days + compare_trading_days
        if len(trading_days) < required:
            raise ValueError(
                f"测试区间交易日不足: 当前 {len(trading_days)} 天，至少需要 {required} 天 "
                f"(预热{warmup_trading_days} + 对比{compare_trading_days})。"
            )

        # 取区间末尾的“预热 + 对比”窗口，避免窗口起点落在春节/国庆长假后的前几天
        effective_days = trading_days[-required:]
        effective_start = effective_days[0].strftime("%Y-%m-%d")
        effective_end = effective_days[-1].strftime("%Y-%m-%d")
        return effective_start, effective_end, {
            "requested_start_date": requested_start_date,
            "requested_end_date": requested_end_date,
            "effective_start_date": effective_start,
            "effective_end_date": effective_end,
            "warmup_trading_days": warmup_trading_days,
            "compare_trading_days": compare_trading_days,
            "trading_days_in_requested_range": len(trading_days),
            "trading_days_in_effective_range": len(effective_days),
        }

    def _run_original_factor_sample(
        self,
        factor_name: str,
        original_code: str,
        instruments: List[str],
        start_date: str,
        end_date: str,
    ) -> Tuple[Optional[Any], Optional[str]]:
        """运行原始因子代码（用临时h5文件），返回(sample_records, error_msg)"""
        import os
        import shutil
        import tempfile
        from pathlib import Path
        import pandas as pd
        try:
            from backend.data_service.realtime_factor_data_loader import RealtimeFactorDataLoader
            from backend.data_service.qe_data_service import build_static_factors

            tmpdir = tempfile.mkdtemp(prefix=f"orig_{factor_name}_")
            try:
                # 用 QE 数据生成与改造后完全相同数据范围的 h5 文件
                loader = RealtimeFactorDataLoader()
                df_pv = loader.load(
                    instruments=instruments,
                    start_date=start_date,
                    end_date=end_date,
                    fields=["open", "close", "high", "low", "volume", "amount", "factor"],
                    adjust="qfq",
                )
                # RDAgent 因子使用无 $ 前缀列名（open, close, ...），
                # RealtimeFactorDataLoader 返回的列名也是无前缀，直接写入即可
                df_pv.to_hdf(os.path.join(tmpdir, "daily_pv.h5"), key="data", mode="w")

                df_static = build_static_factors(instruments, start_date, end_date)
                if not df_static.empty:
                    df_static.to_parquet(os.path.join(tmpdir, "static_factors.parquet"))

                # 创建子目录存放因子脚本，模拟原始目录结构：
                #   tmpdir/                    ← Path(__file__).parent.parent（数据文件所在）
                #   ├── daily_pv.h5
                #   └── factors/               ← Path(__file__).parent
                #       └── {factor_name}.py   ← __file__
                # 这确保 Path(__file__).parent.parent 正确指向 tmpdir（数据文件所在目录）
                factors_subdir = os.path.join(tmpdir, "factors")
                os.makedirs(factors_subdir, exist_ok=True)

                # 生成独立执行脚本（在子进程中运行，避免 os.chdir 线程安全问题）
                project_root = str(Path(__file__).resolve().parents[3])
                runner_code = self._build_original_factor_runner_script(
                    factor_name, tmpdir, instruments, start_date, end_date, project_root
                )
                runner_path = os.path.join(tmpdir, "_runner.py")
                with open(runner_path, "w", encoding="utf-8") as fh:
                    fh.write(runner_code)

                code_path = os.path.join(factors_subdir, f"{factor_name}.py")
                with open(code_path, "w", encoding="utf-8") as fh:
                    fh.write(original_code)

                import subprocess
                import sys
                proc = subprocess.run(
                    [sys.executable, runner_path],
                    capture_output=True, text=True, timeout=600, cwd=tmpdir,
                    encoding="utf-8", errors="replace",
                )

                if proc.returncode != 0:
                    stderr_msg = proc.stderr[-4000:] if proc.stderr else "无错误输出"
                    return None, f"原始因子执行失败(exit={proc.returncode}): {stderr_msg}"

                # 读取子进程输出的结果文件
                result_path = os.path.join(tmpdir, "_result.parquet")
                if os.path.exists(result_path):
                    result = pd.read_parquet(result_path)
                    if result.empty:
                        return None, "原始因子函数返回空 DataFrame"
                    return result, None
                else:
                    stdout_msg = proc.stdout[:300] if proc.stdout else ""
                    return None, f"原始因子执行完成但未生成结果文件。stdout: {stdout_msg}"
            finally:
                shutil.rmtree(tmpdir, ignore_errors=True)
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            logger.warning(f"原始因子执行失败（不影响改造流程）: {factor_name}: {e}")
            return None, f"{type(e).__name__}: {e}\n{tb[:500]}"

    def _build_original_factor_runner_script(
        self,
        factor_name: str,
        tmpdir: str,
        instruments: List[str],
        start_date: str,
        end_date: str,
        project_root: str,
    ) -> str:
        """生成在子进程中执行原始因子代码的 runner 脚本。

        脚本在 tmpdir 作为 cwd 运行，读取 daily_pv.h5 等本地文件，
        执行因子计算后将结果写入 _result.parquet。
        """
        # 使用 repr 确保路径和参数在脚本中正确转义
        return f'''# -*- coding: utf-8 -*-
"""原始因子执行脚本（由 AIstock 因子改造系统自动生成）"""
import sys
import os
import inspect
import traceback

# 确保 cwd 是 tmpdir（subprocess 已通过 cwd 参数设置）
factor_name = {repr(factor_name)}
instruments = {repr(instruments)}
start_date = {repr(start_date)}
end_date = {repr(end_date)}
tmpdir = {repr(tmpdir)}
project_root = {repr(project_root)}

if project_root and project_root not in sys.path:
    sys.path.insert(0, project_root)

from backend.data_service.realtime_factor_data_loader import RealtimeFactorDataLoader as _RFDLoader
from backend.data_service.qe_data_service import build_static_factors as _build_static_factors

_REALTIME_LOADER = _RFDLoader()

class _StaticFactorsLoader:
    def load(self, instruments, start_date, end_date, columns=None):
        df = _build_static_factors(instruments, start_date, end_date)
        if columns and not df.empty:
            available = [c for c in columns if c in df.columns]
            return df[available] if available else df
        return df

_STATIC_FACTORS_LOADER = _StaticFactorsLoader()

def _write_result(result):
    import pandas as pd
    if isinstance(result, pd.Series):
        result = result.to_frame(factor_name)
    if isinstance(result, pd.DataFrame):
        result.to_parquet(os.path.join(tmpdir, "_result.parquet"))
        print(f"OK: shape={{result.shape}}, columns={{list(result.columns)}}")
        return True
    print(f"WARN: 返回类型不是 DataFrame: {{type(result)}}", file=sys.stderr)
    return False

def _try_write_existing_result_h5(code_path):
    import pandas as pd
    candidates = [
        os.path.join(os.path.dirname(code_path), "result.h5"),
        os.path.join(tmpdir, "result.h5"),
    ]
    for path in candidates:
        if os.path.exists(path):
            result = pd.read_hdf(path)
            return _write_result(result)
    return False

def _invoke_factor(func, func_name):
    sig = inspect.signature(func)
    params = list(sig.parameters.values())
    param_names = [p.name for p in params]
    accepts_kwargs = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params)
    can_kwargs = accepts_kwargs or all(
        name in param_names for name in ("instruments", "start_date", "end_date")
    )
    if can_kwargs:
        return func(instruments=instruments, start_date=start_date, end_date=end_date)
    positional = [
        p for p in params
        if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]
    if len(positional) >= 3:
        return func(instruments, start_date, end_date)
    if len(params) == 0 or all(p.default is not inspect.Parameter.empty for p in params):
        return func()
    raise TypeError(f"函数 {{func_name}} 参数无法自动调用: {{param_names}}")

try:
    code_path = os.path.join(tmpdir, "factors", f"{{factor_name}}.py")
    with open(code_path, "r", encoding="utf-8") as f:
        original_code = f.read()

    ns = {{
        "__name__": "__main__",
        "__builtins__": __builtins__,
        "__file__": code_path,
        "_REALTIME_LOADER": _REALTIME_LOADER,
        "_STATIC_FACTORS_LOADER": _STATIC_FACTORS_LOADER,
    }}
    exec(compile(original_code, code_path, "exec"), ns)

    # 查找 calculate_ 函数
    func_name = f"calculate_{{factor_name}}"
    if func_name not in ns:
        funcs = [k for k in ns if k.startswith("calculate_")]
        if not funcs:
            if _try_write_existing_result_h5(code_path):
                sys.exit(0)
            if "compute_factor" in ns:
                result = ns["compute_factor"]()
                if result is not None:
                    if not _write_result(result):
                        sys.exit(1)
                    sys.exit(0)
                if _try_write_existing_result_h5(code_path):
                    sys.exit(0)
            print(f"ERROR: 未找到 calculate_{{factor_name}} 函数", file=sys.stderr)
            sys.exit(1)
        func_name = funcs[0]

    func = ns[func_name]
    result = _invoke_factor(func, func_name)

    if result is None:
        if _try_write_existing_result_h5(code_path):
            sys.exit(0)
        print("ERROR: 因子函数返回 None", file=sys.stderr)
        sys.exit(1)

    if not _write_result(result):
        sys.exit(1)

except Exception:
    traceback.print_exc()
    sys.exit(1)
'''

    def batch_transform(
        self,
        factor_names: Optional[List[str]] = None,
        factor_source: Optional[str] = None,
        max_llm_retries: int = 5,
        llm_model_id: Optional[str] = None,
        only_pending: bool = True,
        max_workers: int = 3,
    ) -> Dict[str, Any]:
        """批量改造因子，支持并发执行。

        Args:
            max_workers: 最大并发数，默认3。设为1则串行执行。
        """
        import concurrent.futures

        factors = self._get_factors_to_transform(factor_names, factor_source, only_pending)
        total = len(factors)
        results = []

        logger.info(f"批量因子改造开始: 共 {total} 个因子, 并发数: {max_workers}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_factor = {
                executor.submit(
                    self.transform_factor,
                    factor_name=fname,
                    factor_source=fsource,
                    max_llm_retries=max_llm_retries,
                    llm_model_id=llm_model_id,
                ): (fname, fsource)
                for fname, fsource in factors
            }
            for future in concurrent.futures.as_completed(future_to_factor):
                fname, fsource = future_to_factor[future]
                try:
                    result = future.result()
                except Exception as e:
                    logger.exception(f"批量改造异常: {fname} ({fsource})")
                    result = {
                        "ok": False, "factor_name": fname,
                        "factor_source": fsource, "status": "FAILED",
                        "message": f"改造异常: {type(e).__name__}: {e}",
                    }
                results.append({
                    "factor_name": fname, "factor_source": fsource,
                    "ok": result.get("ok", False),
                    "status": result.get("status", "UNKNOWN"),
                    "message": result.get("message", ""),
                    "job_id": result.get("job_id"),
                })

        success_count = sum(1 for r in results if r.get("ok"))
        failed_count = total - success_count
        return {"ok": True, "total": total, "success": success_count,
                "failed": failed_count, "results": results}

    def _llm_repair_loop(
        self,
        job_id: str,
        factor_name: str,
        current_code: str,
        original_code: str,
        error_type: str,
        error_msg: Optional[str],
        max_retries: int,
        model_id: Optional[str],
        test_instruments: Optional[List[str]],
        test_start_date: str,
        test_end_date: str,
    ) -> Tuple[Optional[str], List[Dict]]:
        """LLM修复循环（携带历史错误信息迭代修复）"""
        self._update_job(job_id, status="LLM_REPAIRING")
        attempts = []
        error_history: List[Dict] = []  # 历史错误记录，传递给下一次LLM调用

        for attempt_num in range(1, max_retries + 1):
            logger.info(f"LLM修复尝试 {attempt_num}/{max_retries}: {factor_name}")

            try:
                repaired_code = self._llm_repair_code(
                    factor_name, current_code, original_code, error_type, error_msg,
                    model_id, error_history=error_history)
            except RuntimeError as e:
                # 系统配置错误（提示词缺失/LLM服务不可用），直接终止修复循环
                attempts.append({"attempt": attempt_num, "error_type": "system_error",
                                 "error_msg": str(e), "result": "系统配置错误，终止修复"})
                self._update_job(job_id, llm_repair_attempts=attempts)
                logger.error(f"LLM修复循环因系统错误终止: {factor_name}: {e}")
                return None, attempts

            attempt_record: Dict[str, Any] = {
                "attempt": attempt_num, "error_type": error_type,
                "error_msg": error_msg, "llm_provided_code": repaired_code is not None,
            }

            if repaired_code is None:
                attempt_record["result"] = "LLM未返回有效代码"
                attempts.append(attempt_record)
                error_history.append({"attempt": attempt_num, "error_type": error_type,
                                       "error_msg": error_msg, "result": "LLM未返回有效代码"})
                continue

            # 后处理：清理 LLM 引入的兜底模式和 $ 前缀残留
            repaired_code, sanitize_removals, sanitize_warnings = self.transformer.sanitize_llm_output(repaired_code)
            if sanitize_removals:
                logger.info(f"LLM输出后处理清理: {factor_name}: {sanitize_removals}")
            if sanitize_warnings:
                logger.warning(f"LLM输出后处理警告: {factor_name}: {sanitize_warnings}")

            compile_ok, compile_error = self.transformer.compile_test(repaired_code)
            if not compile_ok:
                attempt_record.update({"compile_ok": False, "compile_error": compile_error, "result": "编译失败"})
                attempts.append(attempt_record)
                error_history.append({"attempt": attempt_num, "error_type": "compile",
                                       "error_msg": compile_error, "result": "编译失败"})
                error_type = "compile"
                error_msg = compile_error
                current_code = repaired_code
                continue

            exec_ok, exec_error, _ = self.transformer.execution_test(
                repaired_code, factor_name,
                test_instruments=test_instruments,
                test_start_date=test_start_date,
                test_end_date=test_end_date,
            )
            attempt_record.update({"compile_ok": True, "exec_ok": exec_ok, "exec_error": exec_error})

            if exec_ok:
                attempt_record["result"] = "修复成功"
                attempts.append(attempt_record)
                self._update_job(job_id, llm_repair_attempts=attempts,
                    current_transformed_code=repaired_code, llm_retry_count=attempt_num)
                return repaired_code, attempts
            else:
                attempt_record["result"] = "执行失败"
                attempts.append(attempt_record)
                error_history.append({"attempt": attempt_num, "error_type": "execution",
                                       "error_msg": exec_error, "result": "执行失败"})
                error_type = "execution"
                error_msg = exec_error
                current_code = repaired_code

        self._update_job(job_id, llm_repair_attempts=attempts, llm_retry_count=max_retries)
        return None, attempts

    def _llm_repair_code(
        self,
        factor_name: str,
        current_code: str,
        original_code: str,
        error_type: str,
        error_msg: Optional[str],
        model_id: Optional[str],
        error_history: Optional[List[Dict]] = None,
    ) -> Optional[str]:
        """
        调用LLM修复因子改造代码。
        提示词结构参考 RDAgent CoSTEER evolving_strategy_factor_implementation，
        内容针对因子改造（而非因子研发）场景。
        """
        system_prompt = f"""你是一位专业的Python量化因子改造工程师。你的任务是将RDAgent研发阶段生成的因子代码（原始代码）改造为可以直接从实时数据接口读取数据的版本。

## 改造场景说明

原始因子代码通过读取本地 HDF5/Parquet 文件获取数据。改造目标是将文件读取替换为通过 `_REALTIME_LOADER` 和 `_STATIC_FACTORS_LOADER` 接口实时获取数据，同时保持因子计算逻辑完全不变。

改造后的代码中已注入以下全局对象（运行时可用，无需在代码中定义或导入）：
- `_REALTIME_LOADER`：替代 `pd.read_hdf('daily_pv.h5')`，提供行情数据
- `_STATIC_FACTORS_LOADER`：替代 `pd.read_parquet('static_factors.parquet')`，提供其他因子数据

函数签名固定为：
```python
def calculate_{factor_name}(instruments: list, start_date: str, end_date: str) -> pd.DataFrame:
```

## 数据接口规范

### _REALTIME_LOADER（替代 daily_pv.h5）
```python
df = _REALTIME_LOADER.load(
    instruments=instruments,
    start_date=start_date,
    end_date=end_date,
    fields=['open', 'close', 'high', 'low', 'volume', 'amount', 'factor'],
    adjust='qfq',
)
# 返回 MultiIndex(datetime, instrument) 的 DataFrame
# 列名直接使用：df['close'], df['volume'] 等（无 $ 前缀）
```

### _STATIC_FACTORS_LOADER（替代 static_factors.parquet / daily_basic.h5 / sector_data.h5）
```python
# 加载全部列
static_df = _STATIC_FACTORS_LOADER.load(
    instruments=instruments,
    start_date=start_date,
    end_date=end_date,
)
static_df = static_df.sort_index()

# 或指定列（推荐）
required_cols = ['db_pb', 'sw2_close']
static_df = _STATIC_FACTORS_LOADER.load(
    instruments=instruments,
    start_date=start_date,
    end_date=end_date,
    columns=required_cols,
)
```

**_STATIC_FACTORS_LOADER 包含的列（原始代码中的文件→列名映射）：**

| 原始文件 | 列名前缀 | 示例列 |
|---------|---------|-------|
| `daily_basic.h5` | `db_` | `db_pb`, `db_pe`, `db_turnover_rate`, `db_total_mv` |
| `sector_data.h5` | `sw2_` | `sw2_close`, `sw2_open`, `sw2_pb`, `sw2_pe`, `sw2_pct_change` |
| `static_factors.parquet` | `mf_`, `bb_`, `cp_` 等 | `mf_net_amt`, `bb_total_assets`, `cp_avg_cost` |

**重要**：`daily_basic.h5` 和 `sector_data.h5` 的数据已合并到 `_STATIC_FACTORS_LOADER` 中，列名不变（保留 `db_`/`sw2_` 前缀）。
原始代码中 `db = pd.read_hdf("daily_basic.h5"); db["db_pb"]` 应改为 `static_df["db_pb"]`。
原始代码中 `sector = pd.read_hdf("sector_data.h5"); sector["sw2_close"]` 应改为 `static_df["sw2_close"]`。

## 改造规则

| 原始代码 | 改造后代码 |
|---------|-----------|
| `pd.read_hdf('daily_pv.h5', key='data')` | `_REALTIME_LOADER.load(instruments, start_date, end_date, fields=[...], adjust='qfq')` |
| `pd.read_hdf('daily_basic.h5')` | `_STATIC_FACTORS_LOADER.load(instruments, start_date, end_date)` |
| `pd.read_hdf('sector_data.h5')` | `_STATIC_FACTORS_LOADER.load(instruments, start_date, end_date)` |
| `pd.read_parquet('static_factors.parquet', columns=[...])` | `_STATIC_FACTORS_LOADER.load(instruments, start_date, end_date, columns=[...])` |
| `D.features(instruments, fields, start_time, end_time)` | `_REALTIME_LOADER.load(instruments=instruments, start_date=start_time, end_date=end_time, fields=fields)` |
| `result.to_hdf(...)` / `result.to_parquet(...)` | 删除，直接 return result_df |
| `df['$close']` | `df['close']`（去掉 $ 前缀） |

## ⚠️ 严格禁止事项（违反任何一条即判定改造失败）

1. **绝对禁止 try-except 兜底**：不得在代码中编写任何 try-except 块（参考 RDAgent 硬约束："Don't write any try-except block"）。数据加载失败、列缺失等异常必须直接抛出，由上层系统捕获处理。
2. **绝对禁止空值兜底**：不得使用 `float('nan')` 填充缺失列、不得 `if df.empty: return pd.DataFrame()`、不得在 except 中赋值空 DataFrame。
3. **禁止改变计算逻辑**：因子的数学公式、计算步骤必须与原始代码完全一致。
4. **禁止文件读写**：禁止 pd.read_hdf、pd.read_parquet、pd.read_csv、to_hdf、to_parquet 等。
5. **禁止硬编码**：不得硬编码股票列表或日期。
6. **列名无 $ 前缀**：`_REALTIME_LOADER` 返回的列名为 `close`、`amount` 等，绝对不使用 `$close`、`$amount`。fields 参数也不使用 $ 前缀。

## 常见错误预防（参考 RDAgent error_prevention）

- groupby+rolling 后必须 `reset_index(level=0, drop=True)` 恢复索引对齐
- 使用字段前先检查是否存在，缺失时 `raise ValueError`
- 不要修改 df.index 结构，不要 reset_index(drop=True)
- result_df.index.names 必须继承 df.index.names，禁止手写 ["datetime", "instrument"]

## 你的任务

你的前一次改造代码出现了错误。请仔细分析错误原因，在不改变因子计算逻辑的前提下修复错误。
你必须基于最新的改造代码进行修复，不得修改已经正确的部分。

请直接输出修复后的完整Python代码，不要有任何解释文字，不要用markdown代码块包裹。"""

        # 构建历史错误记录段落（参考 RDAgent queried_former_failed_knowledge 结构）
        history_section = ""
        if error_history:
            history_lines = [
                "## 历史修复记录（之前的尝试均失败，请参考这些错误避免重复犯同样的错误）"
            ]
            for h in error_history[-3:]:
                history_lines.append(
                    f"\n=====第{h['attempt']}次尝试=====\n"
                    f"错误类型: {h['error_type']}\n"
                    f"修复结果: {h['result']}\n"
                    f"错误信息:\n{str(h.get('error_msg', ''))[:300]}"
                )
            history_section = "\n".join(history_lines) + "\n\n"

        user_prompt = f"""## 改造目标因子: {factor_name}

## 原始因子代码（权威参考，计算逻辑不得改变）:
=====原始代码=====
{original_code}

## 当前改造代码（最新一次尝试，需要在此基础上修复）:
=====当前改造代码=====
{current_code}

## 当前错误信息:
=====执行反馈=====
错误类型: {error_type}
错误详情:
{error_msg or '未知错误'}

{history_section}请仔细分析错误原因，修复当前改造代码中的问题。
要求：
1. 必须基于当前改造代码修复，不得修改已经正确的部分
2. 计算逻辑必须与原始代码完全一致
3. 不得引入任何兜底方案

直接输出修复后的完整Python代码，不要用markdown代码块包裹。"""

        try:
            from .prompt_manager import PromptManager, safe_format
            pm = PromptManager()
            prompt_data = pm.get_active_prompt_text("factor_repairer", "repair_factor_code")
            if prompt_data:
                system_prompt = prompt_data["system_prompt"]
                user_prompt = safe_format(prompt_data["user_prompt_template"], 
                    factor_name=factor_name,
                    original_code=original_code,
                    current_code=current_code,
                    error_type=error_type,
                    error_msg=(error_msg or "未知错误"),
                    history_section=history_section,
                )
            else:
                raise ValueError("未配置 factor_repairer/repair_factor_code 的提示词，拒绝使用兜底策略")
        except Exception as e:
            logger.error(f"获取 factor_repairer 提示词失败: {e}")
            raise RuntimeError(
                f"LLM修复提示词获取失败，无法执行修复。"
                f"请检查数据库中 factor_repairer/repair_factor_code 提示词配置。错误: {e}"
            ) from e

        # _call_llm 失败时会抛出 RuntimeError，由调用方 _llm_repair_loop 捕获
        response = _call_llm(system_prompt, user_prompt, model_id, temperature=0.2, max_tokens=6000,
                             agent_type="factor_repairer")
        return self._extract_code_from_response(response)

    def _llm_analysis_review(
        self,
        factor_name: str,
        original_code: str,
        transformed_code: str,
        model_id: Optional[str],
        compile_ok: bool = True,
        exec_ok: bool = True,
        original_code_path: Optional[str] = None,
        transformed_code_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        LLM最终审核。
        提示词结构参考 RDAgent evaluator_final_decision，内容针对因子改造场景。

        审核规则：
        1. 必须从文件系统读取完整的原始代码和改造后代码进行对比（禁止使用数据库代码）。
        2. 代码不完整时必须拒绝通过（final_decision=false）。
        3. 严格验证：数据获取准确性、计算逻辑一致性、计算结果对比一致性。
        """
        import os

        # 从文件系统读取完整代码（权威数据源），禁止使用传入的内存中代码作为审核依据
        original_code_for_review: Optional[str] = None
        transformed_code_for_review: Optional[str] = None
        file_read_errors: List[str] = []

        if original_code_path and os.path.exists(original_code_path):
            try:
                with open(original_code_path, "r", encoding="utf-8") as f:
                    original_code_for_review = f.read()
                logger.info(f"审核：从文件系统读取原始代码: {original_code_path}")
            except Exception as e:
                file_read_errors.append(f"读取原始代码文件失败: {original_code_path}: {e}")
        else:
            file_read_errors.append(
                f"原始代码文件不存在或路径未提供: {original_code_path}。"
                f"审核agent必须读取完整文件，无法继续。"
            )

        if transformed_code_path and os.path.exists(transformed_code_path):
            try:
                with open(transformed_code_path, "r", encoding="utf-8") as f:
                    transformed_code_for_review = f.read()
                logger.info(f"审核：从文件系统读取改造后代码: {transformed_code_path}")
            except Exception as e:
                file_read_errors.append(f"读取改造后代码文件失败: {transformed_code_path}: {e}")
        else:
            file_read_errors.append(
                f"改造后代码文件不存在或路径未提供: {transformed_code_path}。"
                f"审核agent必须读取完整文件，无法继续。"
            )

        # 如果文件读取失败，直接返回审核失败（绝对禁止代码不完整时通过）
        if file_read_errors:
            error_detail = "; ".join(file_read_errors)
            logger.error(f"审核失败：无法从文件系统读取完整代码: {error_detail}")
            return {
                "final_decision": False,
                "final_feedback": f"审核失败：无法从文件系统读取完整的因子源代码文件。"
                                  f"审核规则要求必须读取完整文件，禁止代码不完整时通过。"
                                  f"错误详情: {error_detail}",
                "approved": False,
                "confidence": 1.0,
                "logic_preserved": None,
                "interface_correct": None,
                "issues": file_read_errors,
                "suggestions": ["请确认原始因子文件和改造后因子文件均已正确保存到文件系统"],
            }

        # 验证从文件读取的代码非空
        if not original_code_for_review or not original_code_for_review.strip():
            return {
                "final_decision": False,
                "final_feedback": f"审核失败：从文件系统读取的原始代码为空: {original_code_path}",
                "approved": False, "confidence": 1.0,
                "issues": ["原始代码文件为空"], "suggestions": [],
            }
        if not transformed_code_for_review or not transformed_code_for_review.strip():
            return {
                "final_decision": False,
                "final_feedback": f"审核失败：从文件系统读取的改造后代码为空: {transformed_code_path}",
                "approved": False, "confidence": 1.0,
                "issues": ["改造后代码文件为空"], "suggestions": [],
            }

        system_prompt = """你是量化因子改造代码的最终审核专家。你的任务是对因子改造结果做出最终判断。

## 审核背景

RDAgent研发阶段生成的因子代码（原始代码）通过读取本地文件（daily_pv.h5、static_factors.parquet）获取数据。
改造目标是将文件读取替换为通过 `_REALTIME_LOADER` 和 `_STATIC_FACTORS_LOADER` 接口实时获取数据，
同时保持因子计算逻辑完全不变。

改造后的代码运行时已注入以下全局对象（无需在代码中定义）：
- `_REALTIME_LOADER`：提供行情数据（替代 daily_pv.h5）
- `_STATIC_FACTORS_LOADER`：提供其他因子数据（替代 static_factors.parquet）

## 自动化测试已完成

改造后的代码已经过以下自动化测试，测试结果将在用户消息中提供：
- 编译测试（Python语法检查）
- 执行测试（实际运行并与原始因子结果对比）

## 最终判断逻辑（参考 RDAgent evaluator_final_decision）

最终判断遵循以下逻辑：
1. 若执行测试通过且计算结果与原始因子一致（在容差范围内），改造视为成功
2. 若计算逻辑与原始代码完全一致，且执行测试通过，改造视为成功
3. 若存在以下任一情况，改造视为失败：
   - 因子计算公式或步骤与原始代码不一致
   - 存在残留的文件读写操作（pd.read_hdf、pd.read_parquet 等）
   - 存在 try-except 块（参考 RDAgent 硬约束：因子代码禁止 try-except）
   - 存在兜底方案（NaN填充缺失列、空DataFrame返回、except后pass/continue）
   - 使用了 $ 前缀列名（如 $close、$amount），正确列名无 $ 前缀
   - 函数签名不符合规范
   - 代码无法执行或返回错误格式

## 数据接口列名规范

- `_REALTIME_LOADER.load()` 的 fields 参数和返回列名均无 $ 前缀：`open, close, high, low, volume, amount, factor`
- `_STATIC_FACTORS_LOADER.load()` 返回的列名使用前缀标识数据源：`db_*`（每日基本面）、`mf_*`（资金流向）、`bb_*`（历史基本面）、`cp_*`（筹码分布）
- 若代码中出现 `$close`、`fields=['$open', ...]` 等 $ 前缀用法，必须判定为失败

## ⚠️ 绝对禁止事项

- **绝对禁止**在代码不完整时通过审核
- **绝对禁止**使用数据库中存储的代码进行审核（必须基于文件系统中的完整代码）
- **绝对禁止**因为"_REALTIME_LOADER未定义"等运行时注入对象的原因拒绝通过（这些对象在执行测试中已验证可用）
- **绝对禁止**因为 expanding window 在有限时间范围内产生 NaN 而判定失败（这是正常的窗口预热行为）

## 输出格式

请严格按照以下JSON格式输出，不得包含markdown代码块：
{
    "final_decision": true/false,
    "final_feedback": "详细的最终判断理由，包括：数据获取是否准确、计算逻辑是否一致、执行测试结果分析",
    "logic_preserved": true/false,
    "interface_correct": true/false,
    "issues": ["具体问题列表，若无问题则为空数组"],
    "suggestions": ["修复建议列表，若无建议则为空数组"]
}

重要：final_feedback 字段必须是纯文本，不得包含markdown代码块（```）。"""

        test_status = (
            f"编译测试: {'通过' if compile_ok else '失败'}\n"
            f"执行测试: {'通过（代码已实际运行并与原始因子结果对比，结果一致）' if exec_ok else '失败（代码运行出错或结果与原始因子不一致）'}"
        )

        user_prompt = f"""对因子 {factor_name} 的改造结果进行最终审核。

## 自动化测试结果
{test_status}

注意：_REALTIME_LOADER 和 _STATIC_FACTORS_LOADER 是运行时注入的全局对象，执行测试已证明它们可用，
请勿以"未定义"或"未导入"为由拒绝通过。

## 原始因子代码（从文件系统读取，路径: {original_code_path}）
=====原始代码（完整）=====
{original_code_for_review}

## 改造后因子代码（从文件系统读取，路径: {transformed_code_path}）
=====改造后代码（完整）=====
{transformed_code_for_review}

## 审核要求
请逐项核查：
1. **数据获取准确性**：改造后代码是否正确使用 _REALTIME_LOADER 和 _STATIC_FACTORS_LOADER 替换了原始文件读取，且获取的数据字段与原始代码一致
2. **计算逻辑一致性**：因子的数学公式、计算步骤是否与原始代码完全一致（逐步对比）
3. **执行测试结果**：结合上述自动化测试结果，综合判断改造是否成功
4. **禁止项检查**：是否存在残留文件读写、兜底方案（try-except+pass、NaN填充、空DataFrame返回）

输出JSON格式的最终审核结果。"""

        try:
            from .prompt_manager import PromptManager, safe_format
            pm = PromptManager()
            prompt_data = pm.get_active_prompt_text("factor_analyzer", "analyze_transformation")
            if prompt_data:
                system_prompt = prompt_data["system_prompt"]
                user_prompt = safe_format(prompt_data["user_prompt_template"], 
                    factor_name=factor_name,
                    test_status=test_status,
                    original_code_path=original_code_path,
                    transformed_code_path=transformed_code_path,
                    original_code=original_code_for_review,
                    transformed_code=transformed_code_for_review,
                )
            else:
                raise ValueError("未配置 factor_analyzer/analyze_transformation 的提示词，拒绝使用兜底策略")
        except Exception as e:
            logger.error(f"获取 factor_analyzer 提示词失败: {e}")
            return {
                "final_decision": False, "approved": False, "confidence": 1.0,
                "final_feedback": f"审核失败：无法获取审核提示词，拒绝通过。"
                                  f"量化因子改造必须经过LLM审核确认计算逻辑一致性。错误: {e}",
                "logic_preserved": None, "interface_correct": None,
                "issues": [f"提示词获取失败: {e}"],
                "suggestions": ["请检查数据库中 factor_analyzer/analyze_transformation 提示词配置"],
            }

        try:
            response = _call_llm(system_prompt, user_prompt, model_id, temperature=0.1, max_tokens=4096,
                                 agent_type="factor_analyzer")
        except RuntimeError as e:
            logger.error(f"LLM审核服务调用失败: {e}")
            return {
                "final_decision": False, "approved": False, "confidence": 1.0,
                "final_feedback": f"审核失败：LLM审核服务不可用，无法完成最终审核。"
                                  f"量化因子改造必须经过LLM审核确认计算逻辑一致性，拒绝跳过。错误: {e}",
                "logic_preserved": None, "interface_correct": None,
                "issues": [f"LLM审核服务不可用: {e}"],
                "suggestions": ["请检查LLM服务配置和网络连接"],
            }

        try:
            json_str = self._extract_json_from_response(response)
            result = json.loads(json_str)
            # 统一 approved 字段与 final_decision 保持一致
            result["approved"] = result.get("final_decision", result.get("approved", False))
            return result
        except Exception as e:
            logger.warning(f"解析LLM审核结果失败: {e}, 原始响应: {response[:500]}")
            return {
                "final_decision": False, "approved": False, "confidence": 1.0,
                "final_feedback": f"审核失败：LLM返回的审核结果无法解析为有效JSON。"
                                  f"解析错误: {e}",
                "logic_preserved": None, "interface_correct": None,
                "issues": [f"LLM响应解析失败: {e}"],
                "suggestions": ["请检查审核提示词中的输出格式要求，确保LLM返回标准JSON"],
                "raw_response": response[:1000],
            }

    def _extract_code_from_response(self, response: str) -> str:
        import re
        pattern = re.compile(r'```(?:python)?\s*\n(.*?)```', re.DOTALL)
        matches = pattern.findall(response)
        if matches:
            return matches[0].strip()
        return response.strip()

    def _extract_json_from_response(self, response: str) -> str:
        import re
        pattern = re.compile(r'```(?:json)?\s*\n(.*?)```', re.DOTALL)
        matches = pattern.findall(response)
        if matches:
            return matches[0].strip()
        brace_pattern = re.compile(r'\{.*\}', re.DOTALL)
        match = brace_pattern.search(response)
        if match:
            return match.group(0)
        return response.strip()

    def _resolve_asset_path(self, asset_path: str) -> str:
        """将 asset_path（可能是相对路径）解析为绝对路径"""
        import os
        if os.path.isabs(asset_path):
            return asset_path
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )))
        return os.path.join(project_root, asset_path)

    def _get_original_code_path(self, factor_name: str, factor_source: str) -> Optional[str]:
        """
        获取原始因子源代码文件的绝对路径。
        因子源代码文件是权威数据源，必须从文件系统读取，禁止使用数据库 code_text。
        返回绝对路径字符串，文件不存在时返回 None。
        """
        import os
        sql = "SELECT asset_path FROM aistock_factor_catalog WHERE factor_name = %s AND source = %s LIMIT 1"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (factor_name, factor_source))
                row = cur.fetchone()
                if not row or not row[0]:
                    logger.error(
                        f"因子 {factor_name} ({factor_source}) 在数据库中没有 asset_path，"
                        f"无法定位原始源代码文件。请先运行回填脚本确保 asset_path 字段已填充。"
                    )
                    return None
                asset_path = self._resolve_asset_path(row[0])
                if not os.path.exists(asset_path):
                    logger.error(
                        f"因子 {factor_name} 的源代码文件不存在: {asset_path}。"
                        f"文件系统是因子源码的权威数据源，请确认文件存在后再执行改造。"
                    )
                    return None
                return asset_path

    def _get_original_code(self, factor_name: str, factor_source: str) -> Optional[str]:
        """
        从文件系统读取原始因子源代码。
        因子源代码文件是权威数据源，严禁使用数据库中的 code_text 字段。
        """
        path = self._get_original_code_path(factor_name, factor_source)
        if not path:
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                code = f.read()
            if not code.strip():
                logger.error(f"因子 {factor_name} 的源代码文件为空: {path}")
                return None
            logger.info(f"从文件系统读取原始因子代码: {path}")
            return code
        except Exception as e:
            logger.error(f"读取因子源代码文件失败: {path}: {e}")
            return None

    def _get_factors_to_transform(
        self,
        factor_names: Optional[List[str]],
        factor_source: Optional[str],
        only_pending: bool,
    ) -> List[Tuple[str, str]]:
        """
        获取待改造因子列表。
        以 asset_path 字段非空为前提（文件系统是权威数据源），不依赖 code_text。
        only_pending=True 时跳过已成功改造的因子；only_pending=False 时支持重新改造。
        """
        import os
        conditions = ["asset_path IS NOT NULL AND asset_path != ''"]
        params = []

        if factor_names:
            placeholders = ",".join(["%s"] * len(factor_names))
            conditions.append(f"factor_name IN ({placeholders})")
            params.extend(factor_names)
        if factor_source:
            conditions.append("source = %s")
            params.append(factor_source)
        if only_pending:
            conditions.append(
                "(transformation_status IS NULL OR transformation_status IN ('PENDING', 'FAILED'))"
            )

        where_clause = " AND ".join(conditions)
        sql = f"""
            SELECT factor_name, source, asset_path FROM aistock_factor_catalog
            WHERE {where_clause}
            ORDER BY CASE WHEN source = 'alpha158' THEN 0 ELSE 1 END,
                     is_sota_factor DESC NULLS LAST, factor_name
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()

        # 过滤：文件系统中实际存在源代码文件的因子才纳入改造
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.dirname(os.path.abspath(__file__))
        )))
        result = []
        for factor_name, source, asset_path in rows:
            abs_path = asset_path if os.path.isabs(asset_path) else os.path.join(project_root, asset_path)
            if os.path.exists(abs_path):
                result.append((factor_name, source))
            else:
                logger.warning(f"跳过因子 {factor_name}：asset_path 指向的文件不存在: {abs_path}")
        return result

    def _create_job(self, factor_name: str, factor_source: str, max_llm_retries: int) -> str:
        sql = """
            INSERT INTO qe_factor_transformation_jobs
                (factor_name, factor_source, status, max_llm_retries, started_at, updated_at)
            VALUES (%s, %s, 'PENDING', %s, NOW(), NOW())
            RETURNING job_id
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (factor_name, factor_source, max_llm_retries))
                return cur.fetchone()[0]

    def _update_job(self, job_id: str, **kwargs) -> None:
        if not kwargs:
            return

        set_parts = []
        params = []

        field_map = {
            "status": "text", "original_code_text": "text",
            "current_transformed_code": "text", "final_code_text": "text",
            "rule_transform_result": "jsonb", "compile_test_result": "jsonb",
            "execution_test_result": "jsonb", "llm_repair_attempts": "jsonb",
            "analysis_result": "jsonb", "error_message": "text",
            "llm_retry_count": "int", "completed_at": "timestamptz",
        }

        for key, value in kwargs.items():
            if key in field_map:
                col_type = field_map[key]
                if col_type == "jsonb":
                    set_parts.append(f"{key} = %s::jsonb")
                    params.append(json.dumps(value, ensure_ascii=False, default=str))
                else:
                    set_parts.append(f"{key} = %s")
                    params.append(value)

        if not set_parts:
            return

        set_parts.append("updated_at = NOW()")
        params.append(job_id)
        sql = f"UPDATE qe_factor_transformation_jobs SET {', '.join(set_parts)} WHERE job_id = %s"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)

    def _fail_job(self, job_id: str, factor_name: str, factor_source: str, error_message: str) -> Dict[str, Any]:
        self._update_job(job_id, status="FAILED", error_message=error_message, completed_at=_now_beijing())
        try:
            self._save_realtime_code(factor_name, factor_source, None, "FAILED")
        except Exception as e:
            logger.warning(f"_fail_job 更新数据库状态时出错（不影响失败结果）: {e}")
        logger.error(f"因子改造失败: {factor_name} ({factor_source}): {error_message[:200]}")
        return {"ok": False, "job_id": job_id, "factor_name": factor_name,
                "factor_source": factor_source, "status": "FAILED", "message": error_message}

    def _save_realtime_code(
        self,
        factor_name: str,
        factor_source: str,
        code: Optional[str],
        status: str,
    ) -> Optional[str]:
        """
        将改造后的代码保存到文件系统，并更新数据库中的相关字段。

        规则：
        1. 改造成功（status=SUCCESS）时，必须先将代码写入文件系统，成功后再更新数据库。
           若文件写入失败，则整体视为失败，不更新数据库。
        2. 支持重新改造：若 qe_factors/{factor_name}.py 已存在，直接覆盖。
        3. 严禁修改原始因子的 asset_path 和 code_text 字段。
        4. 改造失败时清空 qe_code_path（保留历史路径供参考则用 COALESCE，此处选择清空以避免指向过期文件）。
        5. 返回写入的 qe_code_path 相对路径（成功时），失败时返回 None。
        """
        import os

        qe_code_path: Optional[str] = None

        # SUCCESS 或 REVIEWING 状态均需写入文件系统
        # REVIEWING：审核前临时写入，让审核 agent 能从文件系统读取完整代码
        # SUCCESS：最终成功，正式写入（覆盖 REVIEWING 状态的临时文件）
        if status in ("SUCCESS", "REVIEWING") and code:
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(
                os.path.dirname(os.path.abspath(__file__))
            )))
            qe_dir = os.path.join(project_root, "rdagent_assets", "qe_factors")
            os.makedirs(qe_dir, exist_ok=True)

            file_name = f"{factor_name}.py"
            file_abs_path = os.path.join(qe_dir, file_name)
            is_overwrite = os.path.exists(file_abs_path)
            try:
                with open(file_abs_path, "w", encoding="utf-8") as f:
                    f.write(code)
                qe_code_path = f"rdagent_assets/qe_factors/{file_name}"
                action = "覆盖" if is_overwrite else "新建"
                logger.info(f"改造后代码已{action}写入文件[{status}]: {file_abs_path}")
            except Exception as e:
                logger.error(f"改造后代码写入文件系统失败，终止后续操作: {factor_name}: {e}")
                raise RuntimeError(f"改造后代码写入文件系统失败: {e}") from e

        # REVIEWING 状态只写文件，不更新数据库 transformation_status（避免污染状态字段）
        if status == "REVIEWING":
            return qe_code_path

        # 其他状态（SUCCESS / FAILED）：更新数据库（仅更新改造相关字段，严禁修改 asset_path 和 code_text）
        sql = """
            UPDATE aistock_factor_catalog
            SET realtime_code_text = %s,
                transformation_status = %s,
                last_transformation_at = NOW(),
                qe_code_path = %s
            WHERE factor_name = %s AND source = %s
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (code, status, qe_code_path, factor_name, factor_source))
        return qe_code_path

    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """获取任务状态"""
        sql = """
            SELECT job_id, factor_name, factor_source, status,
                   rule_transform_result, compile_test_result, execution_test_result,
                   llm_repair_attempts, analysis_result, error_message,
                   llm_retry_count, max_llm_retries,
                   created_at, started_at, completed_at, updated_at
            FROM qe_factor_transformation_jobs
            WHERE job_id = %s
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (job_id,))
                row = cur.fetchone()
                if not row:
                    return None
                cols = [d[0] for d in cur.description]
                result = dict(zip(cols, row))
                for k, v in result.items():
                    if hasattr(v, 'isoformat'):
                        result[k] = v.isoformat()
                return result

    def list_jobs(
        self,
        factor_name: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """列出改造任务"""
        conditions = []
        params = []
        if factor_name:
            conditions.append("factor_name = %s")
            params.append(factor_name)
        if status:
            conditions.append("status = %s")
            params.append(status)

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        sql = f"""
            SELECT job_id, factor_name, factor_source, status,
                   error_message, llm_retry_count, max_llm_retries,
                   created_at, started_at, completed_at
            FROM qe_factor_transformation_jobs
            WHERE {where_clause}
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
        """
        count_sql = f"SELECT COUNT(*) FROM qe_factor_transformation_jobs WHERE {where_clause}"

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(count_sql, params)
                total = cur.fetchone()[0]
                cur.execute(sql, params + [limit, offset])
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]

        for row in rows:
            for k, v in row.items():
                if hasattr(v, 'isoformat'):
                    row[k] = v.isoformat()

        return {"ok": True, "total": total, "items": rows}

    def get_factor_transformation_status(
        self,
        factor_names: Optional[List[str]] = None,
        factor_source: Optional[str] = None,
        limit: int = 200,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """获取因子改造状态列表（从 aistock_factor_catalog）"""
        conditions = []
        params = []
        if factor_names:
            placeholders = ",".join(["%s"] * len(factor_names))
            conditions.append(f"factor_name IN ({placeholders})")
            params.extend(factor_names)
        if factor_source:
            conditions.append("source = %s")
            params.append(factor_source)

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        sql = f"""
            SELECT factor_name, source, transformation_status,
                   last_transformation_at,
                   (qe_code_path IS NOT NULL AND qe_code_path != '') AS has_realtime_code,
                   (asset_path IS NOT NULL AND asset_path != '') AS has_original_code,
                   is_sota_factor, ic, sharpe, qe_code_path
            FROM aistock_factor_catalog
            WHERE {where_clause}
            ORDER BY CASE WHEN source = 'alpha158' THEN 0 ELSE 1 END,
                     is_sota_factor DESC NULLS LAST, factor_name
            LIMIT %s OFFSET %s
        """
        count_sql = f"SELECT COUNT(*) FROM aistock_factor_catalog WHERE {where_clause}"

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(count_sql, params)
                total = cur.fetchone()[0]
                cur.execute(sql, params + [limit, offset])
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]

        for row in rows:
            for k, v in row.items():
                if hasattr(v, 'isoformat'):
                    row[k] = v.isoformat()

        return {"ok": True, "total": total, "items": rows}
