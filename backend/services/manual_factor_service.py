"""手工因子入库 + 统一独立指标计算服务。

支持所有因子（manual + rdagent_task_sync）的独立指标计算，
不依赖 RDAgent workspace，从 DB code_text 直接执行。
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import re
import shlex
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..db.pg_pool import get_conn

logger = logging.getLogger(__name__)

# ── WSL 路径配置 ──
RDAGENT_FACTOR_DATA_WSL = os.getenv(
    "RDAGENT_FACTOR_DATA_WSL",
    "/mnt/f/dev/RD-Agent-main/git_ignore_folder/factor_implementation_source_data",
)
COMPUTE_SCRIPT_WSL = "/mnt/f/Dev/AIstock/scripts/compute_factor_metrics_unified.py"
CONDA_ENV = "rdagent-gpu"
RDAGENT_ROOT_WSL = os.getenv("RDAGENT_ROOT_WSL", "/mnt/f/Dev/RD-Agent-main")

# WSL 原生文件系统中的专用 workspace（数据 symlink 已预置，IO 性能远优于 /mnt/）
FACTOR_WORKSPACE_WSL = os.getenv(
    "FACTOR_WORKSPACE_WSL",
    "/home/lc999/factor_workspace",
)

# 数据文件列表
DATA_FILES = [
    "daily_pv.h5", "daily_basic.h5", "moneyflow.h5",
    "bak_basic.h5", "cyq_perf.h5", "sector_data.h5",
    "static_factors.parquet",
]

FACTOR_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]{2,80}$")

FACTOR_TEMPLATE = '''import pandas as pd
import numpy as np
from pathlib import Path

# 可用数据集（MultiIndex: datetime, instrument）:
# daily_pv.h5    : open, close, high, low, volume, factor, amount
# daily_basic.h5 : db_close, db_turnover_rate, db_turnover_rate_f, db_volume_ratio,
#                   db_pe, db_pe_ttm, db_pb, db_ps, db_ps_ttm,
#                   db_dv_ratio, db_dv_ttm, db_total_share, db_float_share,
#                   db_free_share, db_total_mv, db_circ_mv
# moneyflow.h5   : mf_sm_buy_vol, mf_sm_buy_amt, mf_sm_sell_vol, mf_sm_sell_amt,
#                   mf_md_buy_vol, mf_md_buy_amt, mf_md_sell_vol, mf_md_sell_amt,
#                   mf_lg_buy_vol, mf_lg_buy_amt, mf_lg_sell_vol, mf_lg_sell_amt,
#                   mf_elg_buy_vol, mf_elg_buy_amt, mf_elg_sell_vol, mf_elg_sell_amt,
#                   mf_net_vol, mf_net_amt
# bak_basic.h5   : bb_pe_dyn, bb_total_assets, bb_liquid_assets, bb_fixed_assets,
#                   bb_reserved, bb_reserved_pershare, bb_eps, bb_bvps, bb_undp,
#                   bb_per_undp, bb_rev_yoy, bb_profit_yoy, bb_gpr, bb_npr,
#                   bb_holder_num
# cyq_perf.h5    : cp_his_low, cp_his_high, cp_cost_5pct, cp_cost_15pct,
#                   cp_cost_50pct, cp_cost_85pct, cp_cost_95pct,
#                   cp_weight_avg, cp_winner_rate
# sector_data.h5 : sw2_open, sw2_high, sw2_low, sw2_close, sw2_pct_change,
#                   sw2_vol, sw2_amount, sw2_pe, sw2_pb, sw2_total_mv,
#                   sw2_mf_buy_sm_amt, sw2_mf_sell_sm_amt, sw2_mf_buy_md_amt,
#                   sw2_mf_sell_md_amt, sw2_mf_buy_lg_amt, sw2_mf_sell_lg_amt,
#                   sw2_mf_buy_elg_amt, sw2_mf_sell_elg_amt, sw2_mf_net_amt,
#                   sw2_mf_buy_elg_vol, sw2_mf_sell_elg_vol, sw2_mf_net_vol

DATA_DIR = Path(__file__).resolve().parent.parent
FACTOR_NAME = "{factor_name}"

def compute_factor():
    df = pd.read_hdf(DATA_DIR / "daily_pv.h5")

    # ===== FACTOR COMPUTATION AREA =====
    # 在这里编写因子计算逻辑
    factor = df["close"].pct_change(5)  # 示例：5日动量
    # ===== END FACTOR COMPUTATION =====

    result = factor.to_frame(FACTOR_NAME)
    result.index.names = ["datetime", "instrument"]
    result = result.dropna()
    result.to_hdf(Path(__file__).resolve().parent / "result.h5", key="data")

if __name__ == "__main__":
    compute_factor()
'''


def _win_to_wsl(win_path: str) -> str:
    """Convert Windows path to WSL path."""
    p = win_path.replace("\\", "/")
    if len(p) >= 2 and p[1] == ":":
        drive = p[0].lower()
        p = f"/mnt/{drive}{p[2:]}"
    return p


def _build_wsl_copy_file_script(wsl_source_path: str, windows_target_path: Path) -> str:
    """Build a WSL-side copy script that writes into a Windows temp path."""

    wsl_target_path = _win_to_wsl(str(windows_target_path))
    wsl_target_dir = _win_to_wsl(str(windows_target_path.parent))
    return (
        "set -e\n"
        f"if [ ! -f {shlex.quote(wsl_source_path)} ]; then exit 64; fi\n"
        f"mkdir -p {shlex.quote(wsl_target_dir)}\n"
        f"cp {shlex.quote(wsl_source_path)} {shlex.quote(wsl_target_path)}\n"
    )


class ManualFactorService:
    """手工因子入库 + 统一独立指标计算。"""

    def get_template(self, factor_name: str = "m_example_factor") -> str:
        return FACTOR_TEMPLATE.format(factor_name=factor_name)

    async def validate_factor_code(
        self, factor_name: str, code_text: str
    ) -> Dict[str, Any]:
        """验证因子代码：WSL 原生 workspace 执行 + 检查 result.h5 格式。"""
        if not FACTOR_NAME_PATTERN.match(factor_name):
            return {
                "success": False,
                "message": f"因子名格式错误，需匹配 {FACTOR_NAME_PATTERN.pattern}",
            }

        wsl_factor_dir = f"{FACTOR_WORKSPACE_WSL}/_factor_{factor_name}"

        try:
            import time
            t0 = time.time()

            # 通过 WSL 写入因子代码并执行（含数据文件 symlink）
            data_files_str = " ".join(DATA_FILES)
            validate_script = (
                f"set -e\n"
                f"source ~/miniconda3/etc/profile.d/conda.sh\n"
                f"conda activate {CONDA_ENV}\n"
                f"mkdir -p {wsl_factor_dir}\n"
                f"for f in {data_files_str}; do "
                f'[ -e "{FACTOR_WORKSPACE_WSL}/$f" ] && [ ! -e "{wsl_factor_dir}/$f" ] && '
                f'ln -sf "{FACTOR_WORKSPACE_WSL}/$f" "{wsl_factor_dir}/$f"; done; true\n'
                f"cat > {wsl_factor_dir}/factor.py << 'FACTOREOF'\n{code_text}\nFACTOREOF\n"
                f"cd {wsl_factor_dir} && python factor.py"
            )
            # 写入临时文件避免 Windows 命令行长度限制 (WinError 206)
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".sh", delete=False, encoding="utf-8", newline="\n"
            ) as tmp:
                tmp.write(validate_script)
                tmp_path_v = tmp.name
            try:
                wsl_tmp_v = _win_to_wsl(tmp_path_v)
                proc = await asyncio.create_subprocess_exec(
                    "wsl", "bash", wsl_tmp_v,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                try:
                    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=120)
                except asyncio.TimeoutError:
                    proc.kill()
                    return {"success": False, "message": "因子代码执行超时 (120s)"}
            finally:
                os.unlink(tmp_path_v)

            duration = time.time() - t0

            if proc.returncode != 0:
                err = stderr.decode("utf-8", errors="replace")[-500:]
                return {"success": False, "message": f"执行失败: {err}", "duration_sec": round(duration, 2)}

            wsl_result_h5 = f"{wsl_factor_dir}/result.h5"
            with tempfile.TemporaryDirectory(prefix="aistock_factor_result_") as result_dir:
                win_result_h5 = Path(result_dir) / "result.h5"
                copy_script = _build_wsl_copy_file_script(wsl_result_h5, win_result_h5)
                with tempfile.NamedTemporaryFile(
                    mode="w", suffix=".sh", delete=False, encoding="utf-8", newline="\n"
                ) as tmp_copy:
                    tmp_copy.write(copy_script)
                    tmp_copy_path = tmp_copy.name
                try:
                    proc_copy = await asyncio.create_subprocess_exec(
                        "wsl", "bash", _win_to_wsl(tmp_copy_path),
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )
                    copy_stdout, copy_stderr = await asyncio.wait_for(proc_copy.communicate(), timeout=30)
                finally:
                    os.unlink(tmp_copy_path)

                if proc_copy.returncode == 64:
                    return {"success": False, "message": "执行完成但未生成 result.h5", "duration_sec": round(duration, 2)}
                if proc_copy.returncode != 0:
                    detail = (copy_stderr or copy_stdout).decode("utf-8", errors="replace")[-500:]
                    return {"success": False, "message": f"复制 result.h5 失败: {detail}", "duration_sec": round(duration, 2)}
                if not win_result_h5.exists():
                    return {"success": False, "message": "result.h5 未复制到 Windows 临时目录", "duration_sec": round(duration, 2)}

                import pandas as pd
                try:
                    df = pd.read_hdf(str(win_result_h5))
                except Exception as e:
                    return {"success": False, "message": f"无法读取 result.h5: {e}", "duration_sec": round(duration, 2)}

            if not isinstance(df.index, pd.MultiIndex) or df.index.nlevels != 2:
                return {"success": False, "message": "result.h5 缺少 MultiIndex(datetime, instrument)", "duration_sec": round(duration, 2)}

            shape = list(df.shape)
            sample = df.head(3).reset_index().to_dict(orient="records")

            return {
                "success": True,
                "message": "验证通过",
                "shape": shape,
                "sample_data": sample,
                "duration_sec": round(duration, 2),
            }
        finally:
            # 清理因子子目录
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".sh", delete=False, encoding="utf-8", newline="\n"
            ) as tmp:
                tmp.write(f"rm -rf {wsl_factor_dir}\n")
                tmp_path_cleanup = tmp.name
            try:
                await asyncio.create_subprocess_exec(
                    "wsl", "bash", _win_to_wsl(tmp_path_cleanup),
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )
            finally:
                os.unlink(tmp_path_cleanup)

    async def save_factor(
        self,
        factor_name: str,
        code_text: str,
        description: Optional[str] = None,
        expression: Optional[str] = None,
        ic: Optional[float] = None,
        sharpe: Optional[float] = None,
        annualized_return: Optional[float] = None,
    ) -> Dict[str, Any]:
        """入库因子到 aistock_factor_catalog (source='manual') + LLM 分类评级。"""
        if not FACTOR_NAME_PATTERN.match(factor_name):
            raise ValueError(f"因子名格式错误: {factor_name}")

        now_utc = datetime.now(timezone.utc).isoformat()

        # 保存源代码文件到文件系统
        aistock_root = Path(__file__).resolve().parent.parent.parent
        manual_factors_dir = aistock_root / "rdagent_assets" / "manual_factors"
        manual_factors_dir.mkdir(parents=True, exist_ok=True)
        factor_file = manual_factors_dir / f"{factor_name}.py"
        factor_file.write_text(code_text, encoding="utf-8")
        try:
            asset_path_value = factor_file.relative_to(aistock_root).as_posix()
        except ValueError:
            asset_path_value = str(factor_file)

        sql = """
            INSERT INTO aistock_factor_catalog (
                factor_name, source, catalog_version, generated_at_utc, catalog_source,
                expression, code_text, asset_path
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (factor_name, source) DO UPDATE SET
                catalog_version = EXCLUDED.catalog_version,
                generated_at_utc = EXCLUDED.generated_at_utc,
                expression = COALESCE(EXCLUDED.expression, aistock_factor_catalog.expression),
                code_text = EXCLUDED.code_text,
                asset_path = EXCLUDED.asset_path
            RETURNING id
        """
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (
                    factor_name, "manual", "manual_v1", now_utc, "manual",
                    expression, code_text, asset_path_value,
                ))
                row = cur.fetchone()
                catalog_id = row[0] if row else None

                # 更新 description_cn if provided
                if description:
                    cur.execute(
                        "UPDATE aistock_factor_catalog SET description_cn = %s WHERE id = %s",
                        (description, catalog_id),
                    )

        # LLM 分类（评级只读，由 FactorRatingService 统一管理）
        classification_result = None
        try:
            from .quantevolver.factor_analyst import FactorAnalyst
            analyst = FactorAnalyst()
            classification_result = analyst.analyze_single_factor(
                factor_name=factor_name,
                factor_source="manual",
                use_llm=True,
            )
            logger.info(f"LLM 分类完成: {factor_name} -> {classification_result.get('category')}")
        except Exception as e:
            logger.warning(f"LLM 分类失败 {factor_name}: {e}")

        return {
            "factor_name": factor_name,
            "source": "manual",
            "catalog_id": catalog_id,
            "classification": classification_result,
        }

    async def batch_compute_metrics(
        self,
        factor_names: Optional[List[str]] = None,
        all_available: bool = False,
        data_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """统一批量独立指标计算（所有因子通用）。

        从 DB 读取 code_text → WSL 批量执行 → engine 计算 → UPSERT 指标。

        Parameters
        ----------
        data_date : 快照日期 (YYYYMMDD)。指定后传递给 WSL 计算脚本，
                    脚本将使用对应快照数据。
        """
        import time
        t0 = time.time()

        # Step 1: 获取因子代码
        factors_code = self._get_factors_code(factor_names, all_available)
        if not factors_code:
            return {"success": False, "error": "未找到可计算的因子（需有 code_text）"}

        logger.info(f"批量计算 {len(factors_code)} 个因子的独立指标")

        # Step 2: 通过 WSL 写入因子代码到原生 workspace
        wsl_workspace = FACTOR_WORKSPACE_WSL
        factor_dirs_to_clean = []

        try:
            # 构建 WSL 命令：批量创建因子目录 + 写入 factor.py
            data_files_str = " ".join(DATA_FILES)
            setup_cmds = []
            for fname, code in factors_code.items():
                wsl_fdir = f"{wsl_workspace}/_factor_{fname}"
                factor_dirs_to_clean.append(wsl_fdir)
                # 先清理可能残留的旧目录，写入 factor.py，并创建数据文件 symlink
                setup_cmds.append(f"rm -rf {wsl_fdir} && mkdir -p {wsl_fdir}")
                setup_cmds.append(
                    f"cat > {wsl_fdir}/factor.py << 'FACTOREOF'\n{code}\nFACTOREOF"
                )
                setup_cmds.append(
                    f'for f in {data_files_str}; do '
                    f'[ -e "{wsl_workspace}/$f" ] && [ ! -e "{wsl_fdir}/$f" ] && '
                    f'ln -sf "{wsl_workspace}/$f" "{wsl_fdir}/$f"; done; true'
                )

            setup_script = "\n".join(setup_cmds)
            # 写入临时文件避免 Windows 命令行长度限制 (WinError 206)
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".sh", delete=False, encoding="utf-8", newline="\n"
            ) as tmp:
                tmp.write(setup_script)
                tmp_path = tmp.name
            try:
                wsl_tmp = _win_to_wsl(tmp_path)
                proc = await asyncio.create_subprocess_exec(
                    "wsl", "bash", wsl_tmp,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout_setup, stderr_setup = await asyncio.wait_for(
                    proc.communicate(), timeout=30
                )
            finally:
                os.unlink(tmp_path)
            if proc.returncode != 0:
                err = stderr_setup.decode("utf-8", errors="replace")[-500:]
                return {"success": False, "error": f"WSL 写入因子代码失败: {err}", "duration_sec": round(time.time() - t0, 2)}

            # Step 3: WSL 执行统一计算脚本
            names_arg = " ".join(shlex.quote(n) for n in factors_code.keys())

            wsl_cmd = (
                f"source ~/miniconda3/etc/profile.d/conda.sh && "
                f"conda activate {CONDA_ENV} && "
                f"export PYTHONPATH={RDAGENT_ROOT_WSL}:$PYTHONPATH && "
                f"python {COMPUTE_SCRIPT_WSL} {wsl_workspace} {names_arg}"
            )
            if data_date:
                wsl_cmd += f" --data-date {data_date}"

            # 写入临时文件避免 Windows 命令行长度限制 (WinError 206)
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".sh", delete=False, encoding="utf-8", newline="\n"
            ) as tmp:
                tmp.write(wsl_cmd)
                tmp_path2 = tmp.name
            try:
                wsl_tmp2 = _win_to_wsl(tmp_path2)
                proc = await asyncio.create_subprocess_exec(
                    "wsl", "bash", wsl_tmp2,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )

                # WSL 内部已有单因子 600s 超时，此处只防进程 hang 死
                # 并行4路: 最慢因子600s + engine计算120s + 余量
                timeout = max(900, math.ceil(len(factors_code) / 4) * 650 + 180)
                try:
                    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
                except asyncio.TimeoutError:
                    proc.kill()
                    return {
                        "success": False,
                        "error": f"计算超时 ({timeout}s)，因子列表: {list(factors_code.keys())}",
                        "factor_names": list(factors_code.keys()),
                        "duration_sec": round(time.time() - t0, 2),
                    }
            finally:
                os.unlink(tmp_path2)

            if proc.returncode != 0:
                err_full = stderr.decode("utf-8", errors="replace")
                # 优先取 Traceback 段，否则取最后 2000 字符
                tb_idx = err_full.rfind("Traceback")
                if tb_idx >= 0:
                    err = err_full[tb_idx:][:2000]
                else:
                    err = err_full[-2000:]
                return {"success": False, "error": f"WSL 执行失败: {err}", "duration_sec": round(time.time() - t0, 2)}

            # Step 4: 解析 JSON 输出
            stdout_text = stdout.decode("utf-8", errors="replace").strip()
            # 找到最后一行有效 JSON（stderr 可能混入 stdout）
            json_line = ""
            for line in reversed(stdout_text.split("\n")):
                line = line.strip()
                if line.startswith("{"):
                    json_line = line
                    break

            if not json_line:
                return {
                    "success": False,
                    "error": f"WSL 输出中无有效 JSON: {stdout_text[-500:]}",
                    "duration_sec": round(time.time() - t0, 2),
                }

            try:
                result = json.loads(json_line)
            except json.JSONDecodeError as e:
                return {"success": False, "error": f"JSON 解析失败: {e}", "duration_sec": round(time.time() - t0, 2)}

            if not result.get("success"):
                return {
                    "success": False,
                    "error": result.get("error", "未知错误"),
                    "execution_log": result.get("execution_log"),
                    "factor_names": list(factors_code.keys()),
                    "duration_sec": round(time.time() - t0, 2),
                }

            # Step 5: 写入数据库
            factors_metrics = result.get("factors", {})
            db_result = self._save_metrics_to_db(
                factors_metrics, result.get("calc_batch_id", str(uuid.uuid4())),
                data_date=data_date,
            )

            return {
                "success": True,
                "factors": factors_metrics,
                "execution_log": result.get("execution_log"),
                "db_result": db_result,
                "engine_summary": result.get("engine_summary"),
                "total_duration_sec": round(time.time() - t0, 2),
            }
        finally:
            # 清理因子子目录（保留 workspace 根目录和数据 symlink）
            if factor_dirs_to_clean:
                cleanup_cmd = " && ".join(f"rm -rf {d}" for d in factor_dirs_to_clean)
                with tempfile.NamedTemporaryFile(
                    mode="w", suffix=".sh", delete=False, encoding="utf-8", newline="\n"
                ) as tmp:
                    tmp.write(cleanup_cmd)
                    tmp_path_c = tmp.name
                try:
                    wsl_tmp_c = _win_to_wsl(tmp_path_c)
                    await asyncio.create_subprocess_exec(
                        "wsl", "bash", wsl_tmp_c,
                        stdout=asyncio.subprocess.DEVNULL,
                        stderr=asyncio.subprocess.DEVNULL,
                    )
                finally:
                    os.unlink(tmp_path_c)

    async def full_pipeline(
        self,
        factor_name: str,
        code_text: str,
        description: Optional[str] = None,
        expression: Optional[str] = None,
    ) -> Dict[str, Any]:
        """完整流水线：验证 → 入库 → 指标计算 → LLM 分类。"""
        # Step 1: 验证
        val_result = await self.validate_factor_code(factor_name, code_text)
        if not val_result["success"]:
            return {"success": False, "stage": "validate", "error": val_result["message"]}

        # Step 2: 入库（含 LLM 分类）
        save_result = await self.save_factor(
            factor_name, code_text, description, expression,
        )

        # Step 3: 计算独立指标
        metrics_result = await self.batch_compute_metrics(factor_names=[factor_name])

        # Step 4: 用指标更新评级
        if metrics_result.get("success"):
            fm = metrics_result.get("factors", {}).get(factor_name, {})
            full_metrics = fm.get("full", {})
            if full_metrics:
                # 用计算结果重新 LLM 分类；独立指标只允许官方评估写入 aistock_factor_metrics。
                try:
                    from .quantevolver.factor_analyst import FactorAnalyst
                    analyst = FactorAnalyst()
                    save_result["classification"] = analyst.analyze_single_factor(
                        factor_name=factor_name,
                        factor_source="manual",
                        use_llm=True,
                    )
                except Exception as e:
                    logger.warning(f"二次分类失败: {e}")

        return {
            "success": True,
            "factor_name": factor_name,
            "validation": val_result,
            "save": save_result,
            "metrics": metrics_result,
        }

    # ── 私有方法 ──

    def _get_factors_code(
        self,
        factor_names: Optional[List[str]] = None,
        all_available: bool = False,
    ) -> Dict[str, str]:
        """从 DB 读取因子 code_text。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                if all_available:
                    cur.execute(
                        "SELECT factor_name, code_text FROM aistock_factor_catalog "
                        "WHERE code_text IS NOT NULL AND COALESCE(is_available, true) = true"
                    )
                elif factor_names:
                    ph = ",".join(["%s"] * len(factor_names))
                    cur.execute(
                        f"SELECT factor_name, code_text FROM aistock_factor_catalog "
                        f"WHERE factor_name IN ({ph}) AND code_text IS NOT NULL",
                        factor_names,
                    )
                else:
                    return {}
                return {row[0]: row[1] for row in cur.fetchall()}

    def _save_metrics_to_db(
        self,
        factors_metrics: Dict[str, Dict[str, Any]],
        calc_batch_id: str,
        data_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Legacy no-op：原始代码链不再写入 aistock_factor_metrics。"""
        skipped = sum(len(windows or {}) for windows in factors_metrics.values())
        if skipped:
            logger.info(
                "legacy manual factor metrics writer 跳过 %s 条写入；官方独立指标现仅允许 official evaluation writer 落表 calc_batch_id=%s",
                skipped,
                calc_batch_id,
            )
        return {"inserted": 0, "skipped": skipped, "errors": []}


_UPSERT_METRICS_SQL = """
INSERT INTO aistock_factor_metrics (
    factor_name, calculated_at, data_start, data_end, eval_window,
    return_horizon, universe,
    ic_mean, ic_std, rank_ic_mean, rank_ic_std, icir, rank_icir, ic_positive_ratio,
    top_annual_return, top_excess_annual_return, top_sharpe,
    top_max_drawdown, top_excess_sharpe, benchmark_annual_return,
    group_return_monotonicity, turnover, ic_decay_half_life,
    ic_csz_mean, rank_ic_1d, rank_ic_5d, rank_ic_10d, rank_ic_20d,
    coverage, n_trading_days, source_task_id, calc_batch_id, calc_engine,
    factor_catalog_id, snapshot_date
) VALUES (
    %(factor_name)s, %(calculated_at)s, %(data_start)s, %(data_end)s, %(eval_window)s,
    %(return_horizon)s, %(universe)s,
    %(ic_mean)s, %(ic_std)s, %(rank_ic_mean)s, %(rank_ic_std)s, %(icir)s, %(rank_icir)s, %(ic_positive_ratio)s,
    %(top_annual_return)s, %(top_excess_annual_return)s, %(top_sharpe)s,
    %(top_max_drawdown)s, %(top_excess_sharpe)s, %(benchmark_annual_return)s,
    %(group_return_monotonicity)s, %(turnover)s, %(ic_decay_half_life)s,
    %(ic_csz_mean)s, %(rank_ic_1d)s, %(rank_ic_5d)s, %(rank_ic_10d)s, %(rank_ic_20d)s,
    %(coverage)s, %(n_trading_days)s, %(source_task_id)s, %(calc_batch_id)s, %(calc_engine)s,
    %(factor_catalog_id)s, %(snapshot_date)s
)
ON CONFLICT (factor_name, eval_window, data_start, data_end, snapshot_date)
DO UPDATE SET
    calculated_at = EXCLUDED.calculated_at,
    ic_mean = EXCLUDED.ic_mean,
    ic_std = EXCLUDED.ic_std,
    rank_ic_mean = EXCLUDED.rank_ic_mean,
    rank_ic_std = EXCLUDED.rank_ic_std,
    icir = EXCLUDED.icir,
    rank_icir = EXCLUDED.rank_icir,
    ic_positive_ratio = EXCLUDED.ic_positive_ratio,
    top_annual_return = EXCLUDED.top_annual_return,
    top_excess_annual_return = EXCLUDED.top_excess_annual_return,
    top_sharpe = EXCLUDED.top_sharpe,
    top_max_drawdown = EXCLUDED.top_max_drawdown,
    top_excess_sharpe = EXCLUDED.top_excess_sharpe,
    benchmark_annual_return = EXCLUDED.benchmark_annual_return,
    group_return_monotonicity = EXCLUDED.group_return_monotonicity,
    turnover = EXCLUDED.turnover,
    ic_decay_half_life = EXCLUDED.ic_decay_half_life,
    ic_csz_mean = EXCLUDED.ic_csz_mean,
    rank_ic_1d = EXCLUDED.rank_ic_1d,
    rank_ic_5d = EXCLUDED.rank_ic_5d,
    rank_ic_10d = EXCLUDED.rank_ic_10d,
    rank_ic_20d = EXCLUDED.rank_ic_20d,
    coverage = EXCLUDED.coverage,
    n_trading_days = EXCLUDED.n_trading_days,
    source_task_id = EXCLUDED.source_task_id,
    calc_batch_id = EXCLUDED.calc_batch_id,
    factor_catalog_id = EXCLUDED.factor_catalog_id
"""


class _SingleFactorSaver:
    """单因子指标入库 helper（从 ManualFactorService._save_metrics_to_db 提取）。"""

    @staticmethod
    def save(
        fname: str,
        metrics_by_window: Dict[str, Dict[str, Any]],
        calc_batch_id: str,
        data_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        skipped = len(metrics_by_window or {})
        if skipped:
            logger.info(
                "legacy single-factor stream writer 跳过 %s 条写入；官方独立指标现仅允许 official evaluation writer 落表 factor=%s calc_batch_id=%s",
                skipped,
                fname,
                calc_batch_id,
            )
        return {"inserted": 0, "errors": []}


async def batch_compute_metrics_stream(
    factor_names: Optional[List[str]] = None,
    all_available: bool = False,
    data_date: Optional[str] = None,
):
    """流式批量计算因子指标（async generator）。

    每完成一个因子立即 yield 进度事件 + 入库。
    独立于 ManualFactorService 类，可直接在 router 中调用。
    """
    import time as _time

    svc = ManualFactorService()
    t0 = _time.time()

    factors_code = svc._get_factors_code(factor_names, all_available)
    if not factors_code:
        yield {"type": "error", "error": "未找到可计算的因子（需有 code_text）"}
        return

    factor_list = list(factors_code.keys())
    yield {
        "type": "stream_start",
        "factor_count": len(factor_list),
        "factor_names": factor_list,
    }

    # 写入因子代码到 WSL workspace
    wsl_workspace = FACTOR_WORKSPACE_WSL
    factor_dirs_to_clean = []

    try:
        data_files_str = " ".join(DATA_FILES)
        setup_cmds = []
        for fname, code in factors_code.items():
            wsl_fdir = f"{wsl_workspace}/_factor_{fname}"
            factor_dirs_to_clean.append(wsl_fdir)
            setup_cmds.append(f"rm -rf {wsl_fdir} && mkdir -p {wsl_fdir}")
            setup_cmds.append(
                f"cat > {wsl_fdir}/factor.py << 'FACTOREOF'\n{code}\nFACTOREOF"
            )
            setup_cmds.append(
                f'for f in {data_files_str}; do '
                f'[ -e "{wsl_workspace}/$f" ] && [ ! -e "{wsl_fdir}/$f" ] && '
                f'ln -sf "{wsl_workspace}/$f" "{wsl_fdir}/$f"; done; true'
            )

        setup_script = "\n".join(setup_cmds)
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".sh", delete=False, encoding="utf-8", newline="\n"
        ) as tmp:
            tmp.write(setup_script)
            tmp_path = tmp.name
        try:
            wsl_tmp = _win_to_wsl(tmp_path)
            proc = await asyncio.create_subprocess_exec(
                "wsl", "bash", wsl_tmp,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout_setup, stderr_setup = await asyncio.wait_for(
                proc.communicate(), timeout=30
            )
        finally:
            os.unlink(tmp_path)
        if proc.returncode != 0:
            err = stderr_setup.decode("utf-8", errors="replace")[-500:]
            yield {"type": "error", "error": f"WSL 写入因子代码失败: {err}"}
            return

        # 启动流式计算脚本（--stream 模式）
        names_arg = " ".join(shlex.quote(n) for n in factors_code.keys())
        wsl_cmd = (
            f"source ~/miniconda3/etc/profile.d/conda.sh && "
            f"conda activate {CONDA_ENV} && "
            f"export PYTHONPATH={RDAGENT_ROOT_WSL}:$PYTHONPATH && "
            f"python {COMPUTE_SCRIPT_WSL} {wsl_workspace} {names_arg} --stream"
        )
        if data_date:
            wsl_cmd += f" --data-date {data_date}"

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".sh", delete=False, encoding="utf-8", newline="\n"
        ) as tmp:
            tmp.write(wsl_cmd)
            tmp_path2 = tmp.name
        try:
            wsl_tmp2 = _win_to_wsl(tmp_path2)
            proc = await asyncio.create_subprocess_exec(
                "wsl", "bash", wsl_tmp2,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            completed = {}
            failed = {}
            calc_batch_id = None
            _readline_timeout_count = 0

            # 安全超时：防进程 hang 死
            deadline = _time.time() + max(900, len(factors_code) * 660)

            while True:
                if _time.time() > deadline:
                    proc.kill()
                    yield {"type": "error", "error": f"整体安全超时，已完成 {len(completed)} 个因子"}
                    break

                try:
                    raw_line = await asyncio.wait_for(
                        proc.stdout.readline(), timeout=660,
                    )
                    _readline_timeout_count = 0
                except asyncio.TimeoutError:
                    _readline_timeout_count += 1
                    logger.warning(f"batch_compute_metrics_stream: readline 超时第 {_readline_timeout_count} 次")
                    if _readline_timeout_count >= 3:
                        proc.kill()
                        yield {"type": "error", "error": f"WSL 进程无响应 (连续 {_readline_timeout_count} 次 readline 超时)，已完成 {len(completed)} 个因子"}
                        break
                    continue

                if not raw_line:
                    break  # EOF

                line = raw_line.decode("utf-8", errors="replace").strip()
                if not line:
                    continue
                if not line.startswith("{"):
                    logger.warning(f"batch_compute_metrics_stream: WSL stdout 非 JSON 行: {line[:200]}")
                    continue

                try:
                    msg = json.loads(line)
                except json.JSONDecodeError as e:
                    logger.error(f"batch_compute_metrics_stream: JSON 解析失败: {e}, line={line[:200]}")
                    yield {"type": "error", "error": f"WSL 输出 JSON 解析失败: {e}"}
                    continue

                msg_type = msg.get("type")

                if msg_type == "init":
                    if msg.get("status") != "ok":
                        yield {"type": "error", "error": f"WSL shared context 初始化失败: {msg.get('error')}"}
                        break
                    calc_batch_id = msg.get("calc_batch_id")
                    if not calc_batch_id:
                        yield {"type": "error", "error": "WSL init 消息缺少 calc_batch_id"}
                        break
                    yield msg

                elif msg_type == "factor_done":
                    fname = msg.get("factor_name")
                    if not fname:
                        logger.error(f"batch_compute_metrics_stream: factor_done 消息缺少 factor_name: {msg}")
                        continue
                    status = msg.get("status", "error")

                    if status == "ok" and msg.get("metrics"):
                        if not calc_batch_id:
                            logger.error("batch_compute_metrics_stream: calc_batch_id 未初始化，无法入库")
                            failed[fname] = {**msg, "error": "calc_batch_id 未初始化"}
                        else:
                            db_result = _SingleFactorSaver.save(
                                fname, msg["metrics"],
                                calc_batch_id,
                                data_date=data_date,
                            )
                            completed[fname] = msg
                            msg["db_inserted"] = db_result.get("inserted", 0)
                            if db_result.get("errors"):
                                logger.warning(f"batch_compute_metrics_stream: {fname} DB 部分写入失败: {db_result['errors']}")
                    else:
                        failed[fname] = msg

                    yield {
                        "type": "factor_progress",
                        "factor_name": fname,
                        "status": status,
                        "error": msg.get("error"),
                        "duration": msg.get("duration"),
                        "completed": len(completed),
                        "failed": len(failed),
                        "total": len(factors_code),
                    }

                elif msg_type == "summary":
                    pass  # 逐条 factor_done 已推送

            await proc.wait()

        finally:
            os.unlink(tmp_path2)

        yield {
            "type": "stream_complete",
            "success": len(completed) > 0,
            "completed_count": len(completed),
            "failed_count": len(failed),
            "completed_factors": list(completed.keys()),
            "failed_factors": {k: v.get("error") for k, v in failed.items()},
            "total_duration_sec": round(_time.time() - t0, 2),
        }

    finally:
        if factor_dirs_to_clean:
            cleanup_cmd = " && ".join(f"rm -rf {d}" for d in factor_dirs_to_clean)
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".sh", delete=False, encoding="utf-8", newline="\n"
            ) as tmp:
                tmp.write(cleanup_cmd)
                tmp_path_c = tmp.name
            try:
                wsl_tmp_c = _win_to_wsl(tmp_path_c)
                await asyncio.create_subprocess_exec(
                    "wsl", "bash", wsl_tmp_c,
                    stdout=asyncio.subprocess.DEVNULL,
                    stderr=asyncio.subprocess.DEVNULL,
                )
            finally:
                os.unlink(tmp_path_c)
