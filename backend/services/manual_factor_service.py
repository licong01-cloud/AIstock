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

# WSL validation is diagnostic/input validation only. Official metrics and
# ratings are produced by FactorOfficialEvaluationService + FactorRatingService.
CONDA_ENV = os.getenv("QLIB_WSL_CONDA_ENV", "rdagent-gpu")
FACTOR_WORKSPACE_WSL = os.getenv("FACTOR_WORKSPACE_WSL", "").strip()

# 数据文件列表
DATA_FILES = [
    "daily_pv.h5", "daily_basic.h5", "moneyflow.h5",
    "bak_basic.h5", "cyq_perf.h5", "sector_data.h5",
    "static_factors.parquet",
]

FACTOR_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]{2,80}$")


def _require_factor_workspace_wsl() -> str:
    if FACTOR_WORKSPACE_WSL:
        return FACTOR_WORKSPACE_WSL
    raise RuntimeError(
        "FACTOR_WORKSPACE_WSL is required for manual factor code validation; "
        "official metrics are computed by the factor library official evaluation flow."
    )

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

        try:
            wsl_workspace = _require_factor_workspace_wsl()
        except RuntimeError as exc:
            return {"success": False, "message": str(exc)}
        wsl_factor_dir = f"{wsl_workspace}/_factor_{factor_name}"

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
                f'[ -e "{wsl_workspace}/$f" ] && [ ! -e "{wsl_factor_dir}/$f" ] && '
                f'ln -sf "{wsl_workspace}/$f" "{wsl_factor_dir}/$f"; done; true\n'
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
        """Run the official independent-metrics writer for manual factors.

        This is the single product path for independent metrics. The legacy
        WSL script is no longer a writer and is not called here.
        """
        if not data_date:
            return {
                "success": False,
                "error": "data_date is required; select a factor-library data snapshot before computing official metrics.",
                "official_metrics": True,
            }

        from .quantevolver.factor_official_evaluation_service import FactorOfficialEvaluationService

        svc = FactorOfficialEvaluationService()
        result = await asyncio.to_thread(
            svc.compute,
            factor_names=factor_names,
            data_date=data_date,
            include_disabled=all_available,
        )
        result["official_metrics"] = True
        return result

    def _run_official_rating(self, factor_name: str, source: str = "manual") -> Dict[str, Any]:
        """Run the active official rating rule for one saved manual factor."""
        from .quantevolver.factor_rating_service import factor_rating_service

        rules = factor_rating_service.list_rule_versions()
        rule_version = rules.get("active_version") or rules.get("default_version")
        if not rule_version:
            raise RuntimeError("No active/default factor rating rule version is available")
        return factor_rating_service.run_rating(
            rule_version=rule_version,
            scope_type="selected",
            scope_payload={"selected_factors": [{"factor_name": factor_name, "source": source}]},
            triggered_from="ui_toolbar",
        )

    async def full_pipeline(
        self,
        factor_name: str,
        code_text: str,
        description: Optional[str] = None,
        expression: Optional[str] = None,
        data_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Full manual-factor flow: validate -> save -> official metrics -> classification -> official rating."""
        if not data_date:
            return {
                "success": False,
                "stage": "official_metrics",
                "error": "data_date is required; select a factor-library data snapshot before running full pipeline.",
            }

        val_result = await self.validate_factor_code(factor_name, code_text)
        if not val_result["success"]:
            return {"success": False, "stage": "validate", "error": val_result["message"]}

        save_result = await self.save_factor(
            factor_name, code_text, description, expression,
        )

        metrics_result = await self.batch_compute_metrics(
            factor_names=[factor_name],
            data_date=data_date,
        )
        if not metrics_result.get("success"):
            return {
                "success": False,
                "stage": "official_metrics",
                "factor_name": factor_name,
                "validation": val_result,
                "save": save_result,
                "metrics": metrics_result,
                "error": metrics_result.get("error") or "official metrics failed",
            }

        try:
            from .quantevolver.factor_analyst import FactorAnalyst
            analyst = FactorAnalyst()
            save_result["classification"] = analyst.analyze_single_factor(
                factor_name=factor_name,
                factor_source="manual",
                use_llm=True,
            )
        except Exception as e:
            logger.warning("Manual factor semantic classification failed after official metrics: %s", e)

        try:
            rating_result = await asyncio.to_thread(
                self._run_official_rating,
                factor_name,
                "manual",
            )
        except Exception as e:
            return {
                "success": False,
                "stage": "official_rating",
                "factor_name": factor_name,
                "validation": val_result,
                "save": save_result,
                "metrics": metrics_result,
                "error": str(e),
            }

        return {
            "success": True,
            "factor_name": factor_name,
            "validation": val_result,
            "save": save_result,
            "metrics": metrics_result,
            "rating": rating_result,
        }

    # Private helpers

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


async def batch_compute_metrics_stream(
    factor_names: Optional[List[str]] = None,
    all_available: bool = False,
    data_date: Optional[str] = None,
):
    """Stream the official independent-metrics flow.

    The previous manual-factor WSL metrics script is no longer invoked and does
    not write factor tables.
    """
    yield {
        "type": "stream_start",
        "official_metrics": True,
        "factor_names": factor_names or [],
        "data_date": data_date,
    }
    svc = ManualFactorService()
    result = await svc.batch_compute_metrics(
        factor_names=factor_names,
        all_available=all_available,
        data_date=data_date,
    )
    if not result.get("success"):
        yield {"type": "error", **result}
        return
    yield {"type": "stream_complete", **result}
