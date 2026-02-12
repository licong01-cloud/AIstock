import logging
import threading
import time
import json
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Iterable, List, Literal, Optional, Sequence, Tuple
from urllib.parse import urlparse

from ..db.pg_pool import get_conn
from .rdagent_results_api_client import RDAgentResultsApiClient
from .rdagent_catalog_etl_service import (
    ImportSummary,
    import_alpha158_meta_from_payload,
    import_alpha360_meta_from_payload,
    import_factor_catalog_from_payload,
    import_loop_catalog_from_payload,
    import_model_catalog_from_payload,
    import_strategy_catalog_from_payload,
)


logger = logging.getLogger("aistock.rdagent_sync")


@dataclass
class SyncStatus:
    state: Literal["idle", "running", "success", "failed"] = "idle"
    phase: str = "idle"  # "materializing", "syncing"
    progress: float = 0.0  # 0.0 to 1.0
    started_at_ts: Optional[float] = None
    finished_at_ts: Optional[float] = None
    last_error: Optional[str] = None
    last_result: Optional[Dict[str, Any]] = None


_STATUS = SyncStatus()
_LOCK = threading.RLock()

def get_last_sync_time() -> str:
    """从数据库获取上次同步时间戳"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM app.sync_meta WHERE key = 'rdagent_last_sync_time'")
            row = cur.fetchone()
            return row[0] if row else '2000-01-01T00:00:00Z'


def set_last_sync_time(ts: str) -> None:
    """更新上次同步时间戳"""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE app.sync_meta SET value = %s, updated_at = NOW() WHERE key = 'rdagent_last_sync_time'",
                (ts,)
            )


SyncMode = Literal["upsert", "full_refresh", "reconcile", "materialize_and_sync", "incremental"]


def get_sync_status() -> Dict[str, Any]:
    with _LOCK:
        return {
            "state": _STATUS.state,
            "phase": _STATUS.phase,
            "progress": _STATUS.progress,
            "started_at_ts": _STATUS.started_at_ts,
            "finished_at_ts": _STATUS.finished_at_ts,
            "last_error": _STATUS.last_error,
            "last_result": _STATUS.last_result,
        }


def _set_running(phase: str = "running") -> None:
    _STATUS.state = "running"
    _STATUS.phase = phase
    _STATUS.progress = 0.0
    _STATUS.started_at_ts = time.time()
    _STATUS.finished_at_ts = None
    _STATUS.last_error = None
    _STATUS.last_result = None


def _update_progress(progress: float, phase: Optional[str] = None) -> None:
    with _LOCK:
        _STATUS.progress = progress
        if phase:
            _STATUS.phase = phase


def _set_done_ok(result: Dict[str, Any]) -> None:
    _STATUS.state = "success"
    _STATUS.finished_at_ts = time.time()
    _STATUS.last_result = result


def _set_done_failed(err: str) -> None:
    _STATUS.state = "failed"
    _STATUS.finished_at_ts = time.time()
    _STATUS.last_error = err


def _truncate_all_catalogs() -> None:
    """Truncate all RD-Agent catalog tables in a single transaction.

    Used by full_refresh mode before re-importing everything from RD-Agent.
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            # TRUNCATE preserves table structure and sequences
            cur.execute(
                "TRUNCATE TABLE aistock_factor_catalog, aistock_strategy_catalog, aistock_loop_catalog, aistock_model_catalog, aistock_alpha158_meta RESTART IDENTITY;",
            )


def _reconcile_simple_table(
    *,
    table: str,
    pk_column: str,
    keep_ids: Iterable[str],
) -> None:
    """Delete rows whose primary key is not in current payload ids."""

    ids: List[str] = list({str(x) for x in keep_ids if x is not None})
    if not ids:
        return

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {table} WHERE {pk_column} NOT IN (%s)" % ",".join(["%s"] * len(ids)),
                ids,
            )


def _reconcile_composite_table(
    *,
    table: str,
    pk_columns: Sequence[str],
    keep_keys: Iterable[Tuple[Any, ...]],
) -> None:
    """Delete rows whose composite key is not in current payload keys.

    Implemented via NOT EXISTS on a VALUES list to keep it reasonably efficient.
    """

    keys: List[Tuple[Any, ...]] = list({tuple(k) for k in keep_keys if all(k)})
    if not keys:
        return

    placeholders = ",".join(["(" + ",".join(["%s"] * len(pk_columns)) + ")"] * len(keys))
    params: List[Any] = []
    for k in keys:
        params.extend(k)

    cols = ",".join(pk_columns)
    where_clause = " AND ".join([f"t.{col} = v.{col}" for col in pk_columns])

    sql = f"""
        DELETE FROM {table} AS t
        WHERE NOT EXISTS (
            SELECT 1
            FROM (VALUES {placeholders}) AS v({cols})
            WHERE {where_clause}
        )
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)


def _parse_manifest_summary(*, manifest_path: "Path") -> Dict[str, Any]:
    """解析 manifest.json 的摘要字段（文档 8.3 定义的 DB 索引字段集合）"""

    data = json.loads(manifest_path.read_text(encoding="utf-8"))
    primary_assets = data.get("primary_assets") or {}
    return {
        "manifest_schema_version": data.get("schema_version"),
        "manifest_primary_workspace_id": data.get("primary_workspace_id"),
        "manifest_factor_entry_relpath": primary_assets.get("factor_entry_relpath"),
        "manifest_model_weight_relpath": primary_assets.get("model_weight_relpath"),
        "manifest_config_relpath": primary_assets.get("config_relpath"),
        "source_workspace_path": data.get("source_workspace_path"),
        "log_dir": data.get("log_dir"),
        "log_uri": data.get("log_uri"),
    }


def _short_id(val: Any, keep: int = 10) -> str:
    s = "" if val is None else str(val)
    s = s.strip()
    if not s:
        return ""
    return s if len(s) <= keep else s[:keep]


def _basename_from_log_dir_or_uri(log_dir: Any, log_uri: Any) -> str:
    def _pick(v: Any) -> str:
        if v is None:
            return ""
        s = str(v).strip()
        return s

    raw = _pick(log_dir) or _pick(log_uri)
    if not raw:
        return ""

    # uri (s3/http/file) -> path basename
    try:
        parsed = urlparse(raw)
        if parsed.scheme and parsed.path:
            p = parsed.path.rstrip("/")
            base = p.split("/")[-1]
            return base
    except Exception:
        pass

    # local path
    try:
        p = raw.rstrip("/\\")
        base = p.split("/")[-1].split("\\")[-1]
        return base
    except Exception:
        return ""


def backfill_loop_manifest_summaries(*, loops: List[Dict[str, Any]]) -> Dict[str, Any]:
    """根据本地缓存的 bundle manifest.json 回填 aistock_loop_catalog 的 manifest 摘要字段。

    - loops: 来自 RD-Agent catalog payload 的 loop 列表（需包含 task_run_id/loop_id/asset_bundle_id）
    """

    from .rdagent_asset_service import rdagent_asset_service

    updated = 0
    skipped = 0
    failed = 0

    for lp in loops:
        task_run_id = lp.get("task_run_id")
        loop_id = lp.get("loop_id")
        bundle_id = lp.get("asset_bundle_id")
        if not task_run_id or loop_id is None or not bundle_id:
            skipped += 1
            continue

        bundle_path = rdagent_asset_service.get_bundle_path(str(bundle_id))
        manifest_path = Path(bundle_path) / "manifest.json"
        if not manifest_path.exists():
            skipped += 1
            continue

        try:
            summary = _parse_manifest_summary(manifest_path=manifest_path)

            base = _basename_from_log_dir_or_uri(summary.get("log_dir"), summary.get("log_uri"))
            loop_display_name = f"{base}-loop{int(loop_id)}" if base else f"{_short_id(task_run_id)}-loop{int(loop_id)}"

            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE aistock_loop_catalog
                        SET manifest_schema_version = %s,
                            manifest_primary_workspace_id = %s,
                            manifest_factor_entry_relpath = %s,
                            manifest_model_weight_relpath = %s,
                            manifest_config_relpath = %s,
                            source_workspace_path = %s,
                            log_dir = %s,
                            log_uri = %s,
                            display_name = %s,
                            updated_at = NOW()
                        WHERE task_run_id = %s AND loop_id = %s
                        """,
                        (
                            summary.get("manifest_schema_version"),
                            summary.get("manifest_primary_workspace_id"),
                            summary.get("manifest_factor_entry_relpath"),
                            summary.get("manifest_model_weight_relpath"),
                            summary.get("manifest_config_relpath"),
                            summary.get("source_workspace_path"),
                            summary.get("log_dir"),
                            summary.get("log_uri"),
                            loop_display_name,
                            str(task_run_id),
                            int(loop_id),
                        ),
                    )

                    # 同步更新该 loop 下所有 model 的 display_name（模型表可能早于 manifest 回填入库）
                    cur.execute(
                        """
                        UPDATE aistock_model_catalog
                        SET display_name = %s || '-model-' || SUBSTRING(workspace_id FROM 1 FOR 10)
                        WHERE task_run_id = %s AND loop_id = %s AND workspace_id IS NOT NULL
                        """,
                        (
                            loop_display_name,
                            str(task_run_id),
                            int(loop_id),
                        ),
                    )
                    if cur.rowcount and cur.rowcount > 0:
                        updated += 1
                    else:
                        skipped += 1
        except Exception as e:
            failed += 1
            logger.warning(
                f"回填 manifest 摘要失败: task_run_id={task_run_id} loop_id={loop_id} bundle_id={bundle_id}: {e}"
            )

    return {"updated": updated, "skipped": skipped, "failed": failed}


def run_full_sync(*, mode: SyncMode = "upsert", client: Optional[RDAgentResultsApiClient] = None, sync_metadata_only: bool = False, sync_assets_only: bool = False) -> Dict[str, Any]:
    with _LOCK:
        if _STATUS.state == "running" and _STATUS.phase not in ("materializing", "idle"):
            return get_sync_status()
        
        if mode == "materialize_and_sync":
            _set_running(phase="materializing")
            # 启动物化+同步线程
            import threading
            threading.Thread(target=_materialize_and_sync_task, name="RDAgentMaterializeSync").start()
            return get_sync_status()

        if mode == "incremental":
            _set_running(phase="incremental_syncing")
            import threading
            threading.Thread(target=_incremental_sync_task, name="RDAgentIncrementalSync").start()
            return get_sync_status()

        if _STATUS.phase != "materializing":
            _set_running(phase="syncing")
        else:
            _update_progress(0.5, phase="syncing")

    cli = client or RDAgentResultsApiClient()

    try:
        # 2026-01-05 Fix: 增强探测逻辑，解决分页或过滤导致的 514+ 因子缺失问题
        def _get_all_factors():
            all_factors = []
            _update_progress(0.55, phase="fetching_factors")
            # 1. 尝试从主 Catalog 获取
            factor_payload = cli.get_catalog_factors(params={"limit": 5000})
            main_f = factor_payload.get("factors") or []
            all_factors.extend(main_f)
            
            # 2. 显式探测各来源，确保不漏掉 Alpha158/360 基础因子
            _update_progress(0.6, phase="fetching_factors_src")
            for src in ["qlib_alpha158", "qlib_alpha360", "rdagent_generated"]:
                src_res = cli.get_catalog_factors(params={"source": src, "limit": 2000})
                src_f = src_res.get("factors") or []
                all_factors.extend(src_f)
            
            # 3. 显式探测 alpha158 和 alpha360 标签
            _update_progress(0.65, phase="fetching_factors_tag")
            for tag in ["alpha158", "alpha360", "qlib"]:
                tag_res = cli.get_catalog_factors(params={"tag": tag, "limit": 2000})
                tag_f = tag_res.get("factors") or []
                all_factors.extend(tag_f)
                
            return {
                "factors": all_factors, 
                "version": factor_payload.get("version"), 
                "generated_at_utc": factor_payload.get("generated_at_utc")
            }

        factor_payload = _get_all_factors()
        _update_progress(0.7, phase="fetching_catalogs")
        strategy_payload = cli.get_catalog_strategies()
        loop_payload = cli.get_catalog_loops()
        model_payload = cli.get_catalog_models()

        # 3.5 优先使用增量 API 获取完整 loops 数据（包含 asset_bundle_id）
        # 用于获取 asset_bundle_id / workspace_path 以及嵌套 factors/models（包含自研因子）
        incremental_full = {"loops": []}
        try:
            _update_progress(0.72, phase="fetching_incremental_full")
            incremental_full = cli.get_catalog_incremental(last_sync_time=None, limit=5000)
            
            # 如果增量 API 返回了数据，直接使用增量数据作为 loop_payload
            inc_loops = incremental_full.get("loops") or []
            if inc_loops:
                loop_payload = {
                    "version": incremental_full.get("version"),
                    "generated_at_utc": incremental_full.get("generated_at_utc"),
                    "loops": inc_loops
                }
                logger.info(f"从增量 API 获取到 {len(inc_loops)} 条 loop 记录（包含 asset_bundle_id）")
            else:
                # 增量 API 返回空，回退到基础 catalog 接口
                logger.warning("增量 API 未返回任何 loop 数据，回退到基础 catalog 接口")
                loop_payload = cli.get_catalog_loops()
        except Exception as exc:
            logger.warning(f"拉取 RD-Agent 增量数据失败，回退到基础 catalog 接口: {exc}")
            loop_payload = cli.get_catalog_loops()
        
        # 4. 获取 Alpha 因子专属 Meta (Phase 3 补充)
        alpha158_payload = cli.get_alpha158_meta()
        
        # 尝试获取 Alpha360 Meta (如果接口存在)
        try:
            alpha360_payload = cli._get_json("/alpha360/meta")
        except Exception:
            alpha360_payload = {"factors": []}

        # full_refresh: truncate before re-import
        if mode == "full_refresh":
            _update_progress(0.75, phase="truncating")
            _truncate_all_catalogs()

        # upsert imports (shared by all modes)
        _update_progress(0.8, phase="importing")

        # 如果使用增量 API，需要导入嵌套的因子/模型（自研因子通常在这里出现）
        inc_loops = incremental_full.get("loops") or []
        if inc_loops and incremental_full.get("loops") == loop_payload.get("loops"):
            # 确认使用的是增量数据，导入嵌套因子/模型
            nested_factors = []
            nested_models = []
            for lp in inc_loops:
                bid = lp.get("asset_bundle_id")
                for f in (lp.get("factors") or []):
                    if bid and not f.get("asset_bundle_id"):
                        f["asset_bundle_id"] = bid
                    if not f.get("source"):
                        f["source"] = "rdagent_generated"
                    nested_factors.append(f)
                for m in (lp.get("models") or []):
                    if bid and not m.get("asset_bundle_id"):
                        m["asset_bundle_id"] = bid
                    nested_models.append(m)

            if nested_factors:
                extra_factor_payload = {
                    "factors": nested_factors,
                    "version": incremental_full.get("version"),
                    "generated_at_utc": incremental_full.get("generated_at_utc"),
                    "source": "rdagent_incremental_full",
                }
                import_factor_catalog_from_payload(extra_factor_payload)

            if nested_models:
                extra_model_payload = {
                    "models": nested_models,
                    "version": incremental_full.get("version"),
                    "generated_at_utc": incremental_full.get("generated_at_utc"),
                    "source": "rdagent_incremental_full",
                }
                import_model_catalog_from_payload(extra_model_payload)

        results: Dict[str, ImportSummary] = {
            "factor": import_factor_catalog_from_payload(factor_payload),
            "strategy": import_strategy_catalog_from_payload(strategy_payload),
            "loop": import_loop_catalog_from_payload(loop_payload),
            "model": import_model_catalog_from_payload(model_payload),
            "alpha158": import_alpha158_meta_from_payload(alpha158_payload),
            "alpha360": import_alpha360_meta_from_payload(alpha360_payload),
        }

        # 5. 自动下载资产包 (Phase 3 Section 15.3)
        # 全量/UPSERT同步后，检查所有 Loop 记录，若有 asset_bundle_id 则尝试下载
        # 如果 sync_metadata_only=True，则跳过资产包下载
        # 如果 sync_assets_only=True，则只下载资产包，不更新数据库
        from .rdagent_asset_service import rdagent_asset_service
        loops_data = loop_payload.get("loops") or []
        if loops_data and not sync_metadata_only:
            _update_progress(0.85, phase="downloading_assets")
            bundle_ids = [lp.get("asset_bundle_id") for lp in loops_data if lp.get("asset_bundle_id")]
            unique_bundle_ids = sorted({str(bid) for bid in bundle_ids if str(bid).strip()})
            if not unique_bundle_ids:
                logger.warning("本次同步的 loop_payload 未包含任何 asset_bundle_id，跳过资产包下载（production_bundles 可能为空属于预期）。")
            for lp in loops_data:
                bundle_id = lp.get("asset_bundle_id")
                if bundle_id:
                    try:
                        ok = rdagent_asset_service.download_and_extract_bundle(bundle_id)
                        if not ok:
                            logger.warning(f"资产包 {bundle_id} 下载/解压返回失败（未抛异常），请检查 RD-Agent Results API 与资产包是否存在。")
                    except Exception as be:
                        logger.warning(f"下载资产包 {bundle_id} 失败: {be}")

            # 5.1 下载完成后回填 manifest 摘要字段（8.5.2）
            _update_progress(0.88, phase="backfilling_manifest")
            try:
                mf_res = backfill_loop_manifest_summaries(loops=loops_data)
                logger.info(f"manifest 回填完成: {mf_res}")
            except Exception as e:
                logger.warning(f"manifest 回填失败: {e}")

        # reconcile: delete local records not present in this snapshot
        if mode == "reconcile":
            _update_progress(0.9, phase="reconciling")
            # factors: factor_name
            factor_ids = [f.get("name") for f in factor_payload.get("factors") or []]
            _reconcile_simple_table(table="aistock_factor_catalog", pk_column="factor_name", keep_ids=factor_ids)

            # alpha158: factor_name
            alpha158_ids = [f.get("name") for f in (alpha158_payload.get("meta") or alpha158_payload.get("factors") or [])]
            _reconcile_simple_table(table="aistock_alpha158_meta", pk_column="factor_name", keep_ids=alpha158_ids)

            # alpha360: factor_name
            alpha360_ids = [f.get("name") for f in (alpha360_payload.get("meta") or alpha360_payload.get("factors") or [])]
            _reconcile_simple_table(table="aistock_alpha360_meta", pk_column="factor_name", keep_ids=alpha360_ids)

            # strategies: strategy_id
            strat_ids = [s.get("strategy_id") for s in strategy_payload.get("strategies") or []]
            _reconcile_simple_table(
                table="aistock_strategy_catalog",
                pk_column="strategy_id",
                keep_ids=strat_ids,
            )

            # loops: (task_run_id, loop_id)
            loop_keys: List[Tuple[Any, ...]] = []
            for lp in loop_payload.get("loops") or []:
                tr = lp.get("task_run_id")
                lid = lp.get("loop_id")
                if tr is not None and lid is not None:
                    loop_keys.append((tr, int(lid)))
            _reconcile_composite_table(
                table="aistock_loop_catalog",
                pk_columns=["task_run_id", "loop_id"],
                keep_keys=loop_keys,
            )

            # models: (task_run_id, loop_id, workspace_id)
            model_keys: List[Tuple[Any, ...]] = []
            for m in model_payload.get("models") or []:
                tr = m.get("task_run_id")
                lid = m.get("loop_id")
                wid = m.get("workspace_id")
                if tr is not None and lid is not None and wid is not None:
                    model_keys.append((tr, int(lid), wid))
            _reconcile_composite_table(
                table="aistock_model_catalog",
                pk_columns=["task_run_id", "loop_id", "workspace_id"],
                keep_keys=model_keys,
            )

        out: Dict[str, Any] = {k: vars(v) for k, v in results.items()}
        with _LOCK:
            _STATUS.progress = 1.0
            _set_done_ok(out)
        return get_sync_status()
    except Exception as exc:  # noqa: BLE001
        with _LOCK:
            _set_done_failed(str(exc))
        return get_sync_status()


def _incremental_sync_task():
    """增量同步后台任务 (Phase 3 Section 15.3)"""
    client = RDAgentResultsApiClient()
    from .rdagent_asset_service import rdagent_asset_service
    from .rdagent_archive_service import archive_service
    
    try:
        last_ts = get_last_sync_time()
        _update_progress(0.1, phase="fetching_incremental")
        
        payload = client.get_catalog_incremental(last_sync_time=last_ts)
        loops = payload.get("loops") or []
        total = len(loops)
        
        if total == 0:
            _set_done_ok({"incremental": "no new data"})
            return

        # 1. 导入增量元数据
        _update_progress(0.3, phase="importing_meta")
        import_loop_catalog_from_payload(payload)
        
        # 2. 处理资产包与历史归档 (Section 6 & 15)
        for idx, lp in enumerate(loops):
            tr = lp.get("task_run_id")
            lid = lp.get("loop_id")
            bundle_id = lp.get("asset_bundle_id")
            
            _update_progress(0.3 + (idx / total) * 0.6, phase=f"processing_loop_{idx+1}/{total}")
            
            # 2.1 下载资产包
            if bundle_id:
                try:
                    ok = rdagent_asset_service.download_and_extract_bundle(bundle_id)
                    if not ok:
                        logger.warning(f"资产包 {bundle_id} 下载/解压返回失败（未抛异常），请检查 RD-Agent Results API 与资产包是否存在。")
                except Exception as be:
                    logger.warning(f"下载资产包 {bundle_id} 失败: {be}")

                # 2.1.1 下载完成后回填该 loop 的 manifest 摘要字段
                try:
                    backfill_loop_manifest_summaries(loops=[lp])
                except Exception as e:
                    logger.warning(f"增量回填 manifest 失败: {e}")
            else:
                if idx == 0:
                    logger.warning("本次增量同步的 loops 未提供 asset_bundle_id，跳过资产包下载（production_bundles 可能为空属于预期）。")
            
            # 2.2 导入嵌套的因子元数据 (Phase 3 增强)
            nested_factors = lp.get("factors")
            if nested_factors:
                # 构造一个符合 import_factor_catalog_from_payload 要求的 payload
                # 确保因子也带上 asset_bundle_id 并设置默认 source
                for f in nested_factors:
                    if bundle_id and not f.get("asset_bundle_id"):
                        f["asset_bundle_id"] = bundle_id
                    if not f.get("source"):
                        f["source"] = "rdagent_generated"

                factor_payload = {
                    "factors": nested_factors,
                    "version": payload.get("version"),
                    "generated_at_utc": payload.get("generated_at_utc"),
                    "source": "rdagent_incremental"
                }
                import_factor_catalog_from_payload(factor_payload)
            
            # 2.3 导入嵌套的模型元数据 (Phase 3 增强)
            nested_models = lp.get("models")
            if nested_models:
                # 构造符合 import_model_catalog_from_payload 要求的 payload
                for m in nested_models:
                    if bundle_id and not m.get("asset_bundle_id"):
                        m["asset_bundle_id"] = bundle_id
                
                model_payload = {
                    "models": nested_models,
                    "version": payload.get("version"),
                    "generated_at_utc": payload.get("generated_at_utc"),
                    "source": "rdagent_incremental"
                }
                import_model_catalog_from_payload(model_payload)

            # 2.4 自动触发历史结果归档 (Level 0)
            try:
                # 从 payload 中提取归档所需字段
                metrics = lp.get("metrics") or {}
                # 尝试从 raw_payload 恢复曲线数据 (RD-Agent 侧通常放在 paths 指定的文件中，
                # 但 Catalog 可能已经带了结构化摘要)
                # 若 lp 中已有结构化指标，直接入库记录
                archive_service.archive_strategy_run(
                    strategy_id=lp.get("strategy_id") or "unknown",
                    source="rdagent",
                    version_tag="historical",
                    task_run_id=tr,
                    loop_id=lid,
                    metrics=metrics,
                    equity_curve=lp.get("equity_curve"), # 假设 Catalog 结构已包含
                    start_date=lp.get("start_date"),
                    end_date=lp.get("end_date")
                )
            except Exception as ae:
                logger.warning(f"归档 Loop {tr}/{lid} 失败: {ae}")

            # 2.5 回传同步状态至 RD-Agent (Phase 3 Section 15.3, 16.2)
            if bundle_id:
                client.post_catalog_sync_status(bundle_id, status="synced")
        
        # 3. 更新同步时间戳
        new_ts = payload.get("generated_at_utc") or datetime.now().isoformat()
        set_last_sync_time(new_ts)
        
        _set_done_ok({"incremental_count": total})
    except Exception as exc:
        with _LOCK:
            _set_done_failed(f"Incremental sync failed: {str(exc)}")


def _materialize_and_sync_task():
    """物化并同步的后台任务逻辑"""
    client = RDAgentResultsApiClient()
    try:
        # 1. 触发物化 (长耗时)
        _update_progress(0.1, phase="materializing")
        client.post_ops_materialize_and_refresh()
        _update_progress(0.5, phase="materialize_done")

        # 2. 物化成功后，执行一次增量同步
        run_full_sync(mode="incremental", client=client)
    except Exception as exc:
        with _LOCK:
            _set_done_failed(f"Materialize/Sync failed: {str(exc)}")


def trigger_rdagent_materialize_and_sync() -> Dict[str, Any]:
    """触发 RD-Agent 侧物化并同步最新 Catalog (已整合进 run_full_sync)."""
    return run_full_sync(mode="materialize_and_sync")
