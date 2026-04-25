"""Unified Factor Pipeline — 一键分类+评级+LLM审阅, 支持并行 + 流式 SSE.

流程 (每个因子, 严格顺序):
  Step A: FactorAnalyst.analyze_single_factor(use_llm)
          -> 分类 / direction / signal_mechanism / sector_exposure_corr / description
          -> 内部 upsert 到 qe_factor_classification
          -> Step A 失败则直接标记因子失败，禁止 Step B（评级依赖分类表里的字段）
  Step B: factor_rating_service._grade_factor(factor, rule, enable_llm_audit=...)
          -> 打分 / 硬关 / (可选) LLM 审阅 / 回写 direction+best_horizon 到 classification
          -> 成功后 _upsert_official_rating 写入 qe_factor_official_ratings

并行:
  asyncio.Semaphore(parallelism) 限制同时在跑的 LLM 任务数
  阻塞调用 (litellm / psycopg2) 走 loop.run_in_executor

错误传播原则:
  - 单因子失败不中断其他因子，但必须上报 SSE error 事件并计入 failed
  - run_pipeline_async 顶层错误（规则加载/范围解析/run 记录/run 收尾）必须上报并标记 ok=False
  - 禁止 except:pass / fallback 默认值掩盖错误
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class PipelineRequest:
    scope_type: str                          # selected / filter / all
    selected_factors: Optional[List[Dict[str, str]]] = None
    filters: Optional[Dict[str, Any]] = None
    parallelism: int = 4
    enable_llm_analysis: bool = True         # Step A 是否用 LLM (否则纯规则分类)
    enable_llm_audit: bool = True            # Step B 是否跑 LLM 审阅
    rule_version: Optional[str] = None       # None = 使用当前激活规则

    def resolve_parallelism(self) -> int:
        if self.parallelism is None:
            raise ValueError("parallelism 不能为 None")
        p = int(self.parallelism)
        if p < 1 or p > 16:
            raise ValueError(f"parallelism 必须在 [1,16] 区间, 收到 {p}")
        return p


@dataclass
class _WorkerCtx:
    run_id: str
    rule_version: str
    rule: Dict[str, Any]
    enable_llm_analysis: bool
    enable_llm_audit: bool
    sem: asyncio.Semaphore
    queue: asyncio.Queue
    counters: Dict[str, int] = field(default_factory=lambda: {
        "total": 0, "done": 0, "ok": 0, "failed": 0,
    })


def _now() -> float:
    return time.time()


async def _sse_put(queue: asyncio.Queue, event: str, payload: Dict[str, Any]) -> None:
    await queue.put({"event": event, "ts": _now(), **payload})


def _run_factor_analyst(factor_name: str, factor_source: str, use_llm: bool) -> Dict[str, Any]:
    """阻塞调用 — 在 executor 线程里跑. 失败/结构异常必须抛异常, 不得返回假成功."""
    from .factor_analyst import FactorAnalyst
    fa = FactorAnalyst()
    result = fa.analyze_single_factor(factor_name=factor_name, factor_source=factor_source, use_llm=use_llm)

    # 严格校验返回结构：FactorAnalyst 契约
    #   成功:  {"ok": True, "category": ..., ...}
    #   失败:  {"ok": False, "error": ...}
    if not isinstance(result, dict):
        raise RuntimeError(f"FactorAnalyst 返回非 dict: {type(result).__name__}")
    if result.get("ok") is False:
        err = result.get("error") or "FactorAnalyst 返回 ok=False 但无 error 字段"
        raise RuntimeError(str(err))
    if result.get("ok") is not True:
        raise RuntimeError(f"FactorAnalyst 返回缺少 ok 字段: keys={list(result.keys())[:10]}")
    if not result.get("category"):
        raise RuntimeError("FactorAnalyst 返回缺少 category")
    return result


def _run_grade_and_upsert(
    factor: Dict[str, Any],
    rule: Dict[str, Any],
    run_id: str,
    rule_version: str,
    enable_llm_audit: bool,
) -> Dict[str, Any]:
    """阻塞调用 — 评级 + 写入 official_ratings, 返回简要结果. 任何异常直接上抛."""
    from .factor_rating_service import factor_rating_service as svc
    result = svc._grade_factor(factor, rule, enable_llm_audit=enable_llm_audit)
    svc._upsert_official_rating(run_id, rule_version, factor["id"], result)
    return {
        "official_grade": result.get("official_grade"),
        "official_score": result.get("official_score"),
        "snapshot_date": result.get("snapshot_date"),
        "hard_gate_flags": result.get("hard_gate_flags"),
    }


async def _worker(factor: Dict[str, Any], ctx: _WorkerCtx) -> None:
    """单因子 Step A → Step B 严格顺序; 任一步失败则整体失败, 不继续后续步骤."""
    loop = asyncio.get_running_loop()
    factor_name = factor["factor_name"]
    factor_source = factor["source"]
    factor_id = factor["id"]

    async with ctx.sem:
        t0 = _now()
        await _sse_put(ctx.queue, "factor_start", {
            "factor_id": factor_id, "factor_name": factor_name, "factor_source": factor_source,
        })

        # ── Step A: 分析 + 分类 (写入 qe_factor_classification) ─────────
        await _sse_put(ctx.queue, "factor_step", {
            "factor_name": factor_name, "step": "A", "phase": "start",
            "desc": "FactorAnalyst 分类/方向/机制/行业敞口" + (" (LLM)" if ctx.enable_llm_analysis else " (规则)"),
        })
        try:
            result_a = await loop.run_in_executor(
                None, _run_factor_analyst, factor_name, factor_source, ctx.enable_llm_analysis,
            )
        except Exception as e:  # noqa: BLE001
            logger.exception("Pipeline Step A 失败 factor=%s", factor_name)
            step_a_err = str(e)
            await _sse_put(ctx.queue, "factor_step", {
                "factor_name": factor_name, "step": "A", "phase": "error", "error": step_a_err,
            })
            # 评级依赖 classification 表里的 direction/signal_mechanism/sector_exposure_corr
            # 若 Step A 失败, Step B 会读到空字段 -> 错误的评级. 必须跳过.
            ctx.counters["done"] += 1
            ctx.counters["failed"] += 1
            await _sse_put(ctx.queue, "factor_done", {
                "factor_name": factor_name, "ok": False,
                "step_a_ok": False, "step_a_error": step_a_err,
                "step_b_ok": False, "step_b_error": "skipped: Step A 失败, 评级依赖分类字段",
                "total_elapsed_ms": int((_now() - t0) * 1000),
            })
            await _sse_put(ctx.queue, "progress", {
                "done": ctx.counters["done"], "total": ctx.counters["total"],
                "ok": ctx.counters["ok"], "failed": ctx.counters["failed"],
            })
            return

        await _sse_put(ctx.queue, "factor_step", {
            "factor_name": factor_name, "step": "A", "phase": "done",
            "category": result_a.get("category"),
            "direction": result_a.get("direction"),
            "signal_mechanism": result_a.get("signal_mechanism"),
            "elapsed_ms": int((_now() - t0) * 1000),
        })

        # ── Step B: 评级 + LLM 审阅 + 写入 official_ratings ─────────────
        t_b = _now()
        await _sse_put(ctx.queue, "factor_step", {
            "factor_name": factor_name, "step": "B", "phase": "start",
            "desc": "规则打分 + 硬关卡" + (" + LLM审阅" if ctx.enable_llm_audit else ""),
        })
        try:
            grade_info = await loop.run_in_executor(
                None, _run_grade_and_upsert,
                factor, ctx.rule, ctx.run_id, ctx.rule_version, ctx.enable_llm_audit,
            )
        except Exception as e:  # noqa: BLE001
            logger.exception("Pipeline Step B 失败 factor=%s", factor_name)
            step_b_err = str(e)
            await _sse_put(ctx.queue, "factor_step", {
                "factor_name": factor_name, "step": "B", "phase": "error", "error": step_b_err,
            })
            ctx.counters["done"] += 1
            ctx.counters["failed"] += 1
            await _sse_put(ctx.queue, "factor_done", {
                "factor_name": factor_name, "ok": False,
                "step_a_ok": True, "step_a_error": None,
                "step_b_ok": False, "step_b_error": step_b_err,
                "total_elapsed_ms": int((_now() - t0) * 1000),
            })
            await _sse_put(ctx.queue, "progress", {
                "done": ctx.counters["done"], "total": ctx.counters["total"],
                "ok": ctx.counters["ok"], "failed": ctx.counters["failed"],
            })
            return

        await _sse_put(ctx.queue, "factor_step", {
            "factor_name": factor_name, "step": "B", "phase": "done",
            **grade_info,
            "elapsed_ms": int((_now() - t_b) * 1000),
        })

        # ── 因子成功 ─────────────────────────────────
        ctx.counters["done"] += 1
        ctx.counters["ok"] += 1
        await _sse_put(ctx.queue, "factor_done", {
            "factor_name": factor_name, "ok": True,
            "step_a_ok": True, "step_a_error": None,
            "step_b_ok": True, "step_b_error": None,
            "grade": grade_info.get("official_grade"),
            "score": grade_info.get("official_score"),
            "total_elapsed_ms": int((_now() - t0) * 1000),
        })
        await _sse_put(ctx.queue, "progress", {
            "done": ctx.counters["done"], "total": ctx.counters["total"],
            "ok": ctx.counters["ok"], "failed": ctx.counters["failed"],
        })


async def run_pipeline_async(request: PipelineRequest, queue: asyncio.Queue) -> None:
    """顶层协程：调度并行 worker + 输出 SSE 事件到 queue.

    顶层错误（规则加载/范围解析/run 记录/run 收尾）必须上报 error 事件,
    不得被 warning 静默掉。
    """
    from .factor_rating_service import factor_rating_service as svc

    loop = asyncio.get_running_loop()

    parallelism = request.resolve_parallelism()  # 不合法直接抛, 由 _run_with_sentinel 捕获

    # 1) 解析评级规则
    await loop.run_in_executor(None, svc.sync_rule_versions)
    rules = await loop.run_in_executor(None, svc.list_rule_versions)
    rule_version = request.rule_version or rules.get("active_version") or rules.get("default_version")
    if not rule_version:
        raise RuntimeError("当前无可用评级规则版本")
    rule = await loop.run_in_executor(None, svc.get_rule_detail, rule_version)
    if not rule:
        raise RuntimeError(f"评级规则 {rule_version} 读取失败")

    # 2) 解析范围
    scope_payload = {
        "selected_factors": request.selected_factors or [],
        "filters": request.filters or {},
    }
    factors = await loop.run_in_executor(
        None, svc._resolve_scope, request.scope_type, scope_payload, rule_version,
    )
    total = len(factors)

    # 3) 开启 rating_run（即使 total=0 也记录, 方便审计）
    run_id = str(uuid.uuid4())
    await loop.run_in_executor(
        None, svc._insert_run, run_id, rule_version,
        request.scope_type, scope_payload, "ui_toolbar",
    )

    await _sse_put(queue, "start", {
        "run_id": run_id,
        "rule_version": rule_version,
        "total": total,
        "parallelism": parallelism,
        "enable_llm_analysis": request.enable_llm_analysis,
        "enable_llm_audit": request.enable_llm_audit,
        "scope_type": request.scope_type,
    })

    if total == 0:
        # 空范围立即收尾
        await loop.run_in_executor(
            None, svc._finish_run, run_id, "completed",
            {"total_factors": 0, "success_count": 0, "failed_count": 0}, None,
        )
        await _sse_put(queue, "done", {
            "ok": True, "run_id": run_id, "rule_version": rule_version,
            "total_factors": 0, "success_count": 0, "failed_count": 0,
        })
        return

    # 4) 并行调度
    ctx = _WorkerCtx(
        run_id=run_id,
        rule_version=rule_version,
        rule=rule,
        enable_llm_analysis=request.enable_llm_analysis,
        enable_llm_audit=request.enable_llm_audit,
        sem=asyncio.Semaphore(parallelism),
        queue=queue,
    )
    ctx.counters["total"] = total

    tasks = [asyncio.create_task(_worker(f, ctx)) for f in factors]
    # _worker 内部已捕获所有业务异常并上报事件; 这里若仍有异常说明是 worker 框架层问题
    # (例如 Semaphore / Queue 异常), 必须原样上抛到顶层处理
    results = await asyncio.gather(*tasks, return_exceptions=True)
    framework_errors = [r for r in results if isinstance(r, BaseException)]
    if framework_errors:
        # 把首个异常原样抛出以确保顶层能报告
        first = framework_errors[0]
        logger.error("Pipeline worker 框架层异常 count=%d", len(framework_errors))
        raise first

    # 5) 关闭 run — 任何失败必须上报, 禁止 warning 吞掉
    run_status = "completed" if ctx.counters["ok"] > 0 or ctx.counters["failed"] == 0 else "failed"
    summary = {
        "total_factors": total,
        "success_count": ctx.counters["ok"],
        "failed_count": ctx.counters["failed"],
    }
    await loop.run_in_executor(None, svc._finish_run, run_id, run_status, summary, None)

    await _sse_put(queue, "done", {
        "ok": ctx.counters["failed"] == 0,
        "run_id": run_id,
        "rule_version": rule_version,
        "run_status": run_status,
        **summary,
    })


async def stream_pipeline(request: PipelineRequest):
    """生成器 — 供 FastAPI StreamingResponse 使用. yield 文本行 (SSE 格式)."""
    queue: asyncio.Queue = asyncio.Queue(maxsize=0)

    runner = asyncio.create_task(_run_with_sentinel(request, queue))

    try:
        while True:
            event = await queue.get()
            if event is None:  # sentinel
                break
            yield f"data: {json.dumps(event, ensure_ascii=False, default=str)}\n\n"
    finally:
        if not runner.done():
            runner.cancel()
            try:
                await runner
            except asyncio.CancelledError:
                pass
            except Exception:  # noqa: BLE001
                # 客户端已断开, 仅 log 不再传播
                logger.exception("stream_pipeline 清理阶段 runner 异常")


async def _run_with_sentinel(request: PipelineRequest, queue: asyncio.Queue) -> None:
    """顶层包装: 保证 sentinel 一定被写入, 任何异常都转为 error 事件."""
    try:
        await run_pipeline_async(request, queue)
    except asyncio.CancelledError:
        # 客户端断开/任务被取消, 不上报 error; 直接 sentinel 退出
        raise
    except Exception as e:  # noqa: BLE001
        # 顶层错误必须显式上报, 禁止静默
        logger.exception("Pipeline 顶层异常")
        await queue.put({"event": "error", "ts": _now(), "error": f"{type(e).__name__}: {e}"})
        await queue.put({"event": "done", "ts": _now(), "ok": False})
    finally:
        await queue.put(None)
