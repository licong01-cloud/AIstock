#!/usr/bin/env python3
"""
QE Evolution Diagnostic Script v1.0
====================================
对 QuantEvolver 自动演进实验进行全面诊断分析。

用法:
    python qe_evolution_diagnostic.py <task_id> [--json] [--api-base URL]

输出: 10 段 Markdown 诊断报告（或 --json 模式下的 JSON 结构）

依赖: requests
"""

import sys
import json
import argparse
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

try:
    import requests
except ImportError:
    print("ERROR: 需要 requests 库。请运行: pip install requests", file=sys.stderr)
    sys.exit(1)

DEFAULT_API_BASE = "http://127.0.0.1:8001/api/v1"

# ── 指标键映射（统一大小写） ──
METRIC_KEYS = {
    "IC": ["IC", "ic"],
    "ICIR": ["ICIR", "icir"],
    "Rank_IC": ["Rank_IC", "rank_ic", "RankIC", "Rank IC"],
    "Rank_ICIR": ["Rank_ICIR", "rank_icir", "RankICIR"],
    "sharpe": ["sharpe", "Sharpe", "sharpe_ratio"],
    "annualized_return": ["annualized_return", "ann_return"],
    "max_drawdown": ["max_drawdown", "maxdd"],
    "information_ratio": ["information_ratio", "IR"],
}

# ── 诊断代码 ──
DIAG_CODES = {
    "IC_STAGNANT": "IC 多轮无突破（因子信息量可能饱和）",
    "IC_DECLINING": "IC 持续下降",
    "SHARPE_STAGNANT": "Sharpe 停滞不前",
    "OVERFIT_RISK": "训练 loss 下降但验证 loss 不降（过拟合风险）",
    "EARLY_STOP_TOO_SOON": "模型过早停止训练（best_epoch 过小）",
    "HIGH_TURNOVER": "换手率过高（>0.5），成本侵蚀收益",
    "HIGH_COST_DRAG": "年化成本侵蚀 >5%",
    "STRATEGY_BOTTLENECK": "IC 改善但 Sharpe/收益不改善（策略执行问题）",
    "LOW_DIVERSITY": "连续使用相同 action_type 无改善",
    "NO_SOTA": "全部 Loop 均未达到 SOTA",
    "CONFIG_NO_CHANGE": "配置未发生实质变更",
    "FACTOR_CHURN": "因子频繁增删但效果不稳定",
    "CONSECUTIVE_DEGRADATION": "连续 2+ 轮 IC 退化，需回滚至 SOTA 配置",
    "SOTA_FACTOR_LOSS": "SOTA 配置中核心因子被大量移除（保留率 <60%）",
    "AGGRESSIVE_FACTOR_CHANGE": "单次因子变更超过 50%（过于激进）",
    "DIRECTION_MONOTONIC": "演进方向高度单一（仅使用 1 种 action_type）",
    "FACTOR_FROZEN": "因子列表连续 3+ 轮完全不变（factor_adjust 未实际执行）",
    "MODEL_SWITCH_INEFFECTIVE": "model_switch 后模型实际未变（切换逻辑失效）",
    "BATCH_SIZE_EXCESSIVE": "batch_size 超出已验证最优范围（Qlib 官方 2000-8000，QE 数据库最优 4096）",
}

# 严重度分类
SEVERITY_HIGH = {"IC_DECLINING", "OVERFIT_RISK", "NO_SOTA", "CONSECUTIVE_DEGRADATION",
                 "STRATEGY_BOTTLENECK", "SOTA_FACTOR_LOSS", "FACTOR_FROZEN",
                 "MODEL_SWITCH_INEFFECTIVE"}
SEVERITY_MEDIUM = {"IC_STAGNANT", "SHARPE_STAGNANT", "HIGH_TURNOVER", "HIGH_COST_DRAG",
                   "LOW_DIVERSITY", "FACTOR_CHURN", "AGGRESSIVE_FACTOR_CHANGE",
                   "DIRECTION_MONOTONIC", "EARLY_STOP_TOO_SOON", "CONFIG_NO_CHANGE",
                   "BATCH_SIZE_EXCESSIVE"}


# ================================================================
# 数据获取
# ================================================================

def fetch_task_detail(api_base: str, task_id: str) -> Dict[str, Any]:
    """获取任务详情（含全部 Loops）。"""
    url = f"{api_base}/quantevolver/evolution/tasks/{task_id}"
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if data.get("status") != "success":
        raise ValueError(f"API 返回异常: {data}")
    return data["data"]


def fetch_enhanced_metrics(api_base: str, task_id: str, loop_index: int) -> Optional[Dict]:
    """获取单个 Loop 的增强指标（best-effort）。"""
    loop_id = f"Loop{loop_index}"
    try:
        resp = requests.get(
            f"{api_base}/quantevolver/evolution/tasks/{task_id}/loops/{loop_id}/enhanced-metrics",
            timeout=10,
        )
        if resp.status_code == 200:
            body = resp.json()
            if body.get("status") == "success":
                return body.get("data")
    except Exception:
        pass
    return None


# ================================================================
# 数据解析工具
# ================================================================

def get_metric(metrics: Optional[Dict], key: str) -> Optional[float]:
    """从 metrics_json 中按多种别名提取指标值。"""
    if not metrics:
        return None
    for alias in METRIC_KEYS.get(key, [key]):
        v = metrics.get(alias)
        if v is not None:
            try:
                return float(v)
            except (ValueError, TypeError):
                pass
    return None


def fmt_num(v: Optional[float], decimals: int = 4) -> str:
    if v is None:
        return "-"
    return f"{v:.{decimals}f}"


def fmt_pct(v: Optional[float], decimals: int = 1) -> str:
    if v is None:
        return "-"
    return f"{v * 100:.{decimals}f}%"


def fmt_delta(cur: Optional[float], prev: Optional[float], decimals: int = 4) -> str:
    if cur is None or prev is None:
        return "-"
    d = cur - prev
    sign = "+" if d >= 0 else ""
    return f"{sign}{d:.{decimals}f}"


def parse_json_field(field) -> Dict:
    """安全解析 JSON 字段（可能已经是 dict 或仍是 str）。"""
    if isinstance(field, dict):
        return field
    if isinstance(field, str):
        try:
            return json.loads(field)
        except (json.JSONDecodeError, TypeError):
            pass
    return {}


def diff_lists(prev: List[str], cur: List[str]) -> Tuple[List[str], List[str], List[str]]:
    """计算列表差异：(added, removed, kept)"""
    prev_set, cur_set = set(prev), set(cur)
    return sorted(cur_set - prev_set), sorted(prev_set - cur_set), sorted(prev_set & cur_set)


def diff_dicts(prev: Dict, cur: Dict) -> Dict[str, Tuple]:
    """计算字典差异: key → (old_value, new_value)"""
    changes = {}
    all_keys = set(list(prev.keys()) + list(cur.keys()))
    for k in sorted(all_keys):
        old_v = prev.get(k)
        new_v = cur.get(k)
        if old_v != new_v:
            changes[k] = (old_v, new_v)
    return changes


# ================================================================
# 报告生成
# ================================================================

class EvolutionDiagnostic:
    """QE 演进实验诊断器。"""

    def __init__(self, api_base: str, task_id: str):
        self.api_base = api_base
        self.task_id = task_id
        self.task: Dict = {}
        self.loops: List[Dict] = []
        self.enhanced: Dict[int, Dict] = {}  # loop_index → enhanced_metrics
        self.diagnostics: List[str] = []  # 诊断代码列表

    def load_data(self):
        """从 API 加载所有数据。"""
        detail = fetch_task_detail(self.api_base, self.task_id)
        self.loops = detail.pop("loops", [])
        self.task = detail

        # 确保 loops 按 loop_index 排序
        self.loops.sort(key=lambda l: l.get("loop_index", 0))

        # 解析 JSON 字段
        for loop in self.loops:
            loop["_metrics"] = parse_json_field(loop.get("metrics_json"))
            loop["_config"] = parse_json_field(loop.get("config_json"))
            # 标准化因子字段名: API 返回 factor_list，脚本统一用 factor_names
            cfg = loop["_config"]
            if "factor_names" not in cfg and "factor_list" in cfg:
                cfg["factor_names"] = cfg["factor_list"]
            loop["_analysis"] = parse_json_field(loop.get("agent_analysis"))

        # 加载增强指标（best-effort，逐个获取）并合并到 _metrics
        completed_loops = [l for l in self.loops if l.get("status") == "completed"]
        for loop in completed_loops:
            idx = loop.get("loop_index", 0)
            em = fetch_enhanced_metrics(self.api_base, self.task_id, idx)
            if em:
                self.enhanced[idx] = em
                loop["_metrics"]["enhanced_metrics"] = em

    # ── Section 1: 任务概览 ──
    def section_task_overview(self) -> str:
        t = self.task
        completed = sum(1 for l in self.loops if l.get("status") == "completed")
        failed = sum(1 for l in self.loops if l.get("status") == "failed")
        sota_count = sum(1 for l in self.loops if l.get("is_sota"))
        lines = [
            "## 1. 任务概览",
            "",
            f"| 属性 | 值 |",
            f"|------|-----|",
            f"| Task ID | `{t.get('task_id', '?')}` |",
            f"| 任务名称 | {t.get('task_name', '?')} |",
            f"| 目标描述 | {t.get('target_desc', '-')} |",
            f"| 状态 | **{t.get('status', '?')}** |",
            f"| 来源类型 | {t.get('source_type', 'qe_experiment')} |",
            f"| 演进模式 | {t.get('evolution_mode', 'auto')} |",
            f"| 总轮数上限 | {t.get('max_loops', '?')} |",
            f"| 已完成轮数 | {completed} |",
            f"| 失败轮数 | {failed} |",
            f"| SOTA 次数 | {sota_count} |",
            f"| 创建时间 | {t.get('created_at', '?')} |",
            f"| 最后更新 | {t.get('updated_at', '?')} |",
        ]
        if sota_count == 0 and completed > 0:
            self.diagnostics.append("NO_SOTA")
        return "\n".join(lines)

    # ── Section 2: 指标趋势总览表 ──
    def section_metrics_table(self) -> str:
        lines = ["## 2. 指标趋势总览表", ""]
        header = "| Loop | Action | IC | ICIR | Rank IC | Sharpe | 年化收益 | 最大回撤 | SOTA | 状态 |"
        sep = "|------|--------|-----|------|---------|--------|----------|----------|------|------|"
        lines.extend([header, sep])

        for loop in self.loops:
            m = loop["_metrics"]
            idx = loop.get("loop_index", "?")
            action = loop.get("action_type", "-") or "-"
            sota = "★" if loop.get("is_sota") else ""
            status_map = {"completed": "✓", "failed": "✗", "running": "⟳", "pending": "…"}
            status_icon = status_map.get(loop.get("status", ""), "?")

            row = (
                f"| L{idx} | {action} "
                f"| {fmt_num(get_metric(m, 'IC'))} "
                f"| {fmt_num(get_metric(m, 'ICIR'))} "
                f"| {fmt_num(get_metric(m, 'Rank_IC'))} "
                f"| {fmt_num(get_metric(m, 'sharpe'), 2)} "
                f"| {fmt_pct(get_metric(m, 'annualized_return'))} "
                f"| {fmt_pct(get_metric(m, 'max_drawdown'))} "
                f"| {sota} | {status_icon} |"
            )
            lines.append(row)

        return "\n".join(lines)

    # ── Section 3: 指标变化率分析 ──
    def section_metrics_deltas(self) -> str:
        lines = ["## 3. 指标变化率分析", ""]
        completed = [l for l in self.loops if l.get("status") == "completed"]
        if len(completed) < 2:
            lines.append("*需要至少 2 个已完成的 Loop 才能计算变化率。*")
            return "\n".join(lines)

        header = "| Loop | ΔIC | ΔICIR | ΔRank IC | ΔSharpe | Δ年化收益 | Δ最大回撤 |"
        sep = "|------|-----|-------|----------|---------|----------|----------|"
        lines.extend([header, sep])

        key_list = ["IC", "ICIR", "Rank_IC", "sharpe", "annualized_return", "max_drawdown"]

        # 收集 IC 序列用于趋势判断
        ic_values = []

        for i, loop in enumerate(completed):
            m = loop["_metrics"]
            ic_val = get_metric(m, "IC")
            if ic_val is not None:
                ic_values.append(ic_val)

            if i == 0:
                continue
            prev_m = completed[i - 1]["_metrics"]
            idx = loop.get("loop_index", "?")
            deltas = []
            for key in key_list:
                cur = get_metric(m, key)
                prev = get_metric(prev_m, key)
                if key in ("annualized_return", "max_drawdown"):
                    deltas.append(fmt_delta(cur, prev, 4))
                elif key == "sharpe":
                    deltas.append(fmt_delta(cur, prev, 2))
                else:
                    deltas.append(fmt_delta(cur, prev))
            lines.append(f"| L{idx} | {' | '.join(deltas)} |")

        # 趋势判断
        lines.append("")
        if len(ic_values) >= 3:
            first_half = ic_values[:len(ic_values) // 2]
            second_half = ic_values[len(ic_values) // 2:]
            avg_first = sum(first_half) / len(first_half)
            avg_second = sum(second_half) / len(second_half)
            diff = avg_second - avg_first

            if abs(diff) < 0.002:
                lines.append("**IC 趋势**: 📊 震荡停滞（前半段均值 vs 后半段均值差异 < 0.002）")
                self.diagnostics.append("IC_STAGNANT")
            elif diff > 0:
                lines.append(f"**IC 趋势**: 📈 改善中（后半段均值高 {diff:.4f}）")
            else:
                lines.append(f"**IC 趋势**: 📉 退化中（后半段均值低 {abs(diff):.4f}）")
                self.diagnostics.append("IC_DECLINING")

        # 最佳/最差 Loop
        if ic_values:
            best_ic = max(ic_values)
            worst_ic = min(ic_values)
            best_idx = completed[ic_values.index(best_ic)].get("loop_index", "?")
            worst_idx = completed[ic_values.index(worst_ic)].get("loop_index", "?")
            lines.append(f"**最佳 IC**: Loop {best_idx} ({best_ic:.4f})  |  **最差 IC**: Loop {worst_idx} ({worst_ic:.4f})")

        return "\n".join(lines)

    # ── Section 4: 逐 Loop Agent 决策分析 ──
    def section_loop_analysis(self) -> str:
        lines = ["## 4. 逐 Loop Agent 决策分析", ""]

        for i, loop in enumerate(self.loops):
            idx = loop.get("loop_index", "?")
            status = loop.get("status", "?")
            analysis = loop["_analysis"]
            config = loop["_config"]
            metrics = loop["_metrics"]

            lines.append(f"### Loop {idx} ({status})")
            lines.append("")

            # 4a. Agent 分析师诊断
            report_text = analysis.get("report_text", "")
            if report_text:
                # 截取前 300 字
                preview = report_text[:300].replace("\n", " ")
                if len(report_text) > 300:
                    preview += "..."
                lines.append(f"**分析师诊断**: {preview}")
                lines.append("")

            # 4b. 评估器判定
            is_sota = loop.get("is_sota", False)
            eval_reason = analysis.get("evaluator_reason", analysis.get("evaluation_reason", ""))
            lines.append(f"**SOTA 评估**: {'✅ ACCEPT' if is_sota else '❌ REJECT'}")
            if eval_reason:
                lines.append(f"  - 理由: {eval_reason}")
            lines.append("")

            # 4c. 演进方向
            direction = analysis.get("direction", {})
            if isinstance(direction, dict):
                rec = direction.get("recommended_direction", "-")
                confidence = direction.get("confidence", "-")
                reasoning = direction.get("reasoning", "")
                lines.append(f"**演进方向**: `{rec}` (置信度: {confidence})")
                if reasoning:
                    lines.append(f"  - 理由: {reasoning[:200]}")
            elif isinstance(direction, str):
                lines.append(f"**演进方向**: `{direction}`")
            lines.append("")

            # 4d. 具体变更 (config diff)
            if i > 0:
                prev_config = self.loops[i - 1]["_config"]
                self._render_config_diff(lines, prev_config, config, idx)
            else:
                lines.append("**配置**: 初始基线配置")
                factor_names = config.get("factor_names", [])
                if factor_names:
                    lines.append(f"  - 因子数量: {len(factor_names)}")
                model_id = config.get("model_id", "-")
                lines.append(f"  - 模型: {model_id}")
            lines.append("")

            # 4e. 执行结果
            if status == "completed" and metrics:
                if i > 0:
                    prev_m = self.loops[i - 1]["_metrics"]
                    ic_delta = fmt_delta(get_metric(metrics, "IC"), get_metric(prev_m, "IC"))
                    sharpe_delta = fmt_delta(get_metric(metrics, "sharpe"), get_metric(prev_m, "sharpe"), 2)
                    lines.append(f"**执行结果**: IC={fmt_num(get_metric(metrics, 'IC'))} (Δ{ic_delta}), Sharpe={fmt_num(get_metric(metrics, 'sharpe'), 2)} (Δ{sharpe_delta})")
                else:
                    lines.append(f"**执行结果**: IC={fmt_num(get_metric(metrics, 'IC'))}, Sharpe={fmt_num(get_metric(metrics, 'sharpe'), 2)}")
            elif status == "failed":
                error = analysis.get("error", loop.get("_analysis", {}).get("error_traceback", "未知错误"))
                if isinstance(error, str) and len(error) > 200:
                    error = error[:200] + "..."
                lines.append(f"**执行结果**: ❌ 失败 — {error}")

            # 4f. 因子重要性 (从 enhanced_metrics 提取)
            fi_data = metrics.get("enhanced_metrics", {}).get("feature_importance", {}) if metrics else {}
            if fi_data and fi_data.get("factors"):
                fi_method = fi_data.get("method", "?")
                fi_model_type = fi_data.get("model_type", "?")
                fi_factors = fi_data["factors"]
                sorted_fi = sorted(fi_factors.items(), key=lambda x: -x[1].get("importance", 0))
                n_total = len(sorted_fi)
                n_protected = max(1, int(n_total * 0.7))
                lines.append("")
                lines.append(f"**因子重要性** (方法: {fi_method}, 模型: {fi_model_type})")
                lines.append("")
                lines.append("| 排名 | 因子 | 重要性 | 状态 |")
                lines.append("|------|------|--------|------|")
                for rank_i, (fn, fv) in enumerate(sorted_fi):
                    imp_pct = fv.get("importance", 0) * 100
                    status_tag = "受保护" if rank_i < n_protected else "可淘汰"
                    lines.append(f"| {rank_i + 1} | {fn} | {imp_pct:.1f}% | {status_tag} |")
                top5_conc = fi_data.get("top5_concentration")
                if top5_conc is not None:
                    lines.append(f"\nTop5 集中度: {top5_conc * 100:.1f}%")

            lines.append("")
            lines.append("---")
            lines.append("")

        return "\n".join(lines)

    def _render_config_diff(self, lines: List[str], prev: Dict, cur: Dict, loop_idx):
        """渲染两个 config 之间的差异。"""
        changes_found = False

        # 因子变更
        prev_factors = prev.get("factor_names", [])
        cur_factors = cur.get("factor_names", [])
        if isinstance(prev_factors, str):
            prev_factors = [f.strip() for f in prev_factors.split(",") if f.strip()]
        if isinstance(cur_factors, str):
            cur_factors = [f.strip() for f in cur_factors.split(",") if f.strip()]

        if prev_factors != cur_factors:
            added, removed, _kept = diff_lists(prev_factors, cur_factors)
            lines.append("**因子变更**:")
            if added:
                lines.append(f"  - 新增: {', '.join(added)}")
            if removed:
                lines.append(f"  - 移除: {', '.join(removed)}")
            lines.append(f"  - 总数: {len(prev_factors)} → {len(cur_factors)}")
            changes_found = True

        # 模型变更
        prev_model = prev.get("model_id", "")
        cur_model = cur.get("model_id", "")
        if prev_model != cur_model:
            lines.append(f"**模型变更**: `{prev_model}` → `{cur_model}`")
            changes_found = True

        # 超参数变更
        prev_params = prev.get("custom_params", {}) or {}
        cur_params = cur.get("custom_params", {}) or {}
        if isinstance(prev_params, str):
            prev_params = parse_json_field(prev_params)
        if isinstance(cur_params, str):
            cur_params = parse_json_field(cur_params)

        param_diff = diff_dicts(prev_params, cur_params)
        if param_diff:
            lines.append("**超参数变更**:")
            for k, (old_v, new_v) in param_diff.items():
                lines.append(f"  - `{k}`: {old_v} → {new_v}")
            changes_found = True

        # 策略变更
        prev_strategy = prev.get("strategy_id", "")
        cur_strategy = cur.get("strategy_id", "")
        if prev_strategy != cur_strategy:
            lines.append(f"**策略变更**: `{prev_strategy}` → `{cur_strategy}`")
            changes_found = True

        if not changes_found:
            lines.append("**配置变更**: 无实质变更")
            self.diagnostics.append("CONFIG_NO_CHANGE")

    # ── Section 5: 配置变更追踪 ──
    def section_config_tracking(self) -> str:
        lines = ["## 5. 配置变更追踪", ""]
        completed = [l for l in self.loops if l.get("status") == "completed"]

        # 因子历史
        lines.append("### 因子数量变化")
        for loop in completed:
            idx = loop.get("loop_index", "?")
            factors = loop["_config"].get("factor_names", [])
            if isinstance(factors, str):
                factors = [f.strip() for f in factors.split(",") if f.strip()]
            lines.append(f"  - Loop {idx}: {len(factors)} 个因子")

        # 模型历史
        lines.append("")
        lines.append("### 模型变化")
        for loop in completed:
            idx = loop.get("loop_index", "?")
            model = loop["_config"].get("model_id", "-")
            lines.append(f"  - Loop {idx}: `{model}`")

        # 超参数历史
        lines.append("")
        lines.append("### 关键超参数变化")
        hp_keys_seen = set()
        for loop in completed:
            params = loop["_config"].get("custom_params", {}) or {}
            if isinstance(params, str):
                params = parse_json_field(params)
            hp_keys_seen.update(params.keys())

        if hp_keys_seen:
            header = "| Loop | " + " | ".join(sorted(hp_keys_seen)) + " |"
            sep = "|------|" + "|".join(["------"] * len(hp_keys_seen)) + "|"
            lines.extend([header, sep])
            for loop in completed:
                idx = loop.get("loop_index", "?")
                params = loop["_config"].get("custom_params", {}) or {}
                if isinstance(params, str):
                    params = parse_json_field(params)
                vals = [str(params.get(k, "-")) for k in sorted(hp_keys_seen)]
                lines.append(f"| L{idx} | " + " | ".join(vals) + " |")
        else:
            lines.append("*无自定义超参数记录*")

        # SOTA 因子保留率分析
        sota_loops = [l for l in completed if l.get("is_sota")]
        if sota_loops:
            lines.append("")
            lines.append("### SOTA 因子保留率")
            sota_factors = sota_loops[-1]["_config"].get("factor_names", [])
            if isinstance(sota_factors, str):
                sota_factors = [f.strip() for f in sota_factors.split(",") if f.strip()]
            sota_set = set(sota_factors)
            sota_idx = sota_loops[-1].get("loop_index", 0)
            lines.append(f"SOTA Loop {sota_idx} 因子集: {len(sota_factors)} 个")
            lines.append("")

            for loop in completed:
                if loop.get("loop_index", 0) <= sota_idx:
                    continue
                idx = loop.get("loop_index", "?")
                cur_factors = loop["_config"].get("factor_names", [])
                if isinstance(cur_factors, str):
                    cur_factors = [f.strip() for f in cur_factors.split(",") if f.strip()]
                retained = set(cur_factors) & sota_set
                retention_rate = len(retained) / len(sota_set) * 100 if sota_set else 100
                lost = sorted(sota_set - set(cur_factors))
                lines.append(f"  - Loop {idx}: 保留率 {retention_rate:.0f}% ({len(retained)}/{len(sota_set)})")
                if lost:
                    lines.append(f"    移除的 SOTA 因子: {', '.join(lost)}")

        # 方向多样性分析
        lines.append("")
        lines.append("### 演进方向多样性")
        action_counts: Dict[str, int] = {}
        for loop in completed[1:]:
            at = loop.get("action_type", "unknown") or "unknown"
            action_counts[at] = action_counts.get(at, 0) + 1
        total_actions = sum(action_counts.values())
        if total_actions > 0:
            for at, count in sorted(action_counts.items()):
                pct = count / total_actions * 100
                lines.append(f"  - `{at}`: {count} 次 ({pct:.0f}%)")
            unexplored = {"factor_adjust", "param_tune", "model_switch"} - set(action_counts.keys())
            if unexplored:
                lines.append(f"  - **未探索方向**: {', '.join(sorted(unexplored))}")
        else:
            lines.append("*仅有初始 Loop，无演进方向记录*")

        return "\n".join(lines)

    # ── Section 6: 变更有效性矩阵 ──
    def section_effectiveness_matrix(self) -> str:
        lines = ["## 6. 变更有效性矩阵", ""]
        completed = [l for l in self.loops if l.get("status") == "completed"]

        if len(completed) < 2:
            lines.append("*需要至少 2 个已完成的 Loop。*")
            return "\n".join(lines)

        # 按 action_type 分组计算 delta
        action_stats: Dict[str, Dict] = {}
        for i in range(1, len(completed)):
            cur = completed[i]
            prev = completed[i - 1]
            action = cur.get("action_type", "unknown") or "unknown"

            if action not in action_stats:
                action_stats[action] = {"count": 0, "ic_deltas": [], "sharpe_deltas": [], "improved": 0}

            stats = action_stats[action]
            stats["count"] += 1

            cur_ic = get_metric(cur["_metrics"], "IC")
            prev_ic = get_metric(prev["_metrics"], "IC")
            cur_sharpe = get_metric(cur["_metrics"], "sharpe")
            prev_sharpe = get_metric(prev["_metrics"], "sharpe")

            if cur_ic is not None and prev_ic is not None:
                d = cur_ic - prev_ic
                stats["ic_deltas"].append(d)
                if d > 0.001:
                    stats["improved"] += 1

            if cur_sharpe is not None and prev_sharpe is not None:
                stats["sharpe_deltas"].append(cur_sharpe - prev_sharpe)

        header = "| Action Type | 次数 | IC 平均Δ | Sharpe 平均Δ | 有效率 |"
        sep = "|-------------|------|---------|-------------|--------|"
        lines.extend([header, sep])

        for action, stats in sorted(action_stats.items()):
            ic_avg = sum(stats["ic_deltas"]) / len(stats["ic_deltas"]) if stats["ic_deltas"] else None
            sharpe_avg = sum(stats["sharpe_deltas"]) / len(stats["sharpe_deltas"]) if stats["sharpe_deltas"] else None
            eff_rate = f"{stats['improved'] / stats['count'] * 100:.0f}%" if stats["count"] > 0 else "-"
            lines.append(
                f"| {action} | {stats['count']} "
                f"| {fmt_delta(ic_avg, 0) if ic_avg is not None else '-'} "
                f"| {fmt_delta(sharpe_avg, 0, 2) if sharpe_avg is not None else '-'} "
                f"| {eff_rate} |"
            )

        # 检测低多样性
        if len(action_stats) == 1 and len(completed) > 3:
            sole_action = list(action_stats.keys())[0]
            sole_stats = action_stats[sole_action]
            if sole_stats["improved"] == 0:
                self.diagnostics.append("LOW_DIVERSITY")

        return "\n".join(lines)

    # ── Section 7: 训练质量诊断 ──
    def section_training_quality(self) -> str:
        lines = ["## 7. 训练质量诊断", ""]

        if not self.enhanced:
            lines.append("*无增强指标数据（RDAgent API 不可达或数据未生成）。*")
            return "\n".join(lines)

        header = "| Loop | Best Epoch | Total Epochs | Overfit Ratio | 收敛度 | 诊断 |"
        sep = "|------|-----------|-------------|--------------|--------|------|"
        lines.extend([header, sep])

        for idx in sorted(self.enhanced.keys()):
            em = self.enhanced[idx]
            td = em.get("training_diagnostics", {})
            if not td:
                lines.append(f"| L{idx} | - | - | - | - | 无训练数据 |")
                continue

            best_epoch = td.get("best_epoch")
            train_loss = td.get("train_loss_curve", [])
            val_loss = td.get("val_loss_curve", [])
            total_epochs = len(train_loss) if train_loss else None
            overfit = td.get("overfit_ratio")
            convergence = td.get("convergence_ratio")

            diag = []
            if best_epoch is not None and total_epochs and best_epoch < total_epochs * 0.1:
                diag.append("过早停止")
                self.diagnostics.append("EARLY_STOP_TOO_SOON")
            if overfit is not None and overfit > 1.5:
                diag.append("过拟合")
                self.diagnostics.append("OVERFIT_RISK")
            if not diag:
                diag.append("正常")

            lines.append(
                f"| L{idx} "
                f"| {best_epoch if best_epoch is not None else '-'} "
                f"| {total_epochs if total_epochs else '-'} "
                f"| {fmt_num(overfit, 2) if overfit is not None else '-'} "
                f"| {fmt_num(convergence, 2) if convergence is not None else '-'} "
                f"| {', '.join(diag)} |"
            )

        return "\n".join(lines)

    # ── Section 8: 交易效率诊断 ──
    def section_trade_efficiency(self) -> str:
        lines = ["## 8. 交易效率诊断", ""]

        if not self.enhanced:
            lines.append("*无增强指标数据。*")
            return "\n".join(lines)

        header = "| Loop | 平均换手率 | 成本侵蚀(年化) | 日均交易笔数 |"
        sep = "|------|-----------|---------------|-------------|"
        lines.extend([header, sep])

        for idx in sorted(self.enhanced.keys()):
            em = self.enhanced[idx]
            td = em.get("trade_diagnostics", {})
            turnover = td.get("avg_turnover")
            cost_drag = td.get("cost_drag_annualized")
            trade_count = td.get("daily_trade_count_avg")

            if turnover is not None and turnover > 0.5:
                self.diagnostics.append("HIGH_TURNOVER")
            if cost_drag is not None and cost_drag > 0.05:
                self.diagnostics.append("HIGH_COST_DRAG")

            lines.append(
                f"| L{idx} "
                f"| {fmt_num(turnover, 3) if turnover is not None else '-'} "
                f"| {fmt_pct(cost_drag) if cost_drag is not None else '-'} "
                f"| {fmt_num(trade_count, 1) if trade_count is not None else '-'} |"
            )

        return "\n".join(lines)

    # ── Section 9: 瓶颈识别 ──
    def section_bottleneck(self) -> str:
        lines = ["## 9. 瓶颈识别", ""]
        completed = [l for l in self.loops if l.get("status") == "completed"]

        if len(completed) < 2:
            lines.append("*数据不足，无法进行瓶颈分析。*")
            return "\n".join(lines)

        # 策略瓶颈检测: IC 改善但 Sharpe 不改善
        ic_series = [get_metric(l["_metrics"], "IC") for l in completed]
        sharpe_series = [get_metric(l["_metrics"], "sharpe") for l in completed]
        ic_series_clean = [x for x in ic_series if x is not None]
        sharpe_series_clean = [x for x in sharpe_series if x is not None]

        if len(ic_series_clean) >= 3 and len(sharpe_series_clean) >= 3:
            ic_improving = ic_series_clean[-1] > ic_series_clean[0] + 0.002
            sharpe_not_improving = sharpe_series_clean[-1] <= sharpe_series_clean[0] + 0.05
            if ic_improving and sharpe_not_improving:
                self.diagnostics.append("STRATEGY_BOTTLENECK")

        # 因子频繁变动检测
        factor_changes = 0
        for i in range(1, len(completed)):
            prev_f = completed[i - 1]["_config"].get("factor_names", [])
            cur_f = completed[i]["_config"].get("factor_names", [])
            if prev_f != cur_f:
                factor_changes += 1
        if factor_changes >= len(completed) - 1 and len(completed) >= 3:
            ic_improved = any(
                (get_metric(completed[j]["_metrics"], "IC") or 0) > (get_metric(completed[j - 1]["_metrics"], "IC") or 0) + 0.002
                for j in range(1, len(completed))
            )
            if not ic_improved:
                self.diagnostics.append("FACTOR_CHURN")

        # 连续退化检测
        consecutive_decline = 0
        for i in range(1, len(completed)):
            cur_ic = get_metric(completed[i]["_metrics"], "IC")
            prev_ic = get_metric(completed[i - 1]["_metrics"], "IC")
            if cur_ic is not None and prev_ic is not None and cur_ic < prev_ic - 0.001:
                consecutive_decline += 1
            else:
                consecutive_decline = 0
        if consecutive_decline >= 2:
            self.diagnostics.append("CONSECUTIVE_DEGRADATION")

        # SOTA 因子保留率检测
        sota_loops = [l for l in completed if l.get("is_sota")]
        if sota_loops:
            sota_factors = sota_loops[-1]["_config"].get("factor_names", [])
            if isinstance(sota_factors, str):
                sota_factors = [f.strip() for f in sota_factors.split(",") if f.strip()]
            sota_set = set(sota_factors)
            if sota_set:
                for loop in completed:
                    if loop.get("loop_index", 0) <= sota_loops[-1].get("loop_index", 0):
                        continue
                    cur_factors = loop["_config"].get("factor_names", [])
                    if isinstance(cur_factors, str):
                        cur_factors = [f.strip() for f in cur_factors.split(",") if f.strip()]
                    retained = set(cur_factors) & sota_set
                    retention_rate = len(retained) / len(sota_set) if sota_set else 1.0
                    if retention_rate < 0.6:
                        self.diagnostics.append("SOTA_FACTOR_LOSS")
                        break

        # 激进因子变更检测
        for i in range(1, len(completed)):
            prev_f = completed[i - 1]["_config"].get("factor_names", [])
            cur_f = completed[i]["_config"].get("factor_names", [])
            if isinstance(prev_f, str):
                prev_f = [f.strip() for f in prev_f.split(",") if f.strip()]
            if isinstance(cur_f, str):
                cur_f = [f.strip() for f in cur_f.split(",") if f.strip()]
            if prev_f:
                added, removed, _kept = diff_lists(prev_f, cur_f)
                change_ratio = (len(added) + len(removed)) / len(prev_f) if prev_f else 0
                if change_ratio > 0.5:
                    self.diagnostics.append("AGGRESSIVE_FACTOR_CHANGE")
                    break

        # 方向单一性检测
        action_types = set()
        for loop in completed[1:]:  # 跳过初始 Loop
            at = loop.get("action_type")
            if at:
                action_types.add(at)
        if len(action_types) <= 1 and len(completed) >= 4:
            self.diagnostics.append("DIRECTION_MONOTONIC")

        # 因子列表冻结检测: 连续 3+ 轮因子完全不变
        frozen_streak = 0
        for i in range(1, len(completed)):
            prev_f = set(completed[i - 1]["_config"].get("factor_names", []) if isinstance(completed[i - 1]["_config"].get("factor_names", []), list) else [])
            cur_f = set(completed[i]["_config"].get("factor_names", []) if isinstance(completed[i]["_config"].get("factor_names", []), list) else [])
            if prev_f == cur_f and prev_f:
                frozen_streak += 1
            else:
                frozen_streak = 0
            if frozen_streak >= 3:
                self.diagnostics.append("FACTOR_FROZEN")
                break

        # model_switch 实际无效检测
        for i in range(1, len(completed)):
            if completed[i].get("action_type") == "model_switch":
                prev_model = completed[i - 1]["_config"].get("model_id", "")
                cur_model = completed[i]["_config"].get("model_id", "")
                if prev_model == cur_model and prev_model:
                    self.diagnostics.append("MODEL_SWITCH_INEFFECTIVE")
                    break

        # batch_size 超出验证范围检测
        for loop in completed:
            params = loop["_config"].get("custom_params", {}) or {}
            if isinstance(params, str):
                params = parse_json_field(params)
            bs = params.get("batch_size")
            if bs is not None and isinstance(bs, (int, float)) and bs > 16384:
                self.diagnostics.append("BATCH_SIZE_EXCESSIVE")
                break

        # 输出已识别的诊断
        unique_diags = list(dict.fromkeys(self.diagnostics))  # 去重保序
        if unique_diags:
            for code in unique_diags:
                desc = DIAG_CODES.get(code, code)
                severity = "🔴" if code in SEVERITY_HIGH else "🟡"
                lines.append(f"- {severity} **{code}**: {desc}")
        else:
            lines.append("✅ 未发现明显瓶颈")

        return "\n".join(lines)

    # ── Section 10: 综合结论与建议 ──
    def section_conclusion(self) -> str:
        lines = ["## 10. 综合结论与建议", ""]
        completed = [l for l in self.loops if l.get("status") == "completed"]
        failed = [l for l in self.loops if l.get("status") == "failed"]

        # 整体评级
        if len(completed) < 2:
            rating = "数据不足"
        else:
            first_ic = get_metric(completed[0]["_metrics"], "IC")
            last_ic = get_metric(completed[-1]["_metrics"], "IC")
            best_ic = max((get_metric(l["_metrics"], "IC") or 0) for l in completed)
            sota_count = sum(1 for l in completed if l.get("is_sota"))

            if first_ic is not None and last_ic is not None:
                delta = last_ic - first_ic
                if delta > 0.01:
                    rating = "📈 显著改善"
                elif delta > 0.003:
                    rating = "📊 边际改善"
                elif delta > -0.003:
                    rating = "📉 无效（震荡停滞）"
                else:
                    rating = "🔻 退化"
            else:
                rating = "无法评估"

        lines.append(f"### 整体演进效果: {rating}")
        lines.append("")

        # 统计
        if completed:
            first_ic = get_metric(completed[0]["_metrics"], "IC")
            last_ic = get_metric(completed[-1]["_metrics"], "IC")
            best_ic = max((get_metric(l["_metrics"], "IC") or 0) for l in completed)
            lines.append(f"- 起始 IC: {fmt_num(first_ic)}")
            lines.append(f"- 最终 IC: {fmt_num(last_ic)}")
            lines.append(f"- 最佳 IC: {fmt_num(best_ic)}")
            if first_ic and last_ic:
                lines.append(f"- IC 变化: {fmt_delta(last_ic, first_ic)}")
        lines.append(f"- 失败轮数: {len(failed)}")
        lines.append("")

        # 建议
        lines.append("### 优先建议")
        suggestions = []
        unique_diags = list(dict.fromkeys(self.diagnostics))

        if "CONSECUTIVE_DEGRADATION" in unique_diags:
            suggestions.append("1. **紧急: 启用退化回滚机制**：连续退化应自动回退至 SOTA 配置，而非在退化配置上继续演进。修改 `submit_next_loop` 逻辑。")
        if "IC_STAGNANT" in unique_diags or "IC_DECLINING" in unique_diags:
            suggestions.append("2. **尝试引入新因子来源或切换模型架构**：当前因子/模型组合的信息量可能已饱和，继续微调难以突破。考虑添加不同类别的因子（如基本面、另类数据）或尝试不同的模型结构。")
        if "OVERFIT_RISK" in unique_diags:
            suggestions.append("3. **降低模型复杂度或增加正则化**：训练过拟合明显，建议增大 weight_decay、减少 n_epochs、或简化模型层数。")
        if "HIGH_TURNOVER" in unique_diags or "HIGH_COST_DRAG" in unique_diags:
            suggestions.append("4. **调整策略参数降低换手率**：高换手率侵蚀收益，考虑增大 topk 持仓数或减小 n_drop。")
        if "STRATEGY_BOTTLENECK" in unique_diags:
            suggestions.append("5. **优化策略层**：IC 在改善但 Sharpe 不跟随，问题在策略执行（仓位管理、交易成本）而非信号质量。")
        if "LOW_DIVERSITY" in unique_diags or "DIRECTION_MONOTONIC" in unique_diags:
            suggestions.append("6. **增加演进方向多样性**：当前仅使用单一 action_type，建议在 `_decide_evolution_direction` 中添加硬性规则：连续 N 轮相同方向且无改善时强制切换。")
        if "NO_SOTA" in unique_diags:
            suggestions.append("7. **检查 SOTA 评估阈值**：所有 Loop 均未达到 SOTA，可能基线过高或评估标准过严。")
        if "FACTOR_CHURN" in unique_diags or "AGGRESSIVE_FACTOR_CHANGE" in unique_diags:
            suggestions.append("8. **稳定因子组合，限制单次变更幅度**：因子频繁大幅增删但效果不稳定，建议在 Factor Agent Step 6 增加约束：SOTA 因子保留 ≥60%，单次最多替换 3 个。")
        if "SOTA_FACTOR_LOSS" in unique_diags:
            suggestions.append("9. **保护 SOTA 核心因子**：SOTA 配置中的因子被大量移除，应在 Factor Agent 提示词中强制要求保留已验证有效的因子。")
        if "FACTOR_FROZEN" in unique_diags:
            suggestions.append("10. **紧急: 因子演进管线失效**：因子列表连续多轮完全不变，factor_adjust 决策未被实际执行。检查 FactorAgent→config_composer 因子写入链路，确认因子变更是否被 factor_locked 错误阻止或被 config_composer 覆盖。")
        if "MODEL_SWITCH_INEFFECTIVE" in unique_diags:
            suggestions.append("11. **模型切换逻辑失效**：agent 决定 model_switch 但实际模型未变。检查 ModelAgent Step 5-7 的选择逻辑，确认候选模型是否有效、DB 查询是否命中正确记录。")
        if "BATCH_SIZE_EXCESSIVE" in unique_diags:
            suggestions.append("12. **batch_size 超出最优范围**：当前 batch_size 超过 16384，已验证 Qlib 官方 (2000-8000) 和 QE 数据库最优为 4096。检查 config_composer._DEFAULT_BATCH_SIZE 和 HYPERPARAM_RANGES 配置。")

        if not suggestions:
            suggestions.append("1. 当前演进表现正常，建议继续增加演进轮数观察趋势。")

        lines.extend(suggestions)
        return "\n".join(lines)

    # ── 生成完整报告 ──
    def generate_report(self) -> str:
        sections = [
            f"# QE 演进诊断报告",
            f"",
            f"> 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"> Task ID: `{self.task_id}`",
            f"",
            self.section_task_overview(),
            "",
            self.section_metrics_table(),
            "",
            self.section_metrics_deltas(),
            "",
            self.section_loop_analysis(),
            "",
            self.section_config_tracking(),
            "",
            self.section_effectiveness_matrix(),
            "",
            self.section_training_quality(),
            "",
            self.section_trade_efficiency(),
            "",
            self.section_bottleneck(),
            "",
            self.section_conclusion(),
        ]
        return "\n".join(sections)

    def generate_json(self) -> Dict[str, Any]:
        """生成 JSON 格式的诊断数据。"""
        completed = [l for l in self.loops if l.get("status") == "completed"]

        # 构建 loops 摘要
        loops_summary = []
        for i, loop in enumerate(self.loops):
            m = loop["_metrics"]
            summary = {
                "loop_index": loop.get("loop_index"),
                "status": loop.get("status"),
                "action_type": loop.get("action_type"),
                "is_sota": loop.get("is_sota", False),
                "metrics": {
                    key: get_metric(m, key)
                    for key in METRIC_KEYS
                },
                "agent_direction": None,
                "config_factors_count": 0,
                "config_model_id": None,
            }

            analysis = loop["_analysis"]
            direction = analysis.get("direction", {})
            if isinstance(direction, dict):
                summary["agent_direction"] = direction.get("recommended_direction")
            elif isinstance(direction, str):
                summary["agent_direction"] = direction

            config = loop["_config"]
            factors = config.get("factor_names", [])
            if isinstance(factors, str):
                factors = [f.strip() for f in factors.split(",") if f.strip()]
            summary["config_factors_count"] = len(factors)
            summary["config_model_id"] = config.get("model_id")

            # 因子重要性透传
            fi_data = m.get("enhanced_metrics", {}).get("feature_importance", {})
            if fi_data and fi_data.get("factors"):
                summary["feature_importance"] = fi_data

            loops_summary.append(summary)

        # 变更有效性
        action_effectiveness = {}
        for i in range(1, len(completed)):
            action = completed[i].get("action_type", "unknown") or "unknown"
            cur_ic = get_metric(completed[i]["_metrics"], "IC")
            prev_ic = get_metric(completed[i - 1]["_metrics"], "IC")
            if action not in action_effectiveness:
                action_effectiveness[action] = {"count": 0, "ic_deltas": [], "improved": 0}
            s = action_effectiveness[action]
            s["count"] += 1
            if cur_ic is not None and prev_ic is not None:
                d = cur_ic - prev_ic
                s["ic_deltas"].append(round(d, 6))
                if d > 0.001:
                    s["improved"] += 1

        return {
            "task_id": self.task_id,
            "task_name": self.task.get("task_name"),
            "status": self.task.get("status"),
            "total_loops": len(self.loops),
            "completed_loops": len(completed),
            "sota_count": sum(1 for l in self.loops if l.get("is_sota")),
            "loops": loops_summary,
            "diagnostics": list(dict.fromkeys(self.diagnostics)),
            "action_effectiveness": action_effectiveness,
            "factor_retention": self._compute_factor_retention(completed),
            "direction_diversity": self._compute_direction_diversity(completed),
            "generated_at": datetime.now().isoformat(),
        }

    def _compute_factor_retention(self, completed: List[Dict]) -> Optional[Dict]:
        """计算 SOTA 因子保留率。"""
        sota_loops = [l for l in completed if l.get("is_sota")]
        if not sota_loops:
            return None
        sota_factors = sota_loops[-1]["_config"].get("factor_names", [])
        if isinstance(sota_factors, str):
            sota_factors = [f.strip() for f in sota_factors.split(",") if f.strip()]
        sota_set = set(sota_factors)
        sota_idx = sota_loops[-1].get("loop_index", 0)
        if not sota_set:
            return None

        result = {"sota_loop": sota_idx, "sota_factor_count": len(sota_set), "retention_by_loop": {}}
        for loop in completed:
            if loop.get("loop_index", 0) <= sota_idx:
                continue
            cur_factors = loop["_config"].get("factor_names", [])
            if isinstance(cur_factors, str):
                cur_factors = [f.strip() for f in cur_factors.split(",") if f.strip()]
            retained = set(cur_factors) & sota_set
            result["retention_by_loop"][loop.get("loop_index")] = {
                "retained": len(retained),
                "total_sota": len(sota_set),
                "retention_rate": round(len(retained) / len(sota_set), 4),
                "lost_factors": sorted(sota_set - set(cur_factors)),
            }
        return result

    def _compute_direction_diversity(self, completed: List[Dict]) -> Dict:
        """计算演进方向多样性指标。"""
        action_counts: Dict[str, int] = {}
        for loop in completed[1:]:
            at = loop.get("action_type", "unknown") or "unknown"
            action_counts[at] = action_counts.get(at, 0) + 1
        total = sum(action_counts.values())
        unexplored = sorted({"factor_adjust", "param_tune", "model_switch"} - set(action_counts.keys()))
        return {
            "action_distribution": action_counts,
            "total_actions": total,
            "unique_directions": len(action_counts),
            "unexplored_directions": unexplored,
        }


# ================================================================
# Main
# ================================================================

def main():
    parser = argparse.ArgumentParser(
        description="QE Evolution Diagnostic - 演进实验全面诊断分析",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="示例:\n  python qe_evolution_diagnostic.py qe_20260307_014943\n  python qe_evolution_diagnostic.py qe_20260307_014943 --json",
    )
    parser.add_argument("task_id", help="演进任务 ID")
    parser.add_argument("--json", action="store_true", help="输出 JSON 格式（默认 Markdown）")
    parser.add_argument("--api-base", default=DEFAULT_API_BASE, help=f"API 基础 URL（默认 {DEFAULT_API_BASE}）")
    args = parser.parse_args()

    # Windows 环境强制 UTF-8 输出，避免 GBK 编码错误
    if sys.platform == "win32":
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")

    diag = EvolutionDiagnostic(api_base=args.api_base, task_id=args.task_id)

    try:
        diag.load_data()
    except requests.exceptions.ConnectionError:
        print(f"ERROR: 无法连接 API {args.api_base}，请确认 AIstock 后端正在运行。", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: 加载数据失败: {e}", file=sys.stderr)
        sys.exit(1)

    if not diag.loops:
        print(f"WARNING: 任务 {args.task_id} 没有任何 Loop 记录。", file=sys.stderr)

    if args.json:
        print(json.dumps(diag.generate_json(), ensure_ascii=False, indent=2))
    else:
        print(diag.generate_report())


if __name__ == "__main__":
    main()
