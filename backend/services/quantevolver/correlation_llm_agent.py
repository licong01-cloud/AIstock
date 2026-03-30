# -*- coding: utf-8 -*-
"""因子相关性 LLM 分析 Agent

通过读取两个因子的源代码、指标和描述，调用 LLM 从金融学、数学和代码角度
分析高相关性的成因，并给出保留/剔除建议。

Agent 配置（模型选择、提示词）通过 /quantevolver/prompts 页面管理。
"""
import logging
import os
from typing import Any, Dict, List, Optional

import litellm

from ...db.pg_pool import get_conn
from .llm_client import get_llm_kwargs
from .prompt_manager import PromptManager, safe_format

logger = logging.getLogger("aistock.quantevolver.correlation_llm_agent")

AGENT_TYPE = "correlation_analysis"
PROMPT_KEY = "analyze_pair"
BATCH_PROMPT_KEY = "analyze_batch"

# 项目根目录（backend/services/quantevolver/ → 上 3 层）
_PROJECT_ROOT = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..")
)


class CorrelationLLMAgent:
    """因子相关性 LLM 分析 Agent。"""

    def analyze_pair(self, factor_a: str, factor_b: str) -> Dict[str, Any]:
        """分析因子对相关性，返回 LLM 分析结果。

        Raises:
            RuntimeError: 提示词未配置或 LLM 调用失败
        """
        # 1. 加载两因子上下文
        context_a = self._load_factor_context(factor_a)
        context_b = self._load_factor_context(factor_b)

        # 2. 加载已计算的相关性
        corr_info = self._load_correlation(factor_a, factor_b)

        # 3. 获取提示词
        pm = PromptManager()
        prompt_data = pm.get_active_prompt_text(AGENT_TYPE, PROMPT_KEY)
        if not prompt_data:
            raise RuntimeError(
                f"提示词 {AGENT_TYPE}/{PROMPT_KEY} 未配置或未激活，"
                "请先在 /quantevolver/prompts 页面创建并激活该提示词"
            )

        # 4. 格式化 user prompt
        user_prompt = safe_format(
            prompt_data["user_prompt_template"],
            factor_a_name=factor_a,
            factor_a_description=context_a.get("description") or "无",
            factor_a_code=context_a.get("source_code") or "源代码不可用",
            factor_a_ic=self._fmt(context_a.get("ic_mean")),
            factor_a_sharpe=self._fmt(context_a.get("top_sharpe")),
            factor_a_return=self._fmt(context_a.get("top_annual_return")),
            factor_a_drawdown=self._fmt(context_a.get("top_max_drawdown")),
            factor_a_grade=context_a.get("grade") or "N/A",
            factor_a_category=context_a.get("category") or "N/A",
            factor_b_name=factor_b,
            factor_b_description=context_b.get("description") or "无",
            factor_b_code=context_b.get("source_code") or "源代码不可用",
            factor_b_ic=self._fmt(context_b.get("ic_mean")),
            factor_b_sharpe=self._fmt(context_b.get("top_sharpe")),
            factor_b_return=self._fmt(context_b.get("top_annual_return")),
            factor_b_drawdown=self._fmt(context_b.get("top_max_drawdown")),
            factor_b_grade=context_b.get("grade") or "N/A",
            factor_b_category=context_b.get("category") or "N/A",
            correlation_value=self._fmt(corr_info.get("correlation")),
            window_days=corr_info.get("window_days", "N/A"),
        )

        # 5. 调用 LLM
        kwargs = get_llm_kwargs(AGENT_TYPE)
        logger.info(
            f"调用 LLM 分析因子对 [{factor_a}] vs [{factor_b}], model={kwargs.get('model')}"
        )

        response = litellm.completion(
            messages=[
                {"role": "system", "content": prompt_data["system_prompt"]},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.3,
            max_tokens=2048,
            **kwargs,
        )
        content = response.choices[0].message.content

        return {
            "analysis": content,
            "model": kwargs.get("model", "unknown"),
            "factor_a": factor_a,
            "factor_b": factor_b,
        }

    def analyze_batch(
        self, base_factor: str, compare_factors: List[str]
    ) -> Dict[str, Any]:
        """批量分析基准因子与多个比较因子的相关性，返回每个因子的保留/删除建议。

        Args:
            base_factor: 基准因子名称
            compare_factors: 要比较的因子名称列表（最多 15 个）

        Returns:
            包含每个因子分析结果的字典
        """
        # 1. 加载基准因子上下文
        base_ctx = self._load_factor_context(base_factor)

        # 2. 加载所有比较因子上下文 + 相关性（缓存，避免截断时重复加载）
        compare_data: List[Dict[str, Any]] = []
        for cf in compare_factors:
            ctx = self._load_factor_context(cf)
            corr_info = self._load_correlation(base_factor, cf)
            compare_data.append({"name": cf, "ctx": ctx, "corr_info": corr_info})

        def _build_compare_sections(
            data_list: List[Dict[str, Any]], max_code_chars: int = 0
        ) -> List[str]:
            sections = []
            for item in data_list:
                cf = item["name"]
                ctx = item["ctx"]
                corr_info = item["corr_info"]
                code = ctx.get("source_code") or "源代码不可用"
                if max_code_chars > 0 and len(code) > max_code_chars:
                    code = code[:max_code_chars] + "\n# ... [代码已截断] ..."
                sections.append(
                    f"### 因子: {cf}\n"
                    f"- 相关系数: {self._fmt(corr_info.get('correlation'))}\n"
                    f"- IC均值: {self._fmt(ctx.get('ic_mean'))} | RankIC: {self._fmt(ctx.get('rank_ic_mean'))} "
                    f"| ICIR: {self._fmt(ctx.get('icir'))} | RankICIR: {self._fmt(ctx.get('rank_icir'))}\n"
                    f"- Sharpe: {self._fmt(ctx.get('top_sharpe'))} "
                    f"| 年化收益: {self._fmt(ctx.get('top_annual_return'))} "
                    f"| 最大回撤: {self._fmt(ctx.get('top_max_drawdown'))} "
                    f"| IC正比: {self._fmt(ctx.get('ic_positive_ratio'))}\n"
                    f"- 评级: {ctx.get('grade') or 'N/A'} | 分类: {ctx.get('category') or 'N/A'}\n"
                    f"- 描述: {ctx.get('description') or '无'}\n"
                    f"```python\n{code}\n```"
                )
            return sections

        compare_sections = _build_compare_sections(compare_data)

        # 3. 构建 prompt — 尝试从 DB 加载自定义 prompt，否则使用内置默认
        pm = PromptManager()
        prompt_data = pm.get_active_prompt_text(AGENT_TYPE, BATCH_PROMPT_KEY)

        def _build_prompt(sections: List[str]):
            sections_text = "\n\n".join(sections)
            if prompt_data:
                up = safe_format(
                    prompt_data["user_prompt_template"],
                    base_factor_name=base_factor,
                    base_factor_code=base_ctx.get("source_code") or "源代码不可用",
                    base_factor_ic=self._fmt(base_ctx.get("ic_mean")),
                    base_factor_sharpe=self._fmt(base_ctx.get("top_sharpe")),
                    base_factor_return=self._fmt(base_ctx.get("top_annual_return")),
                    base_factor_drawdown=self._fmt(base_ctx.get("top_max_drawdown")),
                    base_factor_grade=base_ctx.get("grade") or "N/A",
                    base_factor_category=base_ctx.get("category") or "N/A",
                    base_factor_description=base_ctx.get("description") or "无",
                    compare_factors_section=sections_text,
                    compare_count=str(len(compare_factors)),
                )
                sp = prompt_data["system_prompt"]
            else:
                sp = (
                    "你是量化因子分析专家。分析基准因子与多个比较因子的关系，"
                    "判断哪些是冗余因子应该删除。从金融逻辑、代码实现、指标表现三个维度分析。\n\n"
                    "特别注意：当两个因子相关系数 ≥ 0.9 时，必须进行源代码级深度对比分析：\n"
                    "1. 逐行比较核心计算逻辑（数据变换、窗口函数、聚合方式）\n"
                    "2. 指出代码的具体差异行及其对因子值的影响\n"
                    "3. 分析差异是否具有经济学意义（如不同的滞后期、不同的标准化方式）\n"
                    "4. 如果差异仅为变量名/注释/格式差异，明确指出属于代码克隆"
                )
                up = (
                    f"## 基准因子: {base_factor}\n"
                    f"- IC均值: {self._fmt(base_ctx.get('ic_mean'))} | RankIC: {self._fmt(base_ctx.get('rank_ic_mean'))} "
                    f"| ICIR: {self._fmt(base_ctx.get('icir'))} | RankICIR: {self._fmt(base_ctx.get('rank_icir'))}\n"
                    f"- Sharpe: {self._fmt(base_ctx.get('top_sharpe'))} "
                    f"| 年化收益: {self._fmt(base_ctx.get('top_annual_return'))} "
                    f"| 最大回撤: {self._fmt(base_ctx.get('top_max_drawdown'))} "
                    f"| IC正比: {self._fmt(base_ctx.get('ic_positive_ratio'))}\n"
                    f"- 评级: {base_ctx.get('grade') or 'N/A'} | 分类: {base_ctx.get('category') or 'N/A'}\n"
                    f"- 描述: {base_ctx.get('description') or '无'}\n"
                    f"```python\n{base_ctx.get('source_code') or '源代码不可用'}\n```\n\n"
                    f"## 比较因子（共 {len(compare_factors)} 个）\n\n"
                    + sections_text
                    + "\n\n## 请求\n"
                    "请对**所有因子**（包括基准因子自身）逐个分析，给出保留或删除建议。\n"
                    "注意：基准因子不享有特殊地位，如果某个比较因子在指标和代码质量上全面优于基准，"
                    "应建议删除基准因子并保留更优的比较因子。\n\n"
                    "对每个因子，输出格式：\n"
                    f"### {base_factor}（基准）\n"
                    "- **建议**: DELETE / KEEP / MANUAL（需人工判断）\n"
                    "- **独立指标对比**: IC均值/ICIR/Sharpe 的优劣对比\n"
                    "- **源代码分析**: \n"
                    "  - 核心逻辑差异（引用具体代码行或函数）\n"
                    "  - 差异是否影响因子经济含义\n"
                    "  - NaN处理/边界条件/数据清洗的质量差异\n"
                    "- **结论**: 综合理由（2-5句）\n\n"
                    "### [比较因子名]\n"
                    "- **建议**: DELETE / KEEP / MANUAL（需人工判断）\n"
                    "- **独立指标对比**: IC均值/ICIR/Sharpe 的优劣对比\n"
                    "- **源代码分析**: \n"
                    "  - 核心逻辑差异（引用具体代码行或函数）\n"
                    "  - 差异是否影响因子经济含义\n"
                    "  - NaN处理/边界条件/数据清洗的质量差异\n"
                    "- **结论**: 综合理由（2-5句）\n\n"
                    "判断标准：\n"
                    "1. 代码实质相同（仅变量名/注释不同）→ DELETE 指标差的那个\n"
                    "2. 代码有NaN处理等质量差异 → 保留质量更好的\n"
                    "3. 某因子独立指标全面劣于另一因子 → DELETE 劣势方（即使是基准）\n"
                    "4. 各有优劣（如IC强但Sharpe弱）→ MANUAL\n"
                    "5. 指标相近但代码有创新性差异（不同窗口、不同变换）→ KEEP\n"
                    "6. 相关系数≥0.9 且代码核心逻辑等价 → 强烈建议 DELETE 指标差的\n"
                    "7. 相关系数≥0.9 但代码逻辑有实质差异 → MANUAL 并详述差异"
                )
            return sp, up

        system_prompt, user_prompt = _build_prompt(compare_sections)

        # 4. Token 安全检查: 粗估 input tokens（1 token ≈ 3.5 chars for 中英混合）
        total_input_chars = len(system_prompt) + len(user_prompt)
        estimated_tokens = total_input_chars / 3.5
        max_tokens_output = 8192

        if estimated_tokens > 120_000:
            # 超过 120K tokens，按比例截断每个因子代码，重新构建 prompt
            logger.warning(
                f"批量分析 input 估算 {estimated_tokens:.0f} tokens 超过 120K，"
                "将截断因子代码"
            )
            max_code_chars = int(80_000 / max(len(compare_factors), 1))
            truncated_sections = _build_compare_sections(
                compare_data, max_code_chars=max_code_chars
            )
            system_prompt, user_prompt = _build_prompt(truncated_sections)

        # 5. 调用 LLM
        kwargs = get_llm_kwargs(AGENT_TYPE)
        logger.info(
            f"调用 LLM 批量分析 [{base_factor}] vs {compare_factors}, "
            f"model={kwargs.get('model')}, estimated_input_tokens={estimated_tokens:.0f}"
        )

        response = litellm.completion(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.3,
            max_tokens=max_tokens_output,
            **kwargs,
        )
        content = response.choices[0].message.content

        # 6. 解析每个因子的建议（从 LLM 输出中提取，含基准因子）
        all_factors = [base_factor] + list(compare_factors)

        # 定位每个因子在输出中的 **标题位置**（### factor_name 或 ## factor_name）
        # 优先匹配标题格式，避免因子名在其他段落被引用时误匹配
        import re

        factor_positions: List[tuple] = []  # (position, factor_name)
        used_positions: set = set()

        for cf in all_factors:
            best_pos = -1
            # 1) 优先: markdown 标题 "### factor_name" / "## factor_name"
            for pattern in [
                re.compile(r"#{2,3}\s+" + re.escape(cf)),
                re.compile(r"#{2,3}\s+.*?" + re.escape(cf)),
            ]:
                m = pattern.search(content)
                if m and m.start() not in used_positions:
                    best_pos = m.start()
                    break
            # 2) 回退: 裸名匹配（从后往前找，因为标题通常在后面的推荐区）
            if best_pos < 0:
                # 找所有出现位置，取最后一个未被占用的
                start = 0
                positions = []
                while True:
                    idx = content.find(cf, start)
                    if idx < 0:
                        break
                    positions.append(idx)
                    start = idx + 1
                # 优先选标题行的位置（行首带 # 的）
                for p in positions:
                    line_start = content.rfind("\n", 0, p)
                    line_prefix = content[line_start + 1:p].strip() if line_start >= 0 else content[:p].strip()
                    if line_prefix.startswith("#") and p not in used_positions:
                        best_pos = p
                        break
                if best_pos < 0 and positions:
                    best_pos = positions[-1]  # 最后出现的位置

            if best_pos >= 0:
                factor_positions.append((best_pos, cf))
                used_positions.add(best_pos)

        factor_positions.sort(key=lambda x: x[0])

        recommendations = {}
        for i, (pos, cf) in enumerate(factor_positions):
            # 截取到下一个因子位置或文本末尾
            next_pos = factor_positions[i + 1][0] if i + 1 < len(factor_positions) else len(content)
            section = content[pos:next_pos]
            section_upper = section.upper()
            rec = "MANUAL"
            # 优先匹配格式化的建议标记
            if "**: DELETE" in section_upper or "**建议**: DELETE" in section_upper or "** DELETE" in section_upper:
                rec = "DELETE"
            elif "**: KEEP" in section_upper or "**建议**: KEEP" in section_upper or "** KEEP" in section_upper:
                rec = "KEEP"
            elif "**: MANUAL" in section_upper or "**建议**: MANUAL" in section_upper:
                rec = "MANUAL"
            # 回退: 关键词匹配（仅在前 300 字符内搜索，避免跨段落误匹配）
            else:
                short = section[:300]
                short_upper = short.upper()
                if "DELETE" in short_upper or "建议删除" in short or "应删除" in short:
                    rec = "DELETE"
                elif "KEEP" in short_upper or "建议保留" in short or "应保留" in short:
                    rec = "KEEP"
            recommendations[cf] = rec

        # 确保所有因子都有结果（含基准）
        for cf in all_factors:
            if cf not in recommendations:
                recommendations[cf] = "MANUAL"

        return {
            "analysis": content,
            "model": kwargs.get("model", "unknown"),
            "base_factor": base_factor,
            "compare_factors": compare_factors,
            "recommendations": recommendations,
        }

    # ── 内部方法 ──

    def _load_factor_context(self, factor_name: str) -> Dict[str, Any]:
        """从 DB + 文件系统加载因子上下文。

        读取策略: realtime_code_text (DB) → qe_code_path (文件) → asset_path (文件)
        """
        result: Dict[str, Any] = {}
        code_text: Optional[str] = None
        asset_path: Optional[str] = None
        qe_code_path: Optional[str] = None

        with get_conn() as conn:
            with conn.cursor() as cur:
                # catalog: 源代码路径
                cur.execute(
                    """
                    SELECT realtime_code_text, asset_path, qe_code_path
                    FROM aistock_factor_catalog
                    WHERE factor_name = %s LIMIT 1
                    """,
                    (factor_name,),
                )
                row = cur.fetchone()
                if row:
                    code_text = row[0]
                    asset_path = row[1]
                    qe_code_path = row[2]

                # 独立指标 from aistock_factor_metrics
                cur.execute(
                    """
                    SELECT ic_mean, rank_ic_mean, icir, rank_icir,
                           top_sharpe, top_excess_sharpe,
                           top_annual_return, top_excess_annual_return,
                           top_max_drawdown, ic_positive_ratio
                    FROM aistock_factor_metrics
                    WHERE factor_name = %s AND eval_window = 'full'
                    ORDER BY calculated_at DESC LIMIT 1
                    """,
                    (factor_name,),
                )
                mrow = cur.fetchone()
                if mrow:
                    result["ic_mean"] = mrow[0]
                    result["rank_ic_mean"] = mrow[1]
                    result["icir"] = mrow[2]
                    result["rank_icir"] = mrow[3]
                    result["top_sharpe"] = mrow[4]
                    result["top_excess_sharpe"] = mrow[5]
                    result["top_annual_return"] = mrow[6]
                    result["top_excess_annual_return"] = mrow[7]
                    result["top_max_drawdown"] = mrow[8]
                    result["ic_positive_ratio"] = mrow[9]

                # classification: 描述
                cur.execute(
                    """
                    SELECT description, category, grade
                    FROM qe_factor_classification
                    WHERE factor_name = %s LIMIT 1
                    """,
                    (factor_name,),
                )
                cl = cur.fetchone()
                if cl:
                    result["description"] = cl[0] or ""
                    result["category"] = cl[1]
                    result["grade"] = cl[2]

        # 源代码: DB 优先 → 文件系统回退
        source_code = None
        if code_text and code_text.strip():
            source_code = code_text
        else:
            for path_field in [qe_code_path, asset_path]:
                if path_field:
                    full_path = self._resolve_path(path_field)
                    if full_path and os.path.isfile(full_path):
                        try:
                            with open(full_path, "r", encoding="utf-8") as f:
                                source_code = f.read()
                        except Exception as e:
                            logger.warning(f"读取因子源代码失败 [{path_field}]: {e}")
                        break

        result["source_code"] = source_code or ""
        return result

    def _resolve_path(self, rel_path: str) -> Optional[str]:
        """将相对路径解析为绝对路径（相对于项目根目录）。"""
        full = os.path.join(_PROJECT_ROOT, rel_path)
        return full if os.path.exists(full) else None

    def _load_correlation(self, fa: str, fb: str) -> Dict[str, Any]:
        """查询已计算的相关性。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT ON (factor_name) id
                    FROM aistock_factor_catalog WHERE factor_name = %s
                    """,
                    (fa,),
                )
                r1 = cur.fetchone()
                cur.execute(
                    """
                    SELECT DISTINCT ON (factor_name) id
                    FROM aistock_factor_catalog WHERE factor_name = %s
                    """,
                    (fb,),
                )
                r2 = cur.fetchone()
                if r1 and r2:
                    a_id, b_id = min(r1[0], r2[0]), max(r1[0], r2[0])
                    cur.execute(
                        """
                        SELECT correlation, data_window_days
                        FROM qe_factor_correlations
                        WHERE factor_a_id = %s AND factor_b_id = %s
                        """,
                        (a_id, b_id),
                    )
                    row = cur.fetchone()
                    if row:
                        return {"correlation": float(row[0]), "window_days": row[1]}
        return {}

    @staticmethod
    def _fmt(v: Any) -> str:
        """格式化数值，None 返回 'N/A'。"""
        if v is None:
            return "N/A"
        if isinstance(v, float):
            return f"{v:.4f}"
        return str(v)
