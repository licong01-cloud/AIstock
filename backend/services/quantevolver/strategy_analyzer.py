"""
QuantEvolver: StrategyAnalyzer（策略分析师）

功能：
1. Python语法检查：使用ast模块解析策略代码
2. LLM分析：调用LLM对策略代码进行语法和合理性分析
3. 参数提取：从策略代码中动态识别可修改参数
"""
from __future__ import annotations

import ast
import json
import logging
import os
import re
from typing import Any, Dict, List, Optional

logger = logging.getLogger("aistock.quantevolver.strategy_analyzer")


def _get_llm_client():
    """获取LLM客户端（复用RDAgent的litellm配置）。"""
    try:
        import litellm
        return litellm
    except ImportError:
        logger.warning("litellm未安装，LLM分析不可用")
        return None


class StrategyAnalyzer:
    """策略代码分析器"""

    def analyze(self, strategy_id: str, source_code: str) -> Dict[str, Any]:
        """分析策略代码：语法检查 + LLM分析。

        Returns:
            {
                "ok": True/False,
                "analysis": {
                    "syntax_ok": bool,
                    "syntax_errors": [...],
                    "params_detected": [...],
                    "llm_review": {
                        "syntax_issues": [...],
                        "logic_issues": [...],
                        "suggestions": [...],
                        "overall_rating": "A/B/C/D",
                        "summary": "..."
                    }
                }
            }
        """
        analysis = {
            "syntax_ok": False,
            "syntax_errors": [],
            "params_detected": [],
            "llm_review": None,
        }

        # 1. Python语法检查
        syntax_result = self._check_syntax(source_code)
        analysis["syntax_ok"] = syntax_result["ok"]
        analysis["syntax_errors"] = syntax_result.get("errors", [])

        # 2. 参数提取
        if syntax_result["ok"]:
            analysis["params_detected"] = self._extract_params(source_code)

        # 3. LLM分析
        llm_review = self._llm_analyze(strategy_id, source_code)
        if llm_review:
            analysis["llm_review"] = llm_review

        return {"ok": True, "strategy_id": strategy_id, "analysis": analysis}

    def _check_syntax(self, source_code: str) -> Dict[str, Any]:
        """使用ast模块检查Python语法。"""
        try:
            ast.parse(source_code)
            return {"ok": True}
        except SyntaxError as e:
            return {
                "ok": False,
                "errors": [{
                    "line": e.lineno,
                    "col": e.offset,
                    "message": str(e.msg),
                    "text": e.text.strip() if e.text else "",
                }],
            }

    def _extract_params(self, source_code: str) -> List[Dict[str, Any]]:
        """从策略代码的__init__方法中提取可修改参数。"""
        params = []
        try:
            tree = ast.parse(source_code)
            for node in ast.walk(tree):
                if isinstance(node, ast.ClassDef):
                    for item in node.body:
                        if isinstance(item, ast.FunctionDef) and item.name == "__init__":
                            params = self._parse_init_params(item)
                            break
        except Exception as e:
            logger.warning(f"参数提取失败: {e}")
        return params

    def _parse_init_params(self, func_def: ast.FunctionDef) -> List[Dict[str, Any]]:
        """解析__init__函数的参数定义。"""
        params = []
        args = func_def.args

        # 处理keyword-only参数（*后面的参数）
        for i, arg in enumerate(args.kwonlyargs):
            param_name = arg.arg
            # 跳过self, signal, kwargs等非业务参数
            if param_name in ("self", "signal", "model", "dataset",
                              "trade_exchange", "level_infra", "common_infra"):
                continue

            param_info = {"name": param_name, "type": "unknown", "default": None}

            # 获取默认值
            if i < len(args.kw_defaults) and args.kw_defaults[i] is not None:
                default_node = args.kw_defaults[i]
                param_info["default"] = self._ast_value(default_node)
                param_info["type"] = self._infer_type(param_info["default"])

            params.append(param_info)

        # 处理普通参数（排除self）
        defaults_offset = len(args.args) - len(args.defaults)
        for i, arg in enumerate(args.args):
            if arg.arg in ("self",):
                continue
            param_info = {"name": arg.arg, "type": "unknown", "default": None}
            default_idx = i - defaults_offset
            if default_idx >= 0 and default_idx < len(args.defaults):
                default_node = args.defaults[default_idx]
                param_info["default"] = self._ast_value(default_node)
                param_info["type"] = self._infer_type(param_info["default"])
            params.append(param_info)

        return params

    def _ast_value(self, node) -> Any:
        """从AST节点提取Python值。"""
        if isinstance(node, ast.Constant):
            return node.value
        if isinstance(node, ast.Name):
            if node.id == "True":
                return True
            if node.id == "False":
                return False
            if node.id == "None":
                return None
            return node.id
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
            val = self._ast_value(node.operand)
            if isinstance(val, (int, float)):
                return -val
        if isinstance(node, ast.List):
            return [self._ast_value(e) for e in node.elts]
        if isinstance(node, ast.Tuple):
            return tuple(self._ast_value(e) for e in node.elts)
        return str(ast.dump(node))

    def _infer_type(self, value: Any) -> str:
        """推断参数类型。"""
        if isinstance(value, bool):
            return "bool"
        if isinstance(value, int):
            return "int"
        if isinstance(value, float):
            return "float"
        if isinstance(value, str):
            return "str"
        if isinstance(value, list):
            return "list"
        return "unknown"

    def _llm_analyze(self, strategy_id: str, source_code: str) -> Optional[Dict[str, Any]]:
        """使用LLM分析策略代码。"""
        llm = _get_llm_client()
        if llm is None:
            return None

        from .prompt_manager import PromptManager, safe_format
        pm = PromptManager()
        prompt_data = pm.get_active_prompt_text("strategy_analyzer", "analyze_strategy")

        if prompt_data:
            system_prompt = prompt_data["system_prompt"]
            user_prompt = safe_format(prompt_data["user_prompt_template"], 
                strategy_id=strategy_id,
                source_code=source_code[:4000]
            )
        else:
            raise ValueError("未配置 strategy_analyzer/analyze_strategy 的提示词，拒绝使用兜底策略")

        try:
            from .llm_client import get_llm_kwargs
            kwargs = get_llm_kwargs("strategy_analyzer")
            
            response = llm.completion(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.3,
                max_tokens=1500,
                **kwargs
            )
            content = response.choices[0].message.content.strip()
            # 清理可能的markdown代码块
            if content.startswith("```"):
                content = re.sub(r"^```(?:json)?\s*", "", content)
                content = re.sub(r"\s*```$", "", content)
            result = json.loads(content)
            return result
        except Exception as e:
            logger.warning(f"LLM策略分析失败 ({strategy_id}): {e}")
            return {
                "syntax_issues": [],
                "logic_issues": [],
                "suggestions": [],
                "overall_rating": "N/A",
                "summary": f"LLM分析失败: {str(e)[:100]}",
            }
