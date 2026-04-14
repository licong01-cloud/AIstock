"""
因子代码规则转换器

核心转换策略：
1. 将模块级代码包装成 calculate_{factor_name}(instruments, start_date, end_date) 函数
2. 替换 pd.read_hdf('daily_pv.h5') -> _REALTIME_LOADER.load(...)
3. 替换 pd.read_parquet('static_factors.parquet') -> _STATIC_FACTORS_LOADER.load(...)
4. 替换 D.features() -> _REALTIME_LOADER.load(...)
5. 移除 result.h5 写入，改为 return result_df
6. 注入 _REALTIME_LOADER 导入头

严格禁止：
- 任何兜底方案（空值填充、except pass、返回空DataFrame等）
- 改变因子获取数据的逻辑
"""
from __future__ import annotations

import ast
import logging
import re
import textwrap
from dataclasses import dataclass, field
from typing import Any, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger("aistock.factor_code_transformer")


@dataclass
class TransformResult:
    """规则转换结果"""
    success: bool
    transformed_code: str
    original_code: str
    changes: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    error: Optional[str] = None


LOADER_IMPORT_HEADER = '''import sys
import os
import numpy as np
import pandas as pd

# ── 实时数据加载器（由AIstock因子改造系统注入）──
try:
    from backend.data_service.realtime_factor_data_loader import RealtimeFactorDataLoader as _RFDLoader
    _REALTIME_LOADER = _RFDLoader()
except ImportError:
    _backend_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    if _backend_path not in sys.path:
        sys.path.insert(0, _backend_path)
    from backend.data_service.realtime_factor_data_loader import RealtimeFactorDataLoader as _RFDLoader
    _REALTIME_LOADER = _RFDLoader()

# ── 静态因子数据加载器（替代 static_factors.parquet，由AIstock因子改造系统注入）──
# 注意：加载失败时直接抛出异常，禁止任何兜底方案
from backend.data_service.qe_data_service import build_static_factors as _build_static_factors
class _StaticFactorsLoader:
    def load(self, instruments, start_date, end_date, columns=None):
        df = _build_static_factors(instruments, start_date, end_date)
        if columns and not df.empty:
            available = [c for c in columns if c in df.columns]
            df = df[available] if available else df
        return df
_STATIC_FACTORS_LOADER = _StaticFactorsLoader()
'''


class FactorCodeTransformer:
    """因子代码规则转换器"""

    QLIB_FIELDS = [
        "open", "close", "high", "low", "volume", "amount", "factor",
        "vwap", "change", "pct_chg",
    ]

    def transform(self, original_code: str, factor_name: str) -> TransformResult:
        changes = []
        warnings = []
        try:
            code_type = self._detect_code_type(original_code)
            changes.append(f"检测到代码类型: {code_type}")
            if code_type == "unknown":
                return TransformResult(
                    success=False, transformed_code=original_code,
                    original_code=original_code, changes=changes, warnings=warnings,
                    error="无法识别因子代码类型，拒绝自动改造。"
                          "请人工检查代码结构后手动指定类型或手动改造。",
                )
            if code_type in ("rdagent_template", "rdagent_module_level"):
                code, sc, sw = self._transform_module_level_code(original_code, factor_name)
            elif code_type == "rdagent_factor":
                code, sc, sw = self._transform_function_code(original_code, factor_name)
            else:
                code, sc, sw = self._transform_qlib_style(original_code, factor_name)
            changes.extend(sc)
            warnings.extend(sw)
            if "_REALTIME_LOADER" not in code and "RealtimeFactorDataLoader" not in code:
                code = LOADER_IMPORT_HEADER + "\n" + code
                changes.append("注入 RealtimeFactorDataLoader 导入头")
            else:
                changes.append("数据加载器头部已存在，跳过注入")
            code = self._add_transformation_marker(code, factor_name)
            changes.append("添加改造标记注释")
            # 后处理：清理原始代码中残留的 try-except 兜底、空 DataFrame 返回、$ 前缀等违规模式
            code, sanitize_removals, sanitize_warnings = self.sanitize_llm_output(code)
            if sanitize_removals:
                changes.extend(sanitize_removals)
            if sanitize_warnings:
                warnings.extend(sanitize_warnings)
            # 后处理：修复 .rolling().corr() 产生非唯一 MultiIndex 的问题
            code, corr_fix = self._fix_rolling_corr_pattern(code, factor_name)
            if corr_fix:
                changes.append(corr_fix)
            # 后处理：修复 LLM 生成的 .groupby().apply(lambda...).swaplevel() 错误模式
            # groupby(level='instrument').apply(lambda g: ...) 会产生3层索引，
            # swaplevel() 需要替换为 droplevel(0) 才能得到正确的 (datetime, instrument)
            if '.swaplevel()' in code:
                swaplevel_fix_re = re.compile(r'\.swaplevel\(\)\.sort_index\(\)')
                if swaplevel_fix_re.search(code):
                    code = swaplevel_fix_re.sub('.droplevel(0).sort_index()', code)
                    changes.append("修复 LLM 错误: .swaplevel() -> .droplevel(0) (3层索引问题)")
            syntax_ok, syntax_error = self._check_syntax(code)
            if not syntax_ok:
                return TransformResult(
                    success=False, transformed_code=code, original_code=original_code,
                    changes=changes, warnings=warnings,
                    error=f"转换后代码语法错误: {syntax_error}",
                )
            return TransformResult(
                success=True, transformed_code=code, original_code=original_code,
                changes=changes, warnings=warnings,
            )
        except Exception as e:
            logger.exception(f"因子代码转换失败: factor_name={factor_name}")
            return TransformResult(
                success=False, transformed_code=original_code, original_code=original_code,
                changes=changes, warnings=warnings, error=str(e),
            )

    def _strip_comments_and_strings(self, code: str) -> str:
        """移除代码中的注释和字符串字面量，保留代码结构，避免注释内容干扰类型检测"""
        # 移除多行字符串
        result = re.sub(r'""".*?"""', '""', code, flags=re.DOTALL)
        result = re.sub(r"'''.*?'''", "''", result, flags=re.DOTALL)
        # 移除单行注释
        result = re.sub(r'#.*$', '', result, flags=re.MULTILINE)
        # 移除单行字符串（简化处理）
        result = re.sub(r'"[^"]*"', '""', result)
        result = re.sub(r"'[^']*'", "''", result)
        return result

    def _detect_code_type(self, code: str) -> str:
        """检测代码类型，排除注释和字符串中的干扰"""
        effective_code = self._strip_comments_and_strings(code)

        has_qlib = ("D.features" in effective_code
                    or "from qlib" in effective_code
                    or "import qlib" in effective_code)
        has_calc_func = bool(re.search(
            r"^def calculate_\w+\s*\(", effective_code, re.MULTILINE))
        # 用 raw code 检测文件名（文件名在字符串字面量中，strip 后会消失）
        has_h5 = ("result.h5" in code or "daily_pv.h5" in code
                  or "daily_basic.h5" in code or "sector_data.h5" in code)
        has_parquet = "static_factors.parquet" in code
        has_read_hdf = "read_hdf" in effective_code

        # 优先级：有明确函数定义且无qlib依赖 → rdagent_factor
        if has_calc_func and not has_qlib:
            return "rdagent_factor"
        if has_qlib:
            return "qlib_style"
        if has_h5 or has_parquet or has_read_hdf:
            return "rdagent_template"
        if "calculate_" in effective_code:
            return "rdagent_module_level"
        return "unknown"

    def _unwrap_compute_function(self, code: str, factor_name: str):
        """如果代码包含 compute_factor() / main() 等无参函数，提取其函数体作为主代码。
        同时处理模块级常量引用（FACTOR_NAME, DATA_DIR 等）。

        处理逻辑：
        1. 检测 def compute_factor(): 或 def main(): 模式
        2. 提取函数体（去一级缩进）
        3. 如果函数体引用 FACTOR_NAME，在顶部注入 FACTOR_NAME = "{factor_name}"
        4. 如果函数体引用 DATA_DIR（且不在 pd.read_hdf 参数中，因为后续会被替换），移除引用
        """
        match = re.search(r'^def (?:compute_factor|main)\s*\(\s*\)\s*:', code, re.MULTILINE)
        if not match:
            return code, []

        changes = []
        lines = code.split('\n')
        func_start_line = match.start()
        func_indent = len(match.group(0)) - len(match.group(0).lstrip())
        # 如果匹配行有缩进（嵌套函数），取其缩进级别
        match_line = code[:match.start()].count('\n')  # 行号
        actual_line = lines[match_line]
        func_indent = len(actual_line) - len(actual_line.lstrip())

        body_lines = []
        in_func = False
        for i, line in enumerate(lines):
            if i == match_line:
                in_func = True
                continue
            if in_func:
                if line.strip() == '':
                    body_lines.append('')
                    continue
                cur_indent = len(line) - len(line.lstrip())
                if cur_indent <= func_indent and line.strip():
                    # End of function body (dedented to same or lower level)
                    break
                # Remove one level of indentation (4 spaces or 1 tab)
                indent_to_remove = 4
                if line.startswith(' ' * (func_indent + indent_to_remove)):
                    body_lines.append(line[indent_to_remove:])
                elif line.startswith('\t') and func_indent == 0:
                    body_lines.append(line[1:])
                else:
                    body_lines.append(line)

        # Remove trailing empty lines
        while body_lines and body_lines[-1].strip() == '':
            body_lines.pop()

        if not body_lines:
            return code, []

        extracted = '\n'.join(body_lines)

        # 如果提取的代码引用了 FACTOR_NAME，注入常量定义
        if 'FACTOR_NAME' in extracted:
            extracted = f'FACTOR_NAME = "{factor_name}"\n{extracted}'
            changes.append(f"注入 FACTOR_NAME = \"{factor_name}\" 常量")

        func_name = actual_line.strip().split('(')[0].replace('def ', '')
        changes.append(f"提取 {func_name}() 函数体作为主代码（{len(body_lines)} 行）")
        return extracted, changes

    def _transform_module_level_code(self, code: str, factor_name: str):
        """将模块级代码包装成标准函数（解决 unexpected indent 问题）"""
        changes = []
        warnings = []
        lines = code.split("\n")
        import_lines = []
        body_lines = []
        body_started = False
        in_docstring = False
        docstring_char = None
        for line in lines:
            stripped = line.strip()
            if not in_docstring:
                if stripped.startswith('"""') or stripped.startswith("'''"):
                    char = stripped[:3]
                    if stripped.count(char) >= 2 and len(stripped) > 3:
                        body_lines.append(line)
                        body_started = True
                        continue
                    in_docstring = True
                    docstring_char = char
                    body_lines.append(line)
                    body_started = True
                    continue
                if not body_started and (stripped.startswith("import ") or stripped.startswith("from ")):
                    import_lines.append(line)
                    continue
                if stripped:
                    body_started = True
            else:
                body_lines.append(line)
                if docstring_char and docstring_char in stripped and stripped != docstring_char:
                    in_docstring = False
                continue
            body_lines.append(line)
        filtered_imports = []
        for line in import_lines:
            stripped = line.strip()
            if any(p in stripped for p in ["qlib", "from qlib", "import qlib"]):
                filtered_imports.append(f"# [TRANSFORMED] {line}  # qlib import removed")
                changes.append(f"移除qlib import: {stripped[:60]}")
            elif stripped in ("import numpy as np", "import pandas as pd", "import sys", "import os",
                              "from pathlib import Path"):
                pass
            else:
                filtered_imports.append(line)
        body_code = "\n".join(body_lines)

        # Step0: 如果body包含 compute_factor()/main() 等无参函数，先提取其函数体
        # 必须在 dedent 之前执行，否则函数体的缩进会被去除导致无法区分
        body_code, unwrap_changes = self._unwrap_compute_function(body_code, factor_name)
        changes.extend(unwrap_changes)
        unwrapped = bool(unwrap_changes)

        # Step1: dedent 去掉原始模板代码的公共缩进
        # 如果已经通过 unwrap 提取了函数体，跳过 dedent（函数体已有正确的相对缩进）
        if not unwrapped:
            raw_lines = body_code.split("\n")
            min_indent = float("inf")
            for bl in raw_lines:
                stripped_bl = bl.lstrip()
                if stripped_bl and not stripped_bl.startswith("#"):
                    indent_len = len(bl) - len(stripped_bl)
                    if indent_len > 0:
                        min_indent = min(min_indent, indent_len)
            if min_indent == float("inf") or min_indent == 0:
                min_indent = 0
            dedented_lines = []
            for bl in raw_lines:
                if bl.strip() == "":
                    dedented_lines.append("")
                elif min_indent > 0 and len(bl) >= min_indent and bl[:min_indent].strip() == "":
                    dedented_lines.append(bl[min_indent:])
                else:
                    dedented_lines.append(bl)
            body_code = "\n".join(dedented_lines)

        # Step2: 在 dedent 后的代码上做数据替换（此时代码是0缩进的模块级代码）
        body_code, lc, lw = self._replace_data_loads(body_code, factor_name)
        changes.extend(lc)
        warnings.extend(lw)
        body_code, rc = self._replace_h5_write_with_return(body_code, factor_name)
        changes.extend(rc)
        # 清理 print(f"{FACTOR_NAME}: ...") 行（改造后不再需要调试输出）
        body_code = re.sub(
            r'^[ \t]*print\s*\(\s*f?["\'].*?FACTOR_NAME.*?["\'].*?\)\s*$',
            "",
            body_code, flags=re.MULTILINE,
        )
        # 清理 result = result[np.isfinite(result[FACTOR_NAME])] 和类似布尔索引行
        body_code = re.sub(
            r'^[ \t]*\w+\s*=\s*\w+\[np\.isfinite\([^)]+\)\]\s*$',
            "",
            body_code, flags=re.MULTILINE,
        )
        # 替换 FACTOR_NAME → 实际因子名字符串
        body_code = re.sub(r'\bFACTOR_NAME\b', f'"{factor_name}"', body_code)
        body_code = re.sub(
            r'if\s+__name__\s*==\s*[\'"]__main__[\'"]\s*:.*',
            "# [TRANSFORMED] __main__ block removed",
            body_code, flags=re.DOTALL,
        )

        # Step3: 统一加4格缩进，包装成函数体
        indented_body = textwrap.indent(body_code.strip(), "    ")
        func_code = (
            f"def calculate_{factor_name}(instruments: list, start_date: str, end_date: str) -> pd.DataFrame:\n"
            f'    """计算因子 {factor_name} 的值。\n\n'
            f"    Args:\n"
            f"        instruments: 股票代码列表，如 ['000001.SZ', '600000.SH']\n"
            f"        start_date: 开始日期字符串，如 '2024-01-01'\n"
            f"        end_date: 结束日期字符串，如 '2024-01-31'\n\n"
            f"    Returns:\n"
            f"        MultiIndex(datetime, instrument) 的 DataFrame，列名为因子名\n"
            f'    """\n'
            f"{indented_body}\n"
        )
        changes.append(f"将模块级代码包装为 calculate_{factor_name}() 函数")
        import_section = "\n".join(filtered_imports) if filtered_imports else ""
        final_code = (import_section + "\n\n" + func_code).strip()
        return final_code, changes, warnings

    def _transform_function_code(self, code: str, factor_name: str):
        changes = []
        warnings = []
        new_lines = []
        for line in code.split("\n"):
            stripped = line.strip()
            if any(p in stripped for p in ["from qlib", "import qlib"]):
                new_lines.append(f"# [TRANSFORMED] {line}  # qlib import removed")
                changes.append(f"移除qlib import: {stripped[:60]}")
            else:
                new_lines.append(line)
        code = "\n".join(new_lines)
        code, lc, lw = self._replace_data_loads(code, factor_name)
        changes.extend(lc)
        warnings.extend(lw)
        code, rc = self._replace_h5_write_with_return(code, factor_name)
        changes.extend(rc)
        code, sc = self._fix_function_signature(code, factor_name)
        changes.extend(sc)
        return code, changes, warnings

    def _transform_qlib_style(self, code: str, factor_name: str):
        changes = []
        warnings = []
        new_lines = []
        for line in code.split("\n"):
            stripped = line.strip()
            if any(p in stripped for p in ["from qlib", "import qlib"]):
                new_lines.append(f"# [TRANSFORMED] {line}  # qlib import removed")
                changes.append(f"移除qlib import: {stripped[:60]}")
            else:
                new_lines.append(line)
        code = "\n".join(new_lines)
        code, dc, dw = self._replace_d_features(code)
        changes.extend(dc)
        warnings.extend(dw)
        code, lc, lw = self._replace_data_loads(code, factor_name)
        changes.extend(lc)
        warnings.extend(lw)
        code, rc = self._replace_h5_write_with_return(code, factor_name)
        changes.extend(rc)
        if not re.search(r"^def calculate_\w+\s*\(", code, re.MULTILINE):
            code, wc, ww = self._transform_module_level_code(code, factor_name)
            changes.extend(wc)
            warnings.extend(ww)
        return code, changes, warnings

    # ── 数据加载替换 ──────────────────────────────────────────────────

    def _replace_data_loads(self, code: str, factor_name: str):
        """替换各种数据文件读取为实时加载器调用

        支持的文件类型：
        - daily_pv.h5 → _REALTIME_LOADER（OHLCV 行情数据）
        - daily_basic.h5 → _STATIC_FACTORS_LOADER（每日基本面，db_* 前缀列）
        - sector_data.h5 → _STATIC_FACTORS_LOADER（申万行业板块，sw2_* 前缀列）
        - static_factors.parquet → _STATIC_FACTORS_LOADER（综合静态因子）
        """
        changes = []
        warnings = []
        new_code = code

        # ── 替换 pd.read_hdf('daily_pv.h5', ...) → _REALTIME_LOADER.load(...) ──
        # 支持两种路径写法：
        #   1. 字符串字面量: pd.read_hdf('daily_pv.h5') 或 pd.read_hdf('./daily_pv.h5')
        #   2. Python 表达式: pd.read_hdf(DATA_DIR / "daily_pv.h5")
        daily_pv_re = re.compile(
            r"^([ \t]*)(\w+)\s*=\s*pd\.read_hdf\s*\([^)]*daily_pv\.h5[^)]*\)"
            r"(?:\s*\.sort_index\(\))?",
            re.DOTALL | re.MULTILINE,
        )
        def _replace_daily_pv(m: re.Match) -> str:
            indent = m.group(1)
            var = m.group(2)
            changes.append(f"替换 pd.read_hdf(.*daily_pv.h5) -> _REALTIME_LOADER.load() (变量: {var})")
            lines = [
                f"{var} = _REALTIME_LOADER.load(",
                f"    instruments=instruments,",
                f"    start_date=start_date,",
                f"    end_date=end_date,",
                f'    fields=["open", "close", "high", "low", "volume", "amount", "factor"],',
                f'    adjust="qfq",',
                f")",
                f"{var} = {var}.sort_index()",
            ]
            return "\n".join(indent + ln for ln in lines)
        new_code = daily_pv_re.sub(_replace_daily_pv, new_code)

        # ── 替换 pd.read_hdf('daily_basic.h5') → _STATIC_FACTORS_LOADER.load(...) ──
        daily_basic_re = re.compile(
            r"^([ \t]*)(\w+)\s*=\s*pd\.read_hdf\s*\([^)]*daily_basic\.h5[^)]*\)"
            r"(?:\s*\.sort_index\(\))?",
            re.DOTALL | re.MULTILINE,
        )
        def _replace_daily_basic(m: re.Match) -> str:
            indent = m.group(1)
            var = m.group(2)
            changes.append(f"替换 pd.read_hdf(.*daily_basic.h5) -> _STATIC_FACTORS_LOADER.load() (变量: {var})")
            lines = [
                f"# [TRANSFORMED] daily_basic.h5 -> _STATIC_FACTORS_LOADER.load()",
                f"{var} = _STATIC_FACTORS_LOADER.load(",
                f"    instruments=instruments,",
                f"    start_date=start_date,",
                f"    end_date=end_date,",
                f")",
                f"{var} = {var}.sort_index()",
            ]
            return "\n".join(indent + ln for ln in lines)
        new_code = daily_basic_re.sub(_replace_daily_basic, new_code)

        # ── 替换 pd.read_hdf('sector_data.h5') → _STATIC_FACTORS_LOADER.load(...) ──
        sector_data_re = re.compile(
            r"^([ \t]*)(\w+)\s*=\s*pd\.read_hdf\s*\([^)]*sector_data\.h5[^)]*\)"
            r"(?:\s*\.sort_index\(\))?",
            re.DOTALL | re.MULTILINE,
        )
        def _replace_sector_data(m: re.Match) -> str:
            indent = m.group(1)
            var = m.group(2)
            changes.append(f"替换 pd.read_hdf(.*sector_data.h5) -> _STATIC_FACTORS_LOADER.load() (变量: {var})")
            lines = [
                f"# [TRANSFORMED] sector_data.h5 -> _STATIC_FACTORS_LOADER.load()",
                f"{var} = _STATIC_FACTORS_LOADER.load(",
                f"    instruments=instruments,",
                f"    start_date=start_date,",
                f"    end_date=end_date,",
                f")",
                f"{var} = {var}.sort_index()",
            ]
            return "\n".join(indent + ln for ln in lines)
        new_code = sector_data_re.sub(_replace_sector_data, new_code)

        # ── 替换 pd.read_hdf('margin_detail.h5') → _STATIC_FACTORS_LOADER.load(...) ──
        margin_detail_re = re.compile(
            r"^([ \t]*)(\w+)\s*=\s*pd\.read_hdf\s*\([^)]*margin_detail\.h5[^)]*\)"
            r"(?:\s*\.sort_index\(\))?",
            re.DOTALL | re.MULTILINE,
        )
        def _replace_margin_detail(m: re.Match) -> str:
            indent = m.group(1)
            var = m.group(2)
            changes.append(f"替换 pd.read_hdf(.*margin_detail.h5) -> _STATIC_FACTORS_LOADER.load() (变量: {var})")
            lines = [
                f"# [TRANSFORMED] margin_detail.h5 -> _STATIC_FACTORS_LOADER.load()",
                f"{var} = _STATIC_FACTORS_LOADER.load(",
                f"    instruments=instruments,",
                f"    start_date=start_date,",
                f"    end_date=end_date,",
                f")",
                f"{var} = {var}.sort_index()",
            ]
            return "\n".join(indent + ln for ln in lines)
        new_code = margin_detail_re.sub(_replace_margin_detail, new_code)

        # ── 替换 pd.read_hdf('moneyflow.h5') → _STATIC_FACTORS_LOADER.load(...) ──
        moneyflow_h5_re = re.compile(
            r"^([ \t]*)(\w+)\s*=\s*pd\.read_hdf\s*\([^)]*moneyflow\.h5[^)]*\)"
            r"(?:\s*\.sort_index\(\))?",
            re.DOTALL | re.MULTILINE,
        )
        def _replace_moneyflow_h5(m: re.Match) -> str:
            indent = m.group(1)
            var = m.group(2)
            changes.append(f"替换 pd.read_hdf(.*moneyflow.h5) -> _STATIC_FACTORS_LOADER.load() (变量: {var})")
            lines = [
                f"# [TRANSFORMED] moneyflow.h5 -> _STATIC_FACTORS_LOADER.load()",
                f"{var} = _STATIC_FACTORS_LOADER.load(",
                f"    instruments=instruments,",
                f"    start_date=start_date,",
                f"    end_date=end_date,",
                f")",
                f"{var} = {var}.sort_index()",
            ]
            return "\n".join(indent + ln for ln in lines)
        new_code = moneyflow_h5_re.sub(_replace_moneyflow_h5, new_code)

        # ── 替换 pd.read_hdf('cyq_perf.h5') → _STATIC_FACTORS_LOADER.load(...) ──
        cyq_perf_h5_re = re.compile(
            r"^([ \t]*)(\w+)\s*=\s*pd\.read_hdf\s*\([^)]*cyq_perf\.h5[^)]*\)"
            r"(?:\s*\.sort_index\(\))?",
            re.DOTALL | re.MULTILINE,
        )
        def _replace_cyq_perf_h5(m: re.Match) -> str:
            indent = m.group(1)
            var = m.group(2)
            changes.append(f"替换 pd.read_hdf(.*cyq_perf.h5) -> _STATIC_FACTORS_LOADER.load() (变量: {var})")
            lines = [
                f"# [TRANSFORMED] cyq_perf.h5 -> _STATIC_FACTORS_LOADER.load()",
                f"{var} = _STATIC_FACTORS_LOADER.load(",
                f"    instruments=instruments,",
                f"    start_date=start_date,",
                f"    end_date=end_date,",
                f")",
                f"{var} = {var}.sort_index()",
            ]
            return "\n".join(indent + ln for ln in lines)
        new_code = cyq_perf_h5_re.sub(_replace_cyq_perf_h5, new_code)

        # ── 替换 pd.read_parquet('static_factors.parquet') → _STATIC_FACTORS_LOADER.load(...) ──
        # 支持两种形式：columns=[...] 和 columns=var_name，支持 ./static_factors.parquet
        static_parquet_re = re.compile(
            r"^([ \t]*)(\w+)\s*=\s*pd\.read_parquet\s*\(\s*['\"](?:\./)?static_factors\.parquet['\"]"
            r"(?:\s*,\s*columns\s*=\s*(\[[^\]]+\]|\w+))?"
            r"[^)]*\)(?:\s*\.sort_index\(\))?",
            re.DOTALL | re.MULTILINE,
        )
        def _replace_static_parquet(m: re.Match) -> str:
            indent = m.group(1)
            var = m.group(2)
            cols_expr = m.group(3) if m.group(3) else "[]"
            changes.append(f"替换 pd.read_parquet('static_factors.parquet') -> _STATIC_FACTORS_LOADER.load() (变量: {var})")
            lines = [
                f"# [TRANSFORMED] static_factors.parquet -> _STATIC_FACTORS_LOADER.load()",
                f"_static_cols = {cols_expr}",
                f"{var} = _STATIC_FACTORS_LOADER.load(",
                f"    instruments=instruments,",
                f"    start_date=start_date,",
                f"    end_date=end_date,",
                f"    columns=_static_cols if _static_cols else None,",
                f")",
                f"{var} = {var}.sort_index()",
            ]
            return "\n".join(indent + l for l in lines)
        new_code = static_parquet_re.sub(_replace_static_parquet, new_code)

        # ── 替换 result_df.to_hdf('result.h5', ...) 或表达式路径 → 注释掉 ──
        # 使用 [\s\S]*? 匹配任意字符（包括换行），处理 Path(__file__).resolve().parent 等复杂路径
        h5_write_re = re.compile(
            r"(\w+)\.to_hdf\s*\([\s\S]*?result\.h5[\s\S]*?\)",
        )
        if h5_write_re.search(new_code):
            new_code = h5_write_re.sub(
                r"# [TRANSFORMED] \1.to_hdf(result.h5) removed",
                new_code,
            )
            changes.append("移除 result.h5 写入")

        # 替换 qlib.init()
        qlib_init_re = re.compile(r"qlib\.init\s*\([^)]*\)")
        if qlib_init_re.search(new_code):
            new_code = qlib_init_re.sub(
                "# [TRANSFORMED] qlib.init() removed",
                new_code,
            )
            changes.append("移除 qlib.init() 调用")

        # 检查是否还有未处理的文件读取（包括 DATA_DIR / "xxx.h5" 和字符串路径）
        remaining_re = re.compile(
            r"pd\.read_(?:hdf|parquet|csv|feather)\s*\(\s*(?:['\"][^'\"]+['\"]|DATA_DIR\b)"
        )
        remaining = remaining_re.findall(new_code)
        for r in remaining:
            warnings.append(f"发现未处理的文件读取: {r[:60]}，需要LLM辅助处理")

        # 替换 $ 前缀列名：改造后数据接口返回的列名无 $ 前缀
        # 覆盖所有字符串上下文：df['$close']、fields=['$close']、"$close" 等
        dollar_fields = ["open", "close", "high", "low", "volume", "amount", "factor", "vwap"]
        for field in dollar_fields:
            # 匹配引号内的 $field（单引号或双引号）
            pattern = re.compile(rf"""(?<=['"])\${field}(?=['",\]\)])""")
            if pattern.search(new_code):
                new_code = pattern.sub(field, new_code)
                changes.append(f"替换列名 ${field} -> {field}")

        # ── 清理 DATA_DIR / Path(__file__) 残留引用 ──
        # 移除模块级 DATA_DIR = Path(__file__).resolve().parent.parent 定义
        data_dir_def_re = re.compile(
            r"^[ \t]*DATA_DIR\s*=\s*Path\s*\(\s*__file__\s*\)\s*\.resolve\(\)\s*\.parent(?:\s*\.parent)?\s*$",
            re.MULTILINE,
        )
        if data_dir_def_re.search(new_code):
            new_code = data_dir_def_re.sub("# [TRANSFORMED] DATA_DIR removed (data loaded via loaders)", new_code)
            changes.append("移除 DATA_DIR = Path(__file__).resolve().parent.parent 定义")

        # 移除 FACTOR_NAME = "xxx" 定义（如果存在且已被注入替代）
        factor_name_def_re = re.compile(
            r'^[ \t]*FACTOR_NAME\s*=\s*["\'][^"\']+["\']\s*$',
            re.MULTILINE,
        )
        if factor_name_def_re.search(new_code):
            new_code = factor_name_def_re.sub("", new_code)
            changes.append("移除 FACTOR_NAME 常量定义（已由参数替代）")

        # 清理 from pathlib import Path（如果不再使用 Path）
        pathlib_import_re = re.compile(r"^from pathlib import Path\s*$", re.MULTILINE)
        if pathlib_import_re.search(new_code):
            code_without_import = pathlib_import_re.sub("", new_code)
            if "Path(" not in code_without_import and "Path(" not in code_without_import:
                new_code = code_without_import
                changes.append("移除 from pathlib import Path（不再需要）")

        return new_code, changes, warnings

    def sanitize_llm_output(self, code: str) -> Tuple[str, List[str], List[str]]:
        """
        后处理：检测并移除 LLM 修复中常见的违规兜底模式。
        返回 (cleaned_code, removals, warnings)。
        在 LLM 每次返回代码后调用。
        """
        removals: List[str] = []
        warnings: List[str] = []
        new_code = code

        # 1. 移除 except Exception 后返回空 DataFrame 的兜底
        #    匹配: except Exception...: \n ... = pd.DataFrame()
        pat_except_empty_df = re.compile(
            r"^([ \t]*)except\s+(?:Exception|BaseException).*?:\s*\n"
            r"(?:\1[ \t]+[^\n]*\n)*?"       # 可能有多行
            r"\1[ \t]+\w+\s*=\s*pd\.DataFrame\(\)\s*$",
            re.MULTILINE,
        )
        if pat_except_empty_df.search(new_code):
            new_code = pat_except_empty_df.sub(
                lambda m: m.group(1) + "except Exception as _e:\n"
                + m.group(1) + "    raise  # 禁止兜底，必须暴露异常",
                new_code,
            )
            removals.append("移除 except→空DataFrame 兜底，改为 raise")

        # 2. 移除 if df.empty: return pd.DataFrame() 兜底
        pat_empty_return = re.compile(
            r"^([ \t]*)if\s+\w+\.empty\s*:\s*\n"
            r"\1[ \t]+return\s+pd\.DataFrame\(\)\s*$",
            re.MULTILINE,
        )
        if pat_empty_return.search(new_code):
            new_code = pat_empty_return.sub("", new_code)
            removals.append("移除 if empty→return空DataFrame 兜底")

        # 2b. 单行形式: if df.empty: return pd.DataFrame()
        pat_empty_return_oneline = re.compile(
            r"^[ \t]*if\s+\w+\.empty\s*:\s*return\s+pd\.DataFrame\(\)\s*$",
            re.MULTILINE,
        )
        if pat_empty_return_oneline.search(new_code):
            new_code = pat_empty_return_oneline.sub("", new_code)
            removals.append("移除单行 if empty→return空DataFrame 兜底")

        # 3. 检测先填充 NaN 再检查缺失列的矛盾逻辑（仅警告）
        if re.search(r"float\(['\"]nan['\"]\)", new_code):
            if re.search(r"raise\s+ValueError.*[Mm]issing", new_code):
                warnings.append(
                    "检测到矛盾逻辑：先用NaN填充缺失列，又检查缺失列抛异常。"
                    "NaN填充会导致检查永远不触发。"
                )

        # 4. 通用 try-except 解包：保留 try 体（去缩进），移除 except 块
        new_code = self._unwrap_try_except(new_code, removals)

        # 5. 移除 os.path.exists 文件检查（LLM 常添加的错误安全检查）
        #    匹配: import os \n ... \n if not os.path.exists("static_factors.parquet"): \n raise ...
        pat_os_path_block = re.compile(
            r"^[ \t]*(?:import\s+os\s*\n)?"               # 可选的 import os
            r"([ \t]*)if\s+not\s+os\.path\.(?:exists|isfile)\s*\("
            r"\s*['\"](?:\./)?"
            r"(?:static_factors\.parquet|daily_pv\.h5|result\.h5)['\"]"
            r"\s*\)\s*:\s*\n"
            r"(?:\1[ \t]+[^\n]*\n)*?"                      # if 体（raise 等）
            r"(?:\1[ \t]+[^\n]*)\n?",                      # if 体最后一行
            re.MULTILINE,
        )
        if pat_os_path_block.search(new_code):
            count = len(pat_os_path_block.findall(new_code))
            new_code = pat_os_path_block.sub("", new_code)
            removals.append(f"移除 {count} 个 os.path.exists 文件检查（改造后代码不依赖本地文件）")

        # 5b. 清理孤立的 import os（仅当代码中不再有其他 os. 引用时）
        if re.search(r"^\s*import\s+os\s*$", new_code, re.MULTILINE):
            # 检查除 import os 行外是否还有 os. 引用
            code_without_import = re.sub(r"^\s*import\s+os\s*$", "", new_code, flags=re.MULTILINE)
            if not re.search(r"\bos\.", code_without_import):
                new_code = re.sub(r"^\s*import\s+os\s*\n", "", new_code, flags=re.MULTILINE)
                removals.append("移除不再需要的 import os")

        # 6. 检测 $ 前缀残留（LLM 可能重新引入）
        dollar_fields = ["open", "close", "high", "low", "volume", "amount", "factor"]
        for field in dollar_fields:
            pat = re.compile(rf"""(?<=['"])\${field}(?=['",\]\)])""")
            if pat.search(new_code):
                new_code = pat.sub(field, new_code)
                removals.append(f"清理 LLM 重新引入的 ${field} 前缀")

        # 7. 清理多余空行（连续超过2个空行合并为2个）
        new_code = re.sub(r"\n{4,}", "\n\n\n", new_code)

        return new_code, removals, warnings

    def _unwrap_try_except(self, code: str, removals: List[str]) -> str:
        """解包所有 try-except 块：保留 try 体（去一级缩进），移除 except 块。"""
        lines = code.split("\n")
        result = []
        i = 0
        unwrap_count = 0
        while i < len(lines):
            stripped = lines[i].lstrip()
            # 检测 try: 行
            if stripped == "try:" or stripped.startswith("try:  ") or stripped.startswith("try: #"):
                try_indent = len(lines[i]) - len(lines[i].lstrip())
                try_indent_str = lines[i][:try_indent]
                body_lines = []
                i += 1
                # 收集 try 体
                while i < len(lines):
                    if lines[i].strip() == "":
                        body_lines.append("")
                        i += 1
                        continue
                    cur_indent = len(lines[i]) - len(lines[i].lstrip())
                    cur_stripped = lines[i].lstrip()
                    # except/finally 同级 → try 体结束
                    if cur_indent == try_indent and (
                        cur_stripped.startswith("except") or cur_stripped.startswith("finally")
                    ):
                        break
                    body_lines.append(lines[i])
                    i += 1
                # 跳除 except/finally 块（可能有多条 except 分支）
                while i < len(lines):
                    cur_stripped = lines[i].lstrip()
                    cur_indent = len(lines[i]) - len(lines[i].lstrip())
                    if cur_indent == try_indent and (
                        cur_stripped.startswith("except") or cur_stripped.startswith("finally")
                    ):
                        i += 1  # 跳过 except/finally 行本身
                        while i < len(lines):
                            if lines[i].strip() == "":
                                i += 1
                                continue
                            inner_indent = len(lines[i]) - len(lines[i].lstrip())
                            if inner_indent <= try_indent:
                                break
                            i += 1
                    else:
                        break
                # 将 try 体去一级缩进后写入结果
                for bl in body_lines:
                    if bl == "":
                        result.append("")
                    elif bl.startswith(try_indent_str + "    "):
                        result.append(try_indent_str + bl[try_indent + 4:])
                    elif bl.startswith(try_indent_str + "\t"):
                        result.append(try_indent_str + bl[try_indent + 1:])
                    else:
                        result.append(bl)
                unwrap_count += 1
            else:
                result.append(lines[i])
                i += 1
        if unwrap_count > 0:
            removals.append(f"解包 {unwrap_count} 个 try-except 块（因子代码禁止 try-except）")
        return "\n".join(result)

    def _fix_rolling_corr_pattern(self, code: str, factor_name: str) -> Tuple[str, Optional[str]]:
        """修复 .groupby().rolling().corr(other_series).droplevel(0) 导致非唯一 MultiIndex 的问题。

        pandas 的 .groupby().rolling().corr() 会产生 MultiIndex(instrument, datetime)，
        而 .droplevel(0) 后只剩 datetime 索引，导致非唯一索引错误。

        修复方式：替换为 groupby(level='instrument').apply(lambda g: ...) 模式，
        在 apply 内部直接使用 g[col1].rolling().corr(g[col2])，返回 (datetime, instrument) MultiIndex。
        """
        # 匹配模式: df['col1'].groupby(level='instrument').rolling(window=N, ...).corr(df['col2']).droplevel(0)
        corr_pattern = re.compile(
            r"([ \t]*)(\w+)\s*=\s*\(\s*\n?"
            r"\s*(\w+)\['([^']+)'\]\s*\n?"
            r"\s*\.groupby\s*\(\s*level\s*=\s*['\"]instrument['\"]\s*\)\s*\n?"
            r"\s*\.rolling\s*\(\s*window\s*=\s*(\d+)\s*(?:,\s*min_periods\s*=\s*\d+)?\s*\)\s*\n?"
            r"\s*\.corr\s*\(\s*(\w+)\['([^']+)'\]\s*\)\s*\n?"
            r"\s*\.droplevel\s*\(\s*0\s*\)\s*\n?"
            r"\s*\)",
            re.MULTILINE,
        )
        m = corr_pattern.search(code)
        if not m:
            # 尝试单行模式: series = df['col1'].groupby(level='instrument').rolling(N).corr(df['col2']).droplevel(0)
            corr_oneline = re.compile(
                r"([ \t]*)(\w+)\s*=\s*\(?(\w+)\['([^']+)'\]"
                r"\.groupby\s*\(\s*level\s*=\s*['\"]instrument['\"]\s*\)"
                r"\.rolling\s*\(\s*window\s*=\s*(\d+)\s*(?:,\s*min_periods\s*=\s*(\d+))?\s*\)"
                r"\.corr\s*\(\s*(\w+)\['([^']+)'\]\s*\)"
                r"\.droplevel\s*\(\s*0\s*\)\)?",
            )
            m = corr_oneline.search(code)
            if not m:
                return code, None
            indent = m.group(1)
            var = m.group(2)
            df1 = m.group(3)
            col1 = m.group(4)
            window = m.group(5)
            min_periods = m.group(6) or window
            df2 = m.group(7)
            col2 = m.group(8)
        else:
            indent = m.group(1)
            var = m.group(2)
            df1 = m.group(3)
            col1 = m.group(4)
            window = m.group(5)
            min_periods = window
            df2 = m.group(6)
            col2 = m.group(7)

        # 生成替换代码：使用 groupby.apply 在每组内部计算 rolling corr
        # 关键：使用 swaplevel().sort_index() 替代 droplevel(0) 避免 datetime 索引不唯一
        if df1 == df2:
            # 同一个 DataFrame，使用 lambda g: 模式
            fixed = (
                f"{indent}# [FIXED] rolling().corr() -> groupby.apply() + swaplevel 避免非唯一索引\n"
                f"{indent}{var} = {df1}.groupby(level='instrument').apply(\n"
                f"{indent}    lambda g: g['{col1}'].rolling({window}, min_periods={min_periods}).corr(g['{col2}'])\n"
                f"{indent}).swaplevel().sort_index()"
            )
        else:
            # 不同 DataFrame（不常见，但需要处理）
            fixed = (
                f"{indent}# [FIXED] rolling().corr() -> groupby.apply() + swaplevel 避免非唯一索引\n"
                f"{indent}{var} = {df1}.groupby(level='instrument').apply(\n"
                f"{indent}    lambda g: g['{col1}'].rolling({window}, min_periods={min_periods})\n"
                f"{indent}    .corr({df2}.loc[g.index, '{col2}'])\n"
                f"{indent}).swaplevel().sort_index()"
            )

        code = code[:m.start()] + fixed + code[m.end():]
        return code, f"修复 .rolling().corr() 非唯一索引问题 (变量: {var}, window={window})"

    def _replace_h5_write_with_return(self, code: str, factor_name: str):
        """确保函数末尾有 return 语句，自动识别结果变量名"""
        changes = []

        if "return " in code:
            return code, changes  # 已有 return，不处理

        result_var = None

        # 优先从被注释的 to_hdf 中提取变量名
        commented_h5 = re.search(
            r"# \[TRANSFORMED\] (\w+)\.to_hdf",
            code,
        )
        if commented_h5:
            result_var = commented_h5.group(1)

        # 常见结果变量名
        if not result_var:
            for candidate in [
                "result_df", "result", "df_result",
                "factor_df", "final_df", "output_df",
            ]:
                if candidate in code:
                    result_var = candidate
                    break

        # 最后手段：查找最后一个 DataFrame 赋值语句的变量名
        if not result_var:
            last_assign = re.findall(
                r"^[ \t]*(\w+)\s*=\s*", code, re.MULTILINE,
            )
            if last_assign:
                result_var = last_assign[-1]

        if result_var:
            code = code.rstrip() + f"\nreturn {result_var}\n"
            changes.append(f"添加 return {result_var} 语句")
        else:
            changes.append(
                "警告：未能识别结果变量名，无法自动添加 return 语句，需要LLM辅助处理"
            )

        return code, changes

    def _fix_function_signature(self, code: str, factor_name: str):
        """修复函数签名，确保参数为 (instruments, start_date, end_date)"""
        changes = []
        # 匹配 def calculate_xxx() 或 def calculate_xxx(instruments, start_time, end_time)
        sig_re = re.compile(
            r"(def\s+calculate_\w+\s*\()([^)]*?)(\)\s*(?:->.*?)?:)",
            re.DOTALL,
        )
        def _fix_sig(m: re.Match) -> str:
            params = m.group(2).strip()
            if "instruments" in params and "start_date" in params and "end_date" in params:
                return m.group(0)
            new_params = "instruments: list, start_date: str, end_date: str"
            changes.append(f"修复函数签名: ({params}) -> ({new_params})")
            return m.group(1) + new_params + m.group(3)
        code = sig_re.sub(_fix_sig, code)
        # 替换参数名 start_time -> start_date, end_time -> end_date
        code = re.sub(r"\bstart_time\b", "start_date", code)
        code = re.sub(r"\bend_time\b", "end_date", code)
        return code, changes

    def _replace_d_features(self, code: str):
        """替换 D.features() 调用为 _REALTIME_LOADER.load()"""
        changes = []
        warnings = []
        pattern = re.compile(r"D\.features\s*\(([^)]+)\)", re.DOTALL)
        def _replace(m: re.Match) -> str:
            args = m.group(1).replace("\n", " ").strip()
            args = re.sub(r"\bstart_time\b", "start_date", args)
            args = re.sub(r"\bend_time\b", "end_date", args)
            changes.append("替换 D.features() -> _REALTIME_LOADER.load()")
            return f"_REALTIME_LOADER.load({args})"
        new_code = pattern.sub(_replace, code)
        if "D.features" in new_code:
            warnings.append("警告：仍存在未替换的 D.features 调用，需要LLM辅助处理")
        return new_code, changes, warnings

    def _add_transformation_marker(self, code: str, factor_name: str) -> str:
        from datetime import datetime
        marker = (
            f"# ============================================================\n"
            f"# [AISTOCK FACTOR TRANSFORMATION]\n"
            f"# Factor: {factor_name}\n"
            f"# Transformed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"# Original: RDAgent generated code (QLib H5 dependency)\n"
            f"# Transformed: Direct database access via RealtimeFactorDataLoader\n"
            f"# ============================================================\n"
        )
        return marker + code

    def _check_syntax(self, code: str) -> Tuple[bool, Optional[str]]:
        try:
            ast.parse(code)
            return True, None
        except SyntaxError as e:
            return False, f"SyntaxError at line {e.lineno}: {e.msg}"
        except Exception as e:
            return False, str(e)

    def compile_test(self, code: str) -> Tuple[bool, Optional[str]]:
        return self._check_syntax(code)

    def execution_test(
        self,
        code: str,
        factor_name: str,
        test_instruments: Optional[List[str]] = None,
        test_start_date: str = "2024-01-01",
        test_end_date: str = "2024-01-31",
        timeout_seconds: int = 600,
    ) -> Tuple[bool, Optional[str], Optional[Any]]:
        """
        执行测试：在沙箱环境中运行转换后的因子代码，验证能否正常执行。
        增加超时控制，防止死循环或极慢计算阻塞服务。

        Args:
            code: 转换后的因子代码
            factor_name: 因子名称
            test_instruments: 测试用股票列表，默认使用少量样本
            test_start_date: 测试开始日期
            test_end_date: 测试结束日期
            timeout_seconds: 执行超时秒数，默认300秒

        Returns:
            (success, error_message, result_sample)
        """
        import threading

        if test_instruments is None:
            test_instruments = ["000001.SZ", "600000.SH", "000002.SZ"]

        # 构建执行环境，注入必要的全局变量
        # 加载器初始化失败时直接抛出异常，禁止兜底为None
        import numpy as np
        from backend.data_service.realtime_factor_data_loader import RealtimeFactorDataLoader as _RFDLoader
        _loader_instance = _RFDLoader()

        from backend.data_service.qe_data_service import build_static_factors as _build_sf
        class _StaticFactorsLoader:
            def load(self, instruments, start_date, end_date, columns=None):
                df = _build_sf(instruments, start_date, end_date)
                if columns and not df.empty:
                    available = [c for c in columns if c in df.columns]
                    df = df[available] if available else df
                return df
        _static_loader_instance = _StaticFactorsLoader()

        exec_globals = {
            "__name__": "__factor_test__",
            "__builtins__": __builtins__,
            "pd": pd,
            "np": np,
            "_REALTIME_LOADER": _loader_instance,
            "_STATIC_FACTORS_LOADER": _static_loader_instance,
        }

        # 使用线程+超时控制执行，防止死循环阻塞
        container = {"result": None, "error": None}

        def _run_factor():
            try:
                compiled = compile(code, f"<factor_{factor_name}>", "exec")
                exec(compiled, exec_globals)

                calc_func_name = f"calculate_{factor_name}"
                if calc_func_name not in exec_globals:
                    calc_funcs = [k for k in exec_globals if k.startswith("calculate_")]
                    if not calc_funcs:
                        container["error"] = f"未找到 calculate_{factor_name} 函数"
                        return
                    calc_func_name = calc_funcs[0]

                calc_func = exec_globals[calc_func_name]
                container["result"] = calc_func(
                    instruments=test_instruments,
                    start_date=test_start_date,
                    end_date=test_end_date,
                )
            except Exception as e:
                import traceback
                container["error"] = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"

        thread = threading.Thread(target=_run_factor, daemon=True)
        thread.start()
        thread.join(timeout=timeout_seconds)

        if thread.is_alive():
            logger.warning(f"因子执行超时(>{timeout_seconds}秒): {factor_name}")
            return False, (
                f"因子执行超时（>{timeout_seconds}秒），可能存在死循环或计算量过大"
            ), None

        if container["error"]:
            return False, container["error"], None

        result = container["result"]

        if result is None:
            return False, "因子函数返回 None", None

        if isinstance(result, pd.DataFrame):
            if result.empty:
                return False, "因子函数返回空 DataFrame", None

            # 严格校验：禁止全 NaN 数据作为成功结果
            non_nan_cols = [c for c in result.columns if c != 'instrument' and c != 'datetime']
            if not non_nan_cols:
                return False, "因子函数返回的 DataFrame 没有有效的数据列", None

            if result[non_nan_cols].isna().all().all():
                return False, (
                    "因子计算结果全部为 NaN（可能是因为滚动窗口/预热期大于测试数据范围，"
                    "或者是计算逻辑存在除以零等错误）。请修复计算逻辑，或者确保测试数据"
                    "能够满足预热需求（禁止使用 fillna 兜底）。"
                ), None

            return True, None, result

        return True, None, result
