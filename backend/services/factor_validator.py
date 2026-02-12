import importlib.util
import os
import sys
import ast
import logging
from typing import Any

logger = logging.getLogger("aistock.factor_validator")

class FactorSecurityError(Exception):
    """当因子代码包含危险操作时抛出"""
    pass

class FactorValidationError(Exception):
    """当因子逻辑执行报错或结果异常时抛出"""
    pass

class FactorValidator:
    """因子代码审计与沙盒验证器
    
    功能：
    1. 静态检查：禁止危险的 Python 操作（os.system, eval 等）。
    2. 动态加载：将 RD-Agent 生成的 .py 文件安全加载。
    3. 逻辑验证：在少量历史数据上试运行，检查返回格式和 NaN 比例。
    """
    
    # 静态检查黑名单 (REQ-SEC-P3-005)
    FORBIDDEN_KEYWORDS = [
        "os.system", "os.popen", "subprocess", "eval", "exec", 
        "shutil", "builtins.__import__", "pickle.load", "pickle.loads",
        "requests.", "urllib.", "socket.", "open(", "write("
    ]

    # 禁止导入的库（os.path等安全操作除外）
    FORBIDDEN_IMPORTS = [
        "subprocess", "requests", "socket", "urllib", "shutil", 
        "pickle", "marshal", "shelve", "ftplib", "smtplib", "telnetlib"
    ]
    
    # 允许的os模块安全操作
    ALLOWED_OS_OPERATIONS = [
        "os.path", "os.environ.get", "os.getcwd"
    ]

    def __init__(self, plugins_dir: str = "backend/plugins/factors"):
        self.plugins_dir = plugins_dir
        if not os.path.exists(self.plugins_dir):
            os.makedirs(self.plugins_dir, exist_ok=True)

    def validate_and_load(self, factor_id: str, file_path: str) -> Any:
        """
        全流程验证并加载因子。
        返回因子实例或模块。
        """
        # 1. 静态检查
        self._static_audit(file_path)
        
        # 2. 动态加载
        module = self._dynamic_load(factor_id, file_path)
        
        # 3. 沙盒运行验证 (TODO: 对接 DSL 获取真实历史数据进行试运行)
        # self._sandbox_run_test(module)
        
        return module

    def _static_audit(self, file_path: str):
        """扫描代码是否存在越权操作 (REQ-SEC-P3-005)"""
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
            
        # 1. 关键词粗粒度扫描
        for kw in self.FORBIDDEN_KEYWORDS:
            if kw in content:
                raise FactorSecurityError(f"安全审计失败：因子代码包含危险关键词/调用 {kw}")
        
        # 2. AST 细粒度检查
        try:
            tree = ast.parse(content)
            for node in ast.walk(tree):
                # 检查 import
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name.split('.')[0] in self.FORBIDDEN_IMPORTS:
                            raise FactorSecurityError(f"安全审计失败：因子代码尝试导入受限库 {alias.name}")
                elif isinstance(node, ast.ImportFrom):
                    if node.module and node.module.split('.')[0] in self.FORBIDDEN_IMPORTS:
                        raise FactorSecurityError(f"安全审计失败：因子代码尝试从受限库导入 {node.module}")
                
                # 检查 Call (防止类似 getattr(os, 'system') 的绕过)
                if isinstance(node, ast.Call):
                    if isinstance(node.func, ast.Name):
                        if node.func.id in ["eval", "exec", "compile", "open"]:
                            raise FactorSecurityError(f"安全审计失败：非法函数调用 {node.func.id}")
                    elif isinstance(node.func, ast.Attribute):
                        # 递归检查属性调用，如 os.system
                        full_name = self._get_full_attr_name(node.func)
                        for forbidden in self.FORBIDDEN_KEYWORDS:
                            if forbidden in full_name:
                                raise FactorSecurityError(f"安全审计失败：非法属性调用 {full_name}")

        except SyntaxError as e:
            raise FactorValidationError(f"语法错误：因子代码无法解析 {e}")

    def _get_full_attr_name(self, node: ast.Attribute) -> str:
        """递归获取 AST 属性调用的全名"""
        parts = [node.attr]
        curr = node.value
        while isinstance(curr, ast.Attribute):
            parts.append(curr.attr)
            curr = curr.value
        if isinstance(curr, ast.Name):
            parts.append(curr.id)
        return ".".join(reversed(parts))

    def _dynamic_load(self, factor_id: str, file_path: str):
        """将物理文件动态映射为 Python 模块"""
        module_name = f"rd_factor_{factor_id}"
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        if spec is None or spec.loader is None:
            raise FactorValidationError(f"加载失败：无法创建模块规范 {file_path}")
            
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        factor_dir = os.path.dirname(os.path.abspath(str(file_path)))
        added_sys_path = False
        try:
            if factor_dir and factor_dir not in sys.path:
                sys.path.insert(0, factor_dir)
                added_sys_path = True
            spec.loader.exec_module(module)
        except Exception as e:
            raise FactorValidationError(f"运行失败：因子初始化阶段报错 {e}")
        finally:
            if added_sys_path:
                try:
                    if sys.path and sys.path[0] == factor_dir:
                        sys.path.pop(0)
                    elif factor_dir in sys.path:
                        sys.path.remove(factor_dir)
                except Exception:
                    pass
            
        return module

    def _sandbox_run_test(self, module: Any, test_symbol: str = "600519.SH"):
        """在历史数据上进行极简运行测试"""
        # TODO: 集成 DSL 获取最近 30 天数据
        # 验证因子是否包含必要的 compute 接口
        # 验证返回是否为 pd.Series 且长度匹配
        pass
