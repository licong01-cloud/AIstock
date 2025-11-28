"""
调试日志工具 - 为所有操作添加时间戳和详细信息
"""

import sys
import traceback
from datetime import datetime
from typing import Any, Optional
import json


class DebugLogger:
    """统一的调试日志工具"""
    
    def __init__(self, enable_debug: bool = True):
        """
        初始化调试日志器
        
        Args:
            enable_debug: 是否启用调试模式
        """
        self.enable_debug = enable_debug
        self.log_file = "debug.log"
    
    def _get_timestamp(self) -> str:
        """获取当前时间戳"""
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    
    def _format_message(self, level: str, message: str, **kwargs) -> str:
        """
        格式化日志消息
        
        Args:
            level: 日志级别 (INFO/DEBUG/WARNING/ERROR)
            message: 日志消息
            **kwargs: 额外的上下文信息
        """
        timestamp = self._get_timestamp()
        base_msg = f"[{timestamp}] [{level}] {message}"
        
        if kwargs:
            # 添加上下文信息
            context = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
            base_msg += f" | {context}"
        
        return base_msg
    
    def info(self, message: str, **kwargs):
        """记录INFO级别日志"""
        msg = self._format_message("INFO", message, **kwargs)
        print(msg)
        self._write_to_file(msg)
    
    def debug(self, message: str, **kwargs):
        """记录DEBUG级别日志"""
        if self.enable_debug:
            msg = self._format_message("DEBUG", message, **kwargs)
            print(msg)
            self._write_to_file(msg)
    
    def warning(self, message: str, **kwargs):
        """记录WARNING级别日志"""
        msg = self._format_message("WARNING", message, **kwargs)
        try:
            print(f"⚠️ {msg}")
        except UnicodeEncodeError:
            print(f"[WARNING] {msg}")
        self._write_to_file(msg)
    
    def error(self, message: str, error: Optional[Exception] = None, **kwargs):
        """
        记录ERROR级别日志
        
        Args:
            message: 错误消息
            error: 异常对象（可选）
            **kwargs: 额外上下文
        """
        msg = self._format_message("ERROR", message, **kwargs)
        try:
            print(f"❌ {msg}", file=sys.stderr)
        except UnicodeEncodeError:
            print(f"[ERROR] {msg}", file=sys.stderr)
        
        if error:
            # 添加异常详情
            error_details = f"  Exception Type: {type(error).__name__}"
            error_details += f"\n  Exception Message: {str(error)}"
            print(error_details, file=sys.stderr)
            msg += f"\n{error_details}"
            
            # 添加堆栈跟踪
            tb = traceback.format_exc()
            print(f"  Traceback:\n{tb}", file=sys.stderr)
            msg += f"\n  Traceback:\n{tb}"
        
        self._write_to_file(msg)
    
    def function_call(self, func_name: str, args: tuple = (), kwargs: dict = None):
        """
        记录函数调用
        
        Args:
            func_name: 函数名称
            args: 位置参数
            kwargs: 关键字参数
        """
        if not self.enable_debug:
            return
        
        kwargs = kwargs or {}
        args_str = ", ".join([repr(arg) for arg in args])
        kwargs_str = ", ".join([f"{k}={repr(v)}" for k, v in kwargs.items()])
        
        params = ", ".join(filter(None, [args_str, kwargs_str]))
        msg = self._format_message("CALL", f"{func_name}({params})")
        print(msg)
        self._write_to_file(msg)
    
    def function_return(self, func_name: str, result: Any, elapsed_time: Optional[float] = None):
        """
        记录函数返回
        
        Args:
            func_name: 函数名称
            result: 返回值
            elapsed_time: 执行时间（秒）
        """
        if not self.enable_debug:
            return
        
        # 简化返回值显示
        if isinstance(result, dict):
            result_str = f"dict with {len(result)} keys: {list(result.keys())[:5]}"
        elif isinstance(result, list):
            result_str = f"list with {len(result)} items"
        elif hasattr(result, '__len__'):
            try:
                result_str = f"{type(result).__name__} with length {len(result)}"
            except:
                result_str = str(type(result).__name__)
        else:
            result_str = repr(result)[:100]
        
        kwargs = {"result": result_str}
        if elapsed_time:
            kwargs["elapsed"] = f"{elapsed_time:.3f}s"
        
        msg = self._format_message("RETURN", func_name, **kwargs)
        print(msg)
        self._write_to_file(msg)
    
    def data_info(self, data_name: str, data: Any):
        """
        记录数据信息
        
        Args:
            data_name: 数据名称
            data: 数据对象
        """
        if not self.enable_debug:
            return
        
        info = {
            "name": data_name,
            "type": type(data).__name__
        }
        
        # 根据数据类型添加更多信息
        if data is None:
            info["value"] = "None"
        elif isinstance(data, dict):
            info["keys"] = list(data.keys())[:10]
            info["length"] = len(data)
        elif isinstance(data, (list, tuple)):
            info["length"] = len(data)
            if len(data) > 0:
                info["first_item_type"] = type(data[0]).__name__
        elif hasattr(data, 'shape'):  # pandas DataFrame/Series
            info["shape"] = str(data.shape)
        elif hasattr(data, '__len__'):
            try:
                info["length"] = len(data)
            except:
                pass
        
        msg = self._format_message("DATA", f"Data info for {data_name}", **info)
        print(msg)
        self._write_to_file(msg)
    
    def step(self, step_num: int, description: str, **kwargs):
        """
        记录处理步骤
        
        Args:
            step_num: 步骤编号
            description: 步骤描述
            **kwargs: 额外信息
        """
        msg = self._format_message("STEP", f"Step {step_num}: {description}", **kwargs)
        print(f"\n{'='*80}")
        print(msg)
        print('='*80)
        self._write_to_file(msg)
    
    def _write_to_file(self, message: str):
        """将日志写入文件"""
        try:
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(message + '\n')
        except:
            pass  # 忽略文件写入错误


# 全局日志器实例
debug_logger = DebugLogger(enable_debug=True)


def safe_index(lst: list, item: Any, default: int = 0) -> int:
    """
    安全的index操作，避免ValueError
    
    Args:
        lst: 列表
        item: 要查找的元素
        default: 默认值（当元素不存在时）
    
    Returns:
        元素索引，如果不存在返回default
    """
    try:
        if isinstance(lst, list):
            return lst.index(item)
        else:
            debug_logger.error(
                f"safe_index called with non-list type",
                error=TypeError(f"Expected list, got {type(lst).__name__}")
            )
            return default
    except ValueError:
        debug_logger.warning(
            f"Item not found in list",
            item=item,
            list_items=str(lst),
            returning=default
        )
        return default
    except Exception as e:
        debug_logger.error(
            f"Unexpected error in safe_index",
            error=e,
            lst_type=type(lst).__name__,
            item=item
        )
        return default


def log_exception(func):
    """装饰器：自动记录函数异常"""
    def wrapper(*args, **kwargs):
        func_name = func.__name__
        debug_logger.function_call(func_name, args, kwargs)
        
        import time
        start_time = time.time()
        
        try:
            result = func(*args, **kwargs)
            elapsed_time = time.time() - start_time
            debug_logger.function_return(func_name, result, elapsed_time)
            return result
        except Exception as e:
            elapsed_time = time.time() - start_time
            debug_logger.error(
                f"Exception in {func_name}",
                error=e,
                elapsed=f"{elapsed_time:.3f}s"
            )
            raise
    
    return wrapper


if __name__ == "__main__":
    # 测试日志器
    logger = DebugLogger()
    
    logger.info("This is an info message", module="test")
    logger.debug("This is a debug message", value=123)
    logger.warning("This is a warning", reason="test")
    
    try:
        raise ValueError("Test error")
    except Exception as e:
        logger.error("Test error occurred", error=e, context="testing")
    
    logger.data_info("test_dict", {"a": 1, "b": 2, "c": 3})
    logger.data_info("test_list", [1, 2, 3, 4, 5])
    
    logger.step(1, "Initialize system")
    
    # 测试 safe_index
    result = safe_index(["a", "b", "c"], "b")
    print(f"\nTest safe_index result: {result}")
    
    result = safe_index(["a", "b", "c"], "d", default=0)
    print(f"Test safe_index with missing item: {result}")
    
    result = safe_index({"a": 1}, "b", default=-1)
    print(f"Test safe_index with dict: {result}")

