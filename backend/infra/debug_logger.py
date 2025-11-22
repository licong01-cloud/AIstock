"""调试日志工具 - 为 next_app 添加统一的调试日志。

该实现从根目录 debug_logger 迁移而来，保持行为一致。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional
import sys
import traceback
import json


class DebugLogger:
    """统一的调试日志工具"""

    def __init__(self, enable_debug: bool = True):
        self.enable_debug = enable_debug
        self.log_file = "debug.log"

    def _get_timestamp(self) -> str:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    def _format_message(self, level: str, message: str, **kwargs: Any) -> str:
        timestamp = self._get_timestamp()
        base_msg = f"[{timestamp}] [{level}] {message}"
        if kwargs:
            context = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
            base_msg += f" | {context}"
        return base_msg

    def _write_to_file(self, message: str) -> None:
        try:
            with open(self.log_file, "a", encoding="utf-8") as f:
                f.write(message + "\n")
        except Exception:
            # 忽略文件写入错误，避免影响主流程
            pass

    def info(self, message: str, **kwargs: Any) -> None:
        msg = self._format_message("INFO", message, **kwargs)
        print(msg)
        self._write_to_file(msg)

    def debug(self, message: str, **kwargs: Any) -> None:
        if not self.enable_debug:
            return
        msg = self._format_message("DEBUG", message, **kwargs)
        print(msg)
        self._write_to_file(msg)

    def warning(self, message: str, **kwargs: Any) -> None:
        msg = self._format_message("WARNING", message, **kwargs)
        try:
            print(f"⚠️ {msg}")
        except UnicodeEncodeError:
            print(f"[WARNING] {msg}")
        self._write_to_file(msg)

    def error(self, message: str, error: Optional[Exception] = None, **kwargs: Any) -> None:
        msg = self._format_message("ERROR", message, **kwargs)
        try:
            print(f"❌ {msg}", file=sys.stderr)
        except UnicodeEncodeError:
            print(f"[ERROR] {msg}", file=sys.stderr)

        if error is not None:
            error_details = f"  Exception Type: {type(error).__name__}"
            error_details += f"\n  Exception Message: {str(error)}"
            print(error_details, file=sys.stderr)
            msg += f"\n{error_details}"
            tb = traceback.format_exc()
            print(f"  Traceback:\n{tb}", file=sys.stderr)
            msg += f"\n  Traceback:\n{tb}"

        self._write_to_file(msg)

    def function_call(self, func_name: str, args: tuple = (), kwargs: Optional[dict] = None) -> None:
        if not self.enable_debug:
            return
        kwargs = kwargs or {}
        args_str = ", ".join([repr(arg) for arg in args])
        kwargs_str = ", ".join([f"{k}={repr(v)}" for k, v in kwargs.items()])
        params = ", ".join(filter(None, [args_str, kwargs_str]))
        msg = self._format_message("CALL", f"{func_name}({params})")
        print(msg)
        self._write_to_file(msg)

    def function_return(self, func_name: str, result: Any, elapsed_time: Optional[float] = None) -> None:
        if not self.enable_debug:
            return
        if isinstance(result, dict):
            result_str = f"dict with {len(result)} keys: {list(result.keys())[:5]}"
        elif isinstance(result, list):
            result_str = f"list with {len(result)} items"
        elif hasattr(result, "__len__"):
            try:
                result_str = f"{type(result).__name__} with length {len(result)}"
            except Exception:
                result_str = str(type(result).__name__)
        else:
            result_str = repr(result)[:100]
        ctx: dict[str, Any] = {"result": result_str}
        if elapsed_time is not None:
            ctx["elapsed"] = f"{elapsed_time:.3f}s"
        msg = self._format_message("RETURN", func_name, **ctx)
        print(msg)
        self._write_to_file(msg)

    def data_info(self, data_name: str, data: Any) -> None:
        if not self.enable_debug:
            return
        info: dict[str, Any] = {"name": data_name, "type": type(data).__name__}
        if data is None:
            info["value"] = "None"
        elif isinstance(data, dict):
            info["keys"] = list(data.keys())[:10]
            info["length"] = len(data)
        elif isinstance(data, (list, tuple)):
            info["length"] = len(data)
            if data:
                info["first_item_type"] = type(data[0]).__name__
        elif hasattr(data, "shape"):
            info["shape"] = str(getattr(data, "shape", ""))
        elif hasattr(data, "__len__"):
            try:
                info["length"] = len(data)
            except Exception:
                pass
        msg = self._format_message("DATA", f"Data info for {data_name}", **info)
        print(msg)
        self._write_to_file(msg)

    def step(self, step_num: int, description: str, **kwargs: Any) -> None:
        msg = self._format_message("STEP", f"Step {step_num}: {description}", **kwargs)
        print("\n" + "=" * 80)
        print(msg)
        print("=" * 80)
        self._write_to_file(msg)


debug_logger = DebugLogger(enable_debug=True)


def safe_index(lst: list, item: Any, default: int = 0) -> int:
    try:
        if isinstance(lst, list):
            return lst.index(item)
        debug_logger.error(
            "safe_index called with non-list type",
            error=TypeError(f"Expected list, got {type(lst).__name__}"),
        )
        return default
    except ValueError:
        debug_logger.warning(
            "Item not found in list",
            item=item,
            list_items=str(lst),
            returning=default,
        )
        return default
    except Exception as e:  # noqa: BLE001
        debug_logger.error(
            "Unexpected error in safe_index",
            error=e,
            lst_type=type(lst).__name__,
            item=item,
        )
        return default


def log_exception(func):
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
        except Exception as e:  # noqa: BLE001
            elapsed_time = time.time() - start_time
            debug_logger.error(
                f"Exception in {func_name}",
                error=e,
                elapsed=f"{elapsed_time:.3f}s",
            )
            raise

    return wrapper
