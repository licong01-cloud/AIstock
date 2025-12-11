from __future__ import annotations

"""WSL + conda 下执行 Qlib 脚本的通用工具函数.

本模块只依赖环境变量, 不改动 RD-Agent 或 Qlib 脚本本身:

需要在进程环境中预先配置以下变量(可以来自 .env 或外部注入)::

    QLIB_WSL_DISTRO=Ubuntu
    QLIB_WSL_CONDA_SH=~/miniconda3/etc/profile.d/conda.sh
    QLIB_WSL_CONDA_ENV=rdagent-gpu

    QLIB_RDAGENT_ROOT_WSL=/mnt/c/Users/lc999/RD-Agent-main
    QLIB_SCRIPTS_SUBDIR=scripts

    # 以下两个主要给上层逻辑使用, 本模块只做透传(可选)
    QLIB_BIN_ROOT_WIN=C:/Users/lc999/NewAIstock/AIstock/qlib_bin
    QLIB_CSV_ROOT_WIN=C:/Users/lc999/NewAIstock/AIstock/qlib_csv
"""

from dataclasses import dataclass
import os
import shlex
import subprocess
from typing import Iterable, List, Mapping, Optional


@dataclass
class RunResult:
    """WSL 命令执行结果.

    Attributes:
        returncode: 进程退出码
        stdout: 标准输出
        stderr: 标准错误
        cmd: 实际执行的命令行(用于调试)
    """

    returncode: int
    stdout: str
    stderr: str
    cmd: str

    @property
    def ok(self) -> bool:
        """是否执行成功(退出码为 0)."""

        return self.returncode == 0


class QlibWSLConfigError(RuntimeError):
    """WSL/conda 相关配置错误(缺少必要环境变量等)."""


def _get_env(name: str, *, optional: bool = False) -> Optional[str]:
    value = os.getenv(name)
    if not value and not optional:
        raise QlibWSLConfigError(f"缺少必要环境变量: {name}")
    return value


def win_to_wsl_path(win_path: str) -> str:
    """将 Windows 路径转换为 WSL 路径表示.

    仅做简单规则转换, 不检查路径是否真实存在.
    """

    if not win_path:
        return win_path

    p = win_path.replace("\\", "/")
    if len(p) >= 2 and p[1] == ":":
        drive = p[0].lower()
        rest = p[2:]
        if not rest.startswith("/"):
            rest = "/" + rest
        return f"/mnt/{drive}{rest}"
    return p


def build_wsl_qlib_command(
    script_name: str,
    args: Iterable[str] | None = None,
) -> str:
    """构造在 WSL 中运行 Qlib 脚本的 bash 命令串.

    仅负责拼接字符串, 不执行.
    返回的字符串将传给 ``bash -lc"<cmd>"``.
    """

    distro = _get_env("QLIB_WSL_DISTRO")
    conda_sh = _get_env("QLIB_WSL_CONDA_SH")
    conda_env = _get_env("QLIB_WSL_CONDA_ENV")
    rdagent_root_wsl = _get_env("QLIB_RDAGENT_ROOT_WSL")
    scripts_subdir = _get_env("QLIB_SCRIPTS_SUBDIR") or "scripts"

    # 目标脚本所在目录, 例如 /mnt/c/Users/.../RD-Agent-main/scripts
    scripts_dir = f"{rdagent_root_wsl.rstrip('/')}/{scripts_subdir}"

    arg_list: List[str] = list(args or [])

    # 使用 shlex.quote 做最小化转义, 避免空格等问题
    inner_parts = [
        f"source {shlex.quote(conda_sh)}",
        f"conda activate {shlex.quote(conda_env)}",
        f"cd {shlex.quote(scripts_dir)}",
        "python " + shlex.quote(script_name),
    ]

    if arg_list:
        inner_parts[-1] += " " + " ".join(shlex.quote(a) for a in arg_list)

    inner_cmd = " && ".join(inner_parts)

    # 注意: 这里只返回 bash -lc 需要的内部命令串, 外层 wsl 命令由调用者组装
    return inner_cmd


def run_qlib_script_in_wsl(
    script_name: str,
    args: Iterable[str] | None = None,
    *,
    extra_env: Optional[Mapping[str, str]] = None,
    timeout: Optional[float] = None,
) -> RunResult:
    """在 WSL + conda 环境中执行位于 RD-Agent-main/scripts 下的 Qlib 脚本.

    Args:
        script_name: 脚本文件名, 例如 ``"dump_bin.py"`` 或 ``"check_data_health.py"``.
        args: 传给脚本的参数列表, 不需要做 shell 转义.
        extra_env: 额外注入到子进程的环境变量(覆盖同名值).
        timeout: 可选超时时间(秒).

    Returns:
        RunResult: 包含退出码、stdout、stderr 和完整命令行.

    Raises:
        QlibWSLConfigError: 必要环境变量缺失时抛出.
    """

    distro = _get_env("QLIB_WSL_DISTRO")
    inner_cmd = build_wsl_qlib_command(script_name, args)

    # 在 Windows 侧通过 wsl 调用 bash -lc
    # 注意: 这里不使用 shell=True, 而是让 wsl 负责解释 bash -lc
    cmd_list = [
        "wsl",
        "-d",
        distro,
        "bash",
        "-lc",
        inner_cmd,
    ]

    # 继承当前环境, 并可选择性覆盖/追加
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)

    # Windows 默认控制台编码为 GBK，WSL/conda 下的 Python 一般使用 UTF-8 输出，
    # 如果不显式指定 encoding，subprocess 会用本地代码页解码，容易触发 UnicodeDecodeError。
    # 这里强制按 UTF-8 解码，并使用 errors="replace" 保证不会因个别字符导致整个调用失败。
    completed = subprocess.run(
        cmd_list,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
        timeout=timeout,
        check=False,
    )

    # 将命令行以空格拼接, 仅用于日志/调试
    full_cmd_str = " ".join(shlex.quote(part) for part in cmd_list)

    return RunResult(
        returncode=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
        cmd=full_cmd_str,
    )
