"""Compatibility layer for environment configuration management.

This mirrors the behaviour of the legacy Streamlit `ConfigManager`,
so that we can manage the project `.env` file via FastAPI.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Tuple


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = PROJECT_ROOT / ".env"


class ConfigManager:
    """Configuration manager compatible with the legacy implementation."""

    def __init__(self, env_file: Path | str | None = None) -> None:
        self.env_file = Path(env_file) if env_file is not None else ENV_FILE
        # NOTE: Keep in sync with legacy aiagents-stock-main/config_manager.py,
        # but we can safely extend it with new keys used by the new backend.
        self.default_config: Dict[str, Dict[str, Any]] = {
            "DEEPSEEK_API_KEY": {
                "value": "",
                "description": "DeepSeek API密钥",
                "required": True,
                "type": "password",
            },
            "DEEPSEEK_BASE_URL": {
                "value": "https://api.deepseek.com/v1",
                "description": "DeepSeek API地址",
                "required": False,
                "type": "text",
            },
            "TUSHARE_TOKEN": {
                "value": "",
                "description": "Tushare数据接口Token（可选）",
                "required": False,
                "type": "password",
            },
            "TDX_API_BASE": {
                "value": "http://localhost:8080",
                "description": "TDX本地数据源地址（可选）",
                "required": False,
                "type": "text",
            },
            # Qlib / WSL / 路径配置
            "QLIB_WSL_DISTRO": {
                "value": "Ubuntu-22.04",
                "description": "用于运行 Qlib 脚本的 WSL 发行版名称",
                "required": False,
                "type": "text",
            },
            "QLIB_WSL_CONDA_SH": {
                "value": "/home/username/miniconda3/etc/profile.d/conda.sh",
                "description": "WSL 中 conda.sh 的路径（用于激活 Qlib 环境）",
                "required": False,
                "type": "text",
            },
            "QLIB_WSL_CONDA_ENV": {
                "value": "qlib-env",
                "description": "WSL 中用于 Qlib 的 Conda 环境名称",
                "required": False,
                "type": "text",
            },
            "QLIB_RDAGENT_ROOT_WIN": {
                "value": "C:/Users/lc999/NewAIstock/AIstock/RD-Agent-main",
                "description": "Windows 下 RD-Agent 项目根目录（绝对路径）",
                "required": False,
                "type": "text",
            },
            "QLIB_RDAGENT_ROOT_WSL": {
                "value": "/mnt/c/Users/lc999/NewAIstock/AIstock/RD-Agent-main",
                "description": "WSL 中 RD-Agent 项目根目录（绝对路径）",
                "required": False,
                "type": "text",
            },
            "QLIB_SCRIPTS_SUBDIR": {
                "value": "scripts",
                "description": "RD-Agent 中 Qlib 脚本所在子目录，例如 scripts",
                "required": False,
                "type": "text",
            },
            "QLIB_CSV_ROOT_WIN": {
                "value": "C:/Users/lc999/NewAIstock/AIstock/qlib_csv",
                "description": "Windows 下用于存放 Qlib 宽表 CSV 的根目录",
                "required": False,
                "type": "text",
            },
            "QLIB_BIN_ROOT_WIN": {
                "value": "C:/Users/lc999/NewAIstock/AIstock/qlib_bin",
                "description": "Windows 下用于存放 Qlib bin 数据的根目录",
                "required": False,
                "type": "text",
            },
            "ANNOUNCE_PDF_ROOT": {
                "value": "D:/AIstockDB/data/anns",
                "description": "公告PDF本地根目录，例如 D:/AIstockDB/data/anns",
                "required": False,
                "type": "text",
            },
            "TDX_DB_HOST": {
                "value": "127.0.0.1",
                "description": "TimescaleDB主机",
                "required": False,
                "type": "text",
            },
            "TDX_DB_PORT": {
                "value": "5432",
                "description": "TimescaleDB端口",
                "required": False,
                "type": "text",
            },
            "TDX_DB_NAME": {
                "value": "aistock",
                "description": "TimescaleDB数据库名称",
                "required": False,
                "type": "text",
            },
            "TDX_DB_USER": {
                "value": "postgres",
                "description": "TimescaleDB用户名",
                "required": False,
                "type": "text",
            },
            "TDX_DB_PASSWORD": {
                "value": "",
                "description": "TimescaleDB密码",
                "required": False,
                "type": "password",
            },
            "MINIQMT_ENABLED": {
                "value": "false",
                "description": "启用MiniQMT量化交易",
                "required": False,
                "type": "boolean",
            },
            "MINIQMT_ACCOUNT_ID": {
                "value": "",
                "description": "MiniQMT账户ID",
                "required": False,
                "type": "text",
            },
            "MINIQMT_HOST": {
                "value": "127.0.0.1",
                "description": "MiniQMT服务器地址",
                "required": False,
                "type": "text",
            },
            "MINIQMT_PORT": {
                "value": "58610",
                "description": "MiniQMT服务器端口",
                "required": False,
                "type": "text",
            },
            "EMAIL_ENABLED": {
                "value": "false",
                "description": "启用邮件通知",
                "required": False,
                "type": "boolean",
            },
            "SMTP_SERVER": {
                "value": "",
                "description": "SMTP服务器地址",
                "required": False,
                "type": "text",
            },
            "SMTP_PORT": {
                "value": "587",
                "description": "SMTP服务器端口",
                "required": False,
                "type": "text",
            },
            "EMAIL_FROM": {
                "value": "",
                "description": "发件人邮箱",
                "required": False,
                "type": "text",
            },
            "EMAIL_PASSWORD": {
                "value": "",
                "description": "邮箱授权码",
                "required": False,
                "type": "password",
            },
            "EMAIL_TO": {
                "value": "",
                "description": "收件人邮箱",
                "required": False,
                "type": "text",
            },
            "WEBHOOK_ENABLED": {
                "value": "false",
                "description": "启用Webhook通知",
                "required": False,
                "type": "boolean",
            },
            "WEBHOOK_TYPE": {
                "value": "dingtalk",
                "description": "Webhook类型（dingtalk/feishu）",
                "required": False,
                "type": "select",
                "options": ["dingtalk", "feishu"],
            },
            "WEBHOOK_URL": {
                "value": "",
                "description": "Webhook地址",
                "required": False,
                "type": "text",
            },
            "WEBHOOK_KEYWORD": {
                "value": "aiagents通知",
                "description": "Webhook自定义关键词（钉钉安全验证）",
                "required": False,
                "type": "text",
            },
            # 代理池相关配置（功能本身暂不迁移，但允许保留配置项）
            "USE_PROXY": {
                "value": "false",
                "description": "启用代理（静态/动态）",
                "required": False,
                "type": "boolean",
            },
            "PROXYPOOL_ENABLED": {
                "value": "false",
                "description": "启用动态代理池",
                "required": False,
                "type": "boolean",
            },
            "PROXYPOOL_BASE_URL": {
                "value": "",
                "description": "代理池Base URL",
                "required": False,
                "type": "text",
            },
            "PROXYPOOL_AUTH_TYPE": {
                "value": "token",
                "description": "代理池鉴权方式",
                "required": False,
                "type": "select",
                "options": ["token", "basic", "urlparam"],
            },
            "PROXYPOOL_TOKEN": {
                "value": "",
                "description": "代理池Token（鉴权: token/urlparam）",
                "required": False,
                "type": "password",
            },
            "PROXYPOOL_USERNAME": {
                "value": "",
                "description": "代理池用户名（鉴权: basic）",
                "required": False,
                "type": "text",
            },
            "PROXYPOOL_PASSWORD": {
                "value": "",
                "description": "代理池密码（鉴权: basic）",
                "required": False,
                "type": "password",
            },
            "PROXY_REFRESH_INTERVAL_MIN": {
                "value": "10",
                "description": "动态代理刷新间隔(分钟)",
                "required": False,
                "type": "text",
            },
            # 新架构中额外使用的配置，避免被覆盖丢失
            "NEWS_INGEST_VERBOSE_LOG": {
                "value": "false",
                "description": "新闻实时入库脚本是否打印详细日志",
                "required": False,
                "type": "boolean",
            },
        }

    def read_env(self) -> Dict[str, str]:
        """Read .env file and fill missing keys with defaults."""

        config: Dict[str, str] = {}
        if not self.env_file.exists():
            for key, info in self.default_config.items():
                config[key] = str(info.get("value", ""))
            return config

        try:
            with self.env_file.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if "=" not in line:
                        continue
                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip()
                    if value.startswith("\"") and value.endswith("\""):
                        value = value[1:-1]
                    elif value.startswith("'") and value.endswith("'"):
                        value = value[1:-1]
                    config[key] = value
        except Exception as exc:  # pragma: no cover - defensive
            print(f"读取.env文件失败: {exc}")

        for key, info in self.default_config.items():
            if key not in config:
                config[key] = str(info.get("value", ""))

        return config

    def write_env(self, config: Dict[str, str]) -> bool:
        """Write configuration back to .env.

        This follows the grouping and formatting of the legacy implementation.
        Unknown keys are currently ignored (same behaviour as legacy).
        """

        try:
            lines: list[str] = []
            lines.append("# AI股票分析系统环境配置")
            lines.append("# 由系统自动生成和管理")
            lines.append("")

            # DeepSeek
            lines.append("# ========== DeepSeek API配置 ==========")
            lines.append(
                f'DEEPSEEK_API_KEY="{config.get("DEEPSEEK_API_KEY", "")}"'
            )
            lines.append(
                f'DEEPSEEK_BASE_URL="{config.get("DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1")}"'
            )
            lines.append("")

            # Tushare
            lines.append("# ========== Tushare数据接口（可选）==========")
            lines.append(f'TUSHARE_TOKEN="{config.get("TUSHARE_TOKEN", "")}"')
            lines.append("")

            # TDX
            lines.append("# ========== TDX本地数据源（可选）==========")
            lines.append(
                f'TDX_API_BASE="{config.get("TDX_API_BASE", "http://localhost:8080")}"'
            )
            lines.append("")

            # Qlib / WSL / 路径配置
            lines.append("# ========== Qlib / WSL / 路径配置（可选）==========")
            lines.append(
                f'QLIB_WSL_DISTRO="{config.get("QLIB_WSL_DISTRO", "Ubuntu-22.04")}"'
            )
            lines.append(
                f'QLIB_WSL_CONDA_SH="{config.get("QLIB_WSL_CONDA_SH", "/home/username/miniconda3/etc/profile.d/conda.sh")}"'
            )
            lines.append(
                f'QLIB_WSL_CONDA_ENV="{config.get("QLIB_WSL_CONDA_ENV", "qlib-env")}"'
            )
            lines.append(
                f'QLIB_RDAGENT_ROOT_WIN="{config.get("QLIB_RDAGENT_ROOT_WIN", "")}"'
            )
            lines.append(
                f'QLIB_RDAGENT_ROOT_WSL="{config.get("QLIB_RDAGENT_ROOT_WSL", "")}"'
            )
            lines.append(
                f'QLIB_SCRIPTS_SUBDIR="{config.get("QLIB_SCRIPTS_SUBDIR", "scripts")}"'
            )
            lines.append(
                f'QLIB_CSV_ROOT_WIN="{config.get("QLIB_CSV_ROOT_WIN", "")}"'
            )
            lines.append(
                f'QLIB_BIN_ROOT_WIN="{config.get("QLIB_BIN_ROOT_WIN", "")}"'
            )
            lines.append(
                f'ANNOUNCE_PDF_ROOT="{config.get("ANNOUNCE_PDF_ROOT", "")}"'
            )
            lines.append("")

            # TimescaleDB
            lines.append(
                "# ========== TimescaleDB数据库配置（TDX调度与数据入库）=========="
            )
            lines.append(f'TDX_DB_HOST="{config.get("TDX_DB_HOST", "127.0.0.1")}"')
            lines.append(f'TDX_DB_PORT="{config.get("TDX_DB_PORT", "5432")}"')
            lines.append(f'TDX_DB_NAME="{config.get("TDX_DB_NAME", "aistock")}"')
            lines.append(f'TDX_DB_USER="{config.get("TDX_DB_USER", "postgres")}"')
            lines.append(f'TDX_DB_PASSWORD="{config.get("TDX_DB_PASSWORD", "")}"')
            lines.append("")

            # MiniQMT
            lines.append("# ========== MiniQMT量化交易配置（可选）==========")
            lines.append(
                f'MINIQMT_ENABLED="{config.get("MINIQMT_ENABLED", "false")}"'
            )
            lines.append(
                f'MINIQMT_ACCOUNT_ID="{config.get("MINIQMT_ACCOUNT_ID", "")}"'
            )
            lines.append(f'MINIQMT_HOST="{config.get("MINIQMT_HOST", "127.0.0.1")}"')
            lines.append(f'MINIQMT_PORT="{config.get("MINIQMT_PORT", "58610")}"')
            lines.append("")

            # Email
            lines.append("# ========== 邮件通知配置（可选）==========")
            lines.append(
                f'EMAIL_ENABLED="{config.get("EMAIL_ENABLED", "false")}"'
            )
            lines.append(f'SMTP_SERVER="{config.get("SMTP_SERVER", "")}"')
            lines.append(f'SMTP_PORT="{config.get("SMTP_PORT", "587")}"')
            lines.append(f'EMAIL_FROM="{config.get("EMAIL_FROM", "")}"')
            lines.append(f'EMAIL_PASSWORD="{config.get("EMAIL_PASSWORD", "")}"')
            lines.append(f'EMAIL_TO="{config.get("EMAIL_TO", "")}"')
            lines.append("")

            # Webhook
            lines.append("# ========== Webhook通知配置（可选）==========")
            lines.append(
                f'WEBHOOK_ENABLED="{config.get("WEBHOOK_ENABLED", "false")}"'
            )
            lines.append(
                f'WEBHOOK_TYPE="{config.get("WEBHOOK_TYPE", "dingtalk")}"'
            )
            lines.append(f'WEBHOOK_URL="{config.get("WEBHOOK_URL", "")}"')
            lines.append(
                f'WEBHOOK_KEYWORD="{config.get("WEBHOOK_KEYWORD", "aiagents通知")}"'
            )
            lines.append("")

            # Proxy
            lines.append("# ========== 代理池与网络优化（可选）==========")
            lines.append(f'USE_PROXY="{config.get("USE_PROXY", "false")}"')
            lines.append(
                f'PROXYPOOL_ENABLED="{config.get("PROXYPOOL_ENABLED", "false")}"'
            )
            lines.append(
                f'PROXYPOOL_BASE_URL="{config.get("PROXYPOOL_BASE_URL", "")}"'
            )
            lines.append(
                f'PROXYPOOL_AUTH_TYPE="{config.get("PROXYPOOL_AUTH_TYPE", "token")}"'
            )
            lines.append(
                f'PROXYPOOL_TOKEN="{config.get("PROXYPOOL_TOKEN", "")}"'
            )
            lines.append(
                f'PROXYPOOL_USERNAME="{config.get("PROXYPOOL_USERNAME", "")}"'
            )
            lines.append(
                f'PROXYPOOL_PASSWORD="{config.get("PROXYPOOL_PASSWORD", "")}"'
            )
            lines.append(
                f'PROXY_REFRESH_INTERVAL_MIN="{config.get("PROXY_REFRESH_INTERVAL_MIN", "10")}"'
            )
            lines.append("")

            # Extra: news ingestion logging toggle
            lines.append("# ========== 其他配置（自动生成）==========")
            lines.append(
                f'NEWS_INGEST_VERBOSE_LOG="{config.get("NEWS_INGEST_VERBOSE_LOG", "false")}"'
            )

            with self.env_file.open("w", encoding="utf-8") as f:
                f.write("\n".join(lines))

            return True
        except Exception as exc:  # pragma: no cover - defensive
            print(f"保存.env文件失败: {exc}")
            return False

    def get_config_info(self) -> Dict[str, Dict[str, Any]]:
        """Return config metadata + current values, for UI rendering."""

        current_values = self.read_env()
        info: Dict[str, Dict[str, Any]] = {}
        for key, meta in self.default_config.items():
            item: Dict[str, Any] = {
                "value": current_values.get(key, meta.get("value", "")),
                "description": meta.get("description", ""),
                "required": bool(meta.get("required", False)),
                "type": meta.get("type", "text"),
            }
            if "options" in meta:
                item["options"] = list(meta["options"])
            info[key] = item
        return info

    def validate_config(self, config: Dict[str, str]) -> Tuple[bool, str]:
        """Validate configuration before saving."""

        for key, meta in self.default_config.items():
            if meta.get("required") and not config.get(key):
                return False, f"必填项 {meta.get('description', key)} 不能为空"

        api_key = config.get("DEEPSEEK_API_KEY") or ""
        if api_key and len(api_key) < 20:
            return False, "DeepSeek API Key格式不正确（长度太短）"

        return True, "配置验证通过"

    def reload_config(self) -> None:
        """Reload .env into process environment."""

        try:
            from dotenv import load_dotenv

            load_dotenv(dotenv_path=self.env_file, override=True)
        except Exception as exc:  # pragma: no cover - defensive
            print(f"重新加载.env失败: {exc}")


config_manager = ConfigManager()
