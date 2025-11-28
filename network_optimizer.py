"""
网络优化与代理池管理模块

功能：
- 静态代理管理：增删改查、优先级、启用开关
- 动态代理池：多源配置、拉取、轮询选择、健康检查、失败降级
- 连接状态：快速连通测试、网络状态查询
- 配置持久化：proxy_config.json（不含密钥），密钥放在 .env

环境变量：
- USE_PROXY=true/false
- PROXYPOOL_ENABLED=true/false
- PROXYPOOL_BASE_URL=...
- PROXYPOOL_AUTH_TYPE=token/basic/urlparam
- PROXYPOOL_TOKEN=...
- PROXYPOOL_USERNAME=...
- PROXYPOOL_PASSWORD=...
- PROXY_REFRESH_INTERVAL_MIN=10
"""

from __future__ import annotations

import os
import json
import time
import threading
from contextlib import contextmanager
from typing import Dict, List, Optional, Tuple

import requests


_CONFIG_PATH = os.path.join(os.getcwd(), "proxy_config.json")


class NetworkOptimizer:
    def __init__(self):
        self._lock = threading.Lock()
        self.use_proxy = os.getenv("USE_PROXY", "false").lower() == "true"
        self.dynamic_enabled = os.getenv("PROXYPOOL_ENABLED", "false").lower() == "true"
        self.refresh_interval_min = int(os.getenv("PROXY_REFRESH_INTERVAL_MIN", "10") or 10)

        # 内存状态
        self.static_proxies: List[Dict] = []  # [{name, proxy, priority, enabled, description}]
        self.dynamic_sources: Dict[str, Dict] = {}  # name -> {base_url, auth, params, enabled}
        self.dynamic_cache: List[str] = []  # 形如 ["http://host:port", ...]
        self._rr_idx = 0
        self._last_refresh_ts = 0.0

        self._load_config()
        # 仅在动态代理功能启用时才启动后台刷新线程，避免在未启用代理时产生任何自动网络访问
        if self.dynamic_enabled:
            t = threading.Thread(target=self._refresh_loop, daemon=True)
            t.start()

    # ---------- 配置持久化 ----------
    def _load_config(self):
        if not os.path.exists(_CONFIG_PATH):
            self._save_config()
            return
        try:
            with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.static_proxies = data.get("proxy_priority", [])
            # 动态源不保存密钥，仅基本信息
            self.dynamic_sources = {s.get("name"): s for s in data.get("dynamic_sources", []) if s.get("name")}
            self.use_proxy = data.get("use_proxy", self.use_proxy)
        except Exception:
            # 忽略损坏文件
            pass

    def _save_config(self):
        data = {
            "proxy_priority": self.static_proxies,
            "dynamic_sources": list(self.dynamic_sources.values()),
            "use_proxy": self.use_proxy,
        }
        try:
            with open(_CONFIG_PATH, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    # ---------- 静态代理管理 ----------
    def add_proxy(self, name: str, proxy_config: Dict, priority: int = 1, enabled: bool = True,
                  description: Optional[str] = None) -> bool:
        with self._lock:
            # 去重
            self.static_proxies = [p for p in self.static_proxies if p.get("name") != name]
            self.static_proxies.append({
                "name": name,
                "proxy": proxy_config.get("proxy"),
                "priority": int(priority),
                "enabled": bool(enabled),
                "description": description
            })
            # 按优先级排序（小到大）
            self.static_proxies.sort(key=lambda x: x.get("priority", 9999))
            self._save_config()
            return True

    def remove_proxy(self, name: str) -> bool:
        with self._lock:
            before = len(self.static_proxies)
            self.static_proxies = [p for p in self.static_proxies if p.get("name") != name]
            self._save_config()
            return len(self.static_proxies) < before

    def update_proxy(self, old_name: str, new_name: Optional[str] = None, proxy_config: Optional[Dict] = None,
                     priority: Optional[int] = None, enabled: Optional[bool] = None,
                     description: Optional[str] = None) -> bool:
        with self._lock:
            for p in self.static_proxies:
                if p.get("name") == old_name:
                    if new_name is not None:
                        p["name"] = new_name
                    if proxy_config is not None and "proxy" in proxy_config:
                        p["proxy"] = proxy_config["proxy"]
                    if priority is not None:
                        p["priority"] = int(priority)
                    if enabled is not None:
                        p["enabled"] = bool(enabled)
                    if description is not None:
                        p["description"] = description
                    self.static_proxies.sort(key=lambda x: x.get("priority", 9999))
                    self._save_config()
                    return True
            return False

    def toggle_proxy(self, name: str, enabled: bool) -> bool:
        return self.update_proxy(old_name=name, enabled=enabled)

    def update_proxy_priority(self, name: str, priority: int) -> bool:
        return self.update_proxy(old_name=name, priority=priority)

    def get_proxy_list(self) -> List[Dict]:
        return list(self.static_proxies)

    def enable_proxy(self):
        self.use_proxy = True
        self._save_config()

    def disable_proxy(self):
        self.use_proxy = False
        self._save_config()

    def is_proxy_enabled(self) -> bool:
        return self.use_proxy

    # ---------- 动态代理源 ----------
    def add_dynamic_proxy_source(self, name: str, base_url: str, auth: Dict, params: Optional[Dict] = None,
                                 enabled: bool = True) -> bool:
        """
        auth: {
          type: token/basic/urlparam,
          token: ..., username: ..., password: ..., param_key: "token"
        }
        """
        with self._lock:
            self.dynamic_sources[name] = {
                "name": name,
                "base_url": base_url,
                "auth": {k: v for k, v in (auth or {}).items() if k != "token" and k != "password"},
                "params": params or {},
                "enabled": bool(enabled),
            }
            self._save_config()
            return True

    def _fetch_from_source(self, base_url: str, auth: Dict, params: Dict) -> Optional[str]:
        headers = {}
        kwargs: Dict = {"timeout": 5}

        auth_type = (auth or {}).get("type") or os.getenv("PROXYPOOL_AUTH_TYPE", "")
        token = os.getenv("PROXYPOOL_TOKEN", "")
        username = os.getenv("PROXYPOOL_USERNAME", "")
        password = os.getenv("PROXYPOOL_PASSWORD", "")
        param_key = (auth or {}).get("param_key") or "token"

        if auth_type == "token" and token:
            headers["Authorization"] = f"Bearer {token}"
        elif auth_type == "basic" and username:
            kwargs["auth"] = (username, password)
        elif auth_type == "urlparam" and token:
            if params is None:
                params = {}
            params[param_key] = token

        try:
            resp = requests.get(base_url, headers=headers, params=params, **kwargs)
            if resp.ok:
                text = resp.text.strip()
                # 兼容返回 JSON 或 纯字符串
                if text.startswith("http"):
                    return text
                try:
                    data = resp.json()
                    # 常见字段名
                    for key in ["proxy", "data", "ip"]:
                        val = data.get(key)
                        if isinstance(val, str) and val:
                            return val
                        if isinstance(val, dict):
                            # {"ip":"x.x.x.x", "port":"xxxx"}
                            ip = val.get("ip")
                            port = val.get("port")
                            if ip and port:
                                return f"http://{ip}:{port}"
                except Exception:
                    pass
        except Exception:
            return None
        return None

    def _refresh_dynamic_cache(self):
        if not self.dynamic_enabled:
            return
        now = time.time()
        if now - self._last_refresh_ts < self.refresh_interval_min * 60:
            return
        with self._lock:
            proxies: List[str] = []
            for src in self.dynamic_sources.values():
                if not src.get("enabled"):
                    continue
                p = self._fetch_from_source(src.get("base_url", ""), src.get("auth", {}), src.get("params", {}))
                if p:
                    proxies.append(p)
            if proxies:
                self.dynamic_cache = proxies
                self._rr_idx = 0
                self._last_refresh_ts = now

    def _refresh_loop(self):
        while True:
            try:
                self._refresh_dynamic_cache()
            except Exception:
                pass
            time.sleep(60)

    def get_dynamic_proxy(self) -> Optional[str]:
        self._refresh_dynamic_cache()
        with self._lock:
            if not self.dynamic_cache:
                return None
            proxy = self.dynamic_cache[self._rr_idx % len(self.dynamic_cache)]
            self._rr_idx += 1
            return proxy

    def get_dynamic_proxy_from_source(self, name: str) -> Optional[str]:
        src = self.dynamic_sources.get(name)
        if not src or not src.get("enabled"):
            return None
        return self._fetch_from_source(src.get("base_url", ""), src.get("auth", {}), src.get("params", {}))

    # ---------- 健康检查与网络状态 ----------
    def test_proxy_fast(self, proxy_config: Dict) -> bool:
        proxy = proxy_config.get("proxy") if isinstance(proxy_config, dict) else str(proxy_config)
        if not proxy:
            return False
        proxies = {"http": proxy, "https": proxy}
        try:
            r = requests.get("https://www.baidu.com", proxies=proxies, timeout=3)
            return r.ok
        except Exception:
            return False

    def test_proxy_list(self) -> List[Tuple[str, bool]]:
        results: List[Tuple[str, bool]] = []
        for p in self.static_proxies:
            if not p.get("enabled"):
                continue
            ok = self.test_proxy_fast(p)
            results.append((p.get("name"), ok))
        # 也测试一个动态代理
        dyn = self.get_dynamic_proxy()
        if dyn:
            results.append(("dynamic", self.test_proxy_fast({"proxy": dyn})))
        return results

    def test_network_connection(self) -> bool:
        try:
            r = requests.get("https://www.baidu.com", timeout=3)
            return r.ok
        except Exception:
            return False

    def get_network_status(self) -> Dict:
        return {
            "use_proxy": self.use_proxy,
            "dynamic_enabled": self.dynamic_enabled,
            "dynamic_cache_size": len(self.dynamic_cache),
            "static_proxies": len([p for p in self.static_proxies if p.get("enabled")]),
            "last_refresh": self._last_refresh_ts,
        }

    # ---------- 应用入口 ----------
    def get_active_proxy(self) -> Optional[str]:
        if not self.use_proxy:
            return None
        # 优先静态按优先级
        for p in self.static_proxies:
            if p.get("enabled") and self.test_proxy_fast(p):
                return p.get("proxy")
        # 再尝试动态
        dyn = self.get_dynamic_proxy()
        if dyn and self.test_proxy_fast({"proxy": dyn}):
            return dyn
        return None

    def get_requests_proxies(self) -> Optional[Dict[str, str]]:
        proxy = self.get_active_proxy()
        if not proxy:
            return None
        return {"http": proxy, "https": proxy}

    @contextmanager
    def apply(self):
        """临时设置环境代理（供 requests/akshare 等内部使用）"""
        proxies = self.get_requests_proxies()
        old_http = os.environ.get("http_proxy")
        old_https = os.environ.get("https_proxy")
        try:
            if proxies:
                os.environ["http_proxy"] = proxies.get("http", "")
                os.environ["https_proxy"] = proxies.get("https", "")
            yield proxies
        finally:
            # 还原
            if old_http is None:
                os.environ.pop("http_proxy", None)
            else:
                os.environ["http_proxy"] = old_http
            if old_https is None:
                os.environ.pop("https_proxy", None)
            else:
                os.environ["https_proxy"] = old_https


network_optimizer = NetworkOptimizer()


