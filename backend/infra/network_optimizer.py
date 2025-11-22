"""网络优化与代理池管理模块（next_app 内部拷贝）。

从根目录 network_optimizer 迁移而来，保持配置和行为一致。
"""

from __future__ import annotations

import json
import os
import threading
import time
from contextlib import contextmanager
from typing import Dict, List, Optional, Tuple

import requests


_CONFIG_PATH = os.path.join(os.getcwd(), "proxy_config.json")


class NetworkOptimizer:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.use_proxy = os.getenv("USE_PROXY", "false").lower() == "true"
        self.dynamic_enabled = os.getenv("PROXYPOOL_ENABLED", "false").lower() == "true"
        self.refresh_interval_min = int(os.getenv("PROXY_REFRESH_INTERVAL_MIN", "10") or 10)

        self.static_proxies: List[Dict] = []
        self.dynamic_sources: Dict[str, Dict] = {}
        self.dynamic_cache: List[str] = []
        self._rr_idx = 0
        self._last_refresh_ts = 0.0

        self._load_config()
        if self.dynamic_enabled:
            t = threading.Thread(target=self._refresh_loop, daemon=True)
            t.start()

    # ---------- 配置持久化 ----------
    def _load_config(self) -> None:
        if not os.path.exists(_CONFIG_PATH):
            self._save_config()
            return
        try:
            with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.static_proxies = data.get("proxy_priority", [])
            self.dynamic_sources = {
                s.get("name"): s
                for s in data.get("dynamic_sources", [])
                if s.get("name")
            }
            self.use_proxy = data.get("use_proxy", self.use_proxy)
        except Exception:
            pass

    def _save_config(self) -> None:
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
    def add_proxy(
        self,
        name: str,
        proxy_config: Dict,
        priority: int = 1,
        enabled: bool = True,
        description: Optional[str] = None,
    ) -> bool:
        with self._lock:
            self.static_proxies = [p for p in self.static_proxies if p.get("name") != name]
            self.static_proxies.append(
                {
                    "name": name,
                    "proxy": proxy_config.get("proxy"),
                    "priority": int(priority),
                    "enabled": bool(enabled),
                    "description": description,
                }
            )
            self.static_proxies.sort(key=lambda x: x.get("priority", 9999))
            self._save_config()
            return True

    def remove_proxy(self, name: str) -> bool:
        with self._lock:
            before = len(self.static_proxies)
            self.static_proxies = [p for p in self.static_proxies if p.get("name") != name]
            self._save_config()
            return len(self.static_proxies) < before

    def update_proxy(
        self,
        old_name: str,
        new_name: Optional[str] = None,
        proxy_config: Optional[Dict] = None,
        priority: Optional[int] = None,
        enabled: Optional[bool] = None,
        description: Optional[str] = None,
    ) -> bool:
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

    def enable_proxy(self) -> None:
        self.use_proxy = True
        self._save_config()

    def disable_proxy(self) -> None:
        self.use_proxy = False
        self._save_config()

    def is_proxy_enabled(self) -> bool:
        return self.use_proxy

    # ---------- 动态代理源 ----------
    def add_dynamic_proxy_source(
        self,
        name: str,
        base_url: str,
        auth: Dict,
        params: Optional[Dict] = None,
        enabled: bool = True,
    ) -> bool:
        with self._lock:
            self.dynamic_sources[name] = {
                "name": name,
                "base_url": base_url,
                "auth": {k: v for k, v in (auth or {}).items() if k not in {"token", "password"}},
                "params": params or {},
                "enabled": bool(enabled),
            }
            self._save_config()
            return True

    def _fetch_from_source(self, base_url: str, auth: Dict, params: Dict) -> Optional[str]:
        headers: Dict[str, str] = {}
        kwargs: Dict[str, object] = {"timeout": 5}

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
            resp = requests.get(base_url, headers=headers, params=params, **kwargs)  # type: ignore[arg-type]
            if resp.ok:
                text = resp.text.strip()
                if text.startswith("http"):
                    return text
                try:
                    data = resp.json()
                    for key in ["proxy", "data", "ip"]:
                        val = data.get(key)
                        if isinstance(val, str) and val:
                            return val
                        if isinstance(val, dict):
                            ip = val.get("ip")
                            port = val.get("port")
                            if ip and port:
                                return f"http://{ip}:{port}"
                except Exception:
                    pass
        except Exception:
            return None
        return None

    def _refresh_dynamic_cache(self) -> None:
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
                p = self._fetch_from_source(
                    src.get("base_url", ""), src.get("auth", {}), src.get("params", {})
                )
                if p:
                    proxies.append(p)
            if proxies:
                self.dynamic_cache = proxies
                self._rr_idx = 0
                self._last_refresh_ts = now

    def _refresh_loop(self) -> None:
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

    def get_network_status(self) -> Dict[str, object]:
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
        for p in self.static_proxies:
            if p.get("enabled") and self.test_proxy_fast(p):
                return p.get("proxy")
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
    def apply(self):  # type: ignore[override]
        proxies = self.get_requests_proxies()
        old_http = os.environ.get("http_proxy")
        old_https = os.environ.get("https_proxy")
        try:
            if proxies:
                os.environ["http_proxy"] = proxies.get("http", "")
                os.environ["https_proxy"] = proxies.get("https", "")
            yield proxies
        finally:
            if old_http is None:
                os.environ.pop("http_proxy", None)
            else:
                os.environ["http_proxy"] = old_http
            if old_https is None:
                os.environ.pop("https_proxy", None)
            else:
                os.environ["https_proxy"] = old_https


network_optimizer = NetworkOptimizer()
