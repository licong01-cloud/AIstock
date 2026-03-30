"""
Grafana 仪表盘自动导入脚本
用法: python setup_grafana.py
功能: 等待 Grafana 就绪 → 导入 Node Exporter Full + PostgreSQL 社区仪表盘
"""
import json
import sys
import time
import urllib.request
import urllib.error

GRAFANA_URL = "http://localhost:3001"
GRAFANA_USER = "admin"
GRAFANA_PASS = "aistock2026"

# 社区仪表盘 gnetId → (uid, title)
DASHBOARDS = [
    {
        "gnetId": 1860,
        "uid": "node-exporter-full",
        "title": "Node Exporter Full",
        "desc": "系统监控 — CPU / 内存 / 磁盘 / 网络",
    },
    {
        "gnetId": 9628,
        "uid": "pg-overview",
        "title": "PostgreSQL Database",
        "desc": "数据库监控 — 连接 / 事务 / 缓存 / 锁",
    },
]


def _auth_header() -> str:
    import base64
    cred = base64.b64encode(f"{GRAFANA_USER}:{GRAFANA_PASS}".encode()).decode()
    return f"Basic {cred}"


def wait_for_grafana(timeout: int = 120):
    print(f"等待 Grafana 就绪 ({GRAFANA_URL}) ...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            req = urllib.request.Request(f"{GRAFANA_URL}/api/health")
            resp = urllib.request.urlopen(req, timeout=5)
            data = json.loads(resp.read())
            if data.get("database") == "ok":
                print("  Grafana 已就绪!")
                return True
        except Exception:
            pass
        time.sleep(2)
    print("  超时: Grafana 未能在规定时间内就绪")
    return False


def import_dashboard(dashboard: dict) -> bool:
    gnet_id = dashboard["gnetId"]
    uid = dashboard["uid"]
    title = dashboard["title"]

    print(f"\n导入仪表盘: {title} (gnetId={gnet_id}) ...")

    # 1. 从 grafana.com 下载仪表盘 JSON
    download_url = f"https://grafana.com/api/dashboards/{gnet_id}/revisions/latest/download"
    try:
        req = urllib.request.Request(download_url)
        resp = urllib.request.urlopen(req, timeout=30)
        dashboard_json = json.loads(resp.read())
    except Exception as e:
        print(f"  下载失败: {e}")
        return False

    # 2. 设置自定义 UID
    dashboard_json["uid"] = uid
    dashboard_json["id"] = None
    dashboard_json["title"] = title

    # 3. 通过 Grafana API 导入
    payload = json.dumps({
        "dashboard": dashboard_json,
        "overwrite": True,
        "inputs": [
            {
                "name": "DS_PROMETHEUS",
                "type": "datasource",
                "pluginId": "prometheus",
                "value": "Prometheus",
            }
        ],
        "folderId": 0,
    }).encode()

    try:
        req = urllib.request.Request(
            f"{GRAFANA_URL}/api/dashboards/import",
            data=payload,
            headers={
                "Content-Type": "application/json",
                "Authorization": _auth_header(),
            },
            method="POST",
        )
        resp = urllib.request.urlopen(req, timeout=30)
        result = json.loads(resp.read())
        dash_url = result.get("importedUrl", "")
        print(f"  导入成功: {GRAFANA_URL}{dash_url}")
        return True
    except urllib.error.HTTPError as e:
        body = e.read().decode() if e.fp else ""
        print(f"  导入失败 (HTTP {e.code}): {body[:300]}")
        return False
    except Exception as e:
        print(f"  导入失败: {e}")
        return False


def main():
    if not wait_for_grafana():
        sys.exit(1)

    ok_count = 0
    for d in DASHBOARDS:
        if import_dashboard(d):
            ok_count += 1

    print(f"\n{'='*50}")
    print(f"导入完成: {ok_count}/{len(DASHBOARDS)} 成功")
    print()
    print("仪表盘 URL:")
    print(f"  系统监控: {GRAFANA_URL}/d/node-exporter-full?orgId=1&kiosk&theme=dark")
    print(f"  数据库监控: {GRAFANA_URL}/d/pg-overview?orgId=1&kiosk&theme=dark")
    print()
    print(f"Grafana 管理: {GRAFANA_URL} (admin / aistock2026)")

    if ok_count < len(DASHBOARDS):
        sys.exit(1)


if __name__ == "__main__":
    main()
