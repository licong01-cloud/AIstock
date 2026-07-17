"""
Patch the 'Node Exporter Full' dashboard to add RDAgent process monitoring panels at the bottom.
"""

import requests

GRAFANA_URL = "http://localhost:3001"
GRAFANA_AUTH = ("admin", "aistock2026")
DASHBOARD_UID = "node-exporter-full"


def main():
    # Fetch current dashboard
    resp = requests.get(f"{GRAFANA_URL}/api/dashboards/uid/{DASHBOARD_UID}", auth=GRAFANA_AUTH)
    resp.raise_for_status()
    data = resp.json()
    dashboard = data["dashboard"]

    # Get datasource UID
    ds_resp = requests.get(f"{GRAFANA_URL}/api/datasources/name/Prometheus", auth=GRAFANA_AUTH)
    ds_resp.raise_for_status()
    ds_uid = ds_resp.json()["uid"]

    # Remove old custom panels if re-running
    custom_titles = {
        "RDAgent 进程监控",
        "Process RSS (rdagent / qlib / python)",
        "VmHWM 内存峰值 (rdagent)",
        "RSS 增长速率 (内存泄漏检测)",
        "线程数 / 文件描述符",
        "硬件温度监控",
        "CPU / 主板温度",
        "CPU / 主板 / GPU 温度",
        "GPU 功耗 / 显存",
    }
    old_count = len(dashboard["panels"])
    dashboard["panels"] = [
        p for p in dashboard["panels"]
        if p.get("title") not in custom_titles
    ]
    removed = old_count - len(dashboard["panels"])
    if removed:
        print(f"[INFO] Removed {removed} old custom panels")

    # Find max Y and max panel ID
    max_y = 0
    max_id = 0
    for p in dashboard["panels"]:
        gp = p.get("gridPos", {})
        bottom = gp.get("y", 0) + gp.get("h", 0)
        if bottom > max_y:
            max_y = bottom
        pid = p.get("id", 0)
        if pid > max_id:
            max_id = pid

    base_y = max_y
    next_id = max_id + 1
    ds = {"type": "prometheus", "uid": ds_uid}

    new_panels = [
        # === Row: Hardware Temperature ===
        {
            "id": next_id,
            "type": "row",
            "title": "硬件温度监控",
            "collapsed": False,
            "gridPos": {"h": 1, "w": 24, "x": 0, "y": base_y},
            "panels": [],
        },
        # Temperature panel
        {
            "id": next_id + 1,
            "title": "CPU / 主板温度",
            "type": "timeseries",
            "gridPos": {"h": 8, "w": 24, "x": 0, "y": base_y + 1},
            "datasource": ds,
            "targets": [
                {
                    "expr": 'hw_temperature_celsius{sensor=~"Core.*|CCD.*|Motherboard"}',
                    "legendFormat": "{{hardware}} {{sensor}}",
                    "refId": "A",
                },
                {
                    "expr": 'node_hwmon_temp_celsius{chip=~".*k10temp.*|.*coretemp.*"} * on(instance) group_left() (node_uname_info{nodename="rdagent-node1"} or vector(0))',
                    "legendFormat": "215 CPU {{sensor}}",
                    "refId": "B",
                },
            ],
            "fieldConfig": {
                "defaults": {
                    "unit": "celsius",
                    "custom": {"fillOpacity": 5, "lineWidth": 2},
                    "thresholds": {
                        "mode": "absolute",
                        "steps": [
                            {"color": "green", "value": None},
                            {"color": "yellow", "value": 70},
                            {"color": "red", "value": 85},
                        ],
                    },
                    "color": {"mode": "continuous-GrYlRd"},
                },
                "overrides": [],
            },
            "options": {"tooltip": {"mode": "multi"}},
        },
        # === Row: RDAgent Process ===
        {
            "id": next_id + 2,
            "type": "row",
            "title": "RDAgent 进程监控",
            "collapsed": False,
            "gridPos": {"h": 1, "w": 24, "x": 0, "y": base_y + 9},
            "panels": [],
        },
        # Process RSS by group (textfile collector)
        {
            "id": next_id + 3,
            "title": "Process RSS (rdagent / qlib / python)",
            "type": "timeseries",
            "gridPos": {"h": 8, "w": 12, "x": 0, "y": base_y + 10},
            "datasource": ds,
            "targets": [
                {
                    "expr": 'sum by (group) (process_memory_vmrss_bytes{group=~"rdagent_.*|qlib_qrun|python|uvicorn"})',
                    "legendFormat": "{{group}}",
                    "refId": "A",
                },
                {
                    "expr": 'namedprocess_namegroup_memory_bytes{memtype="resident",groupname=~".*"}',
                    "legendFormat": "215: {{groupname}}",
                    "refId": "B",
                },
            ],
            "fieldConfig": {
                "defaults": {
                    "unit": "bytes",
                    "custom": {"fillOpacity": 10, "lineWidth": 2},
                },
                "overrides": [],
            },
            "options": {"tooltip": {"mode": "multi"}},
        },
        # VmHWM peak
        {
            "id": next_id + 4,
            "title": "VmHWM 内存峰值 (rdagent)",
            "type": "timeseries",
            "gridPos": {"h": 8, "w": 12, "x": 12, "y": base_y + 10},
            "datasource": ds,
            "targets": [
                {
                    "expr": 'process_memory_vmhwm_bytes{group=~"rdagent_.*|qlib_qrun"}',
                    "legendFormat": "HWM {{group}} pid={{pid}}",
                    "refId": "A",
                },
                {
                    "expr": 'process_memory_vmrss_bytes{group=~"rdagent_.*|qlib_qrun"}',
                    "legendFormat": "RSS {{group}} pid={{pid}}",
                    "refId": "B",
                },
            ],
            "fieldConfig": {
                "defaults": {
                    "unit": "bytes",
                    "custom": {"fillOpacity": 5, "lineWidth": 2},
                },
                "overrides": [],
            },
            "options": {"tooltip": {"mode": "multi"}},
        },
        # RSS growth rate
        {
            "id": next_id + 5,
            "title": "RSS 增长速率 (内存泄漏检测)",
            "type": "timeseries",
            "gridPos": {"h": 8, "w": 12, "x": 0, "y": base_y + 18},
            "datasource": ds,
            "targets": [
                {
                    "expr": 'deriv(sum by (group) (process_memory_vmrss_bytes{group=~"rdagent_.*|qlib_qrun"})[5m:])',
                    "legendFormat": "{{group}} growth",
                    "refId": "A",
                },
            ],
            "fieldConfig": {
                "defaults": {
                    "unit": "Bps",
                    "custom": {"fillOpacity": 10, "lineWidth": 2, "gradientMode": "scheme"},
                    "thresholds": {
                        "mode": "absolute",
                        "steps": [
                            {"color": "green", "value": None},
                            {"color": "yellow", "value": 1048576},
                            {"color": "red", "value": 10485760},
                        ],
                    },
                    "color": {"mode": "continuous-GrYlRd"},
                },
                "overrides": [],
            },
            "options": {"tooltip": {"mode": "multi"}},
        },
        # Threads + FDs
        {
            "id": next_id + 6,
            "title": "线程数 / 文件描述符",
            "type": "timeseries",
            "gridPos": {"h": 8, "w": 12, "x": 12, "y": base_y + 18},
            "datasource": ds,
            "targets": [
                {
                    "expr": "process_threads",
                    "legendFormat": "{{group}} threads",
                    "refId": "A",
                },
                {
                    "expr": "process_open_fds",
                    "legendFormat": "{{group}} FDs",
                    "refId": "B",
                },
            ],
            "fieldConfig": {
                "defaults": {
                    "custom": {"fillOpacity": 5, "lineWidth": 2},
                },
                "overrides": [],
            },
            "options": {"tooltip": {"mode": "multi"}},
        },
    ]

    dashboard["panels"].extend(new_panels)
    dashboard["version"] += 1

    # Remove dashboard id for API update
    dashboard.pop("id", None)

    payload = {
        "dashboard": dashboard,
        "folderId": data.get("meta", {}).get("folderId", 0),
        "overwrite": True,
    }

    resp = requests.post(
        f"{GRAFANA_URL}/api/dashboards/db",
        json=payload,
        auth=GRAFANA_AUTH,
        headers={"Content-Type": "application/json"},
    )
    if resp.status_code == 200:
        url = resp.json().get("url", "")
        print(f"[OK] Dashboard patched: {GRAFANA_URL}{url}")
        print(f"     Added {len(new_panels)} panels (2 rows + 5 charts) at y={base_y}")
    else:
        print(f"[ERROR] {resp.status_code}: {resp.text}")


if __name__ == "__main__":
    main()
