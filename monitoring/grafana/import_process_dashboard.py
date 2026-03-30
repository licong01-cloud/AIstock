"""
Import Process Exporter Grafana dashboard + custom rdagent memory panels.

Usage:
    python import_process_dashboard.py

Imports community dashboard #22161 and adds custom panels for:
- rdagent RSS timeline (by process cmdline)
- VmPeak/VmHWM timeline (from textfile collector)
- Memory growth rate alert panel
"""

import json
import requests

GRAFANA_URL = "http://localhost:3001"
GRAFANA_USER = "admin"
GRAFANA_PASSWORD = "aistock2026"
DATASOURCE_NAME = "Prometheus"


def get_session():
    s = requests.Session()
    s.auth = (GRAFANA_USER, GRAFANA_PASSWORD)
    s.headers.update({"Content-Type": "application/json"})
    return s


def get_datasource_uid(session):
    resp = session.get(f"{GRAFANA_URL}/api/datasources/name/{DATASOURCE_NAME}")
    resp.raise_for_status()
    return resp.json()["uid"]


def import_community_dashboard(session, ds_uid):
    """Import community dashboard #22161 (Named Processes)."""
    # Fetch dashboard JSON from Grafana.com API
    resp = requests.get("https://grafana.com/api/dashboards/22161/revisions/1/download")
    resp.raise_for_status()
    dashboard_json = resp.json()

    payload = {
        "dashboard": dashboard_json,
        "overwrite": True,
        "inputs": [
            {
                "name": "DS_PROMETHEUS",
                "type": "datasource",
                "pluginId": "prometheus",
                "value": ds_uid,
            },
            {
                "name": "VAR_PORT_NODE_EXPORTER",
                "type": "constant",
                "value": "9256",
            },
        ],
        "folderId": 0,
    }

    resp = session.post(f"{GRAFANA_URL}/api/dashboards/import", json=payload)
    if resp.status_code == 200:
        url = resp.json().get("importedUrl", "")
        print(f"[OK] Community dashboard #22161 imported: {GRAFANA_URL}{url}")
    else:
        print(f"[WARN] Community dashboard import: {resp.status_code} {resp.text}")


def create_custom_dashboard(session, ds_uid):
    """Create custom rdagent process memory dashboard."""
    dashboard = {
        "dashboard": {
            "title": "RDAgent Process Memory",
            "tags": ["rdagent", "process", "memory"],
            "timezone": "browser",
            "refresh": "15s",
            "time": {"from": "now-6h", "to": "now"},
            "panels": [
                # Panel 1: RSS by process group (process-exporter)
                {
                    "id": 1,
                    "title": "Process RSS (process-exporter)",
                    "type": "timeseries",
                    "gridPos": {"h": 8, "w": 24, "x": 0, "y": 0},
                    "targets": [
                        {
                            "expr": 'namedprocess_namegroup_memory_bytes{memtype="resident"}',
                            "legendFormat": "{{groupname}}",
                            "datasource": {"uid": ds_uid},
                        }
                    ],
                    "fieldConfig": {
                        "defaults": {
                            "unit": "bytes",
                            "custom": {"fillOpacity": 10},
                        }
                    },
                },
                # Panel 2: rdagent-specific RSS focus
                {
                    "id": 2,
                    "title": "RDAgent / Qlib RSS",
                    "type": "timeseries",
                    "gridPos": {"h": 8, "w": 12, "x": 0, "y": 8},
                    "targets": [
                        {
                            "expr": 'namedprocess_namegroup_memory_bytes{memtype="resident",groupname=~"fin_.*|qlib_qrun"}',
                            "legendFormat": "{{groupname}}",
                            "datasource": {"uid": ds_uid},
                        }
                    ],
                    "fieldConfig": {
                        "defaults": {
                            "unit": "bytes",
                            "custom": {"fillOpacity": 10},
                        }
                    },
                },
                # Panel 3: VmPeak/VmHWM from textfile collector
                {
                    "id": 3,
                    "title": "VmPeak / VmHWM (textfile collector)",
                    "type": "timeseries",
                    "gridPos": {"h": 8, "w": 12, "x": 12, "y": 8},
                    "targets": [
                        {
                            "expr": 'process_memory_vmhwm_bytes{cmdline_short=~".*rdagent.*|.*qrun.*"}',
                            "legendFormat": "HWM pid={{pid}}",
                            "datasource": {"uid": ds_uid},
                        },
                        {
                            "expr": 'process_memory_vmpeak_bytes{cmdline_short=~".*rdagent.*|.*qrun.*"}',
                            "legendFormat": "VmPeak pid={{pid}}",
                            "datasource": {"uid": ds_uid},
                        },
                    ],
                    "fieldConfig": {
                        "defaults": {
                            "unit": "bytes",
                            "custom": {"fillOpacity": 5},
                        }
                    },
                },
                # Panel 4: Memory growth rate (deriv)
                {
                    "id": 4,
                    "title": "RSS Growth Rate (bytes/sec)",
                    "type": "timeseries",
                    "gridPos": {"h": 8, "w": 24, "x": 0, "y": 16},
                    "targets": [
                        {
                            "expr": 'deriv(namedprocess_namegroup_memory_bytes{memtype="resident",groupname=~"fin_.*|qlib_qrun"}[5m])',
                            "legendFormat": "{{groupname}} growth",
                            "datasource": {"uid": ds_uid},
                        }
                    ],
                    "fieldConfig": {
                        "defaults": {
                            "unit": "Bps",
                            "custom": {"fillOpacity": 10},
                            "thresholds": {
                                "steps": [
                                    {"color": "green", "value": None},
                                    {"color": "yellow", "value": 1048576},
                                    {"color": "red", "value": 10485760},
                                ]
                            },
                        }
                    },
                },
                # Panel 5: Thread count
                {
                    "id": 5,
                    "title": "Thread Count",
                    "type": "timeseries",
                    "gridPos": {"h": 8, "w": 12, "x": 0, "y": 24},
                    "targets": [
                        {
                            "expr": 'namedprocess_namegroup_num_threads{groupname=~"fin_.*|qlib_qrun|python"}',
                            "legendFormat": "{{groupname}}",
                            "datasource": {"uid": ds_uid},
                        }
                    ],
                },
                # Panel 6: Open file descriptors
                {
                    "id": 6,
                    "title": "Open File Descriptors",
                    "type": "timeseries",
                    "gridPos": {"h": 8, "w": 12, "x": 12, "y": 24},
                    "targets": [
                        {
                            "expr": 'namedprocess_namegroup_open_filedesc{groupname=~"fin_.*|qlib_qrun|python"}',
                            "legendFormat": "{{groupname}}",
                            "datasource": {"uid": ds_uid},
                        }
                    ],
                },
            ],
            "uid": "rdagent-process-memory",
            "version": 1,
        },
        "overwrite": True,
        "folderId": 0,
    }

    resp = session.post(f"{GRAFANA_URL}/api/dashboards/db", json=dashboard)
    if resp.status_code == 200:
        url = resp.json().get("url", "")
        print(f"[OK] Custom dashboard created: {GRAFANA_URL}{url}")
    else:
        print(f"[ERROR] Custom dashboard: {resp.status_code} {resp.text}")


def main():
    session = get_session()

    # Verify Grafana connectivity
    resp = session.get(f"{GRAFANA_URL}/api/health")
    resp.raise_for_status()
    print(f"[OK] Grafana connected: {resp.json()}")

    ds_uid = get_datasource_uid(session)
    print(f"[OK] Datasource UID: {ds_uid}")

    import_community_dashboard(session, ds_uid)
    create_custom_dashboard(session, ds_uid)

    print("\nDone. Dashboards available at:")
    print(f"  Community: {GRAFANA_URL}/d/ (check Grafana sidebar)")
    print(f"  Custom:    {GRAFANA_URL}/d/rdagent-process-memory/rdagent-process-memory")


if __name__ == "__main__":
    main()
