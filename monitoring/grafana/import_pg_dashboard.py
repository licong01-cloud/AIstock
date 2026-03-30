"""
导入自定义 PostgreSQL 综合监控仪表盘到 Grafana
覆盖: Session/等待事件/锁/缓存命中/表膨胀/索引使用/数据库大小/长查询
"""
import json
import requests
import time

GRAFANA_URL = "http://localhost:3001"
GRAFANA_AUTH = ("admin", "aistock2026")

def wait_grafana():
    for i in range(30):
        try:
            r = requests.get(f"{GRAFANA_URL}/api/health", timeout=3)
            if r.ok:
                return True
        except:
            pass
        time.sleep(1)
    raise RuntimeError("Grafana not ready")

dashboard = {
    "dashboard": {
        "id": None,
        "uid": "pg-advanced",
        "title": "PostgreSQL 高级监控",
        "tags": ["postgresql", "aistock"],
        "timezone": "browser",
        "schemaVersion": 39,
        "refresh": "30s",
        "time": {"from": "now-1h", "to": "now"},
        "templating": {
            "list": [
                {
                    "name": "datasource",
                    "type": "datasource",
                    "query": "prometheus",
                    "current": {"text": "Prometheus", "value": "Prometheus"},
                }
            ]
        },
        "panels": [
            # ===== Row: 概览 =====
            {
                "type": "row",
                "title": "📊 概览",
                "gridPos": {"h": 1, "w": 24, "x": 0, "y": 0},
                "collapsed": False,
            },
            # 数据库大小
            {
                "type": "stat",
                "title": "数据库大小",
                "datasource": {"type": "prometheus", "uid": "${datasource}"},
                "targets": [
                    {
                        "expr": 'pg_database_size_size_bytes{datname="aistock"}',
                        "legendFormat": "aistock",
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "unit": "bytes",
                        "thresholds": {
                            "steps": [
                                {"color": "green", "value": None},
                                {"color": "yellow", "value": 5e9},
                                {"color": "red", "value": 20e9},
                            ]
                        },
                    }
                },
                "gridPos": {"h": 4, "w": 4, "x": 0, "y": 1},
            },
            # 活跃连接数
            {
                "type": "stat",
                "title": "活跃连接数",
                "datasource": {"type": "prometheus", "uid": "${datasource}"},
                "targets": [
                    {
                        "expr": 'sum(pg_session_counts_count{state="active"})',
                        "legendFormat": "active",
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "thresholds": {
                            "steps": [
                                {"color": "green", "value": None},
                                {"color": "yellow", "value": 10},
                                {"color": "red", "value": 50},
                            ]
                        },
                    }
                },
                "gridPos": {"h": 4, "w": 4, "x": 4, "y": 1},
            },
            # 总连接数
            {
                "type": "stat",
                "title": "总连接数",
                "datasource": {"type": "prometheus", "uid": "${datasource}"},
                "targets": [
                    {
                        "expr": "sum(pg_session_counts_count)",
                        "legendFormat": "total",
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "thresholds": {
                            "steps": [
                                {"color": "green", "value": None},
                                {"color": "yellow", "value": 50},
                                {"color": "red", "value": 100},
                            ]
                        },
                    }
                },
                "gridPos": {"h": 4, "w": 4, "x": 8, "y": 1},
            },
            # 缓存命中率
            {
                "type": "gauge",
                "title": "Buffer 缓存命中率",
                "datasource": {"type": "prometheus", "uid": "${datasource}"},
                "targets": [
                    {
                        "expr": 'pg_cache_hit_ratio_cache_hit_ratio{datname="aistock"}',
                        "legendFormat": "hit ratio",
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "unit": "percentunit",
                        "min": 0,
                        "max": 1,
                        "thresholds": {
                            "steps": [
                                {"color": "red", "value": None},
                                {"color": "yellow", "value": 0.9},
                                {"color": "green", "value": 0.99},
                            ]
                        },
                    }
                },
                "gridPos": {"h": 4, "w": 4, "x": 12, "y": 1},
            },
            # 被锁查询数
            {
                "type": "stat",
                "title": "被锁查询数",
                "datasource": {"type": "prometheus", "uid": "${datasource}"},
                "targets": [
                    {
                        "expr": "pg_blocking_queries_blocked_count",
                        "legendFormat": "blocked",
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "thresholds": {
                            "steps": [
                                {"color": "green", "value": None},
                                {"color": "orange", "value": 1},
                                {"color": "red", "value": 5},
                            ]
                        },
                    }
                },
                "gridPos": {"h": 4, "w": 4, "x": 16, "y": 1},
            },
            # Buffers 写入速率
            {
                "type": "timeseries",
                "title": "Buffers 写入速率",
                "datasource": {"type": "prometheus", "uid": "${datasource}"},
                "targets": [
                    {"expr": "rate(pg_stat_bgwriter_buffers_checkpoint_total[5m])", "legendFormat": "checkpoint"},
                    {"expr": "rate(pg_stat_bgwriter_buffers_clean_total[5m])", "legendFormat": "bgwriter"},
                    {"expr": "rate(pg_stat_bgwriter_buffers_backend_total[5m])", "legendFormat": "backend"},
                    {"expr": "rate(pg_stat_bgwriter_buffers_alloc_total[5m])", "legendFormat": "alloc"},
                ],
                "fieldConfig": {"defaults": {"unit": "ops", "custom": {"fillOpacity": 20, "lineWidth": 2}}},
                "gridPos": {"h": 4, "w": 4, "x": 20, "y": 1},
            },
            # ===== Row: 连接与会话 =====
            {
                "type": "row",
                "title": "🔌 连接与会话",
                "gridPos": {"h": 1, "w": 24, "x": 0, "y": 5},
                "collapsed": False,
            },
            # 会话状态分布 (时序)
            {
                "type": "timeseries",
                "title": "会话状态趋势",
                "datasource": {"type": "prometheus", "uid": "${datasource}"},
                "targets": [
                    {"expr": 'pg_session_counts_count{datname="aistock", state="active"}', "legendFormat": "active"},
                    {"expr": 'pg_session_counts_count{datname="aistock", state="idle"}', "legendFormat": "idle"},
                    {"expr": 'pg_session_counts_count{datname="aistock", state="idle in transaction"}', "legendFormat": "idle in txn"},
                ],
                "fieldConfig": {"defaults": {"custom": {"fillOpacity": 30, "lineWidth": 2, "stacking": {"mode": "normal"}}}},
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 6},
            },
            # 会话状态饼图
            {
                "type": "piechart",
                "title": "当前会话状态分布",
                "datasource": {"type": "prometheus", "uid": "${datasource}"},
                "targets": [
                    {"expr": 'pg_session_counts_count{datname="aistock"}', "legendFormat": "{{state}}"},
                ],
                "gridPos": {"h": 8, "w": 6, "x": 12, "y": 6},
            },
            # 等待事件
            {
                "type": "bargauge",
                "title": "活跃等待事件",
                "datasource": {"type": "prometheus", "uid": "${datasource}"},
                "targets": [
                    {"expr": "pg_wait_events_session_count > 0", "legendFormat": "{{wait_event_type}}: {{wait_event}}"},
                ],
                "fieldConfig": {
                    "defaults": {
                        "thresholds": {
                            "steps": [
                                {"color": "green", "value": None},
                                {"color": "orange", "value": 3},
                                {"color": "red", "value": 10},
                            ]
                        }
                    }
                },
                "gridPos": {"h": 8, "w": 6, "x": 18, "y": 6},
            },
            # ===== Row: 活跃查询 =====
            {
                "type": "row",
                "title": "🔍 活跃查询与长事务",
                "gridPos": {"h": 1, "w": 24, "x": 0, "y": 14},
                "collapsed": False,
            },
            # 活跃 Session 表格
            {
                "type": "table",
                "title": "活跃会话详情",
                "datasource": {"type": "prometheus", "uid": "${datasource}"},
                "targets": [
                    {
                        "expr": 'pg_active_sessions_query_duration_seconds{state=~"active|idle in transaction"}',
                        "format": "table",
                        "instant": True,
                    }
                ],
                "transformations": [
                    {"id": "organize", "options": {
                        "excludeByName": {"Time": True, "__name__": True, "job": True, "instance": True, "server": True, "backend_type": True},
                        "renameByName": {
                            "datname": "数据库",
                            "usename": "用户",
                            "application_name": "应用",
                            "client_addr": "客户端IP",
                            "state": "状态",
                            "wait_event_type": "等待类型",
                            "wait_event": "等待事件",
                            "Value": "持续时间(秒)",
                            "query_text": "SQL (截断)",
                        },
                    }},
                    {"id": "sortBy", "options": {"fields": [{"desc": True, "displayName": "持续时间(秒)"}]}},
                ],
                "fieldConfig": {
                    "overrides": [
                        {"matcher": {"id": "byName", "options": "持续时间(秒)"}, "properties": [{"id": "unit", "value": "s"}, {"id": "decimals", "value": 1}]},
                        {"matcher": {"id": "byName", "options": "SQL (截断)"}, "properties": [{"id": "custom.width", "value": 400}]},
                    ]
                },
                "gridPos": {"h": 8, "w": 24, "x": 0, "y": 15},
            },
            # ===== Row: 锁 =====
            {
                "type": "row",
                "title": "🔒 锁信息",
                "gridPos": {"h": 1, "w": 24, "x": 0, "y": 23},
                "collapsed": False,
            },
            # 锁类型分布
            {
                "type": "timeseries",
                "title": "锁类型分布趋势",
                "datasource": {"type": "prometheus", "uid": "${datasource}"},
                "targets": [
                    {"expr": 'pg_locks_count_lock_count{lock_status="granted"}', "legendFormat": "{{mode}} (granted)"},
                ],
                "fieldConfig": {"defaults": {"custom": {"fillOpacity": 20, "lineWidth": 2}}},
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 24},
            },
            # 等待锁
            {
                "type": "timeseries",
                "title": "等待中的锁",
                "datasource": {"type": "prometheus", "uid": "${datasource}"},
                "targets": [
                    {"expr": 'pg_locks_count_lock_count{lock_status="waiting"}', "legendFormat": "{{mode}} (waiting)"},
                    {"expr": "pg_blocking_queries_blocked_count", "legendFormat": "blocked queries"},
                ],
                "fieldConfig": {
                    "defaults": {
                        "custom": {"fillOpacity": 30, "lineWidth": 2},
                        "thresholds": {"steps": [{"color": "green", "value": None}, {"color": "red", "value": 1}]},
                    }
                },
                "gridPos": {"h": 8, "w": 12, "x": 12, "y": 24},
            },
            # ===== Row: 表与索引 =====
            {
                "type": "row",
                "title": "📦 表与索引",
                "gridPos": {"h": 1, "w": 24, "x": 0, "y": 32},
                "collapsed": False,
            },
            # TOP 表大小 (含 hypertable)
            {
                "type": "table",
                "title": "TOP 表大小 (含 Hypertable)",
                "datasource": {"type": "prometheus", "uid": "${datasource}"},
                "targets": [
                    {"expr": "topk(20, pg_table_size_total_bytes)", "format": "table", "instant": True},
                ],
                "transformations": [
                    {"id": "organize", "options": {
                        "excludeByName": {"Time": True, "__name__": True, "job": True, "instance": True, "server": True},
                        "renameByName": {"schemaname": "Schema", "table_name": "表名", "table_type": "类型", "Value": "总大小"},
                        "indexByName": {"schemaname": 0, "table_name": 1, "table_type": 2, "Value": 3},
                    }},
                    {"id": "sortBy", "options": {"fields": [{"desc": True, "displayName": "总大小"}]}},
                ],
                "fieldConfig": {
                    "overrides": [
                        {"matcher": {"id": "byName", "options": "总大小"}, "properties": [{"id": "unit", "value": "bytes"}]},
                        {"matcher": {"id": "byName", "options": "类型"}, "properties": [
                            {"id": "mappings", "value": [
                                {"type": "value", "options": {"hypertable": {"text": "⏱ 时序表", "color": "blue"}, "regular": {"text": "📋 普通表", "color": "green"}}},
                            ]},
                        ]},
                    ]
                },
                "gridPos": {"h": 10, "w": 12, "x": 0, "y": 33},
            },
            # Hypertable 详情
            {
                "type": "table",
                "title": "TimescaleDB Hypertable 详情",
                "datasource": {"type": "prometheus", "uid": "${datasource}"},
                "targets": [
                    {"expr": "topk(15, pg_hypertable_detail_total_bytes)", "format": "table", "instant": True, "legendFormat": "total"},
                    {"expr": "pg_hypertable_detail_chunk_count", "format": "table", "instant": True, "legendFormat": "chunks"},
                ],
                "transformations": [
                    {"id": "seriesToColumns", "options": {"byField": "table_name"}},
                    {"id": "organize", "options": {
                        "excludeByName": {"Time 1": True, "Time 2": True, "__name__ 1": True, "__name__ 2": True,
                                          "job 1": True, "job 2": True, "instance 1": True, "instance 2": True,
                                          "server 1": True, "server 2": True,
                                          "schemaname 2": True, "num_dimensions": True,
                                          "main_table_bytes": True, "chunks_bytes": True},
                        "renameByName": {"schemaname 1": "Schema", "table_name": "Hypertable",
                                         "Value #total": "总大小", "Value #chunks": "Chunk 数"},
                    }},
                    {"id": "sortBy", "options": {"fields": [{"desc": True, "displayName": "总大小"}]}},
                ],
                "fieldConfig": {
                    "overrides": [
                        {"matcher": {"id": "byName", "options": "总大小"}, "properties": [{"id": "unit", "value": "bytes"}]},
                    ]
                },
                "gridPos": {"h": 10, "w": 12, "x": 12, "y": 33},
            },
            # 表膨胀 - 普通表
            {
                "type": "bargauge",
                "title": "普通表膨胀 (Dead Tuple 比例)",
                "datasource": {"type": "prometheus", "uid": "${datasource}"},
                "targets": [
                    {"expr": "topk(10, pg_table_bloat_dead_ratio > 0)", "legendFormat": "{{schemaname}}.{{table_name}}"},
                ],
                "fieldConfig": {
                    "defaults": {
                        "unit": "percentunit",
                        "thresholds": {
                            "steps": [
                                {"color": "green", "value": None},
                                {"color": "yellow", "value": 0.1},
                                {"color": "red", "value": 0.3},
                            ]
                        },
                    }
                },
                "options": {"orientation": "horizontal"},
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 43},
            },
            # 表膨胀 - Hypertable (时序表)
            {
                "type": "bargauge",
                "title": "Hypertable 膨胀 (Dead Tuple 比例)",
                "datasource": {"type": "prometheus", "uid": "${datasource}"},
                "targets": [
                    {"expr": "topk(10, pg_hypertable_bloat_dead_ratio > 0)", "legendFormat": "{{schemaname}}.{{table_name}}"},
                ],
                "fieldConfig": {
                    "defaults": {
                        "unit": "percentunit",
                        "thresholds": {
                            "steps": [
                                {"color": "green", "value": None},
                                {"color": "yellow", "value": 0.05},
                                {"color": "red", "value": 0.2},
                            ]
                        },
                    }
                },
                "options": {"orientation": "horizontal"},
                "gridPos": {"h": 8, "w": 12, "x": 12, "y": 43},
            },
            # 未使用的索引
            {
                "type": "table",
                "title": "未使用的索引 (浪费空间)",
                "datasource": {"type": "prometheus", "uid": "${datasource}"},
                "targets": [
                    {"expr": "topk(15, pg_unused_indexes_index_bytes)", "format": "table", "instant": True},
                ],
                "transformations": [
                    {"id": "organize", "options": {
                        "excludeByName": {"Time": True, "__name__": True, "job": True, "instance": True, "server": True, "index_scans": True},
                        "renameByName": {"schemaname": "Schema", "table_name": "表名", "index_name": "索引名", "Value": "索引大小"},
                    }},
                    {"id": "sortBy", "options": {"fields": [{"desc": True, "displayName": "索引大小"}]}},
                ],
                "fieldConfig": {
                    "overrides": [
                        {"matcher": {"id": "byName", "options": "索引大小"}, "properties": [{"id": "unit", "value": "bytes"}]},
                    ]
                },
                "gridPos": {"h": 8, "w": 12, "x": 12, "y": 43},
            },
            # 索引使用排行
            {
                "type": "bargauge",
                "title": "TOP 索引使用次数",
                "datasource": {"type": "prometheus", "uid": "${datasource}"},
                "targets": [
                    {"expr": "topk(10, pg_index_usage_index_scans)", "legendFormat": "{{table_name}}.{{index_name}}"},
                ],
                "options": {"orientation": "horizontal"},
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 51},
            },
            # Hypertable 大小对比柱图
            {
                "type": "barchart",
                "title": "Hypertable 大小对比",
                "datasource": {"type": "prometheus", "uid": "${datasource}"},
                "targets": [
                    {"expr": "topk(10, pg_hypertable_detail_total_bytes)", "legendFormat": "{{schemaname}}.{{table_name}}"},
                ],
                "fieldConfig": {"defaults": {"unit": "bytes", "custom": {"fillOpacity": 80}}},
                "options": {"orientation": "horizontal", "xTickLabelRotation": 0},
                "gridPos": {"h": 8, "w": 12, "x": 12, "y": 51},
            },
            # ===== Row: BGWriter/Checkpoint =====
            {
                "type": "row",
                "title": "💾 Checkpoint & BGWriter",
                "gridPos": {"h": 1, "w": 24, "x": 0, "y": 59},
                "collapsed": False,
            },
            # Checkpoint 写入时间
            {
                "type": "timeseries",
                "title": "Checkpoint 写入/同步时间",
                "datasource": {"type": "prometheus", "uid": "${datasource}"},
                "targets": [
                    {"expr": "rate(pg_stat_bgwriter_checkpoint_write_time_total[5m])", "legendFormat": "write time"},
                    {"expr": "rate(pg_stat_bgwriter_checkpoint_sync_time_total[5m])", "legendFormat": "sync time"},
                ],
                "fieldConfig": {"defaults": {"unit": "ms", "custom": {"fillOpacity": 20, "lineWidth": 2}}},
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 60},
            },
            # Buffers 分配趋势
            {
                "type": "timeseries",
                "title": "Buffers 分配趋势",
                "datasource": {"type": "prometheus", "uid": "${datasource}"},
                "targets": [
                    {"expr": "rate(pg_stat_bgwriter_buffers_alloc_total[5m])", "legendFormat": "alloc/s"},
                    {"expr": "rate(pg_stat_bgwriter_buffers_backend_total[5m])", "legendFormat": "backend/s"},
                ],
                "fieldConfig": {"defaults": {"unit": "ops", "custom": {"fillOpacity": 20, "lineWidth": 2}}},
                "gridPos": {"h": 8, "w": 12, "x": 12, "y": 60},
            },
        ],
    },
    "overwrite": True,
}

if __name__ == "__main__":
    wait_grafana()
    r = requests.post(
        f"{GRAFANA_URL}/api/dashboards/db",
        auth=GRAFANA_AUTH,
        headers={"Content-Type": "application/json"},
        json=dashboard,
        timeout=30,
    )
    print(f"Status: {r.status_code}")
    print(r.json())
