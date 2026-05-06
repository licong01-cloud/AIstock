# Prometheus 历史指标清理

AIstock 的 Prometheus 运行在 `monitoring/docker-compose.yml`，当前保留策略为：

- `--storage.tsdb.retention.time=14d`
- `--storage.tsdb.retention.size=30GB`
- `--web.enable-admin-api`
- `--web.enable-lifecycle`

当时间或容量任一条件触发时，Prometheus 会按 TSDB block 删除最老的数据。`retention.size=30GB`
用于控制长期上限，`retention.time=14d` 用于控制最长保留时间；两者同时设置时，先达到的条件生效。

AIstock 后端提供只读与执行接口：

- `GET /api/v1/prometheus-admin/status`
- `POST /api/v1/prometheus-admin/cleanup/preview`
- `POST /api/v1/prometheus-admin/cleanup`

手动执行清理必须提供确认文本：

```json
{
  "older_than_days": 14,
  "confirm_text": "DELETE_PROMETHEUS_HISTORY"
}
```

执行接口会调用 Prometheus Admin API 的 `delete_series`，随后默认调用 `clean_tombstones`。
这会释放 Docker 内部 Prometheus 数据卷中的空间；Windows 宿主机上的 Docker Desktop
`docker_data.vhdx` 通常还需要在 Docker/WSL 停止后单独压缩，F 盘才会实际变小。
