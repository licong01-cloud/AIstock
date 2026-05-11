# regime_label_daily.py — cron 部署示例（NOT YET ENABLED）

> **状态**：示例文档。**未在任何环境启用**。需用户授权后由 ops 接入。
> **关联脚本**：`scripts/regime_label_daily.py`（T10/T16）
> **关联设计**：`docs/analysis/regime_label_design_20260510.md`
> **DDL 前置**：`backend/db/init_market_regime_label_20260510.sql`（DRAFT，未应用）

## 触发时机

每个交易日盘后 **16:00 之后**（A 股 15:00 收盘 + Tushare 注入 `market.index_daily` 落地缓冲）。

只在工作日跑（脚本内部已用 `weekday() < 5` 过滤，cron 层无需再约束）。
非交易日（节假日）脚本本身会因 `market.index_daily` 当日缺数据而 raise `ValueError("missing CSI300 data")`，由调度层捕获日志即可。

## 选项 A：systemd timer（推荐）

`/etc/systemd/system/regime-label-daily.service`

```ini
[Unit]
Description=AIstock daily market regime label
After=network-online.target

[Service]
Type=oneshot
User=aistock
Group=aistock
WorkingDirectory=/opt/aistock
EnvironmentFile=/opt/aistock/.env
ExecStart=/opt/aistock/.venv/bin/python scripts/regime_label_daily.py --method simple_quadrant
StandardOutput=journal
StandardError=journal
```

`/etc/systemd/system/regime-label-daily.timer`

```ini
[Unit]
Description=Run regime_label_daily at 16:05 on weekdays

[Timer]
OnCalendar=Mon..Fri 16:05:00
Persistent=true
RandomizedDelaySec=300

[Install]
WantedBy=timers.target
```

启用（**等用户授权后再做**）：

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now regime-label-daily.timer
sudo systemctl list-timers | grep regime
```

## 选项 B：crontab

```cron
# m h dom mon dow command
5 16 * * 1-5 cd /opt/aistock && /opt/aistock/.venv/bin/python scripts/regime_label_daily.py --method simple_quadrant >> /var/log/aistock/regime_label.log 2>&1
```

`logrotate` 配置（`/etc/logrotate.d/aistock-regime`）：

```
/var/log/aistock/regime_label.log {
    weekly
    rotate 8
    compress
    delaycompress
    missingok
    notifempty
}
```

## 干跑验证（部署前）

部署到生产前先干跑最近一个月，确认 `market.index_daily` 数据完整、不抛异常：

```bash
cd /opt/aistock
.venv/bin/python scripts/regime_label_daily.py --dry-run-1m
```

预期：每个工作日打印一行 `YYYY-MM-DD <regime> confidence=<float>`，无 `ERROR` / `Traceback`，无数据库写入。

## Backfill（首次部署）

DDL 应用后、cron 启用前，跑一次历史回填（5 年覆盖 percentile 基线）：

```bash
.venv/bin/python scripts/regime_label_daily.py --backfill --start 2019-01-01 --end 2026-05-10
```

预计耗时：~10 分钟（每日 3 个查询 × ~1300 工作日，单连接串行）。

## 故障观察

- **Tushare 数据延迟**：`ValueError: missing CSI300 data for YYYY-MM-DD` → 让 timer 在 17:00 / 18:00 重试（systemd `OnFailure=` 或 `RandomizedDelaySec` 已留 5 分钟容差）。
- **percentile 历史不足**：`fetch_percentile` 返回 `None` 时 `classify_simple_quadrant` 落到 `('oscillation', 0.0)`，仍会写入 `regime_label`，由消费方判断 `confidence == 0` 跳过。
- **重复触发**：`upsert_regime_label` 用 `ON CONFLICT (trade_date, source_method) DO UPDATE`，重跑幂等。

## 多方法并存

将来加入 `hmm_viterbi` / `bbq` / `ensemble` 时，可并行加 timer：

```ini
ExecStart=/opt/aistock/.venv/bin/python scripts/regime_label_daily.py --method hmm_viterbi
```

`PRIMARY KEY (trade_date, source_method)` 保证多方法标签共存，不互相覆盖。ETL 默认读 `simple_quadrant`，可在 `paper_v2` handler 配置切换。
