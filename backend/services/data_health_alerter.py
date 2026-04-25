"""Data health alert generator.

Consumes DatasetCheckResult objects from data_completeness.py and:
1. Determines severity based on configurable rules
2. Writes alerts to market.data_alerts table
3. Sends email notifications for error/critical alerts via NotificationService
4. Deduplicates: same dataset + alert_type on the same calendar day → skip

Usage::

    from .data_completeness import DataCompletenessChecker
    from .data_health_alerter import DataHealthAlerter

    checker = DataCompletenessChecker(db_cfg)
    results = checker.check_all()

    alerter = DataHealthAlerter(db_cfg)
    alerts = alerter.generate(results, stage="freshness_check")
    alerter.flush(alerts)
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import psycopg2

from ..db.pg_pool import get_conn as _pool_get_conn

_logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Alert data class
# ---------------------------------------------------------------------------

@dataclass
class DataAlert:
    severity: str           # info | warning | error | critical
    dataset: str
    alert_type: str         # stale | low_coverage | gap | zero_rows | api_failure | retry_exhausted
    title: str
    message: str
    details: Dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Severity rules
# ---------------------------------------------------------------------------

def classify_completeness_alert(
    status: str,
    dataset: str,
    coverage_pct: Optional[float],
    is_fresh: bool,
    consecutive_failures: int = 0,
) -> Optional[DataAlert]:
    """Classify a DatasetCheckResult into an alert (or None if no action needed)."""
    if status == "ok":
        return None

    # empty table → critical
    if status == "empty":
        return DataAlert(
            severity="critical",
            dataset=dataset,
            alert_type="stale",
            title=f"{dataset} 表为空",
            message=f"数据集 {dataset} 表中无任何数据，请检查同步任务是否正常",
            details={"status": status},
        )

    # stale (not fresh) — severity depends on lag
    if status == "stale":
        if consecutive_failures >= 2:
            sev = "critical"
        else:
            sev = "error"
        return DataAlert(
            severity=sev,
            dataset=dataset,
            alert_type="stale",
            title=f"{dataset} 数据未更新",
            message=f"数据集 {dataset} 数据滞后于最新交易日",
            details={"status": status, "consecutive_failures": consecutive_failures},
        )

    # low coverage
    if status == "low_coverage":
        if coverage_pct is not None and coverage_pct < 0.70:
            sev = "critical"
        elif coverage_pct is not None and coverage_pct < 0.90:
            sev = "warning"
        else:
            sev = "warning"
        return DataAlert(
            severity=sev,
            dataset=dataset,
            alert_type="low_coverage",
            title=f"{dataset} 数据覆盖率不足",
            message=f"数据集 {dataset} 最新日覆盖率仅 {coverage_pct:.1%}" if coverage_pct else f"数据集 {dataset} 数据覆盖率不足",
            details={"status": status, "coverage_pct": coverage_pct},
        )

    # gap
    if status == "gap":
        return DataAlert(
            severity="error",
            dataset=dataset,
            alert_type="gap",
            title=f"{dataset} 存在数据间隙",
            message=f"数据集 {dataset} 存在中间日期数据缺失",
            details={"status": status},
        )

    # error
    if status == "error":
        return DataAlert(
            severity="error",
            dataset=dataset,
            alert_type="api_failure",
            title=f"{dataset} 数据检查失败",
            message=f"数据集 {dataset} 数据完整性检查执行出错",
            details={"status": status},
        )

    return None


def classify_retry_alert(
    dataset: str,
    retry_result: str,  # "recovered" | "exhausted"
    original_status: str = "",
) -> DataAlert:
    """Classify a retry outcome into an alert."""
    if retry_result == "recovered":
        return DataAlert(
            severity="info",
            dataset=dataset,
            alert_type="stale",
            title=f"{dataset} 重试后已恢复",
            message=f"数据集 {dataset} 经自动重试后数据已补齐",
            details={"retry_result": retry_result},
        )
    else:  # exhausted
        return DataAlert(
            severity="error",
            dataset=dataset,
            alert_type="retry_exhausted",
            title=f"{dataset} 重试耗尽仍失败",
            message=f"数据集 {dataset} 经3次自动重试后仍未成功，需人工介入",
            details={"retry_result": retry_result, "original_status": original_status},
        )


def classify_zero_rows_alert(dataset: str) -> DataAlert:
    return DataAlert(
        severity="error",
        dataset=dataset,
        alert_type="zero_rows",
        title=f"{dataset} 同步成功但插入0行",
        message=f"数据集 {dataset} 任务报告成功但未插入任何数据行，已将重新触发同步",
        details={},
    )


# ---------------------------------------------------------------------------
# Alerter
# ---------------------------------------------------------------------------

# Default DB config — override via constructor or env
_DEFAULT_DB_CFG = dict(
    host=os.getenv("TDX_DB_HOST", "localhost"),
    port=int(os.getenv("TDX_DB_PORT", "5432")),
    user=os.getenv("TDX_DB_USER", "postgres"),
    password=os.getenv("TDX_DB_PASSWORD", ""),
    dbname=os.getenv("TDX_DB_NAME", "aistock"),
)


class DataHealthAlerter:
    """Write alerts to DB and send email notifications."""

    def __init__(self, db_cfg: Optional[Dict[str, Any]] = None) -> None:
        self._db_cfg = db_cfg or _DEFAULT_DB_CFG
        self._notification_service = None  # lazy

    def _conn(self):
        return psycopg2.connect(**self._db_cfg)

    def _get_notification_service(self):
        if self._notification_service is None:
            try:
                from notification_service import NotificationService
                self._notification_service = NotificationService()
            except Exception as exc:
                _logger.warning("NotificationService not available, email alerts disabled: %s", exc)
                self._notification_service = False
        return self._notification_service if self._notification_service else None

    # -- DB operations --------------------------------------------------------

    def _alert_exists_today(self, dataset: str, alert_type: str) -> bool:
        """Check if an alert for this dataset+type was already created today."""
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM market.data_alerts"
                    " WHERE dataset = %s AND alert_type = %s"
                    "   AND created_at >= CURRENT_DATE"
                    " LIMIT 1",
                    (dataset, alert_type),
                )
                return cur.fetchone() is not None

    def _insert_alert(self, alert: DataAlert) -> Optional[str]:
        """Insert an alert row. Returns alert_id string or None on dedup."""
        if self._alert_exists_today(alert.dataset, alert.alert_type):
            _logger.debug("dedup: skip %s/%s (already alerted today)", alert.dataset, alert.alert_type)
            return None

        alert_id = str(uuid.uuid4())
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO market.data_alerts
                       (alert_id, severity, dataset, alert_type, title, message, details)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    (
                        alert_id, alert.severity, alert.dataset, alert.alert_type,
                        alert.title, alert.message,
                        json.dumps(alert.details, ensure_ascii=False, default=str),
                    ),
                )
        return alert_id

    # -- email ----------------------------------------------------------------

    def _should_email(self, alert: DataAlert) -> bool:
        """Check if email notifications are enabled and this severity warrants it."""
        enabled = (os.getenv("DATA_ALERT_EMAIL_ENABLED") or "").strip().lower() in {"1", "true", "yes"}
        if not enabled:
            return False
        return alert.severity in {"error", "critical"}

    def _send_email(self, alerts: List[DataAlert]) -> None:
        """Send email notification for error/critical alerts."""
        svc = self._get_notification_service()
        if not svc:
            return

        email_alerts = [a for a in alerts if self._should_email(a)]
        if not email_alerts:
            return

        # Build email body
        lines = ["AIstock 数据健康报警", "=" * 40, ""]
        for a in email_alerts:
            lines.append(f"[{a.severity.upper()}] {a.title}")
            lines.append(f"  数据集: {a.dataset}")
            lines.append(f"  {a.message}")
            lines.append("")
        body = "\n".join(lines)

        try:
            svc.send_notification(subject=f"[AIstock数据报警] {len(email_alerts)} 个数据集需要关注", body=body)
            _logger.info("sent email alert for %d datasets", len(email_alerts))
        except Exception as exc:
            _logger.error("failed to send email alert: %s", exc)

    # -- public API -----------------------------------------------------------

    def generate(
        self,
        check_results: List[Any],  # List[DatasetCheckResult]
        stage: str = "freshness_check",
        retry_outcomes: Optional[Dict[str, str]] = None,  # dataset → "recovered"|"exhausted"
        zero_rows_datasets: Optional[List[str]] = None,
        consecutive_map: Optional[Dict[str, int]] = None,
    ) -> List[DataAlert]:
        """Generate alerts from check results and retry outcomes.

        Args:
            check_results: from DataCompletenessChecker.check_all()
            stage: "freshness_check" | "auto_retry" | "weekend_compensation"
            retry_outcomes: dataset → "recovered" | "exhausted" (auto_retry stage)
            zero_rows_datasets: datasets that succeeded but inserted 0 rows
            consecutive_map: dataset → consecutive failure count
        """
        alerts: List[DataAlert] = []
        consecutive_map = consecutive_map or {}

        # From completeness check results
        for r in check_results:
            status = getattr(r, "status", "ok")
            if status == "ok":
                continue
            dataset = getattr(r, "dataset", "?")
            coverage = getattr(r, "coverage_pct", None)
            is_fresh = getattr(r, "is_fresh", False)
            consecutive = consecutive_map.get(dataset, 0)

            alert = classify_completeness_alert(status, dataset, coverage, is_fresh, consecutive)
            if alert:
                alerts.append(alert)

        # From retry outcomes
        if retry_outcomes:
            for ds, outcome in retry_outcomes.items():
                alert = classify_retry_alert(ds, outcome)
                if alert:
                    alerts.append(alert)

        # From zero-rows detection
        if zero_rows_datasets:
            for ds in zero_rows_datasets:
                alerts.append(classify_zero_rows_alert(ds))

        # Bump severity for weekend compensation failures
        if stage == "weekend_compensation":
            for a in alerts:
                if a.severity == "error":
                    a.severity = "critical"
                    a.title = f"[周末补偿失败] {a.title}"

        return alerts

    def flush(self, alerts: List[DataAlert]) -> Dict[str, int]:
        """Write alerts to DB and send emails. Returns {severity: count}."""
        counts: Dict[str, int] = {}
        email_alerts: List[DataAlert] = []

        for alert in alerts:
            aid = self._insert_alert(alert)
            if aid:
                counts[alert.severity] = counts.get(alert.severity, 0) + 1
                if self._should_email(alert):
                    email_alerts.append(alert)

        if email_alerts:
            self._send_email(email_alerts)

        if counts:
            _logger.info("flushed %d alerts: %s", sum(counts.values()), counts)
        return counts
