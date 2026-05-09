"""RL 执行模型二维版本管理。

二维版本:
  dev_version: 开发版本 (模型架构/超参/特征修改)
  roll_tag:    滚动标签 (训练数据时间窗口)
  version_tag: dev_version + '-' + roll_tag (自动生成)
"""
from __future__ import annotations

import json
import logging
import math
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import numpy as np
import psycopg2.extras

from ...db.pg_pool import get_conn

logger = logging.getLogger("aistock.rl_execution.registry")


def _sanitize(obj: Any) -> Any:
    """清理 numpy/float 类型，防止 JSON 序列化失败。"""
    if isinstance(obj, (np.floating, float)):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return float(obj)
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize(v) for v in obj]
    return obj


class RLModelRegistry:
    """RL 模型版本注册表。"""

    def register_model(
        self,
        dev_version: str,
        roll_tag: str = "init",
        policy_path: str = "",
        dev_description: str = "",
        parent_dev: str = "",
        train_type: str = "initial",
        train_start: Optional[date] = None,
        train_end: Optional[date] = None,
        purge_end: Optional[date] = None,
        train_epochs: Optional[int] = None,
        train_duration_sec: Optional[int] = None,
        train_config: Optional[dict] = None,
        state_dim: Optional[int] = None,
        action_dim: Optional[int] = None,
        network_arch: str = "",
        status: str = "draft",
    ) -> int:
        """注册新模型版本，返回 id。"""
        sql = """
            INSERT INTO app.rl_model_versions (
                dev_version, roll_tag, policy_path,
                dev_description, parent_dev,
                train_type, train_start, train_end, purge_end,
                train_epochs, train_duration_sec, train_config,
                state_dim, action_dim, network_arch, status
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (dev_version, roll_tag) DO UPDATE SET
                policy_path = EXCLUDED.policy_path,
                dev_description = COALESCE(EXCLUDED.dev_description, app.rl_model_versions.dev_description),
                parent_dev = COALESCE(EXCLUDED.parent_dev, app.rl_model_versions.parent_dev),
                train_type = EXCLUDED.train_type,
                train_start = EXCLUDED.train_start,
                train_end = EXCLUDED.train_end,
                purge_end = EXCLUDED.purge_end,
                train_epochs = EXCLUDED.train_epochs,
                train_duration_sec = EXCLUDED.train_duration_sec,
                train_config = EXCLUDED.train_config,
                state_dim = EXCLUDED.state_dim,
                action_dim = EXCLUDED.action_dim,
                network_arch = EXCLUDED.network_arch,
                status = EXCLUDED.status
            RETURNING id
        """
        config_json = psycopg2.extras.Json(_sanitize(train_config)) if train_config else None
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (
                    dev_version, roll_tag, policy_path,
                    dev_description or None, parent_dev or None,
                    train_type, train_start, train_end, purge_end,
                    train_epochs, train_duration_sec, config_json,
                    state_dim, action_dim, network_arch or None, status,
                ))
                row_id = cur.fetchone()[0]
        logger.info(f"Registered model {dev_version}-{roll_tag} (id={row_id})")
        return row_id

    def update_eval(
        self,
        dev_version: str,
        roll_tag: str,
        oracle_gap_bps: Optional[float] = None,
        pa_bps: Optional[float] = None,
        ffr: Optional[float] = None,
        vs_twap_bps: Optional[float] = None,
        urgency_cost_bps: Optional[float] = None,
        details: Optional[dict] = None,
    ) -> None:
        """更新评估指标。"""
        sql = """
            UPDATE app.rl_model_versions SET
                eval_oracle_gap_bps = COALESCE(%s, eval_oracle_gap_bps),
                eval_pa_bps = COALESCE(%s, eval_pa_bps),
                eval_ffr = COALESCE(%s, eval_ffr),
                eval_vs_twap_bps = COALESCE(%s, eval_vs_twap_bps),
                eval_urgency_cost_bps = COALESCE(%s, eval_urgency_cost_bps),
                eval_details = COALESCE(%s, eval_details)
            WHERE dev_version = %s AND roll_tag = %s
        """
        details_json = psycopg2.extras.Json(_sanitize(details)) if details else None
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (
                    _sanitize(oracle_gap_bps), _sanitize(pa_bps),
                    _sanitize(ffr), _sanitize(vs_twap_bps),
                    _sanitize(urgency_cost_bps), details_json,
                    dev_version, roll_tag,
                ))
        logger.info(f"Updated eval for {dev_version}-{roll_tag}")

    def activate(self, dev_version: str, roll_tag: str) -> None:
        """激活指定版本。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE app.rl_model_versions
                       SET status = 'active', activated_at = NOW()
                       WHERE dev_version = %s AND roll_tag = %s""",
                    (dev_version, roll_tag),
                )

    def deactivate(self, dev_version: str, roll_tag: str) -> None:
        """停用指定版本。"""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE app.rl_model_versions
                       SET status = 'inactive'
                       WHERE dev_version = %s AND roll_tag = %s""",
                    (dev_version, roll_tag),
                )

    def get_latest_active(self, dev_version: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """获取最新激活的模型。"""
        sql = """
            SELECT * FROM app.rl_model_versions
            WHERE status = 'active'
        """
        params: list = []
        if dev_version:
            sql += " AND dev_version = %s"
            params.append(dev_version)
        sql += " ORDER BY activated_at DESC NULLS LAST LIMIT 1"

        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
                return dict(row) if row else None

    def get_for_backtest(self, backtest_start: date) -> Optional[Dict[str, Any]]:
        """获取符合 purge gap 约束的最新激活模型。

        规则: purge_end <= backtest_start (即 train_end + 3月 <= 回测起始日)
        """
        sql = """
            SELECT * FROM app.rl_model_versions
            WHERE status = 'active' AND purge_end <= %s
            ORDER BY train_end DESC NULLS LAST LIMIT 1
        """
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, (backtest_start,))
                row = cur.fetchone()
                return dict(row) if row else None

    def list_versions(
        self,
        dev_version: Optional[str] = None,
        status: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """列出版本。"""
        sql = "SELECT * FROM app.rl_model_versions WHERE 1=1"
        params: list = []
        if dev_version:
            sql += " AND dev_version = %s"
            params.append(dev_version)
        if status:
            sql += " AND status = %s"
            params.append(status)
        sql += " ORDER BY dev_version, roll_tag"

        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                return [dict(r) for r in cur.fetchall()]

    def compare_versions(self, version_tags: List[str]) -> List[Dict[str, Any]]:
        """多版本指标对比。version_tags 格式: ["v1-init", "v2-init"]"""
        if not version_tags:
            return []
        placeholders = ",".join(["%s"] * len(version_tags))
        sql = f"""
            SELECT version_tag, dev_version, roll_tag, train_end,
                   eval_oracle_gap_bps, eval_pa_bps, eval_ffr,
                   eval_vs_twap_bps, eval_urgency_cost_bps, status
            FROM app.rl_model_versions
            WHERE version_tag IN ({placeholders})
            ORDER BY dev_version, roll_tag
        """
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, version_tags)
                return [dict(r) for r in cur.fetchall()]

    def get_dev_lineage(self, dev_version: Optional[str] = None) -> List[Dict[str, Any]]:
        """开发版本谱系: 每个 dev_version 的描述、parent、滚动版本数。"""
        sql = """
            SELECT dev_version,
                   MAX(dev_description) AS dev_description,
                   MAX(parent_dev) AS parent_dev,
                   COUNT(*) AS roll_count,
                   MAX(train_end) AS latest_train_end,
                   ARRAY_AGG(roll_tag ORDER BY roll_tag) AS roll_tags
            FROM app.rl_model_versions
        """
        params: list = []
        if dev_version:
            sql += " WHERE dev_version = %s"
            params.append(dev_version)
        sql += " GROUP BY dev_version ORDER BY dev_version"

        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                return [dict(r) for r in cur.fetchall()]


model_registry = RLModelRegistry()
