"""RL 执行模型版本管理 — Pydantic 请求/响应模型。"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class RLModelVersion(BaseModel):
    id: int
    dev_version: str
    roll_tag: str
    version_tag: str
    dev_description: str | None = None
    parent_dev: str | None = None
    policy_path: str
    train_type: str
    train_start: date | None = None
    train_end: date | None = None
    purge_end: date | None = None
    train_epochs: int | None = None
    train_duration_sec: int | None = None
    train_config: Dict[str, Any] | None = None
    state_dim: int | None = None
    action_dim: int | None = None
    network_arch: str | None = None
    eval_oracle_gap_bps: float | None = None
    eval_pa_bps: float | None = None
    eval_ffr: float | None = None
    eval_vs_twap_bps: float | None = None
    eval_urgency_cost_bps: float | None = None
    eval_details: Dict[str, Any] | None = None
    status: str
    created_at: datetime
    activated_at: datetime | None = None


class RLModelCompareRequest(BaseModel):
    version_tags: List[str]


class RLModelCompareItem(BaseModel):
    version_tag: str
    dev_version: str
    roll_tag: str
    train_end: date | None = None
    eval_oracle_gap_bps: float | None = None
    eval_pa_bps: float | None = None
    eval_ffr: float | None = None
    eval_vs_twap_bps: float | None = None
    eval_urgency_cost_bps: float | None = None
    status: str


class RLDevLineage(BaseModel):
    dev_version: str
    dev_description: str | None = None
    parent_dev: str | None = None
    roll_count: int
    latest_train_end: date | None = None
    roll_tags: List[str]
