"""RL 模型部署 — 加载 policy 用于 Qlib 回测推理。

用法:
  from backend.services.rl_execution.deploy import load_policy

  # 按 purge gap 自动选择
  policy = load_policy(backtest_start=date(2026, 1, 1))

  # 指定版本
  policy = load_policy(version_tag="v1-init")
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Optional

import torch

logger = logging.getLogger("aistock.rl_execution.deploy")


class RLPolicy:
    """RL 策略推理封装。

    加载训练好的 policy.pt，提供 predict() 接口供 RLExecutionStrategy 调用。
    """

    def __init__(self, checkpoint_path: str, device: str = "cpu") -> None:
        self.device = torch.device(device)
        ckpt = torch.load(checkpoint_path, map_location=self.device, weights_only=False)

        from rl_execution.network.mlp_network import ExecutionMLP

        # 从 checkpoint 或 config 推断网络参数
        config = ckpt.get("config", {})
        net_cfg = config.get("network", {})
        hidden_dim = net_cfg.get("hidden_dim", 128)
        output_dim = net_cfg.get("output_dim", 32)
        action_dim = config.get("action", {}).get("total_actions", 32)

        try:
            from gymnasium import spaces
        except ImportError:
            from gym import spaces

        obs_space = spaces.Box(low=-float("inf"), high=float("inf"), shape=(17,))

        self.network = ExecutionMLP(obs_space=obs_space, hidden_dim=hidden_dim, output_dim=output_dim).to(self.device)
        self.actor_head = torch.nn.Linear(output_dim, action_dim).to(self.device)

        self.network.load_state_dict(ckpt["network"])
        self.actor_head.load_state_dict(ckpt["actor_head"])
        self.network.eval()
        self.actor_head.eval()

        self._checkpoint_path = checkpoint_path
        logger.info(f"Loaded RL policy from {checkpoint_path}")

    def predict(self, obs) -> int:
        """给定 17 维观测向量，返回最优动作 (0-31)。"""
        import numpy as np
        if isinstance(obs, np.ndarray):
            obs_t = torch.FloatTensor(obs).unsqueeze(0).to(self.device)
        else:
            obs_t = obs.unsqueeze(0).to(self.device) if obs.dim() == 1 else obs.to(self.device)

        with torch.no_grad():
            features = self.network(obs_t)
            logits = self.actor_head(features)
            return logits.argmax(dim=-1).item()

    def predict_batch(self, obs_batch) -> list:
        """批量推理。obs_batch: [B, 17] ndarray → list of actions。"""
        import numpy as np
        obs_t = torch.FloatTensor(np.asarray(obs_batch)).to(self.device)
        with torch.no_grad():
            features = self.network(obs_t)
            logits = self.actor_head(features)
            return logits.argmax(dim=-1).cpu().tolist()


def load_policy(
    backtest_start: Optional[date] = None,
    version_tag: Optional[str] = None,
    policy_path: Optional[str] = None,
    device: str = "cpu",
) -> RLPolicy:
    """加载 RL 策略。

    优先级: policy_path > version_tag > backtest_start (purge gap 自动选择)
    """
    if policy_path:
        return RLPolicy(policy_path, device=device)

    if version_tag:
        from .model_registry import model_registry
        parts = version_tag.split("-", 1)
        rows = model_registry.list_versions(dev_version=parts[0])
        for r in rows:
            if r.get("version_tag") == version_tag:
                return RLPolicy(r["policy_path"], device=device)
        raise FileNotFoundError(f"Version {version_tag} not found in registry")

    if backtest_start:
        from .model_registry import model_registry
        model = model_registry.get_for_backtest(backtest_start)
        if model is None:
            raise FileNotFoundError(f"No active model with purge_end <= {backtest_start}")
        logger.info(f"Auto-selected model {model['version_tag']} for backtest starting {backtest_start}")
        return RLPolicy(model["policy_path"], device=device)

    raise ValueError("Must provide policy_path, version_tag, or backtest_start")
