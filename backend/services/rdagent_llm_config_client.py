"""RD-Agent LLM Config API Client for AIstock.

This module provides a client for interacting with RD-Agent's LLM configuration API.
It replaces direct .env file manipulation with API calls.
"""

from __future__ import annotations

from typing import Any

import httpx

# RD-Agent API configuration
RDAGENT_API_BASE = "http://127.0.0.1:9000"
RDAGENT_LLM_CONFIG_PREFIX = "/llm-config"
TIMEOUT = 30.0


class RDAgentLLMConfigClient:
    """Client for RD-Agent LLM configuration API."""

    def __init__(self, base_url: str | None = None) -> None:
        """Initialize client.

        Args:
            base_url: RD-Agent API base URL. Defaults to http://127.0.0.1:9000
        """
        self.base_url = base_url or RDAGENT_API_BASE
        self.client = httpx.AsyncClient(timeout=TIMEOUT)

    async def close(self) -> None:
        """Close HTTP client."""
        await self.client.aclose()

    async def get_current_config(self) -> dict[str, Any]:
        """Get current LLM configuration from RD-Agent.

        Returns:
            Dict containing current configuration including:
            - backend: Backend class path
            - chat_model: Default chat model
            - stage_mappings: LITELLM_CHAT_MODEL_MAP content
            - embedding_config: Embedding model configuration
            - api_credentials: API credentials (masked)
            - last_updated: ISO timestamp
        """
        url = f"{self.base_url}{RDAGENT_LLM_CONFIG_PREFIX}/current-config"
        response = await self.client.get(url)
        response.raise_for_status()
        return response.json()

    async def update_config(
        self,
        stage_mappings: list[dict[str, Any]],
        api_credentials: dict[str, str] | None = None,
        change_reason: str | None = None,
        backup_reason: str = "aistock_ui_update",
    ) -> dict[str, Any]:
        """Update LLM configuration via RD-Agent API.

        Args:
            stage_mappings: List of stage mapping dicts with keys:
                - stage_name: str
                - model_id/full_model_id: str
                - temperature: float (optional)
                - max_tokens: int (optional)
            api_credentials: Optional dict of API credentials to update
                Keys should be env var names like "DEEPSEEK_API_KEY", "OPENAI_API_BASE", etc.
            change_reason: Reason for the change
            backup_reason: Reason for creating backup

        Returns:
            Dict containing:
            - ok: bool - Whether update succeeded
            - message: str - Status message
            - backup_path: str - Path to backup file
            - updated_keys: list - List of updated environment variable names
            - verification_passed: bool - Whether verification passed
        """
        url = f"{self.base_url}{RDAGENT_LLM_CONFIG_PREFIX}/update-config"

        # Transform stage mappings to match API format
        api_stage_mappings = []
        for mapping in stage_mappings:
            api_mapping = {
                "stage_name": mapping.get("stage_name"),
                "model_id": mapping.get("model_id") or mapping.get("full_model_id"),
            }
            if mapping.get("temperature") is not None:
                api_mapping["temperature"] = mapping["temperature"]
            if mapping.get("max_tokens") is not None:
                api_mapping["max_tokens"] = mapping["max_tokens"]
            api_stage_mappings.append(api_mapping)

        payload = {
            "stage_mappings": api_stage_mappings,
            "backup_reason": backup_reason,
        }

        if api_credentials:
            payload["api_credentials"] = api_credentials

        if change_reason:
            payload["change_reason"] = change_reason

        response = await self.client.post(url, json=payload)
        response.raise_for_status()
        return response.json()

    async def verify_model(
        self,
        model_id: str,
        api_key: str | None = None,
        api_base: str | None = None,
        custom_prompt: str | None = None,
    ) -> dict[str, Any]:
        """Verify model API availability.

        Args:
            model_id: Model identifier (e.g., "deepseek/deepseek-v4-pro")
            api_key: Optional API key to use for verification
            api_base: Optional API base URL
            custom_prompt: Optional custom verification prompt

        Returns:
            Dict containing:
            - ok: bool - Whether verification succeeded
            - provider: str - Provider name
            - model_id: str - Model ID
            - message: str - Status message
            - response_time: float - API response time in seconds
            - error_details: str | None - Error details if failed
        """
        url = f"{self.base_url}{RDAGENT_LLM_CONFIG_PREFIX}/verify-model"

        payload: dict[str, Any] = {"model_id": model_id}
        if api_key:
            payload["api_key"] = api_key
        if api_base:
            payload["api_base"] = api_base
        if custom_prompt:
            payload["custom_prompt"] = custom_prompt

        response = await self.client.post(url, json=payload)
        response.raise_for_status()
        return response.json()

    async def verify_rdagent_integration(self) -> dict[str, Any]:
        """Verify RD-Agent can work with current configuration.

        Returns:
            Dict containing:
            - ok: bool - Whether integration check passed
            - backend: str - Backend class path
            - can_import: bool - Whether backend can be imported
            - can_instantiate: bool - Whether backend can be instantiated
            - test_completion: bool - Whether test completion works
            - error_details: str | None - Error details if failed
        """
        url = f"{self.base_url}{RDAGENT_LLM_CONFIG_PREFIX}/verify-rdagent"
        response = await self.client.post(url)
        response.raise_for_status()
        return response.json()

    async def rollback(self, backup_path: str) -> dict[str, Any]:
        """Rollback to a previous backup.

        Args:
            backup_path: Path to backup file

        Returns:
            Dict containing:
            - ok: bool - Whether rollback succeeded
            - message: str - Status message
        """
        url = f"{self.base_url}{RDAGENT_LLM_CONFIG_PREFIX}/rollback"
        response = await self.client.post(url, json={"backup_path": backup_path})
        response.raise_for_status()
        return response.json()

    async def health_check(self) -> dict[str, Any]:
        """Check LLM config service health.

        Returns:
            Dict containing:
            - status: str - Service status
            - service: str - Service name
        """
        url = f"{self.base_url}{RDAGENT_LLM_CONFIG_PREFIX}/health"
        response = await self.client.get(url)
        response.raise_for_status()
        return response.json()


# Singleton client instance for reuse
_client: RDAgentLLMConfigClient | None = None


def get_llm_config_client() -> RDAgentLLMConfigClient:
    """Get or create LLM config client singleton."""
    global _client
    if _client is None:
        _client = RDAgentLLMConfigClient()
    return _client


async def close_llm_config_client() -> None:
    """Close LLM config client singleton."""
    global _client
    if _client is not None:
        await _client.close()
        _client = None
