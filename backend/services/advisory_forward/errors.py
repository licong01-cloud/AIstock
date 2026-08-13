from __future__ import annotations

from backend.services.trading_core.errors import InvalidStateTransitionError


class AdvisoryForwardActiveEpisodeStateConflictError(InvalidStateTransitionError):
    error_code = "ADVISORY_FORWARD_ACTIVE_EPISODE_STATE_CONFLICT"
