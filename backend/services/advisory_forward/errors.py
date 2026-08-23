from __future__ import annotations

from backend.services.trading_core.errors import InvalidStateTransitionError


REASON_MODEL_EVALUATION_IDENTITY_CONFLICT = (
    "ADVISORY_FORWARD_MODEL_EVALUATION_IDENTITY_CONFLICT"
)


class AdvisoryForwardActiveEpisodeStateConflictError(InvalidStateTransitionError):
    error_code = "ADVISORY_FORWARD_ACTIVE_EPISODE_STATE_CONFLICT"


class AdvisoryForwardModelEvaluationError(InvalidStateTransitionError):
    def __init__(self, message: str, *, reason_code: str, context: dict | None = None) -> None:
        super().__init__(message, context=context or {})
        self.error_code = reason_code
        self.reason_code = reason_code
