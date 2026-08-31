from __future__ import annotations

from typing import Any, Mapping


class AdvisoryModelFirstError(RuntimeError):
    """Typed failure raised by the model-first research pipeline."""

    def __init__(
        self,
        message: str,
        *,
        reason_code: str,
        context: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.context = dict(context or {})

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": "failed",
            "reason_code": self.reason_code,
            "message": str(self),
            "context": self.context,
        }
