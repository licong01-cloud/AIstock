"""Research Pipeline backend service package."""

from .models import (
    RESEARCH_PROMOTE_CONFIRM,
    RESEARCH_RETRY_STAGE_CONFIRM,
    RESEARCH_RUN_STAGE_CONFIRM,
    ArtifactRefRecord,
    ComparisonRecord,
    ExperimentRecord,
    ExternalRunLinkRecord,
    PipelineEventRecord,
    StageAttemptRecord,
    StagePlanRecord,
)
from .offline import evaluate_criteria, evaluate_offline_stage
from .service import ResearchPipelineService

__all__ = [
    "ArtifactRefRecord",
    "ComparisonRecord",
    "ExperimentRecord",
    "ExternalRunLinkRecord",
    "PipelineEventRecord",
    "RESEARCH_PROMOTE_CONFIRM",
    "RESEARCH_RETRY_STAGE_CONFIRM",
    "RESEARCH_RUN_STAGE_CONFIRM",
    "ResearchPipelineService",
    "StageAttemptRecord",
    "StagePlanRecord",
    "evaluate_criteria",
    "evaluate_offline_stage",
]
