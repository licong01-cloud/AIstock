"""Research Pipeline backend service package."""

from .models import (
    RESEARCH_HMM_BACKFILL_EXECUTE_CONFIRM,
    RESEARCH_PROMOTE_CONFIRM,
    RESEARCH_RETRY_STAGE_CONFIRM,
    RESEARCH_RUN_STAGE_CONFIRM,
    ArtifactRefRecord,
    BackfillRunRecord,
    BacktestRecord,
    ComparisonRecord,
    ExperimentRecord,
    ExternalRunLinkRecord,
    PipelineEventRecord,
    StageAttemptRecord,
    StagePlanRecord,
)
from .hmm_backtest_recorder import (
    BACKFILL_TYPE,
    BACKTEST_RECORDING_STAGE,
    HMM_BACKFILL_ENABLED_ENV,
    HMM_BACKFILL_WRITE_ENABLED_ENV,
    HMM_RECORDING_ENABLED_ENV,
    HMMBacktestRecorder,
)
from .offline import evaluate_criteria, evaluate_offline_stage
from .service import ResearchPipelineService

__all__ = [
    "ArtifactRefRecord",
    "BACKFILL_TYPE",
    "BACKTEST_RECORDING_STAGE",
    "BackfillRunRecord",
    "BacktestRecord",
    "HMM_BACKFILL_ENABLED_ENV",
    "HMM_BACKFILL_WRITE_ENABLED_ENV",
    "HMM_RECORDING_ENABLED_ENV",
    "HMMBacktestRecorder",
    "ComparisonRecord",
    "ExperimentRecord",
    "ExternalRunLinkRecord",
    "PipelineEventRecord",
    "RESEARCH_HMM_BACKFILL_EXECUTE_CONFIRM",
    "RESEARCH_PROMOTE_CONFIRM",
    "RESEARCH_RETRY_STAGE_CONFIRM",
    "RESEARCH_RUN_STAGE_CONFIRM",
    "ResearchPipelineService",
    "StageAttemptRecord",
    "StagePlanRecord",
    "evaluate_criteria",
    "evaluate_offline_stage",
]
