"""Manual QE archive payload processing service.

This service is intentionally not wired into QE webhooks or FastAPI startup.
Callers must pass payloads that have already been collected from DB/API paths.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .payload_extractor import ExtractedArchivePayload, QEArchivePayloadExtractor
from .repository import QEArchiveRepository


@dataclass(frozen=True)
class ArchivePayloadResult:
    run_id: str
    dry_run: bool
    stats: dict[str, Any]
    extracted: ExtractedArchivePayload


class QEArchiveService:
    """Archive normalized QE payloads through explicit repository calls."""

    def __init__(
        self,
        *,
        repository: QEArchiveRepository | None = None,
        extractor: QEArchivePayloadExtractor | None = None,
    ) -> None:
        self._repository = repository or QEArchiveRepository()
        self._extractor = extractor or QEArchivePayloadExtractor()

    def process_payload(
        self,
        payload: Mapping[str, Any],
        *,
        event_type: str | None = None,
        source_system: str | None = None,
        source_id: str | None = None,
        source_sub_id: str | None = None,
        dry_run: bool = True,
    ) -> ArchivePayloadResult:
        """Extract records and optionally write them to `qe_archive`.

        `dry_run=True` is the safe default for manual validation and future
        backfill previews; production hooks must opt in explicitly.
        """

        extracted = self._extractor.extract(
            payload,
            event_type=event_type,
            source_system=source_system,
            source_id=source_id,
            source_sub_id=source_sub_id,
        )
        stats = dict(extracted.stats)
        stats.update(
            {
                "run_id": extracted.run.run_id,
                "dry_run": dry_run,
                "data_context_count": len(extracted.data_contexts),
                "account_summary_count": 1 if extracted.account_summary else 0,
                "symbol_summary_count": len(extracted.symbol_summaries),
                "trade_count": len(extracted.trades),
                "execution_event_count": len(extracted.execution_events),
                "raw_payload_count": len(extracted.raw_payloads),
            }
        )

        if not dry_run:
            self._write(extracted)
            stats["written"] = True
        else:
            stats["written"] = False

        return ArchivePayloadResult(
            run_id=extracted.run.run_id,
            dry_run=dry_run,
            stats=stats,
            extracted=extracted,
        )

    def _write(self, extracted: ExtractedArchivePayload) -> None:
        repo = self._repository
        run_id = repo.upsert_run(extracted.run)
        repo.upsert_run_source(extracted.source)
        repo.upsert_run_config(extracted.config)
        repo.upsert_reproducibility_manifest(extracted.reproducibility_manifest)
        for context in extracted.data_contexts:
            repo.upsert_data_context(context)
        if extracted.account_summary is not None:
            repo.upsert_account_summary(extracted.account_summary)
        repo.upsert_metric_batch(extracted.metrics, replace_existing=True)
        repo.replace_run_curves(run_id, extracted.curves)
        repo.replace_run_factors(run_id, extracted.factors)
        repo.replace_run_symbol_summaries(run_id, extracted.symbol_summaries)
        repo.replace_run_trades(run_id, extracted.trades)
        repo.replace_run_execution_events(run_id, extracted.execution_events)
        repo.replace_raw_payloads(run_id, extracted.raw_payloads)
