"""StrategyPackage runtime and rebalance helpers.

Only package-based runtime artifacts are accepted. The implementation supports
single-alpha packages with explicit score/rank artifacts now; multi-alpha stays
schema-compatible but fails until component runtime artifacts are provided.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from backend.services.selection_center.hmm_runtime import SectorHMMRuntime
from backend.services.selection_center.models import SelectionCandidate, SignalSnapshot, TargetPosition
from backend.services.selection_center.runtime_profile import normalize_selection_runtime_config, parse_selection_runtime_profile
from backend.services.strategy_package.models import AlphaMode, StrategyPackageManifest
from backend.services.strategy_package.selection_artifact import (
    StrategyPackageSelectionArtifactRepository,
    selection_artifact_runtime_hash,
)
from backend.services.strategy_package.live_inference import (
    AUTHORITATIVE_SELECTION_SCOPE,
    AUTHORITATIVE_SELECTION_SOURCE_TYPE,
)
from backend.services.strategy_package.validators import StrategyPackageValidator
from backend.services.trading_core.errors import (
    DataUnavailableError,
    StrategyPackageValidationError,
    UnsupportedFeatureError,
)
from backend.services.trading_core.models import OrderIntent, OrderSide, PositionLot


class StrategyPackageRuntime:
    """Load package runtime scores into a strict signal snapshot."""

    def __init__(
        self,
        validator: StrategyPackageValidator | None = None,
        hmm_runtime: SectorHMMRuntime | None = None,
        artifact_repository: StrategyPackageSelectionArtifactRepository | None = None,
    ) -> None:
        self.validator = validator or StrategyPackageValidator()
        self.hmm_runtime = hmm_runtime or SectorHMMRuntime()
        self.artifact_repository = artifact_repository or StrategyPackageSelectionArtifactRepository()

    def build_signal_snapshot(
        self,
        *,
        manifest: StrategyPackageManifest,
        trade_date: date,
        data_source: str,
        runtime_config: dict[str, Any] | None = None,
    ) -> SignalSnapshot:
        self.validator.validate_manifest(manifest)
        if not manifest.manifest_sha256:
            raise StrategyPackageValidationError(
                "strategy package manifest must be frozen before runtime",
                context={"package_id": manifest.package_id},
            )
        config = normalize_selection_runtime_config(runtime_config or {})
        if manifest.alpha_mode == AlphaMode.MULTI_ALPHA:
            if not self._has_multi_alpha_runtime(manifest, config):
                raise UnsupportedFeatureError(
                    "multi_alpha runtime artifacts are not available",
                    context={"package_id": manifest.package_id},
                )
        rows = self._load_score_rows(manifest, config, trade_date=trade_date, data_source=data_source)
        valid_no_candidate = bool(config.get("valid_no_candidate"))
        no_candidate_reason = config.get("no_candidate_reason")
        if not rows:
            if valid_no_candidate:
                return SignalSnapshot(
                    package_id=manifest.package_id,
                    manifest_sha256=manifest.manifest_sha256,
                    trade_date=trade_date,
                    data_source=data_source,
                    candidates=[],
                    runtime_config=config,
                    valid_no_candidate=True,
                    no_candidate_reason=str(no_candidate_reason or "runtime declared no candidate"),
                )
            raise StrategyPackageValidationError(
                "selection runtime produced no candidates",
                context={"package_id": manifest.package_id, "trade_date": trade_date.isoformat()},
            )
        candidates = [self._candidate_from_row(row, manifest.package_id) for row in rows]
        candidates.sort(key=lambda item: item.rank)
        runtime_profile = parse_selection_runtime_profile(config)
        if runtime_profile.hmm.enabled:
            candidates = self.hmm_runtime.adjust_candidates(
                candidates=candidates,
                trade_date=trade_date,
                profile=runtime_profile.hmm,
                package_id=manifest.package_id,
                manifest_sha256=manifest.manifest_sha256,
            )
        return SignalSnapshot(
            package_id=manifest.package_id,
            manifest_sha256=manifest.manifest_sha256,
            trade_date=trade_date,
            data_source=data_source,
            candidates=candidates,
            runtime_config=config,
        )

    def _load_score_rows(
        self,
        manifest: StrategyPackageManifest,
        config: dict[str, Any],
        *,
        trade_date: date,
        data_source: str,
    ) -> list[dict[str, Any]]:
        if "selection_scores" in config:
            raise StrategyPackageValidationError(
                "runtime_config.selection_scores cannot be used as StrategyPackage signal input; "
                "generate an authoritative live selection artifact first",
                context={"package_id": manifest.package_id, "trade_date": trade_date.isoformat()},
            )
        selection_runtime = manifest.strategy_config.get("selection_runtime")
        if not isinstance(selection_runtime, dict):
            selection_runtime = {}
        runtime_hash = selection_artifact_runtime_hash(config)
        artifact_error: DataUnavailableError | None = None
        try:
            artifact = self.artifact_repository.get(
                package_id=manifest.package_id,
                manifest_sha256=manifest.manifest_sha256 or "",
                trade_date=trade_date,
                data_source=data_source,
                runtime_config_hash=runtime_hash,
            )
        except DataUnavailableError as exc:
            artifact_error = exc
        else:
            if artifact.status.value != "SUCCEEDED":
                raise DataUnavailableError(
                    "selection score artifact is not succeeded",
                    context={
                        "package_id": manifest.package_id,
                        "artifact_id": artifact.artifact_id,
                        "status": artifact.status.value,
                        "error_json": artifact.error_json,
                    },
                )
            self._require_authoritative_artifact(artifact, config)
            if not artifact.scores_json:
                raise DataUnavailableError(
                    "selection score artifact contains no scores",
                    context={"package_id": manifest.package_id, "artifact_id": artifact.artifact_id},
                )
            return artifact.scores_json

        if "scores" in selection_runtime:
            raise StrategyPackageValidationError(
                "manifest strategy_config.selection_runtime.scores is not authoritative; "
                "StrategyPackage runtime must use live/latest-data inference artifacts",
                context={"package_id": manifest.package_id, "trade_date": trade_date.isoformat()},
            )
        scores_path = selection_runtime.get("scores_path")
        if scores_path:
            raise StrategyPackageValidationError(
                "manifest strategy_config.selection_runtime.scores_path is not authoritative; "
                "StrategyPackage runtime must use live/latest-data inference artifacts",
                context={
                    "package_id": manifest.package_id,
                    "trade_date": trade_date.isoformat(),
                    "scores_path": str(scores_path),
                },
            )
        if artifact_error is not None:
            raise artifact_error
        raise DataUnavailableError(
            "selection score artifact is missing; generate selection artifact first",
            context={"package_id": manifest.package_id, "trade_date": trade_date.isoformat()},
        )

    def _require_authoritative_artifact(self, artifact: Any, config: dict[str, Any]) -> None:
        metadata = artifact.metadata or {}
        source_type = metadata.get("source_type")
        authority_scope = metadata.get("authority_scope")
        if source_type == AUTHORITATIVE_SELECTION_SOURCE_TYPE and authority_scope == AUTHORITATIVE_SELECTION_SCOPE:
            return
        raise DataUnavailableError(
            "selection score artifact is not authoritative live inference output",
            context={
                "package_id": artifact.package_id,
                "artifact_id": artifact.artifact_id,
                "trade_date": artifact.trade_date.isoformat(),
                "data_source": artifact.data_source,
                "source_type": source_type,
                "authority_scope": authority_scope,
                "required_source_type": AUTHORITATIVE_SELECTION_SOURCE_TYPE,
                "required_authority_scope": AUTHORITATIVE_SELECTION_SCOPE,
            },
        )

    def _candidate_from_row(self, row: dict[str, Any], package_id: str) -> SelectionCandidate:
        missing = [key for key in ("symbol", "score", "rank") if row.get(key) is None]
        if missing:
            raise StrategyPackageValidationError(
                "selection candidate is missing required score fields",
                context={"package_id": package_id, "missing": missing, "row": row},
            )
        return SelectionCandidate(
            symbol=str(row["symbol"]),
            score=float(row["score"]),
            rank=int(row["rank"]),
            target_weight=float(row["target_weight"]) if row.get("target_weight") is not None else None,
            target_quantity=int(row["target_quantity"]) if row.get("target_quantity") is not None else None,
            reference_price=float(row["reference_price"]) if row.get("reference_price") is not None else None,
            component_scores=row.get("component_scores") or {},
            reason=row.get("reason"),
        )

    @staticmethod
    def _has_multi_alpha_runtime(manifest: StrategyPackageManifest, config: dict[str, Any]) -> bool:
        runtime = manifest.strategy_config.get("selection_runtime")
        return bool(config.get("multi_alpha_component_scores") or (isinstance(runtime, dict) and runtime.get("component_scores")))


class TargetPositionEngine:
    """Convert selection candidates into target positions."""

    def build_targets(
        self,
        *,
        snapshot: SignalSnapshot,
        total_equity: float,
        top_k: int,
    ) -> list[TargetPosition]:
        if snapshot.valid_no_candidate:
            raise StrategyPackageValidationError(
                "valid_no_candidate snapshots cannot produce target positions",
                context={"package_id": snapshot.package_id, "reason": snapshot.no_candidate_reason},
            )
        if total_equity <= 0:
            raise StrategyPackageValidationError("total_equity must be positive for target positions")
        if top_k <= 0:
            raise StrategyPackageValidationError("top_k must be positive for target positions")
        selected = sorted(snapshot.candidates, key=lambda item: item.rank)[:top_k]
        if not selected:
            raise StrategyPackageValidationError("target position engine received no candidates")
        targets: list[TargetPosition] = []
        for candidate in selected:
            if candidate.target_quantity is not None:
                if candidate.reference_price is None:
                    raise DataUnavailableError(
                        "target_quantity requires reference_price for traceability",
                        context={"package_id": snapshot.package_id, "symbol": candidate.symbol},
                    )
                quantity = candidate.target_quantity
                target_weight = candidate.target_weight
            else:
                if candidate.target_weight is None:
                    raise StrategyPackageValidationError(
                        "candidate is missing target position information",
                        context={"package_id": snapshot.package_id, "symbol": candidate.symbol},
                    )
                if candidate.reference_price is None:
                    raise DataUnavailableError(
                        "candidate reference_price is required to compute target quantity",
                        context={"package_id": snapshot.package_id, "symbol": candidate.symbol},
                    )
                raw_quantity = int(total_equity * candidate.target_weight / candidate.reference_price)
                quantity = (raw_quantity // 100) * 100
                target_weight = candidate.target_weight
            if quantity <= 0:
                raise StrategyPackageValidationError(
                    "target position rounds to zero shares",
                    context={"package_id": snapshot.package_id, "symbol": candidate.symbol},
                )
            targets.append(
                TargetPosition(
                    symbol=candidate.symbol,
                    target_quantity=quantity,
                    target_weight=target_weight,
                    reference_price=candidate.reference_price,
                    score=candidate.score,
                    rank=candidate.rank,
                    reason=candidate.reason or "selection_target",
                    metadata={"snapshot_id": snapshot.snapshot_id, "component_scores": candidate.component_scores},
                )
            )
        return targets


class RebalanceEngine:
    """Diff current positions and target positions into OrderIntent objects."""

    def build_order_intents(
        self,
        *,
        package_id: str,
        portfolio_id: str,
        trade_date: date,
        current_positions: dict[str, PositionLot],
        target_positions: list[TargetPosition],
    ) -> list[OrderIntent]:
        if not target_positions:
            raise StrategyPackageValidationError(
                "rebalance requires target positions",
                context={"package_id": package_id, "portfolio_id": portfolio_id},
            )
        target_by_symbol = {target.symbol: target for target in target_positions}
        symbols = sorted(set(current_positions) | set(target_by_symbol))
        intents: list[OrderIntent] = []
        for symbol in symbols:
            current_qty = current_positions.get(symbol).quantity if symbol in current_positions else 0
            target_qty = target_by_symbol[symbol].target_quantity if symbol in target_by_symbol else 0
            delta = target_qty - current_qty
            if delta == 0:
                continue
            side = OrderSide.BUY if delta > 0 else OrderSide.SELL
            quantity = abs(delta)
            if quantity % 100 != 0:
                raise StrategyPackageValidationError(
                    "rebalance quantity is not a 100-share round lot",
                    context={"package_id": package_id, "symbol": symbol, "quantity": quantity},
                )
            intents.append(
                OrderIntent(
                    package_id=package_id,
                    portfolio_id=portfolio_id,
                    symbol=symbol,
                    side=side,
                    quantity=quantity,
                    target_trade_date=trade_date,
                    metadata={
                        "target_quantity": target_qty,
                        "current_quantity": current_qty,
                        "rebalance_reason": "target_position_diff",
                    },
                )
            )
        return intents
