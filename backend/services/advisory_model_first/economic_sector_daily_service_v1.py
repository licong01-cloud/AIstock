"""M1 daily consumer: original DB lists, explicit family, no qualification or writes."""

from dataclasses import dataclass
import hashlib
import os
from pathlib import Path

from backend.services.advisory_model_first.economic_sector_classification_source_v1 import (
    EconomicSectorClassificationSourceV1,
)
from backend.services.advisory_model_first.economic_sector_daily_family_v1 import (
    FAMILY,
    SCOPE_KEYS,
    EconomicSectorPriceDailyFamilyV1,
)
from backend.services.advisory_model_first.economic_sector_daily_source_v1 import EconomicSectorReadonlyDailySourceV1
from backend.services.advisory_model_first.economic_sector_list_source_v1 import (
    EconomicSectorPublishedListSourceV1,
    _day,
    _invalid,
)
from backend.services.advisory_model_first.economic_sector_price_readonly_bundle_v1 import (
    _ReadSet,
    _object,
    _read_file,
    load_sector_research_bundle_v1,
)
from backend.services.advisory_model_first.economic_sector_price_source_v1 import structural_crosswalk_v1
from backend.services.advisory_model_first.entry_price_daily_service import BoundedEntryReadSession, EntryWorkBudget
from backend.services.industry_pit.artifact_store import read_candidate_bundle
from backend.services.industry_pit.resolver import IndustryPitResolver
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

CONFIG_KEYS = {
    "schema_version",
    "model_family",
    "plan_ref",
    "trained_manifest_ref",
    "evaluated_manifest_ref",
    "crosswalk_ref",
    "code_map_ref",
    "taxonomy_ref",
    "classification_authority_root",
    "price_context_mode",
    "pit_universe_key",
}


@dataclass(frozen=True)
class SectorDailyConfigurationV1:
    bundle: object
    roles: dict
    weights: dict
    crosswalk: dict
    index_codes: dict
    classification_source: object
    price_context_mode: str
    pit_universe_key: str | None
    config_sha256: str
    pins: tuple

    def verify_unchanged(self):
        for path, digest, size in self.pins:
            body = _read_file(path, 2 * 1024**2)
            if (hashlib.sha256(body).hexdigest(), len(body)) != (digest, size):
                _invalid("daily configuration or source metadata changed during consumption")


def load_sector_daily_configuration_v1(path):
    """Only small published metadata plus the public classification bundle, no rows/fit."""
    reads = _ReadSet()
    body = _read_file(path, 1024**2)
    reads.pins[Path(path)] = (hashlib.sha256(body).hexdigest(), len(body))
    reads.total = len(body)
    config = _object(body)
    if (
        set(config) != CONFIG_KEYS
        or config["schema_version"] != "economic_sector_daily_config_v1"
        or config["model_family"] != FAMILY
        or config["price_context_mode"] not in {"LIVE_DB", "CANONICAL_HISTORICAL"}
        or config["price_context_mode"] == "LIVE_DB"
        and config["pit_universe_key"] is not None
        or config["price_context_mode"] == "CANONICAL_HISTORICAL"
        and not config["pit_universe_key"]
    ):
        _invalid("daily M1 configuration schema/family/source mode differs")
    bundle = load_sector_research_bundle_v1(
        **{key: config[key] for key in ("plan_ref", "trained_manifest_ref", "evaluated_manifest_ref")}
    )
    metadata_ref = bundle.plan.feature_manifest_ref
    metadata_path = Path(metadata_ref.artifact_uri)
    metadata = reads.json(metadata_path, metadata_ref.model_dump())
    descriptor = metadata["files"]["identity.json"]
    recipe = reads.json(metadata_path.parent / "identity.json", descriptor)["recipe"]
    roles, weights = recipe["roles"], recipe["terminal_weights"]
    if (
        not isinstance(roles, dict)
        or set(roles) != {"lstm", "fund"}
        or len(set(roles.values())) != 2
        or not isinstance(weights, dict)
        or set(weights) != set(roles.values())
    ):
        _invalid("daily M1 original two-leg recipe is missing")
    _, snapshot = reads.reference(config["crosswalk_ref"], "sector_daily_crosswalk")
    _, code_map = reads.reference(config["code_map_ref"], "sector_daily_code_map")
    _, taxonomy = reads.reference(config["taxonomy_ref"], "sector_daily_taxonomy")
    crosswalk = structural_crosswalk_v1(snapshot, code_map=code_map, taxonomy=taxonomy)
    codes = {
        crosswalk[row["industry_code"]]: row["index_code"]
        for row in snapshot["rows"]
        if crosswalk[row["industry_code"]] is not None
    }
    resolver = None
    authority_root = config["classification_authority_root"]
    if authority_root is not None:
        root = Path(authority_root)
        if not root.is_absolute() or root.drive.upper() == "C:" or root.resolve() != root.absolute():
            _invalid("daily classification authority needs an explicit non-C original path")
        published = read_candidate_bundle(artifact_root=root, forbidden_roots=(Path(__file__).resolve().parents[3],))
        receipt = published.classification_receipt
        if receipt.taxonomy_contract_id != taxonomy["contract_id"] or receipt.taxonomy_version != taxonomy["version"]:
            _invalid("daily classification taxonomy contradicts the structural crosswalk")
        resolver = IndustryPitResolver(
            receipt=receipt,
            intervals=published.classification_intervals,
            known_taxonomy_versions=((receipt.taxonomy_contract_id, receipt.taxonomy_version),),
        )
        reads.read(root / "candidate_bundle_manifest.json")
    loaded = SectorDailyConfigurationV1(
        bundle,
        roles,
        weights,
        crosswalk,
        codes,
        EconomicSectorClassificationSourceV1(resolver=resolver),
        config["price_context_mode"],
        config["pit_universe_key"],
        sha(config),
        tuple((file, digest, size) for file, (digest, size) in reads.pins.items()),
    )
    loaded.verify_unchanged()
    return loaded


class AdvisorySectorDailyServiceV1:
    def __init__(
        self,
        *,
        config_path=None,
        config_loader=None,
        session_factory=None,
        list_source_factory=None,
        family_factory=None,
    ):
        self._path = config_path or os.getenv("AISTOCK_ADVISORY_SECTOR_PRICE_CONFIG", "").strip() or None
        self._load = config_loader or load_sector_daily_configuration_v1
        self._session = session_factory or (lambda: BoundedEntryReadSession(EntryWorkBudget(30.0)))
        self._list_source = list_source_factory or EconomicSectorPublishedListSourceV1
        self._family = family_factory or self._build_family

    @staticmethod
    def _build_family(config):
        source = EconomicSectorReadonlyDailySourceV1(
            crosswalk=config.crosswalk,
            expected_crosswalk_values_sha256=sha(config.crosswalk),
            index_codes=config.index_codes,
        )
        return EconomicSectorPriceDailyFamilyV1(
            bundle=config.bundle, source=source, classification_source=config.classification_source
        )

    def read_day(self, *, program_id, target_date=None, list_version_id=None):
        return self._compute(program_id, [dict(target_date=target_date, list_version_id=list_version_id)])[0]

    def read_batch(self, *, program_id, target_dates):
        if not isinstance(target_dates, (tuple, list)) or not 1 <= len(target_dates) <= 20:
            _invalid("daily historical batch must contain one to twenty original target dates")
        days = [_day(value) for value in target_dates]
        if len(set(days)) != len(days):
            _invalid("daily historical batch contains duplicate target dates")
        return self._compute(program_id, [dict(target_date=day, list_version_id=None) for day in days])

    def _compute(self, program_id, requests):
        if not isinstance(program_id, str) or not program_id.strip():
            _invalid("daily API program id is missing")
        days = [
            _day(request["target_date"]).isoformat() if request["target_date"] is not None else None
            for request in requests
        ]
        common = dict(
            schema_version="economic_sector_daily_service_v1",
            model_family=FAMILY,
            program_id=program_id,
            decision_use="NAVIGATION_ONLY",
            deployable=False,
            economic_effectiveness="NOT_CONFIRMED",
            package_qualification_rechecked=False,
            database_written=False,
            outcomes_read=False,
            fit_count=0,
        )
        if self._path is None:
            return [
                dict(
                    common,
                    status="NOT_CONFIGURED",
                    requested_target_date=day,
                    requested_list_version_id=request["list_version_id"],
                    candidates=[],
                    unmodeled_items=[],
                )
                for day, request in zip(days, requests, strict=True)
            ]
        config = self._load(self._path)
        session = self._session()
        try:
            reader = self._list_source(
                read_session=session,
                price_context_mode=config.price_context_mode,
                pit_universe_key=config.pit_universe_key,
            )
            inputs = [
                reader.load_day(
                    program_id=program_id,
                    component_roles=config.roles,
                    terminal_weights=config.weights,
                    model_scope={key: config.bundle.scope[key] for key in SCOPE_KEYS},
                    **request,
                )
                for request in requests
            ]
        finally:
            session.close()  # The read-stage budget does NOT run during batch CPU inference.
        packets = [
            dict(
                candidates=value["candidates"],
                calendar=value["calendar"],
                price_contexts=value["price_contexts"],
                component_roles=config.roles,
                terminal_weights=config.weights,
                scope=value["scope"],
            )
            for value in inputs
        ]
        predictions = self._family(config).predict_batch(packets=packets)
        if len(predictions) != len(inputs):
            _invalid("M1 family changed the number of original published days")
        config.verify_unchanged()
        results = []
        for day, request, value, prediction in zip(days, requests, inputs, predictions, strict=True):
            receipt = value["candidate_receipt"]
            if (
                prediction.get("model_family") != FAMILY
                or prediction.get("scope") != value["scope"]
                or prediction.get("decision_date") != receipt["decision_date"]
                or prediction.get("target_date") != receipt["target_date"]
                or [row["instrument"] for row in prediction["candidates"]] != value["candidates"].instrument.tolist()
            ):
                _invalid("M1 family changed its original roster or identity")
            result = dict(
                common,
                status=prediction["status"],
                requested_target_date=day,
                requested_list_version_id=request["list_version_id"],
                decision_date=prediction["decision_date"],
                target_date=prediction["target_date"],
                candidate_receipt=receipt,
                model_sha256=prediction["model_sha256"],
                bundle_sha256=prediction["bundle_sha256"],
                config_sha256=config.config_sha256,
                source_review_policy_sha256=receipt["source_review_policy_sha256"],
                model_parent_policy_identity=value["scope"]["parent_policy_identity"],
                model_value_policy_identity=value["scope"]["policy_identity"],
                candidates=prediction["candidates"],
                unmodeled_items=receipt["unmodeled_items"],
                prediction=prediction,
            )
            result["projection_sha256"] = sha(result)
            results.append(result)
        return results
