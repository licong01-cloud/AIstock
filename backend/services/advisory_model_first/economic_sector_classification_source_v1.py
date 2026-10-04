"""Advisory D-visible company classification; no package qualification."""
import re

import pandas as pd

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import _records
from backend.services.advisory_model_first.economic_entry_labels import KEY, _frame
from backend.services.advisory_model_first.economic_sector_daily_core_v1 import CLASS_FIELDS
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.industry_pit.contracts import (
    AuthorityType, KnowledgeTimePolicy, ResearchBasis, ResolutionRequest,
    ResolvedIndustryIdentity, UnavailableIndustryIdentity, UnavailableReason,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

SOFT = {UnavailableReason.CLASSIFICATION_KNOWLEDGE_TIME_UNVERIFIED,
        UnavailableReason.CLASSIFICATION_AUTHORITY_UNAVAILABLE}


def _invalid(message):
    raise AdvisoryModelFirstError(message, reason_code='ADVISORY_SECTOR_CLASSIFICATION_INVALID')


def original_daily_keys_v1(candidates):
    if (not isinstance(candidates, pd.DataFrame) or not candidates.columns.is_unique
            or not set(KEY).issubset(candidates.columns) or len(candidates) > 20):
        _invalid('classification needs the original bounded candidate keys')
    keys = _frame(candidates.loc[:, KEY], KEY, set(KEY))
    if (keys.instrument.duplicated().any() or keys[KEY[0]].nunique() > 1 or keys[KEY[1]].nunique() > 1
            or not keys[KEY[1]].gt(keys[KEY[0]]).all()
            or not keys.instrument.map(lambda value: bool(re.fullmatch(r'\d{6}\.(SH|SZ|BJ)', value))).all()):
        _invalid('classification candidate dates or symbols contradict one original day')
    return keys


class EconomicSectorClassificationSourceV1:
    """Consume the existing resolver, never current membership or release cutoff."""

    def __init__(self, *, resolver=None):
        self._resolver = resolver

    def load_day(self, *, candidates):
        keys = original_daily_keys_v1(candidates)
        resolver = self._resolver
        authority = resolver.receipt if resolver is not None else None
        if authority is not None and (authority.authority_type is not AuthorityType.CLASSIFICATION
                or authority.research_basis is not ResearchBasis.AS_PUBLISHED_PIT
                or authority.knowledge_time_policy is not KnowledgeTimePolicy.CAUSAL_DAILY_NEXT_TRADE):
            _invalid('classification source is not D-visible company classification')
        output, unknown = [], []
        for decision, target, symbol in keys.itertuples(index=False, name=None):
            day, category, known, reason = decision.date(), None, None, 'CLASSIFICATION_SOURCE_UNAVAILABLE'
            if resolver is not None:
                value = resolver.resolve(ResolutionRequest(symbol, day, AuthorityType.CLASSIFICATION,
                    authority.taxonomy_contract_id, authority.taxonomy_version, authority.receipt_hash,
                    KnowledgeTimePolicy.CAUSAL_DAILY_NEXT_TRADE, ResearchBasis.AS_PUBLISHED_PIT))
                if (not isinstance(value, (ResolvedIndustryIdentity, UnavailableIndustryIdentity))
                        or value.canonical_symbol != symbol or value.trade_date != day
                        or value.authority_type is not AuthorityType.CLASSIFICATION
                        or value.authority_receipt_hash != authority.receipt_hash):
                    _invalid('classification resolver returned a foreign source key')
                if isinstance(value, ResolvedIndustryIdentity):
                    if (value.non_as_known_taxonomy or value.known_from is None or value.known_from > day
                            or value.taxonomy_contract_id != authority.taxonomy_contract_id
                            or value.taxonomy_version != authority.taxonomy_version):
                        _invalid('classification cannot substitute future or non-as-known taxonomy')
                    category, known, reason = value.identity.l2_code, value.known_from, None
                elif value.reason in SOFT:
                    reason = value.reason.value
                else:
                    _invalid('classification resolver reports an actual identity conflict')
            output.append(dict(zip(CLASS_FIELDS, (decision, target, symbol, category, known))))
            if reason:
                unknown.append(dict(instrument=symbol, reason_code=reason))
        frame = pd.DataFrame(output, columns=CLASS_FIELDS)
        receipt = dict(schema_version='economic_sector_classification_source_v1',
            candidate_keys_sha256=sha(_records(keys)), classification_sha256=sha(_records(frame)),
            authority_receipt_hash=authority.receipt_hash if authority is not None else None,
            unknown=unknown, population_policy='PRESERVE_ORIGINAL_FROZEN_KEYS',
            database_written=False, outcomes_read=False, native_capture=False)
        return frame, receipt
