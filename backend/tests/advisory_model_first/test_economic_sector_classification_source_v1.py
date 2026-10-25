from dataclasses import replace
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_sector_classification_source_v1 import EconomicSectorClassificationSourceV1
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.industry_pit.contracts import UnavailableReason
from backend.tests.advisory_model_first.test_economic_context_consumer_v1 import roster, source as source


def test_public_classification_keeps_missing_symbols_and_has_no_dataset_cutoff(source):
    source.identity['cutoff'] = '2024-12-31'
    keys = roster(('2025-09-01',))
    frame, receipt = EconomicSectorClassificationSourceV1(resolver=source.classification).load_day(candidates=keys)
    assert frame.instrument.tolist() == keys.instrument.tolist()
    assert frame.classification_l2_code.tolist()[0] == '340400'
    assert frame.classification_l2_code.iloc[1:].isna().all()
    assert len(receipt['unknown']) == 2 and not receipt['native_capture']
    assert receipt['authority_receipt_hash'] == source.classification.receipt.receipt_hash


def test_no_resolver_and_empty_roster_are_explicit_not_fake_classification():
    keys = roster(('2025-09-01',))
    consumer = EconomicSectorClassificationSourceV1()
    frame, receipt = consumer.load_day(candidates=keys)
    assert len(frame) == 3 and frame.classification_l2_code.isna().all()
    assert len(receipt['unknown']) == 3 and receipt['authority_receipt_hash'] is None
    empty, receipt = consumer.load_day(candidates=keys.iloc[:0])
    assert empty.empty and not receipt['unknown']


@pytest.mark.parametrize('poison', ['foreign_symbol', 'foreign_day', 'foreign_receipt', 'future', 'taxonomy', 'non_as_known', 'conflict'])
def test_resolver_contradictions_cannot_be_hidden_as_normal_unknown(source, poison):
    resolver = source.classification

    def resolve(request):
        value = resolver.resolve(request)
        changes = dict(foreign_symbol={'canonical_symbol': '600000.SH'},
            foreign_day={'trade_date': request.trade_date.replace(day=2)},
            foreign_receipt={'authority_receipt_hash': 'c'*64}, future={'known_from': request.trade_date.replace(day=2)},
            taxonomy={'taxonomy_version': 'CURRENT'}, non_as_known={'non_as_known_taxonomy': True})
        if poison == 'conflict':
            from backend.services.industry_pit.contracts import UnavailableIndustryIdentity
            return UnavailableIndustryIdentity('UNAVAILABLE', request.canonical_symbol, request.trade_date,
                request.authority_type, UnavailableReason.INTERVAL_OVERLAP, (), request.authority_receipt_hash)
        return replace(value, **changes[poison])

    consumer = EconomicSectorClassificationSourceV1(resolver=SimpleNamespace(receipt=resolver.receipt, resolve=resolve))
    with pytest.raises(AdvisoryModelFirstError):
        consumer.load_day(candidates=roster(('2025-09-01',)).iloc[:1])


def test_duplicate_and_multi_day_keys_do_not_reach_resolver():
    consumer = EconomicSectorClassificationSourceV1()
    for keys in (roster(), pd.concat([roster(('2025-09-01',)), roster(('2025-09-01',))])):
        with pytest.raises(AdvisoryModelFirstError):
            consumer.load_day(candidates=keys)
