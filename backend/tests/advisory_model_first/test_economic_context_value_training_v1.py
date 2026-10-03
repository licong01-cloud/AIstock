from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_context_value_training_v1 import (
    assemble_context_value_rows_v1, context_support_v1, predict_context_value_v1, train_context_value_v1,
)
from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.tests.advisory_model_first.test_economic_value_anchor_training_v1 import training_fixture


def rows_fixture():
    args,_=training_fixture()
    args['context']=args['inputs'].loc[:,KEY].assign(classification_l2_code='110100',classification_known_from=pd.Timestamp('2024-01-01'))
    return args,assemble_context_value_rows_v1(**args)


def captured_fit(monkeypatch):
    import sklearn.linear_model as linear
    calls=[]

    class Captured:
        def __init__(self,**parameters):
            self.parameters=parameters

        def fit(self,matrix,target):
            calls.append((matrix.copy(),target.copy(),self.parameters))
            self.coef_=np.zeros(matrix.shape[1])
            self.intercept_=float(target.mean())
            return self

    monkeypatch.setattr(linear,'Ridge',Captured)
    monkeypatch.setattr(linear,'QuantileRegressor',Captured)
    return calls


def test_matched_supervision_purge_train_only_support_scaler_and_no_test_selection(monkeypatch):
    args,rows=rows_fixture()
    calls=captured_fit(monkeypatch)
    count=[]
    first=train_context_value_v1(rows=rows,configuration=args['configuration'],before_fit=lambda *a:count.append(a))
    assert len(count)==4 and rows.purged.sum()==15
    assert first.diagnostics['train_rows']==45 and first.diagnostics['validation_rows']==15
    assert calls[0][0].shape[0]==calls[2][0].shape[0] and calls[0][1].equals(calls[2][1])
    assert not any(name.startswith('category_') for name in calls[0][0].columns)
    assert 'category_gap_110100' in calls[2][0].columns
    rows.loc[rows.split.eq('test'),[*D_FEATURES,'gross_value_ratio','path_min_value_ratio','actual_gap_bps']]=999999.
    rows.loc[rows.split.eq('test'),'classification_l2_code']='999999'
    second=train_context_value_v1(rows=rows,configuration=args['configuration'],before_fit=lambda *a:None)
    assert first.model_sha256==second.model_sha256 and first.diagnostics==second.diagnostics
    for before,after in zip(calls[:4],calls[4:],strict=True):
        pd.testing.assert_frame_equal(before[0],after[0])


def test_unknown_retained_future_or_missing_candidate_fail_closed():
    args,_=rows_fixture()
    args['context'].loc[0,['classification_l2_code','classification_known_from']]=None
    rows=assemble_context_value_rows_v1(**args)
    assert len(rows)==90 and pd.isna(rows.classification_l2_code.iloc[0])
    args['context'].loc[1,'classification_known_from']=args['inputs'][KEY[1]].iloc[1]
    with pytest.raises(ValueError,match='after D'):
        assemble_context_value_rows_v1(**args)
    args['context']=args['context'].iloc[2:]
    with pytest.raises(ValueError):
        assemble_context_value_rows_v1(**args)


def test_support_uses_no_label_maturity_and_fitted_mutation_cannot_be_hidden(monkeypatch):
    args,rows=rows_fixture()
    support=context_support_v1(rows,train_end=args['configuration'].train_end)
    rows['gross_value_ratio'],rows['path_min_value_ratio'],rows['training_eligible']=np.nan,np.nan,False
    assert context_support_v1(rows,train_end=args['configuration'].train_end)==support
    _,rows=rows_fixture()
    captured_fit(monkeypatch)
    fit=train_context_value_v1(rows=rows,configuration=args['configuration'],before_fit=lambda *a:None)
    bad=replace(fit,model_sha256='0'*64)
    with pytest.raises(ValueError,match='identity changed'):
        predict_context_value_v1(fitted=bad,rows=rows.iloc[:1],arm='candidate')
