COMMENT ON COLUMN app.advisory_strategy_binding_version.effective_from_trade_date IS
    'Inclusive lower bound for decision_as_of_trade_date; a binding applies when effective_from_trade_date <= T.';

COMMENT ON COLUMN app.advisory_strategy_binding_version.effective_to_trade_date IS
    'Exclusive upper bound for decision_as_of_trade_date; NULL means open-ended and a binding applies only when T < effective_to_trade_date.';
