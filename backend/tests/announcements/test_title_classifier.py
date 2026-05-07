import datetime as dt

from backend.services.announcements.title_classifier import AnnouncementTitleClassifier


def test_delisting_title_blocks_buy():
    classifier = AnnouncementTitleClassifier()

    result = classifier.classify("关于公司股票被实施退市风险警示暨停牌的公告")

    assert result.event_type == "stock_st_imposed"
    assert result.risk_level == "P0_BLOCK"
    assert result.action == "block_buy"
    assert result.needs_llm == "NO"


def test_risk_warning_removal_application_is_review_not_hard_block():
    classifier = AnnouncementTitleClassifier()

    result = classifier.classify("关于申请撤销公司股票退市风险警示的公告")

    assert result.event_type == "stock_st_removal_applied"
    assert result.risk_level == "P2_REVIEW"
    assert result.action == "warn_review"


def test_risk_warning_removal_confirmed_is_record_only():
    classifier = AnnouncementTitleClassifier()

    result = classifier.classify("关于公司股票交易撤销退市风险警示及其他风险警示暨股票停复牌的公告")

    assert result.event_type == "stock_st_removed_confirmed"
    assert result.risk_level == "P3_POSITIVE_CANDIDATE"
    assert result.action == "record_only"


def test_partial_risk_warning_removal_with_continued_warning_is_high_risk():
    classifier = AnnouncementTitleClassifier()

    result = classifier.classify("关于申请撤销部分其他风险警示暨继续被实施其他风险警示的公告")

    assert result.event_type == "stock_st_added_or_continued"
    assert result.risk_level == "P1_HIGH"
    assert result.action == "warn_high"


def test_possible_delisting_title_blocks_buy():
    classifier = AnnouncementTitleClassifier()

    result = classifier.classify("关于公司股票停牌暨可能被终止上市的风险提示公告")

    assert result.event_type == "stock_delisting_risk_warning"
    assert result.risk_level == "P0_BLOCK"
    assert result.action == "block_buy"


def test_convertible_bond_delisting_is_not_stock_hard_block():
    classifier = AnnouncementTitleClassifier()

    result = classifier.classify("关于实施“永鼎转债”赎回暨摘牌的第七次提示性公告")

    assert result.event_type == "convertible_bond_delisting_or_redemption"
    assert result.risk_level == "P4_NEUTRAL"
    assert result.action == "record_only"


def test_corporate_bond_repayment_delisting_is_archived():
    classifier = AnnouncementTitleClassifier()

    result = classifier.classify("2021年面向专业投资者公开发行公司债券2024年本息兑付及摘牌公告")

    assert result.event_type == "generic_bond_delisting_or_repayment"
    assert result.risk_level == "P4_NEUTRAL"
    assert result.action == "discard_or_archive"


def test_inquiry_letter_requires_review_and_llm():
    classifier = AnnouncementTitleClassifier()

    result = classifier.classify("关于深圳证券交易所年报问询函回复的公告")

    assert result.event_type == "inquiry_concern_letter"
    assert result.risk_level == "P2_REVIEW"
    assert result.needs_llm == "YES"


def test_investor_relations_is_neutral():
    classifier = AnnouncementTitleClassifier()

    result = classifier.classify("2026年4月30日投资者关系活动记录表")

    assert result.event_type == "investor_relations_neutral"
    assert result.risk_level == "P4_NEUTRAL"
    assert result.action == "discard_or_archive"


def test_effective_date_rules_prevent_leakage():
    classifier = AnnouncementTitleClassifier()
    trading_days = [
        dt.date(2026, 5, 4),
        dt.date(2026, 5, 5),
        dt.date(2026, 5, 6),
        dt.date(2026, 5, 7),
    ]

    preopen = classifier.infer_effective_date(
        dt.date(2026, 5, 5),
        dt.datetime(2026, 5, 5, 8, 30, tzinfo=dt.timezone(dt.timedelta(hours=8))),
        trading_days,
    )
    after_open = classifier.infer_effective_date(
        dt.date(2026, 5, 5),
        dt.datetime(2026, 5, 5, 15, 30, tzinfo=dt.timezone(dt.timedelta(hours=8))),
        trading_days,
    )
    midnight = classifier.infer_effective_date(
        dt.date(2026, 5, 5),
        dt.datetime(2026, 5, 5, 0, 0, tzinfo=dt.timezone(dt.timedelta(hours=8))),
        trading_days,
    )
    missing = classifier.infer_effective_date(dt.date(2026, 5, 5), None, trading_days)

    assert preopen.source_time_quality == "EXACT"
    assert preopen.effective_trade_date == dt.date(2026, 5, 5)
    assert after_open.effective_trade_date == dt.date(2026, 5, 6)
    assert midnight.source_time_quality == "MIDNIGHT_DEFAULT"
    assert midnight.effective_trade_date == dt.date(2026, 5, 6)
    assert missing.source_time_quality == "MISSING"
    assert missing.effective_trade_date == dt.date(2026, 5, 6)


def test_live_mode_can_use_local_first_seen_for_date_only_announcements():
    classifier = AnnouncementTitleClassifier()
    trading_days = [
        dt.date(2026, 5, 4),
        dt.date(2026, 5, 5),
        dt.date(2026, 5, 6),
    ]
    first_seen_at = dt.datetime(2026, 5, 5, 7, 0, tzinfo=dt.timezone(dt.timedelta(hours=8)))

    live = classifier.infer_effective_date(
        dt.date(2026, 5, 5),
        None,
        trading_days,
        first_seen_at=first_seen_at,
        time_mode="live",
    )
    backtest = classifier.infer_effective_date(
        dt.date(2026, 5, 5),
        None,
        trading_days,
        first_seen_at=first_seen_at,
        time_mode="backtest",
    )

    assert live.source_time_quality == "LOCAL_FIRST_SEEN"
    assert live.effective_rule == "local_first_seen_before_preopen"
    assert live.effective_trade_date == dt.date(2026, 5, 5)
    assert live.available_at == first_seen_at
    assert backtest.source_time_quality == "MISSING"
    assert backtest.effective_trade_date == dt.date(2026, 5, 6)


def test_live_mode_delays_local_first_seen_after_preopen():
    classifier = AnnouncementTitleClassifier()
    trading_days = [
        dt.date(2026, 5, 5),
        dt.date(2026, 5, 6),
    ]

    result = classifier.infer_effective_date(
        dt.date(2026, 5, 5),
        None,
        trading_days,
        first_seen_at=dt.datetime(2026, 5, 5, 10, 30, tzinfo=dt.timezone(dt.timedelta(hours=8))),
        time_mode="paper",
    )

    assert result.source_time_quality == "LOCAL_FIRST_SEEN"
    assert result.effective_rule == "local_first_seen_after_preopen_next_trading_day"
    assert result.effective_trade_date == dt.date(2026, 5, 6)
