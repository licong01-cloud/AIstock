import datetime as dt

from backend.services.announcements.title_classifier import AnnouncementTitleClassifier


def test_delisting_title_blocks_buy():
    classifier = AnnouncementTitleClassifier()

    result = classifier.classify("关于公司股票被实施退市风险警示暨停牌的公告")

    assert result.event_type == "delisting_or_risk_warning"
    assert result.risk_level == "P0_BLOCK"
    assert result.action == "block_buy"
    assert result.needs_llm == "NO"


def test_risk_warning_removal_is_not_hard_block():
    classifier = AnnouncementTitleClassifier()

    result = classifier.classify("关于申请撤销公司股票退市风险警示的公告")

    assert result.event_type == "risk_warning_removed"
    assert result.risk_level == "P3_POSITIVE_CANDIDATE"
    assert result.action == "record_only"


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
