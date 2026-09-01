import datetime as dt

from backend.services.announcements.title_classifier import (
    ISSUER_UNVERIFIED_EVENT_TYPES,
    AnnouncementTitleClassifier,
    normalize_security_name,
    taxonomy_rows,
)


def test_taxonomy_registers_every_fail_closed_issuer_event_type() -> None:
    rows = {row["event_type"]: row for row in taxonomy_rows()}

    assert set(ISSUER_UNVERIFIED_EVENT_TYPES) <= rows.keys()
    for event_type in ISSUER_UNVERIFIED_EVENT_TYPES:
        assert rows[event_type]["risk_level"] == "P2_REVIEW"
        assert rows[event_type]["default_action"] == "warn_review"
        assert rows[event_type]["needs_llm"] == "YES"


def test_delisted_display_markers_do_not_change_security_identity():
    assert normalize_security_name("退美都(退)") == normalize_security_name("*ST美都")
    assert normalize_security_name("普利退(退)") == normalize_security_name("*ST普利")
    assert normalize_security_name("退市工新") == normalize_security_name("退工新(退)")


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


def test_delisting_risk_is_not_confirmed_terminal_evidence():
    classifier = AnnouncementTitleClassifier()

    result = classifier.classify("关于公司股票存在终止上市风险的第三次提示性公告")

    assert result.event_type == "stock_delisting_risk_warning"


def test_negated_delisting_condition_is_neutral():
    classifier = AnnouncementTitleClassifier()

    result = classifier.classify("董事会关于不存在其他暂停上市或者终止上市情形的说明")

    assert result.event_type == "stock_delisting_negated"
    assert result.action == "record_only"


def test_related_entity_delisting_is_not_issuer_terminal_evidence():
    classifier = AnnouncementTitleClassifier()

    result = classifier.classify("关于收到子公司大东海股票终止上市决定的公告")

    assert result.event_type == "related_entity_delisting"
    assert result.action == "warn_review"


def test_listing_guidance_termination_is_neutral():
    classifier = AnnouncementTitleClassifier()

    result = classifier.classify("关于控股子公司终止上市辅导的提示性公告")

    assert result.event_type == "listing_guidance_terminated"


def test_confirmed_delisting_requires_issuer_bound_subject():
    classifier = AnnouncementTitleClassifier()

    result, binding = classifier.classify_with_issuer(
        "关于中弘控股股份有限公司股票终止上市的公告",
        ts_code="000001.SZ",
        announcement_name="中弘退",
        security_name="平安银行",
        security_fullname="平安银行股份有限公司",
        security_list_status="L",
    )

    assert binding.status == "unverified"
    assert result.event_type == "stock_delisting_reference_unverified"


def test_cross_security_risk_warning_is_downgraded_from_trading_signal():
    classifier = AnnouncementTitleClassifier()

    result, binding = classifier.classify_with_issuer(
        "关于中弘控股股份有限公司股票存在终止上市风险的提示性公告",
        ts_code="000001.SZ",
        announcement_name="中弘退",
        security_name="平安银行",
        security_fullname="平安银行股份有限公司",
    )

    assert binding.status == "unverified"
    assert result.event_type == "stock_event_issuer_unverified"
    assert result.action == "warn_review"


def test_title_fullname_cannot_override_mismatched_announcement_name():
    classifier = AnnouncementTitleClassifier()

    result, binding = classifier.classify_with_issuer(
        "关于平安银行股份有限公司股票终止上市的公告",
        ts_code="000001.SZ",
        announcement_name="中弘退",
        security_name="平安银行",
        security_fullname="平安银行股份有限公司",
        security_list_status="L",
    )

    assert binding.fullname_match is True
    assert binding.name_match is False
    assert binding.status == "unverified"
    assert binding.reason == "announcement_name_does_not_match_security"
    assert result.event_type == "stock_delisting_reference_unverified"


def test_title_fullname_is_allowed_only_when_source_name_is_missing():
    classifier = AnnouncementTitleClassifier()

    result, binding = classifier.classify_with_issuer(
        "关于中弘控股股份有限公司股票终止上市的公告",
        ts_code="000979.SZ",
        announcement_name="",
        security_name="中弘退(退)",
        security_fullname="中弘控股股份有限公司",
        security_list_status="D",
    )

    assert binding.name_match is False
    assert binding.fullname_match is True
    assert binding.status == "verified"
    assert binding.reason == "title_name_or_fullname_match"
    assert binding.terminal_subject == "self"
    assert result.event_type == "stock_delisting_confirmed"


def test_confirmed_delisting_accepts_matching_fullname_and_delisted_name_suffix():
    classifier = AnnouncementTitleClassifier()

    result, binding = classifier.classify_with_issuer(
        "关于中弘控股股份有限公司股票终止上市的公告",
        ts_code="000979.SZ",
        announcement_name="中弘退",
        security_name="中弘退(退)",
        security_fullname="中弘控股股份有限公司",
        security_list_status="D",
    )

    assert binding.status == "verified"
    assert binding.terminal_subject == "self"
    assert result.event_type == "stock_delisting_confirmed"


def test_matching_announcement_name_does_not_authorize_other_company_subject():
    classifier = AnnouncementTitleClassifier()

    result, binding = classifier.classify_with_issuer(
        "关于中华映管股份有限公司股票将终止上市的提示性公告",
        ts_code="000536.SZ",
        announcement_name="华映科技",
        security_name="华映科技",
        security_fullname="华映科技(集团)股份有限公司",
        security_list_status="L",
    )

    assert binding.status == "unverified"
    assert binding.reason == "terminal_subject_not_proven"
    assert result.event_type == "stock_delisting_reference_unverified"


def test_live_confirmed_delisting_does_not_wait_for_security_master_status_update():
    classifier = AnnouncementTitleClassifier()

    result, binding = classifier.classify_with_issuer(
        "关于公司股票终止上市决定的公告",
        ts_code="000536.SZ",
        announcement_name="华映科技",
        security_name="华映科技",
        security_fullname="华映科技(集团)股份有限公司",
        security_list_status="L",
    )

    assert binding.status == "verified"
    assert binding.terminal_subject == "self"
    assert result.event_type == "stock_delisting_confirmed"
    assert result.action == "block_buy"


def test_verified_issuer_with_generic_stock_terminal_title_is_self_subject():
    classifier = AnnouncementTitleClassifier()

    result, binding = classifier.classify_with_issuer(
        "股票终止上市暨摘牌的公告",
        ts_code="600070.SH",
        announcement_name="*ST富润",
        security_name="*ST富润(退)",
        security_fullname="浙江富润数字科技股份有限公司",
        security_list_status="D",
    )

    assert binding.status == "verified"
    assert binding.terminal_subject == "self"
    assert result.event_type == "stock_delisting_confirmed"


def test_predecision_notice_is_high_risk_but_not_terminal_confirmation():
    classifier = AnnouncementTitleClassifier()

    result = classifier.classify("关于收到终止上市事先告知书的公告")

    assert result.event_type == "stock_delisting_predecision"
    assert result.action == "block_buy"


def test_convertible_bond_delisting_is_not_stock_hard_block():
    classifier = AnnouncementTitleClassifier()

    result = classifier.classify("关于实施“永鼎转债”赎回暨摘牌的第七次提示性公告")

    assert result.event_type == "convertible_bond_delisting_or_redemption"
    assert result.risk_level == "P4_NEUTRAL"
    assert result.action == "record_only"


def test_mixed_stock_and_convertible_bond_risk_keeps_stock_risk_semantics():
    classifier = AnnouncementTitleClassifier()

    result = classifier.classify("关于公司股票及可转换公司债券可能被终止上市的风险提示公告")

    assert result.event_type == "stock_delisting_risk_warning"


def test_mixed_stock_and_convertible_bond_decision_is_terminal_confirmation():
    classifier = AnnouncementTitleClassifier()

    result = classifier.classify("关于收到公司股票及可转换公司债券终止上市决定的公告")

    assert result.event_type == "stock_delisting_confirmed"


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


def test_exact_rec_time_later_than_ann_date_anchors_effective_date_to_observation():
    classifier = AnnouncementTitleClassifier()
    trading_days = [
        dt.date(2019, 5, 17),
        dt.date(2019, 5, 20),
        dt.date(2019, 5, 23),
        dt.date(2019, 5, 24),
    ]

    result = classifier.infer_effective_date(
        dt.date(2019, 5, 17),
        dt.datetime(2019, 5, 23, 22, 5, tzinfo=dt.timezone(dt.timedelta(hours=8))),
        trading_days,
    )

    assert result.available_at == dt.datetime(
        2019, 5, 23, 22, 5, tzinfo=dt.timezone(dt.timedelta(hours=8))
    )
    assert result.effective_trade_date == dt.date(2019, 5, 24)
