"""Title-rule based announcement event classifier.

The first rollout intentionally uses deterministic title rules only.  The same
classifier is used by historical backfill and future live polling so that
backtest/live behavior stays versioned and reproducible.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
from bisect import bisect_left, bisect_right
from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import Iterable, Mapping, Optional, Pattern, Sequence
from zoneinfo import ZoneInfo


RULE_VERSION = "aistock_announcement_title_rules_v1_20260506"
ENGINE_NAME = "AnnouncementTitleClassifier"
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


@dataclass(frozen=True)
class TitleRule:
    """One deterministic title classification rule."""

    event_type: str
    risk_level: str
    action: str
    needs_llm: str
    pattern: Pattern[str]
    description: str
    exclude: Optional[Pattern[str]] = None


@dataclass(frozen=True)
class ClassificationResult:
    """Normalized output of a title classification."""

    event_type: str
    risk_level: str
    action: str
    needs_llm: str
    matched_rule: str
    matched_text: str
    confidence: Decimal
    severity_score: Decimal
    description: str
    rule_version: str = RULE_VERSION


@dataclass(frozen=True)
class EffectiveDateResult:
    """Leakage-safe date semantics for one announcement signal."""

    source_time_quality: str
    effective_trade_date: dt.date
    effective_rule: str
    available_at: Optional[dt.datetime] = None


def rx(pattern: str) -> Pattern[str]:
    return re.compile(pattern, re.IGNORECASE)


RULES: list[TitleRule] = [
    # ST-first rules are ordered deliberately: bond-like delisting/repayment
    # notices and removal/continuation cases must not fall through to stock
    # hard-block rules just because the title also contains "摘牌" or "风险警示".
    TitleRule(
        "convertible_bond_delisting_or_redemption",
        "P4_NEUTRAL",
        "record_only",
        "NO",
        rx(
            r"((可转债|转债|可转换公司债券).*(赎回|兑付|摘牌|停止交易|到期|回售|付息)"
            r"|(赎回|兑付|摘牌|停止交易|到期|回售|付息).*(可转债|转债|可转换公司债券))"
        ),
        "Convertible-bond redemption, repayment, delisting, or similar bond-only notice; not a stock ST hard block.",
    ),
    TitleRule(
        "generic_bond_delisting_or_repayment",
        "P4_NEUTRAL",
        "discard_or_archive",
        "NO",
        rx(
            r"((公司债券|企业债券|债券持有人|债券简称|债券代码|中期票据|短期融资券|资产支持证券|ABS)"
            r".*(本息兑付|兑付|付息|摘牌|回售|赎回|到期)"
            r"|(本息兑付|兑付|付息|摘牌|回售|赎回|到期)"
            r".*(公司债券|企业债券|债券持有人|债券简称|债券代码|中期票据|短期融资券|资产支持证券|ABS))"
        ),
        "Generic bond repayment, interest, put-back, redemption, or delisting notice; archive outside stock hard-risk rules.",
    ),
    TitleRule(
        "stock_st_added_or_continued",
        "P1_HIGH",
        "warn_high",
        "NO",
        rx(r"(撤销部分.*(继续|仍将|仍被).*(风险警示)|继续(被)?实施.*风险警示|叠加实施.*风险警示|公司股票.*叠加.*风险警示)"),
        "Stock risk warning is added, stacked, or continues after partial removal; high-risk but separated from first-time hard block.",
    ),
    TitleRule(
        "stock_st_removal_applied",
        "P2_REVIEW",
        "warn_review",
        "NO",
        rx(r"((申请|拟申请).*(撤销|取消).*(退市风险警示|其他风险警示|风险警示|ST)|申请摘帽|申请.*摘帽|撤销.*风险警示.*进展)"),
        "Application or progress for risk-warning removal; not confirmed removal and not a hard block.",
    ),
    TitleRule(
        "stock_st_removed_confirmed",
        "P3_POSITIVE_CANDIDATE",
        "record_only",
        "NO",
        rx(r"(撤销|取消|申请撤销).*(退市风险警示|其他风险警示|风险警示)|摘帽|撤销.*ST"),
        "Confirmed stock risk-warning removal candidate; do not treat as hard block.",
        exclude=rx(r"(申请|拟申请|进展|部分|继续|仍将|仍被)"),
    ),
    TitleRule(
        "stock_delisting_confirmed",
        "P0_BLOCK",
        "block_buy",
        "NO",
        rx(r"(终止上市|强制退市|退市整理期|股票.*摘牌|将被终止上市|股票.*将.*摘牌|收到.*终止上市.*决定|作出.*终止上市.*决定)"),
        "Confirmed or near-confirmed stock delisting event.",
        exclude=rx(r"(可转债|转债|可转换公司债券|公司债券|企业债券|债券持有人|中期票据|短期融资券|资产支持证券|ABS|撤销|取消|申请撤销|摘帽|可能|触及|风险提示)"),
    ),
    TitleRule(
        "stock_delisting_risk_warning",
        "P0_BLOCK",
        "block_buy",
        "NO",
        rx(r"(可能被终止上市|触及.*终止上市|股票.*停牌.*可能被终止上市|股票.*终止上市.*风险提示|财务类终止上市情形)"),
        "Stock may be delisted or has touched delisting conditions.",
        exclude=rx(r"(可转债|转债|可转换公司债券|公司债券|企业债券|债券持有人|中期票据|短期融资券|资产支持证券|ABS|撤销|取消|申请撤销|摘帽)"),
    ),
    TitleRule(
        "stock_st_imposed",
        "P0_BLOCK",
        "block_buy",
        "NO",
        rx(r"((被|将被|拟被|股票交易被|股票将被|股票被).*(实施|实行).*(退市风险警示|其他风险警示|风险警示)|实施退市风险警示|实施其他风险警示|证券简称.*(变更|变更为).*\*?ST|公司股票.*\*?ST)"),
        "Stock is or will be subject to delisting risk warning, other risk warning, or ST name change.",
        exclude=rx(r"(可转债|转债|可转换公司债券|公司债券|企业债券|债券持有人|中期票据|短期融资券|资产支持证券|ABS|撤销|取消|申请撤销|摘帽|继续|叠加)"),
    ),
    TitleRule(
        "bankruptcy_restructuring",
        "P0_BLOCK",
        "block_buy",
        "NO",
        rx(r"(破产|重整|预重整|清算|债权人会议|不能清偿到期债务)"),
        "Bankruptcy, restructuring, liquidation, or insolvency-related title.",
    ),
    TitleRule(
        "regulatory_investigation_penalty",
        "P1_HIGH",
        "warn_high",
        "OPTIONAL",
        rx(r"(立案|调查通知书|涉嫌.*违法|行政处罚|处罚决定|纪律处分|公开谴责|监管措施|监管警示|警示函|市场禁入|移送司法|刑事|拘留|取保候审)"),
        "Regulatory investigation, penalty, discipline, or criminal/legal enforcement.",
    ),
    TitleRule(
        "debt_default_overdue",
        "P1_HIGH",
        "warn_high",
        "OPTIONAL",
        rx(r"(债务逾期|贷款逾期|票据逾期|债券违约|债务违约|未能.*兑付|不能按期.*兑付|本息兑付.*风险|流动性风险)"),
        "Debt overdue/default or bond repayment risk.",
    ),
    TitleRule(
        "audit_opinion_internal_control_risk",
        "P1_HIGH",
        "warn_high",
        "OPTIONAL",
        rx(r"(非标准审计|非无保留意见|保留意见|否定意见|无法表示意见|内部控制.*否定|内部控制.*重大缺陷|财务报告.*重大缺陷|审计报告.*带强调事项)"),
        "Audit opinion, internal-control, or financial-reporting severe issue.",
    ),
    TitleRule(
        "capital_occupation_illegal_guarantee",
        "P1_HIGH",
        "warn_high",
        "OPTIONAL",
        rx(r"(资金占用|占用公司资金|非经营性占用|违规担保|违规对外担保|违规资金往来)"),
        "Fund occupation or illegal guarantee.",
    ),
    TitleRule(
        "inquiry_concern_letter",
        "P2_REVIEW",
        "warn_review",
        "YES",
        rx(r"(问询函|关注函|监管函|问询函回复|关注函回复|年报问询|审核问询|落实函|反馈意见通知书|行政许可.*反馈意见|审核.*意见)"),
        "Exchange inquiry/concern/supervision letter or reply; needs context.",
    ),
    TitleRule(
        "key_personnel_change",
        "P2_REVIEW",
        "warn_review",
        "YES",
        rx(r"((董事长|总经理|总裁|财务负责人|首席财务官|董事会秘书|董秘).*(辞职|离任|变更|聘任|代行)|高级管理人员.*(辞职|离任))"),
        "Key executive or finance/disclosure officer change.",
    ),
    TitleRule(
        "litigation_arbitration_freeze",
        "P2_REVIEW",
        "warn_review",
        "YES",
        rx(r"(诉讼|仲裁|判决|裁定|执行通知|司法冻结|轮候冻结|股份冻结|查封|拍卖|强制执行)"),
        "Litigation, arbitration, court execution, freeze, or auction.",
    ),
    TitleRule(
        "pledge_shareholder_change_reduction",
        "P2_REVIEW",
        "warn_review",
        "YES",
        rx(r"(质押|解除质押|补充质押|平仓风险|减持|被动减持|权益变动|持股变动|股份转让|协议转让|表决权委托)"),
        "Pledge, reduction, transfer, or shareholder-rights change.",
    ),
    TitleRule(
        "control_change_ma_restructuring",
        "P2_REVIEW",
        "warn_review",
        "YES",
        rx(r"(控制权变更|实际控制人.*变更|重大资产重组|并购重组|发行股份购买资产|购买资产|出售资产|资产出售|资产收购|要约收购|吸收合并|重大交易)"),
        "Control change, M&A, restructuring, acquisition, or disposal.",
    ),
    TitleRule(
        "guarantee_financial_assistance_related_party",
        "P2_REVIEW",
        "warn_review",
        "YES",
        rx(r"(提供担保|对外担保|担保额度|财务资助|关联交易|关联方|资金拆借|委托贷款)"),
        "Guarantee, financial assistance, related-party transaction, or lending.",
    ),
    TitleRule(
        "performance_forecast_revision_impairment",
        "P2_REVIEW",
        "warn_review",
        "YES",
        rx(r"(业绩预告|业绩快报|业绩修正|业绩预告修正|业绩快报修正|亏损|扭亏|预亏|减值准备|资产减值|商誉减值|会计差错|前期会计差错|更正财务|追溯调整)"),
        "Performance forecast/express, revision, loss, impairment, or accounting correction.",
    ),
    TitleRule(
        "financing_dilution_debt_instruments",
        "P2_REVIEW",
        "warn_review",
        "YES",
        rx(r"(向特定对象发行|非公开发行|定增|配股|可转换公司债|可转债|公司债券|短期融资券|中期票据|融资租赁|授信额度|募集资金|资产支持专项计划|资产支持证券|ABS)"),
        "Financing, dilution, debt instrument, credit line, or proceeds event.",
    ),
    TitleRule(
        "suspension_resumption",
        "P2_REVIEW",
        "warn_review",
        "NO",
        rx(r"(停牌|复牌|临时停牌|继续停牌)"),
        "Suspension/resumption title; tradability must also use suspend_d.",
    ),
    TitleRule(
        "stock_price_abnormal_volatility",
        "P2_REVIEW",
        "warn_review",
        "NO",
        rx(r"(股票交易异常波动|严重异常波动|风险提示公告|异动公告)"),
        "Abnormal trading movement or generic risk-tip title.",
    ),
    TitleRule(
        "positive_contract_order_project",
        "P3_POSITIVE_CANDIDATE",
        "record_only",
        "OPTIONAL",
        rx(r"(中标|预中标|签订.*合同|重大合同|订单|框架协议|战略合作|项目投产|投产|产能|获得.*补助|政府补助|产品获批|注册证|临床试验|专利|新药|一致性评价)"),
        "Potential positive operating event; alpha use requires validation.",
    ),
    TitleRule(
        "buyback_increase_holding_dividend",
        "P3_POSITIVE_CANDIDATE",
        "record_only",
        "OPTIONAL",
        rx(r"(回购|增持|员工持股计划|股权激励|激励计划|股票期权|限制性股票|利润分配|现金分红|权益分派|股份奖励)"),
        "Buyback, increase holding, employee incentive, or distribution candidate.",
    ),
    TitleRule(
        "investor_relations_neutral",
        "P4_NEUTRAL",
        "discard_or_archive",
        "NO",
        rx(r"(投资者关系活动记录|投资者关系管理信息|业绩说明会|网上说明会|调研活动|接待机构投资者|路演活动)"),
        "Investor-relations disclosure; archive but do not generate trading action.",
    ),
    TitleRule(
        "periodic_report_neutral",
        "P4_NEUTRAL",
        "discard_or_archive",
        "NO",
        rx(r"(年度报告|半年度报告|季度报告|第一季度报告|第三季度报告|摘要|审计报告|财务报告|内部控制评价报告|社会责任报告|ESG报告|环境、社会及治理报告)"),
        "Periodic report or routine report disclosure.",
    ),
    TitleRule(
        "meeting_resolution_neutral",
        "P4_NEUTRAL",
        "discard_or_archive",
        "NO",
        rx(r"(董事会.*决议|监事会.*决议|股东大会|临时股东大会|会议决议|独立董事.*意见|法律意见书|律师事务所.*意见)"),
        "Meeting, resolution, independent opinion, or legal opinion.",
    ),
    TitleRule(
        "governance_document_neutral",
        "P4_NEUTRAL",
        "discard_or_archive",
        "NO",
        rx(r"(公司章程|制度|规则|细则|管理办法|工作办法|议事规则|独立董事制度|董事会.*工作规程|监事会.*工作规程)$"),
        "Routine governance document.",
    ),
    TitleRule(
        "ipo_refinancing_review_neutral",
        "P4_NEUTRAL",
        "discard_or_archive",
        "NO",
        rx(r"(招股说明书|上市公告书|上市保荐书|发行保荐书|保荐.*报告|上市委.*会议|注册申请|审核中心|申报稿|上会稿)"),
        "IPO/refinancing review document; usually not a secondary-market warning.",
    ),
    TitleRule(
        "routine_correction_supplement_neutral",
        "P4_NEUTRAL",
        "discard_or_archive",
        "NO",
        rx(r"(更正公告|补充公告|提示性公告|进展公告|公告的更正|公告的补充)$"),
        "Routine correction/supplement/progress title not caught by risk rules.",
    ),
    TitleRule(
        "routine_professional_report_neutral",
        "P4_NEUTRAL",
        "discard_or_archive",
        "NO",
        rx(r"(验资报告|专项报告|核查意见|鉴证报告|审阅报告|评估报告|估值报告|资信评级报告|跟踪评级报告|受托管理事务报告|持续督导.*报告)$"),
        "Routine professional intermediary report.",
    ),
    TitleRule(
        "routine_personnel_change_neutral",
        "P4_NEUTRAL",
        "discard_or_archive",
        "NO",
        rx(r"(补选|选举|换届|聘任|任职|辞职|离任|调整.*董事|调整.*监事|高级管理人员变动)"),
        "Routine personnel change not caught as key-person risk.",
    ),
]


DEFAULT_RESULT = ClassificationResult(
    event_type="unclassified_archive",
    risk_level="P4_NEUTRAL",
    action="archive_for_rule_mining",
    needs_llm="SAMPLE_ONLY",
    matched_rule="DEFAULT",
    matched_text="",
    confidence=Decimal("0.30"),
    severity_score=Decimal("0.00"),
    description="Unclassified by v0 title rules; archive for rule mining and sample-based QA, not automatic LLM.",
)


CONFIDENCE_BY_LEVEL: Mapping[str, Decimal] = {
    "P0_BLOCK": Decimal("0.95"),
    "P1_HIGH": Decimal("0.85"),
    "P2_REVIEW": Decimal("0.65"),
    "P3_POSITIVE_CANDIDATE": Decimal("0.55"),
    "P4_NEUTRAL": Decimal("0.50"),
}

SEVERITY_BY_LEVEL: Mapping[str, Decimal] = {
    "P0_BLOCK": Decimal("1.00"),
    "P1_HIGH": Decimal("0.80"),
    "P2_REVIEW": Decimal("0.50"),
    "P3_POSITIVE_CANDIDATE": Decimal("0.20"),
    "P4_NEUTRAL": Decimal("0.00"),
}


class AnnouncementTitleClassifier:
    """Deterministic announcement title classifier."""

    def __init__(
        self,
        rules: Sequence[TitleRule] = tuple(RULES),
        rule_version: str = RULE_VERSION,
        pre_open_cutoff: dt.time = dt.time(9, 25),
        market_close_cutoff: dt.time = dt.time(15, 0),
    ) -> None:
        self.rules = list(rules)
        self.rule_version = rule_version
        self.pre_open_cutoff = pre_open_cutoff
        self.market_close_cutoff = market_close_cutoff

    @staticmethod
    def normalize_title(title: str) -> str:
        return re.sub(r"\s+", "", title or "")

    def classify(self, title: str) -> ClassificationResult:
        normalized = self.normalize_title(title)
        for rule in self.rules:
            if rule.exclude is not None and rule.exclude.search(normalized):
                continue
            match = rule.pattern.search(normalized)
            if not match:
                continue
            return ClassificationResult(
                event_type=rule.event_type,
                risk_level=rule.risk_level,
                action=rule.action,
                needs_llm=rule.needs_llm,
                matched_rule=rule.event_type,
                matched_text=match.group(0)[:200],
                confidence=CONFIDENCE_BY_LEVEL.get(rule.risk_level, Decimal("0.50")),
                severity_score=SEVERITY_BY_LEVEL.get(rule.risk_level, Decimal("0.00")),
                description=rule.description,
                rule_version=self.rule_version,
            )
        return ClassificationResult(**{**asdict(DEFAULT_RESULT), "rule_version": self.rule_version})

    def infer_effective_date(
        self,
        ann_date: dt.date,
        rec_time: Optional[dt.datetime],
        trading_days: Sequence[dt.date],
        *,
        first_seen_at: Optional[dt.datetime] = None,
        time_mode: str = "backtest",
    ) -> EffectiveDateResult:
        """Apply leakage-safe effective-date rules for backtest/live parity."""

        if not trading_days:
            raise ValueError("trading_days is required to infer announcement effective date")

        mode = (time_mode or "backtest").strip().lower()
        observed_mode = mode in {"live", "paper", "paper_live", "observed", "simulation"}

        if rec_time is None:
            if observed_mode and first_seen_at is not None:
                return self._effective_from_first_seen(first_seen_at, trading_days)
            return EffectiveDateResult(
                "MISSING",
                self._next_trading_day(trading_days, ann_date, strictly_after=True),
                "missing_rec_time_next_trading_day",
            )

        local_time = self._to_shanghai(rec_time)
        if local_time.time().replace(tzinfo=None) == dt.time(0, 0):
            if observed_mode and first_seen_at is not None:
                return self._effective_from_first_seen(first_seen_at, trading_days)
            return EffectiveDateResult(
                "MIDNIGHT_DEFAULT",
                self._next_trading_day(trading_days, ann_date, strictly_after=True),
                "midnight_default_next_trading_day",
            )

        ann_idx = bisect_left(trading_days, ann_date)
        ann_is_trading = ann_idx < len(trading_days) and trading_days[ann_idx] == ann_date
        if local_time.time().replace(tzinfo=None) <= self.pre_open_cutoff:
            effective = (
                ann_date
                if ann_is_trading
                else self._next_trading_day(trading_days, ann_date, strictly_after=False)
            )
            return EffectiveDateResult("EXACT", effective, "exact_before_preopen", local_time)

        return EffectiveDateResult(
            "EXACT",
            self._next_trading_day(trading_days, ann_date, strictly_after=True),
            "exact_after_preopen_next_trading_day",
            local_time,
        )

    def _effective_from_first_seen(
        self,
        first_seen_at: dt.datetime,
        trading_days: Sequence[dt.date],
    ) -> EffectiveDateResult:
        local_seen = self._to_shanghai(first_seen_at)
        local_date = local_seen.date()
        local_time = local_seen.time().replace(tzinfo=None)
        local_idx = bisect_left(trading_days, local_date)
        local_is_trading = local_idx < len(trading_days) and trading_days[local_idx] == local_date
        if local_time <= self.pre_open_cutoff:
            effective = (
                local_date
                if local_is_trading
                else self._next_trading_day(trading_days, local_date, strictly_after=False)
            )
            return EffectiveDateResult(
                "LOCAL_FIRST_SEEN",
                effective,
                "local_first_seen_before_preopen",
                local_seen,
            )
        return EffectiveDateResult(
            "LOCAL_FIRST_SEEN",
            self._next_trading_day(trading_days, local_date, strictly_after=True),
            "local_first_seen_after_preopen_next_trading_day",
            local_seen,
        )

    @staticmethod
    def _to_shanghai(value: dt.datetime) -> dt.datetime:
        if value.tzinfo is None:
            value = value.replace(tzinfo=SHANGHAI_TZ)
        return value.astimezone(SHANGHAI_TZ)

    @staticmethod
    def _next_trading_day(
        trading_days: Sequence[dt.date],
        base_date: dt.date,
        *,
        strictly_after: bool,
    ) -> dt.date:
        idx = bisect_right(trading_days, base_date) if strictly_after else bisect_left(trading_days, base_date)
        if idx >= len(trading_days):
            raise ValueError(f"trading calendar has no effective date after {base_date.isoformat()}")
        return trading_days[idx]


def taxonomy_rows(rules: Iterable[TitleRule] = RULES) -> list[dict[str, str]]:
    """Return one row per event type for seeding market.ann_event_taxonomy."""

    rows: dict[str, dict[str, str]] = {
        DEFAULT_RESULT.event_type: {
            "event_type": DEFAULT_RESULT.event_type,
            "risk_level": DEFAULT_RESULT.risk_level,
            "default_action": DEFAULT_RESULT.action,
            "needs_llm": DEFAULT_RESULT.needs_llm,
            "description": DEFAULT_RESULT.description,
        }
    }
    for rule in rules:
        rows.setdefault(
            rule.event_type,
            {
                "event_type": rule.event_type,
                "risk_level": rule.risk_level,
                "default_action": rule.action,
                "needs_llm": rule.needs_llm,
                "description": rule.description,
            },
        )
    return sorted(rows.values(), key=lambda row: row["event_type"])


def rule_config_json(rules: Iterable[TitleRule] = RULES) -> list[dict[str, str]]:
    return [
        {
            "event_type": rule.event_type,
            "risk_level": rule.risk_level,
            "action": rule.action,
            "needs_llm": rule.needs_llm,
            "pattern": rule.pattern.pattern,
            "exclude": rule.exclude.pattern if rule.exclude is not None else None,
            "description": rule.description,
        }
        for rule in rules
    ]


def rule_config_hash(rules: Iterable[TitleRule] = RULES) -> str:
    payload = json.dumps(rule_config_json(rules), ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def title_hash(title: str) -> str:
    return hashlib.sha256((title or "").encode("utf-8")).hexdigest()
