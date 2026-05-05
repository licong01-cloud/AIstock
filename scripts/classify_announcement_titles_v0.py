"""Classify announcement titles with AIstock v0 rule taxonomy.

The classifier is intentionally title-first and deterministic so the same logic
can be reused later by backtests and live warning jobs. It produces aggregate
reports only; it does not mutate database rows.
"""
from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import json
import os
import re
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Pattern, Tuple

import psycopg2
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env", override=True)

DB_CFG = dict(
    host=os.getenv("TDX_DB_HOST", "localhost"),
    port=int(os.getenv("TDX_DB_PORT", "5432")),
    dbname=os.getenv("TDX_DB_NAME", "aistock"),
    user=os.getenv("TDX_DB_USER", "postgres"),
    password=os.getenv("TDX_DB_PASSWORD", ""),
    application_name="AIstock-announcement-title-classification-v0",
)


@dataclasses.dataclass(frozen=True)
class Rule:
    event_type: str
    risk_level: str
    action: str
    needs_llm: str
    pattern: Pattern[str]
    description: str
    exclude: Optional[Pattern[str]] = None


def rx(pattern: str) -> Pattern[str]:
    return re.compile(pattern, re.IGNORECASE)


RULES: List[Rule] = [
    Rule(
        "risk_warning_removed",
        "P3_POSITIVE_CANDIDATE",
        "record_only",
        "NO",
        rx(r"(撤销|取消|申请撤销).*(退市风险警示|其他风险警示|风险警示)|摘帽|撤销.*ST"),
        "Risk-warning removal candidate; do not treat as hard block.",
    ),
    Rule(
        "delisting_or_risk_warning",
        "P0_BLOCK",
        "block_buy",
        "NO",
        rx(r"(终止上市|强制退市|退市整理期|摘牌|可能被终止上市|退市风险警示|实施其他风险警示|被实施.*风险警示|公司股票.*ST|变更为\*?ST)"),
        "Hard risk warning or delisting event.",
    ),
    Rule(
        "bankruptcy_restructuring",
        "P0_BLOCK",
        "block_buy",
        "NO",
        rx(r"(破产|重整|预重整|清算|债权人会议|不能清偿到期债务)"),
        "Bankruptcy, restructuring, liquidation, or insolvency-related title.",
    ),
    Rule(
        "regulatory_investigation_penalty",
        "P1_HIGH",
        "warn_high",
        "OPTIONAL",
        rx(r"(立案|调查通知书|涉嫌.*违法|行政处罚|处罚决定|纪律处分|公开谴责|监管措施|监管警示|警示函|市场禁入|移送司法|刑事|拘留|取保候审)"),
        "Regulatory investigation, penalty, discipline, or criminal/legal enforcement.",
    ),
    Rule(
        "debt_default_overdue",
        "P1_HIGH",
        "warn_high",
        "OPTIONAL",
        rx(r"(债务逾期|贷款逾期|票据逾期|债券违约|债务违约|未能.*兑付|不能按期.*兑付|本息兑付.*风险|流动性风险)"),
        "Debt overdue/default or bond repayment risk.",
    ),
    Rule(
        "audit_opinion_internal_control_risk",
        "P1_HIGH",
        "warn_high",
        "OPTIONAL",
        rx(r"(非标准审计|非无保留意见|保留意见|否定意见|无法表示意见|内部控制.*否定|内部控制.*重大缺陷|财务报告.*重大缺陷|审计报告.*带强调事项)"),
        "Audit opinion, internal-control, or financial-reporting severe issue.",
    ),
    Rule(
        "capital_occupation_illegal_guarantee",
        "P1_HIGH",
        "warn_high",
        "OPTIONAL",
        rx(r"(资金占用|占用公司资金|非经营性占用|违规担保|违规对外担保|违规资金往来)"),
        "Fund occupation or illegal guarantee.",
    ),
    Rule(
        "governance_document_neutral",
        "P4_NEUTRAL",
        "discard_or_archive",
        "NO",
        rx(r"(公司章程|制度|规则|细则|管理办法|工作办法|议事规则|独立董事制度|董事会.*工作规程|监事会.*工作规程)$"),
        "Routine governance document.",
    ),
    Rule(
        "inquiry_concern_letter",
        "P2_REVIEW",
        "warn_review",
        "YES",
        rx(r"(问询函|关注函|监管函|问询函回复|关注函回复|年报问询|审核问询|落实函|反馈意见通知书|行政许可.*反馈意见|审核.*意见)"),
        "Exchange inquiry/concern/supervision letter or reply; needs context.",
    ),
    Rule(
        "key_personnel_change",
        "P2_REVIEW",
        "warn_review",
        "YES",
        rx(r"((董事长|总经理|总裁|财务负责人|首席财务官|董事会秘书|董秘).*(辞职|离任|变更|聘任|代行)|高级管理人员.*(辞职|离任))"),
        "Key executive or finance/disclosure officer change.",
    ),
    Rule(
        "litigation_arbitration_freeze",
        "P2_REVIEW",
        "warn_review",
        "YES",
        rx(r"(诉讼|仲裁|判决|裁定|执行通知|司法冻结|轮候冻结|股份冻结|查封|拍卖|强制执行)"),
        "Litigation, arbitration, court execution, freeze, or auction.",
    ),
    Rule(
        "pledge_shareholder_change_reduction",
        "P2_REVIEW",
        "warn_review",
        "YES",
        rx(r"(质押|解除质押|补充质押|平仓风险|减持|被动减持|权益变动|持股变动|股份转让|协议转让|表决权委托)"),
        "Pledge, reduction, transfer, or shareholder-rights change.",
    ),
    Rule(
        "control_change_ma_restructuring",
        "P2_REVIEW",
        "warn_review",
        "YES",
        rx(r"(控制权变更|实际控制人.*变更|重大资产重组|并购重组|发行股份购买资产|购买资产|出售资产|资产出售|资产收购|要约收购|吸收合并|重大交易)"),
        "Control change, M&A, restructuring, acquisition, or disposal.",
    ),
    Rule(
        "guarantee_financial_assistance_related_party",
        "P2_REVIEW",
        "warn_review",
        "YES",
        rx(r"(提供担保|对外担保|担保额度|财务资助|关联交易|关联方|资金拆借|委托贷款)"),
        "Guarantee, financial assistance, related-party transaction, or lending.",
    ),
    Rule(
        "performance_forecast_revision_impairment",
        "P2_REVIEW",
        "warn_review",
        "YES",
        rx(r"(业绩预告|业绩快报|业绩修正|业绩预告修正|业绩快报修正|亏损|扭亏|预亏|减值准备|资产减值|商誉减值|会计差错|前期会计差错|更正财务|追溯调整)"),
        "Performance forecast/express, revision, loss, impairment, or accounting correction.",
    ),
    Rule(
        "financing_dilution_debt_instruments",
        "P2_REVIEW",
        "warn_review",
        "YES",
        rx(r"(向特定对象发行|非公开发行|定增|配股|可转换公司债|可转债|公司债券|短期融资券|中期票据|融资租赁|授信额度|募集资金|资产支持专项计划|资产支持证券|ABS)"),
        "Financing, dilution, debt instrument, credit line, or proceeds event.",
    ),
    Rule(
        "suspension_resumption",
        "P2_REVIEW",
        "warn_review",
        "NO",
        rx(r"(停牌|复牌|临时停牌|继续停牌)"),
        "Suspension/resumption title; tradability must also use suspend_d.",
    ),
    Rule(
        "positive_contract_order_project",
        "P3_POSITIVE_CANDIDATE",
        "record_only",
        "OPTIONAL",
        rx(r"(中标|预中标|签订.*合同|重大合同|订单|框架协议|战略合作|项目投产|投产|产能|获得.*补助|政府补助|产品获批|注册证|临床试验|专利|新药|一致性评价)"),
        "Potential positive operating event; alpha use requires validation.",
    ),
    Rule(
        "buyback_increase_holding_dividend",
        "P3_POSITIVE_CANDIDATE",
        "record_only",
        "OPTIONAL",
        rx(r"(回购|增持|员工持股计划|股权激励|激励计划|股票期权|限制性股票|利润分配|现金分红|权益分派|股份奖励)"),
        "Buyback, increase holding, employee incentive, or distribution candidate.",
    ),
    Rule(
        "periodic_report_neutral",
        "P4_NEUTRAL",
        "discard_or_archive",
        "NO",
        rx(r"(年度报告|半年度报告|季度报告|第一季度报告|第三季度报告|摘要|审计报告|财务报告|内部控制评价报告|社会责任报告|ESG报告|环境、社会及治理报告)"),
        "Periodic report or routine report disclosure.",
    ),
    Rule(
        "meeting_resolution_neutral",
        "P4_NEUTRAL",
        "discard_or_archive",
        "NO",
        rx(r"(董事会.*决议|监事会.*决议|股东大会|临时股东大会|会议决议|独立董事.*意见|法律意见书|律师事务所.*意见)"),
        "Meeting, resolution, independent opinion, or legal opinion.",
    ),
    Rule(
        "ipo_refinancing_review_neutral",
        "P4_NEUTRAL",
        "discard_or_archive",
        "NO",
        rx(r"(招股说明书|上市公告书|上市保荐书|发行保荐书|保荐.*报告|上市委.*会议|注册申请|审核中心|申报稿|上会稿)"),
        "IPO/refinancing review document; usually not a secondary-market warning.",
    ),
    Rule(
        "routine_correction_supplement_neutral",
        "P4_NEUTRAL",
        "discard_or_archive",
        "NO",
        rx(r"(更正公告|补充公告|提示性公告|进展公告|公告的更正|公告的补充)$"),
        "Routine correction/supplement/progress title not caught by risk rules.",
    ),
    Rule(
        "routine_professional_report_neutral",
        "P4_NEUTRAL",
        "discard_or_archive",
        "NO",
        rx(r"(验资报告|专项报告|核查意见|鉴证报告|审阅报告|评估报告|估值报告|资信评级报告|跟踪评级报告|受托管理事务报告|持续督导.*报告)$"),
        "Routine professional intermediary report.",
    ),
    Rule(
        "routine_personnel_change_neutral",
        "P4_NEUTRAL",
        "discard_or_archive",
        "NO",
        rx(r"(补选|选举|换届|聘任|任职|辞职|离任|调整.*董事|调整.*监事|高级管理人员变动)"),
        "Routine personnel change not caught as key-person risk.",
    ),
]

DEFAULT_CLASS = {
    "event_type": "unclassified_archive",
    "risk_level": "P4_NEUTRAL",
    "action": "archive_for_rule_mining",
    "needs_llm": "SAMPLE_ONLY",
    "description": "Unclassified by v0 title rules; archive for rule mining and sample-based QA, not automatic LLM.",
}


def classify_title(title: str) -> Dict[str, str]:
    normalized = re.sub(r"\s+", "", title or "")
    for rule in RULES:
        if rule.exclude is not None and rule.exclude.search(normalized):
            continue
        if rule.pattern.search(normalized):
            return {
                "event_type": rule.event_type,
                "risk_level": rule.risk_level,
                "action": rule.action,
                "needs_llm": rule.needs_llm,
                "description": rule.description,
            }
    return dict(DEFAULT_CLASS)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Classify announcement titles from market.anns")
    parser.add_argument("--start-date", default="2018-08-01")
    parser.add_argument("--end-date", default="2026-04-30")
    parser.add_argument("--json-out", default=None)
    parser.add_argument("--md-out", default=None)
    parser.add_argument("--sample-per-type", type=int, default=8)
    parser.add_argument("--progress-every", type=int, default=500000)
    return parser.parse_args()


def output_paths(args: argparse.Namespace) -> Tuple[Path, Path]:
    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = Path(args.json_out) if args.json_out else ROOT / "reports" / "anns" / f"announcement_title_classification_v0_{ts}.json"
    md_path = Path(args.md_out) if args.md_out else ROOT / "docs" / "analysis" / f"announcement_title_classification_v0_{ts}.md"
    json_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.parent.mkdir(parents=True, exist_ok=True)
    return json_path, md_path


def stream_rows(conn, start_date: str, end_date: str):
    cur = conn.cursor(name="announcement_title_classification_v0_cursor")
    cur.itersize = 20000
    cur.execute(
        """
        SELECT ann_date, ts_code, name, title, url, rec_time
          FROM market.anns
         WHERE ann_date BETWEEN %s AND %s
         ORDER BY ann_date, ts_code, title
        """,
        (start_date, end_date),
    )
    try:
        for row in cur:
            yield row
    finally:
        cur.close()


def run() -> int:
    args = parse_args()
    json_path, md_path = output_paths(args)
    started = time.time()

    conn = psycopg2.connect(**DB_CFG)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM market.anns WHERE ann_date BETWEEN %s AND %s",
                (args.start_date, args.end_date),
            )
            total_rows = int(cur.fetchone()[0])

        counts_by_type: Counter[str] = Counter()
        counts_by_level: Counter[str] = Counter()
        counts_by_action: Counter[str] = Counter()
        counts_by_needs_llm: Counter[str] = Counter()
        counts_by_year_level: Dict[str, Counter[str]] = defaultdict(Counter)
        samples: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        source_counts: Counter[str] = Counter()

        processed = 0
        for ann_date, ts_code, name, title, url, rec_time in stream_rows(conn, args.start_date, args.end_date):
            result = classify_title(str(title or ""))
            event_type = result["event_type"]
            risk_level = result["risk_level"]
            action = result["action"]
            needs_llm = result["needs_llm"]

            counts_by_type[event_type] += 1
            counts_by_level[risk_level] += 1
            counts_by_action[action] += 1
            counts_by_needs_llm[needs_llm] += 1
            counts_by_year_level[str(ann_date.year)][risk_level] += 1
            if "eastmoney.com" in str(url or ""):
                source_counts["eastmoney_url"] += 1
            elif "cninfo.com.cn" in str(url or ""):
                source_counts["cninfo_url"] += 1
            else:
                source_counts["other_url"] += 1

            if len(samples[event_type]) < args.sample_per_type:
                samples[event_type].append(
                    {
                        "ann_date": ann_date.isoformat(),
                        "ts_code": ts_code,
                        "name": name,
                        "title": title,
                        "risk_level": risk_level,
                        "action": action,
                        "needs_llm": needs_llm,
                        "rec_time": rec_time.isoformat() if rec_time else None,
                    }
                )

            processed += 1
            if args.progress_every > 0 and processed % args.progress_every == 0:
                elapsed = time.time() - started
                print(f"[PROGRESS] {processed}/{total_rows} rows rate={processed / max(elapsed, 1):.0f}/s", flush=True)

        rules_payload = [
            {
                "event_type": r.event_type,
                "risk_level": r.risk_level,
                "action": r.action,
                "needs_llm": r.needs_llm,
                "pattern": r.pattern.pattern,
                "description": r.description,
            }
            for r in RULES
        ]
        summary = {
            "rule_version": "aistock_announcement_title_rules_v0_20260505",
            "scope": {"start_date": args.start_date, "end_date": args.end_date},
            "total_rows": total_rows,
            "processed_rows": processed,
            "source_counts": dict(source_counts),
            "counts_by_event_type": dict(counts_by_type.most_common()),
            "counts_by_risk_level": dict(counts_by_level.most_common()),
            "counts_by_action": dict(counts_by_action.most_common()),
            "counts_by_needs_llm": dict(counts_by_needs_llm.most_common()),
            "counts_by_year_level": {year: dict(counter) for year, counter in sorted(counts_by_year_level.items())},
            "samples": samples,
            "rules": rules_payload,
            "elapsed_sec": round(time.time() - started, 3),
        }

        with json_path.open("w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2, default=str)

        write_markdown(md_path, summary)
        print(json.dumps({"json": str(json_path), "markdown": str(md_path), "summary": summary["counts_by_risk_level"]}, ensure_ascii=False), flush=True)
    finally:
        conn.close()
    return 0


def write_markdown(path: Path, summary: Dict[str, Any]) -> None:
    lines: List[str] = []
    lines.append("# AIstock Announcement Title Classification v0")
    lines.append("")
    lines.append(f"- Rule version: `{summary['rule_version']}`")
    lines.append(f"- Scope: `{summary['scope']['start_date']}` to `{summary['scope']['end_date']}`")
    lines.append(f"- Rows processed: `{summary['processed_rows']:,}`")
    lines.append(f"- Source URL mix: `{summary['source_counts']}`")
    lines.append("")
    lines.append("## Risk Level Counts")
    lines.append("")
    lines.append("| risk_level | rows | pct |")
    lines.append("|---|---:|---:|")
    total = max(int(summary["processed_rows"]), 1)
    for key, value in summary["counts_by_risk_level"].items():
        lines.append(f"| {key} | {value:,} | {value / total:.2%} |")
    lines.append("")
    lines.append("## Event Type Counts")
    lines.append("")
    lines.append("| event_type | rows | pct |")
    lines.append("|---|---:|---:|")
    for key, value in summary["counts_by_event_type"].items():
        lines.append(f"| {key} | {value:,} | {value / total:.2%} |")
    lines.append("")
    lines.append("## Engine Interpretation")
    lines.append("")
    lines.append("- `P0_BLOCK`: title alone is enough to block new buys in backtest and live overlay.")
    lines.append("- `P1_HIGH`: high-risk warning; title can trigger risk reduction/watchlist, PDF/LLM optional for explanation.")
    lines.append("- `P2_REVIEW`: candidate risk/complex event; keep signal, and use PDF/LLM when position impact is material.")
    lines.append("- `P3_POSITIVE_CANDIDATE`: record only in phase 1; no positive alpha boost until event-study validation.")
    lines.append("- `P4_NEUTRAL`: routine archive/discard for warning engine.")
    lines.append("")
    lines.append("## Samples")
    lines.append("")
    for event_type, rows in summary["samples"].items():
        lines.append(f"### {event_type}")
        for row in rows:
            safe_title = str(row["title"]).replace("|", " ")
            lines.append(f"- `{row['ann_date']}` `{row['ts_code']}` {safe_title}")
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(run())
