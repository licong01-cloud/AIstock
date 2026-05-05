# Announcement Title Classification v0 DB Summary

- Rule version: `aistock_announcement_title_rules_v0_20260505`
- market.anns rows: `5131337`
- classified rows: `5131337`
- risk signal rows: `1414628`
- coverage ratio: `1.000000`

## Risk Level Counts

| risk_level | rows |
| --- | ---: |
| P4_NEUTRAL | 3311376 |
| P2_REVIEW | 1299530 |
| P3_POSITIVE_CANDIDATE | 405333 |
| P1_HIGH | 89648 |
| P0_BLOCK | 25450 |

## Risk Signals

| risk_level | action | rows |
| --- | --- | ---: |
| P2_REVIEW | warn_review | 1299530 |
| P1_HIGH | warn_high | 89648 |
| P0_BLOCK | block_buy | 25450 |

## Source Time Quality

| source_time_quality | rows |
| --- | ---: |
| EXACT | 4894061 |
| MISSING | 201023 |
| MIDNIGHT_DEFAULT | 36253 |

## Top Event Types

| event_type | risk_level | rows |
| --- | --- | ---: |
| unclassified_archive | P4_NEUTRAL | 1187023 |
| meeting_resolution_neutral | P4_NEUTRAL | 1109764 |
| financing_dilution_debt_instruments | P2_REVIEW | 463415 |
| periodic_report_neutral | P4_NEUTRAL | 421466 |
| buyback_increase_holding_dividend | P3_POSITIVE_CANDIDATE | 350287 |
| guarantee_financial_assistance_related_party | P2_REVIEW | 234735 |
| pledge_shareholder_change_reduction | P2_REVIEW | 199155 |
| investor_relations_neutral | P4_NEUTRAL | 160158 |
| inquiry_concern_letter | P2_REVIEW | 117413 |
| governance_document_neutral | P4_NEUTRAL | 114825 |
| routine_professional_report_neutral | P4_NEUTRAL | 111246 |
| control_change_ma_restructuring | P2_REVIEW | 92512 |
| performance_forecast_revision_impairment | P2_REVIEW | 87025 |
| routine_personnel_change_neutral | P4_NEUTRAL | 82795 |
| routine_correction_supplement_neutral | P4_NEUTRAL | 75842 |
| capital_occupation_illegal_guarantee | P1_HIGH | 65323 |
| positive_contract_order_project | P3_POSITIVE_CANDIDATE | 53566 |
| ipo_refinancing_review_neutral | P4_NEUTRAL | 48257 |
| stock_price_abnormal_volatility | P2_REVIEW | 41959 |
| litigation_arbitration_freeze | P2_REVIEW | 38495 |
| key_personnel_change | P2_REVIEW | 18308 |
| delisting_or_risk_warning | P0_BLOCK | 16901 |
| regulatory_investigation_penalty | P1_HIGH | 16077 |
| bankruptcy_restructuring | P0_BLOCK | 8549 |
| audit_opinion_internal_control_risk | P1_HIGH | 7444 |
| suspension_resumption | P2_REVIEW | 6513 |
| risk_warning_removed | P3_POSITIVE_CANDIDATE | 1480 |
| debt_default_overdue | P1_HIGH | 804 |
