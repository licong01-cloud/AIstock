# Phase 13 Non-Hard Financial Distress Signal Policy Config Proposal - 2026-05-09

This is a research-only configuration proposal. It converts the Phase 12 finding into a reproducible policy shape, but does not connect the signal to QE runtime, Selection Center, Paper Trading, QMT, simulated trading, or live trading.

## Scope

```text
+------------------+---------------------------------------------------------------------+
| item             | value                                                               |
+------------------+---------------------------------------------------------------------+
| branch           | codex/financial-distress-rerank-20260508                            |
| phase            | 13                                                                  |
| purpose          | propose the first non-hard financial distress score-down config      |
| runtime impact   | none                                                                |
| DB schema impact | none in this phase                                                   |
| source evidence  | Phase 10, Phase 11, Phase 12 QE overlay and direct-event research    |
| decision style   | score-down only; no hard buy ban; no forced sell                    |
+------------------+---------------------------------------------------------------------+
```

## Evidence Chain

```text
+-------+-----------------------------------+---------------------------------------------+
| phase | evidence                          | implication                                 |
+-------+-----------------------------------+---------------------------------------------+
| 10    | 10-30bn indicator decline works   | keep as primary medium-cap candidate         |
| 11    | direct event returns are mixed    | do not treat as unconditional bad event      |
| 12    | context score-down has safe shape | use rank-aware decay; avoid hard actions     |
+-------+-----------------------------------+---------------------------------------------+
```

Key interpretation: the signal is not a pure downside event. It is useful only when the base model already ranks the stock highly, so the policy should reduce priority inside a candidate list instead of directly banning or forcing exit.

## Proposed First Profile

```text
+----------------------+--------------------------------------------------------------+
| field                | proposed value                                               |
+----------------------+--------------------------------------------------------------+
| profile_id           | event_signal_policy_fin_distress_score_down_v1_20260509     |
| profile_status       | DRAFT                                                        |
| policy_scope         | research_overlay                                             |
| time_mode            | backtest first; paper/live only after runtime validation      |
| default_action_mode  | risk_first                                                   |
| positive_overlay     | disabled                                                     |
| hard_buy_block       | disabled                                                     |
| force_exit           | disabled                                                     |
| first_rule           | indicator_large_decline_mv_10_30bn_score_down_v1            |
| default_active_td    | 20 trading days                                              |
| compare_active_td    | 60 trading days                                              |
| score_down_profile   | rank_decay_balanced                                          |
| sector_relief        | configurable but not decision driver                         |
+----------------------+--------------------------------------------------------------+
```

## First Rule Definition

```text
+----------------------+--------------------------------------------------------------+
| field                | value                                                        |
+----------------------+--------------------------------------------------------------+
| rule_key             | indicator_large_decline_mv_10_30bn_score_down_v1            |
| source_event_type    | financial_indicator_large_decline                            |
| source_rule_version  | unified_event_signal_rules_v0_20260506                       |
| market_cap_filter    | PIT market cap bucket = 10-30bn CNY                          |
| lifecycle_kind       | window                                                       |
| state_family         | financial_distress_warning                                   |
| state_type           | indicator_decline_10_30bn_score_down                         |
| policy_risk_level    | P2_REVIEW                                                    |
| primary_action       | score_down                                                   |
| block_buy            | false                                                        |
| block_add            | false                                                        |
| force_exit           | false                                                        |
| sell_only            | false                                                        |
| validity_td_default  | 20                                                           |
| validity_td_compare  | 60                                                           |
| priority             | after ST hard-risk rules, before neutral record-only events  |
+----------------------+--------------------------------------------------------------+
```

## Context Score-Down Parameters

The research simulator demotes rank, not raw model score. A penalty of `12%` with `top_k=50` means about `ceil(50 * 12%) = 6` rank slots of demotion before re-sorting.

```text
+--------------------------+--------+---------------------------------------------------+
| parameter                | value  | meaning                                           |
+--------------------------+--------+---------------------------------------------------+
| top_k                    | 50     | apply only inside the selected candidate universe |
| base_pct                 | 10.0%  | default rank-demotion percentage                  |
| top_quintile_add_pct     | 10.0%  | extra penalty for original ranks 1-10             |
| top_half_add_pct         | 5.0%   | extra penalty for original ranks 11-25            |
| loss_ge_100_add_pct      | 5.0%   | add if loss / market cap >= 100%                  |
| loss_ge_50_add_pct       | 2.5%   | add if loss / market cap >= 50%                   |
| repeated_loss_add_pct    | 2.5%   | add if trailing loss reports >= 2                 |
| miss_gap_ge_50_add_pct   | 2.5%   | add if expectation miss gap >= 50                 |
| multi_signal_add_pct     | 2.5%   | add if multiple active financial signals coexist  |
| decay_floor              | 50.0%  | minimum multiplier at the end of active window    |
| max_pct                  | 30.0%  | cap for final rank-demotion percentage            |
+--------------------------+--------+---------------------------------------------------+
```

## Recommended Runtime Shape Later

```text
+------+------------------------------------------------------+---------------------------------------------------+
| step | future engine step                                   | parity requirement                                |
+------+------------------------------------------------------+---------------------------------------------------+
| 1    | generate or load raw event_signal rows                | same source rule_version for backtest and live    |
| 2    | expand event rows into active state spans             | same policy profile and time_mode semantics       |
| 3    | generate base model candidate ranks                   | event signal must not change raw alpha generation |
| 4    | join active overlays to candidate TopK                | apply only to visible signals by trade date       |
| 5    | calculate rank-aware penalty                          | use same config_hash in backtest, paper, live     |
| 6    | rerank after demotion and refill from lower ranks      | persist enough trace for audit                    |
+------+------------------------------------------------------+---------------------------------------------------+
```

Important: for this signal family, the engine should not write a static positive or negative alpha factor. The overlay depends on candidate rank, active age, and signal severity, so it belongs after base alpha scoring and before final selection/trade generation.

## Existing Table Mapping

No schema change is required for Phase 13. Existing policy tables can store the proposal as a draft later, but this phase only documents the config.

```text
+----------------------------------+--------------------------------------+---------------------------------------------+
| table                            | current ability                       | phase-13 use                                 |
+----------------------------------+--------------------------------------+---------------------------------------------+
| event_signal_policy_profile      | versioned profile + config_hash       | store full policy JSON after approval        |
| event_signal_effect_rule         | per-rule match/action/params          | store rule_params for context score-down     |
| event_signal_state_span          | active per-symbol state window        | store warning state with expiry date         |
| event_signal_daily_overlay       | per-date per-symbol overlay           | store warn/score_down row, no hard action    |
| event_signal_validation_result   | QE/event-study validation evidence    | store future single-rule and stacked results |
+----------------------------------+--------------------------------------+---------------------------------------------+
```

Caveat: `event_signal_daily_overlay` can store the active warning and static metadata, but the final rank penalty is candidate-list dependent. If future consumers require a full audit trail of every candidate rerank decision, add a separate application-trace table or reuse the consuming selection run trace. Do not overload raw source tables.

## Draft Config JSON

```json
{
  "profile_id": "event_signal_policy_fin_distress_score_down_v1_20260509",
  "profile_version": "fin_distress_score_down_v1_20260509",
  "profile_status": "DRAFT",
  "policy_scope": "research_overlay",
  "time_modes_supported": ["backtest", "paper", "live", "observed"],
  "positive_overlay_enabled": false,
  "hard_actions_enabled": false,
  "consumer_enablement": {
    "qe_runtime": false,
    "selection_center": false,
    "paper_trading": false,
    "qmt_live": false
  },
  "rules": [
    {
      "rule_key": "indicator_large_decline_mv_10_30bn_score_down_v1",
      "event_type": "financial_indicator_large_decline",
      "source_rule_version": "unified_event_signal_rules_v0_20260506",
      "market_cap_bucket": "mv_10bn_to_30bn_yuan",
      "primary_action": "score_down",
      "block_buy": false,
      "block_add": false,
      "force_exit": false,
      "sell_only": false,
      "validity_trading_days_default": 20,
      "validity_trading_days_candidates": [20, 60],
      "context_score_down_profile": "rank_decay_balanced",
      "top_k": 50,
      "ranking_date_mode": "previous",
      "rank_penalty_params": {
        "base_pct": 0.10,
        "top_quintile_add_pct": 0.10,
        "top_half_add_pct": 0.05,
        "loss_ge_100_add_pct": 0.05,
        "loss_ge_50_add_pct": 0.025,
        "repeated_loss_add_pct": 0.025,
        "miss_gap_ge_50_add_pct": 0.025,
        "multi_signal_add_pct": 0.025,
        "decay_floor": 0.50,
        "max_pct": 0.30
      }
    }
  ],
  "promotion_gates": {
    "minimum_qe_loops": 10,
    "additional_experiment_required": true,
    "true_qe_rerun_required_before_runtime": true,
    "no_hard_action_for_financial_distress": true,
    "positive_alpha_boost_disabled_until_model_validation": true
  }
}
```

## Promotion Gates

```text
+--------------------------+-------------------+---------------------------------------------------+
| gate                     | required status   | reason                                            |
+--------------------------+-------------------+---------------------------------------------------+
| same 10-loop overlay     | passed            | already supported by Phase 12                     |
| direct event study       | mixed             | prevents hard ban or forced sell                  |
| additional QE experiments| required          | reduce dependence on one experiment family        |
| true QE rerun            | required          | current overlay is artifact-level approximation   |
| runtime isolation        | required          | no QE/Paper/Selection/QMT coupling before approval|
| DB write path            | not required now  | no schema or data mutation in Phase 13            |
+--------------------------+-------------------+---------------------------------------------------+
```

## Decision

```text
+--------------------------------------+----------------------+-------------------------------------------------+
| candidate                            | phase-13 decision    | next action                                     |
+--------------------------------------+----------------------+-------------------------------------------------+
| indicator_large_decline_mv_10_30bn   | CONFIG_DRAFT_READY   | validate on more QE loops/experiments           |
| smallcap_loss_mv50                   | KEEP_BENCHMARK       | keep as comparator, not first runtime candidate  |
| indicator_decline_30_100bn           | WATCHLIST_ONLY       | do not promote                                  |
| structured_financial_risk_10_30bn    | RESEARCH_ONLY        | use for coverage comparison only                |
| positive score-up financial signals  | DEFER                | wait for model training and separate validation  |
+--------------------------------------+----------------------+-------------------------------------------------+
```

## Next Phase

Phase 14 should not be LLM/PDF yet. The better next step is additional offline validation:

1. Validate this exact config on more QE experiments and loops if artifacts are available.
2. Compare `20td` and `60td` on the same config, keeping `20td` as default unless 60td materially improves tail risk.
3. If additional evidence remains positive, design a research-only persistence path that writes draft policy rows and validation rows, still without runtime consumers.
4. Only after that decide whether a true QE rerun or Selection Center integration is justified.
