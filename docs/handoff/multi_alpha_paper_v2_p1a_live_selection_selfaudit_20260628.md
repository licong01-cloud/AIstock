# 多 Alpha Paper v2 P1a Live Selection 自审记录（2026-06-28）

## 范围确认

- worktree：F:\Dev\AIstock_worktrees\multi-alpha-paper-v2-p1a-live-selection-20260628
- branch：eature/multi-alpha-paper-v2-p1a-live-selection-20260628
- 权威设计：docs/analysis/multi_alpha_paper_v2_route_architecture_20260626.md，已确认 d0a54580 与 P0 eb1e0d50 在 origin/main。
- 本轮只实现 P1a 信号层：MultiAlphaLivePredictionProvider、MultiAlphaWeightService、SelectionScoreArtifact authoritative 接线、StrategyPackageRuntime artifact 读取。
- 未触碰执行层、Research Assistant、前端、PaperPortfolio 单 package_id 主契约；未新增 DDL/迁移/生产 DB 写入；未启动或重启服务。

## 设计验收矩阵（P1a 子集）

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-004 每日 live inference 重算，pred-store 仅作 evidence | ackend/services/strategy_package/multi_alpha_live.py MultiAlphaLivePredictionProvider.generate_artifacts; ackend/services/strategy_package/selection_artifact.py MULTI_ALPHA 分支 | ackend/tests/strategy_package/test_multi_alpha_live_selection.py::test_multi_alpha_live_selection_artifact_is_authoritative_and_deterministic 断言 per-seed provider 调用与 metadata prediction_source_policy=live_inference_only | pass | P1b 真实 Paper dry-run 不在本轮范围 |
| F-005 live ic_weighted 只用成熟 label 窗口 | ackend/services/strategy_package/multi_alpha_live.py MultiAlphaWeightService.weights_for_apply_date | 	est_multi_alpha_live_rolling_weight_failures 覆盖 unavailable / insufficient / all non-positive；happy path 使用 label_horizon + settlement_lag 后的成熟窗口 | pass | 默认不新增表；权重输入通过 artifact config/metric provider 接缝，生产可接真实 provider |
| F-006 seed ensemble -> leg normalization -> combine -> final topK | ackend/services/strategy_package/multi_alpha_live.py _ensemble_seed_frames, _normalize_leg_frame, _align_component_frames, _combine_aligned, _artifact_rows | deterministic test 断言 component universe 60、final_topk 25、component_scores/weights 存在；coverage low negative test | pass | 不先腿 topK 截断 |
| F-007 no-silent failure | ackend/services/strategy_package/multi_alpha_live.py reason_code 常量与 _raise; ackend/services/strategy_package/runtime.py non-authoritative 拒绝 | negative tests 覆盖 leg missing、child sha mismatch、seed missing、coverage low、weight unavailable、label insufficient、all non-positive、topk mismatch、runtime disabled、deadline、non-authoritative | pass | - |
| F-008 SINGLE_ALPHA 不回归 | ackend/services/strategy_package/selection_artifact.py MULTI_ALPHA 早分支；SINGLE_ALPHA 原路径保持 | tk pytest -q backend/tests/strategy_package/test_multi_alpha_live_selection.py backend/tests/strategy_package/test_multi_alpha_promotion.py backend/tests/selection_center/test_runtime_selection.py = 77 passed | pass | - |
| A-6 信号层时延门 | ackend/services/strategy_package/multi_alpha_live.py _ensure_deadline | 	est_multi_alpha_deadline_gate_fails_before_inference 断言 multi_alpha_selection_artifact_deadline_exceeded 且 provider 未被调用 | pass | - |
| 确定性 replay | multi_alpha_selection_artifact_runtime_hash; metadata component/weight/combined sha | deterministic test 同 manifest/runtime_config/trade_date 重跑 scores_json 与 artifact sha 一致 | pass | artifact_id 可重写，sha/scores 为判定口径 |

## 新增 reason_code 清单

- multi_alpha_runtime_not_enabled
- multi_alpha_leg_missing
- multi_alpha_child_manifest_mismatch
- multi_alpha_seed_prediction_missing
- multi_alpha_component_coverage_low
- multi_alpha_weight_unavailable
- multi_alpha_label_window_insufficient
- multi_alpha_weight_all_non_positive
- multi_alpha_topk_runtime_mismatch
- multi_alpha_prediction_not_authoritative
- multi_alpha_selection_artifact_deadline_exceeded

## 生产门禁

- production_ddl_gate=noop：未新增表，weight artifact 存在 SelectionScoreArtifact.metadata。
- production_frontend_dependency_gate=noop：未改前端。
- production_backend_dependency_gate=noop：未新增依赖。
- 运行时激活：需要用户后续按发布窗口重启后端；本 PR 不启动/重启服务。
