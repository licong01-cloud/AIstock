# HMM QE Visible List Prune - 2026-05-05

## Scope

- Module: HMM / QE compose selector.
- Goal: remove ineffective HMM candidates from the QE-visible `sector_hmm` list.
- Retain only:
  - Loop10 current best: `ce4952c1-4b0d-46a7-81f2-ae1d4a249555` / snapshot `6ea64754-003d-48d8-ad9e-d0e7857716c8`.
  - Earliest currently visible baseline/control: `b99c907b-873a-4173-a4ee-5eab266f8c49` / snapshot `bbec3863-fb67-445f-938e-66f092d18696`.

## Change Method

- Did not physically delete HMM model artifacts or historical snapshots.
- Archived ineffective visible configs by changing `model_train_configs.model_type` from `sector_hmm` to `sector_hmm_disabled_ineffective_20260505`.
- This hides them from QE compose/evolution selectors because frontend fetches `/hmm-training/configs?model_type=sector_hmm`.
- Historical references by `snapshot_id` remain resolvable through snapshot detail/runtime paths.

## Archived Configs

- `a5b84940-6fe9-4e40-bef0-5963b99511f9` - `HMM_TEST_L10_FPBVALZ_BOTTOM15_PENALTY_0p985__qe20260505_stage2`
- `a82472c9-efbd-4839-8b93-c5b3dbdd7f62` - `HMM_TEST_L10_FPBVALZ_BOTTOM20_PENALTY_0p985__qe20260505_stage2`
- `cfff90c6-e64a-4c09-a4a6-17592a6667c7` - `HMM_TEST_L10_VOLCOMP_BOTTOM15_PENALTY_0p99__qe20260505_stage2`
- `c718d1f3-6ab5-46df-ac7f-c43c723149fe` - `HMM_TEST_L10_VOLCOMP_RISKONLY_0p995__qe20260505_stage2`
- `cec0d1c1-0801-4add-8b15-35c013a1c80f` - `HMM_TEST_L10_FPBVALZ_BOTTOM15_PENALTY_0p98__qe20260505`
- `4816274e-bee5-46d7-baff-5e95d97ef8a9` - `HMM_TEST_L10_FPBVALZ_BOTTOM20_PENALTY_0p98__qe20260505`
- `6e1eab11-8f74-4974-99f1-fe33e773c0f3` - `HMM_TEST_L10_FPBVALZ_BOTTOM25_PENALTY_0p98__qe20260505`

## Validation Commands

```powershell
# API selector validation on production backend without restart
python - <<'PY'
import requests
base='http://127.0.0.1:8001/api/v1'
rows=requests.get(f'{base}/hmm-training/configs', params={'model_type':'sector_hmm'}, timeout=60).json()
print(len(rows))
for row in rows: print(row['config_id'], row['display_name'])
PY

# DB visibility check
# SELECT model_type, count(*) FROM model_train_configs WHERE model_type LIKE 'sector_hmm%' GROUP BY model_type;
```

## Observed Results

- `/api/v1/hmm-training/configs?model_type=sector_hmm` returned exactly 2 configs.
- Both retained configs returned one completed snapshot with coefficient artifacts.
- DB counts include `sector_hmm = 2` and `sector_hmm_disabled_ineffective_20260505 = 7`.
- QE compose/evolution frontend source confirms both selectors use `model_type=sector_hmm`:
  - `frontend/src/app/quantevolver/compose/page.tsx`
  - `frontend/src/app/quantevolver/evolution/page.tsx`

## Evidence Paths

- Before backup: `.codex_tmp/hmm_qe_visible_prune_20260505/before_visible_sector_hmm_20260505_100430.json`
- DB update result: `.codex_tmp/hmm_qe_visible_prune_20260505/prune_result_20260505_100430.json`
- API validation snapshot: `.codex_tmp/hmm_qe_visible_prune_20260505/api_validation_20260505_100717.json`
- Stage2 attribution output: `.codex_tmp/hmm_qe_candidate_attribution_stage2/qe_20260505_043910_4ec8/hmm_qe_candidate_attribution.md`

## Residual Risks

- This is a logical archive, not physical deletion, to avoid breaking historical QE/Paper references and protected HMM artifacts.
- Existing browser sessions may need page refresh to reload the selector.
- The original unfixed HMM baseline is not currently visible in `sector_hmm`; the retained baseline is the earliest currently visible QE baseline/control.
