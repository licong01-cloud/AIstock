# QE/HMM Experiment Infrastructure Issues For QE Team

Created: 2026-05-08

Owner requested boundary: QE development team should resolve the QE/runtime issues below. HMM research should continue focusing on model evolution and should not carry QE infrastructure fixes unless explicitly reassigned.

## Scope

This document records two QE-side issues observed while validating HMM models in QE experiments:

1. Backtest-only parallel loop failure caused by malformed MLflow/Qlib metric files.
2. Strategy-level fixed single-stock order cap limiting actual HMM sizing effect.

These issues affect the reliability or interpretation of HMM QE experiments, but they are not HMM model-training defects.

## Issue 1: Backtest-Only Parallel Loop Fails With Malformed MLflow Metric

### Observed Experiment

- Task: `qe_20260508_120507_d279`
- Loop: `Loop4`
- Loop label: `PROD_L5__autocycle_r5_return_ref`
- HMM snapshot: `5cdaa81c-76ce-41b4-bb7b-dbb3c279d587`
- Runtime mode: backtest-only, remote node `rdagent-node1`, parallel execution
- Status at diagnosis: task still running; Loop1/2/3/5 completed, Loop4 failed, Loop6/7/8 still running.

### Symptom

Loop4 failed after it had already:

- Prepared all factors successfully.
- Loaded the existing trained model from source recorder.
- Generated `pred.pkl`.
- Computed IC and Rank IC.
- Created a new QE recorder binding.

The failure occurred while Qlib/MLflow tried to read metric files:

```text
ValueError: Metric 'Rank IC' is malformed. No data found.
```

The final error in `run.log` was:

```text
[ERROR] loop=Loop4 error=QLib backtest failed with return code 1
```

### Evidence

Remote loop log showed the backtest-only path was used:

```text
[INFO] Backtest-only mode: skipping model training, loading existing model
[INFO] Loaded trained model from recorder 0c290fd68d924a67a3e8f3605fa0782a
[INFO] QE recorder binding written: .../Loop4/qe_current_recorder.json recorder_id=dc50c2d2365e4c4699a1c4d79016c0b1 mode=backtest_only
```

The run also printed valid IC values before failing:

```text
{'IC': np.float64(0.062456985295656485),
 'ICIR': np.float64(0.6411419980885462),
 'Rank IC': np.float64(0.10531296512699834),
 'Rank ICIR': np.float64(0.978942196646338)}
```

Then Qlib/MLflow failed while reading metric artifacts:

```text
File ".../mlflow/store/tracking/file_store.py", line 831, in _get_metric_from_file
    raise ValueError(f"Metric '{metric_name}' is malformed. No data found.")
ValueError: Metric 'Rank IC' is malformed. No data found.
```

The current backtest-only loop setup symlinks each loop's `mlruns` to the same source model run directory:

```text
Symlink mlruns:
/home/lc999/projects/RD-Agent-main/qe_workspace/qe_20260506_220823_6489/Loop1/mlruns
to
/home/lc999/projects/RD-Agent-main/qe_workspace/qe_20260508_120507_d279/Loop4/mlruns
```

Other loops in the same task also used the same source `mlruns` path and completed successfully. This points to an intermittent parallel file-store/metric-write problem rather than a deterministic HMM configuration error.

### Likely Root Cause

The most likely root cause is unsafe reuse of the source `mlruns` directory during parallel backtest-only execution:

- Backtest-only needs to read the trained source model/recorder.
- The current loop also writes a new Qlib recorder under an `mlruns` path that is symlinked to the source model task.
- Multiple loops run in parallel and may create/read MLflow file-store metrics under the same underlying experiment store.
- A metric file such as `Rank IC` can exist but be empty or partially written when MLflow reads the run metadata.

This is a QE/runtime isolation problem. It is not evidence that the L5 HMM model itself is invalid.

### Impact

- Individual loop failure can occur even when the model, factor data, and backtest logic are valid.
- Automated HMM experiment cycles may incorrectly treat a valid HMM candidate/reference as failed.
- If failures are not rerun, analysis tables can miss one reference loop and distort HMM comparison.
- The problem is more likely under higher parallelism and backtest-only reuse of trained models.

### Recommended QE Fix

The QE team should make backtest-only recorder isolation explicit:

1. Use the source `mlruns` only for reading the trained model artifact.
2. Write the new backtest recorder into a loop-local, non-symlinked MLflow tracking directory.
3. Avoid parallel loops writing new runs into the same source `mlruns` file store.
4. Add a fail-fast check before launching a loop:
   - source model recorder exists and is readable;
   - target recorder path is loop-local;
   - target recorder path is not the same real path as the source `mlruns`.
5. Add retry handling for malformed empty metric files only after confirming the target recorder path is isolated.

### Short-Term Workaround

For the active experiment, wait until the other loops finish, then rerun only failed Loop4 with the same config. This avoids increasing concurrent MLflow write pressure while Loop6/7/8 are still running.

## Issue 2: Fixed 5M Single-Stock Cap Suppresses Actual HMM Position-Sizing Effect

### Observed Experiments

The issue was diagnosed across recent HMM QE validation tasks, including:

- `qe_20260507_225143_7765`
- `qe_20260508_030507_774e`
- `qe_20260508_060509_1268`

In the 8-loop diagnostics, final cash was high across both HMM and no-HMM loops. Example from `qe_20260508_060509_1268`:

- No-HMM Loop1 final cash: about `61.12M`
- HMM loops final cash: about `60.63M` to `65.96M`
- All loops ended with 50 stocks.

This pattern is consistent with a fixed per-stock cap:

```text
final NAV around 312M to 317M
50 stocks * 5M max per stock = about 250M invested
remaining cash = about 60M to 67M
```

### Symptom

The strategy intends to apply score-weighted/top-ranked sizing, but actual position value is capped by a fixed single-order/position value. As account NAV grows, the fixed cap dominates the weight calculation.

The effective behavior becomes close to:

```text
target_value = min(total_account_value * target_weight, 5,000,000)
```

For a portfolio around 300M NAV and `max_weight = 5%`, intended top position size could be about 15M. A fixed 5M cap forces it to one third of the intended exposure.

### Impact On HMM Validation

This does not make HMM completely invalid, but it changes what the backtest is measuring:

- The current results reflect capacity-constrained HMM score/rank behavior.
- They do not fully measure HMM's intended effect on position sizing.
- HMM adjustments to stock priority or sector penalty may be muted because final per-name exposure is clipped.
- High final cash should not be interpreted as HMM excluding too many stocks; no-HMM also showed high idle cash and all loops held 50 stocks.
- Comparisons between HMM versions remain partially useful, but the absolute return and sizing effect may be understated.

### Recommended QE Fix

QE should expose and persist strategy capacity parameters as first-class experiment parameters:

1. Make `max_single_order_value`, `max_weight`, and `max_position_ratio` visible in task/loop config and UI.
2. Avoid hidden strategy defaults for HMM validation tasks.
3. Support parameter-only custom experiments that do not require code changes.
4. For HMM sizing tests, use:

```yaml
strategy_params:
  max_single_order_value: 1000000000.0
  max_weight: 0.05
  max_position_ratio: 0.95
```

This lets `max_weight=5%` and `max_position_ratio=95%` control sizing instead of a fixed 5M cap.

### Required Validation

The QE team should add a capacity audit to result extraction:

- final cash;
- final stock count;
- actual per-stock position value distribution;
- top-k target weight vs actual filled weight;
- total gross exposure;
- number of positions clipped by `max_single_order_value`;
- turnover and cost changes after removing the fixed cap.

The HMM team should compare HMM candidates only after this capacity mode is clearly labeled in the experiment.

## HMM Research Boundary

For HMM evolution work, these two QE-side issues should be treated as infrastructure assumptions:

- If a loop fails with malformed MLflow metric files, rerun the loop after other loops finish; do not discard the HMM model based on that failure alone.
- If an experiment uses the fixed 5M cap, interpret it as a capacity-constrained comparison.
- For future HMM validation, prefer clearly labeled parameter-only capacity experiments so the model effect is not hidden by strategy defaults.

HMM development should continue on model retraining, sector-state features, coefficient mapping, and candidate selection. QE development should own the recorder isolation and strategy capacity parameterization fixes.
