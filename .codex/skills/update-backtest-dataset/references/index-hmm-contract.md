# Domestic index and HMM contract

## Frozen universe

`index_universe_version=qe_hmm_domestic_core_v1` requires exactly:

| daily code | semantic role | required from | HMM benchmark | weight API code |
|---|---|---|---|---|
| `000001.SH` | `shanghai_composite` | 2018-08-01 | no | `null` |
| `000016.SH` | `super_large_cap` | 2018-08-01 | no | `null` |
| `000300.SH` | `hmm_benchmark_large_cap` | 2018-08-01 | yes, unchanged | `399300.SZ` |
| `000688.SH` | `star_50` | 2020-01-02 | no | `null` |
| `000852.SH` | `small_cap_1000` | 2018-08-01 | no | `null` |
| `000905.SH` | `mid_cap_500` | 2018-08-01 | no | `null` |
| `000985.CSI` | `all_a_proxy` | 2018-08-01 | no | `null` |
| `932000.CSI` | `micro_cap_2000` | 2018-08-01 | no | `null` |
| `399001.SZ` | `shenzhen_component` | 2018-08-01 | no | `null` |
| `399006.SZ` | `chinext_component` | 2018-08-01 | no | `null` |
| `399102.SZ` | `chinext_composite` | 2018-08-01 | no | `null` |
| `399107.SZ` | `shenzhen_a_composite` | 2018-08-01 | no | `null` |

Do not expand this list from “popular indices.” Any code/role/start/weight mapping change requires a new universe and semantic version.

`weight_api_code` is a frozen future mapping only. v1 does not download or consume index weights and must not call an effective date a publication vintage. The `000688.SH` 2019-12-31 base row is excluded; required training coverage begins 2020-01-02.

## Source and provider

`market.index_daily` frozen content partitions are authoritative. Tushare `index_daily` may populate only DB-missing keys in candidate-local immutable provider CAS/overlay. It never writes DB.

For every overlapping key, compare every canonical field within the frozen numeric tolerance. Any mismatch, duplicate, required NULL/non-finite value or remaining calendar gap fails the full profile. Do not use TDX, a neighboring index, zero, forward-fill or role substitution.

Build a per-code A-share trading-calendar coverage matrix from `required_from` through cutoff and bind its digest, provider content refs and source roots into the release.

## Required outputs

Daily Qlib bin:

- `instruments/index.txt` contains exactly one three-column `code,start,end` row for each of the 12 codes; `start` is the
  frozen per-code `required_from` and `end` is the effective cutoff. Code-only rows, duplicates and extra indices fail.
- stock PIT `instruments/all.txt` contains none of them;
- index and stock calendars/feature offsets are compatible;
- per-code feature values and units match canonical index rows.

Factor bundle:

- `index_daily.h5`, key `data`;
- `MultiIndex[datetime,instrument]`;
- `metadata/index_context_manifest.json`.

H5 columns and units:

| Column | Formula/unit |
|---|---|
| `idx_open_point` | `open`, index points |
| `idx_high_point` | `high`, index points |
| `idx_low_point` | `low`, index points |
| `idx_close_point` | `close`, index points |
| `idx_pre_close_point` | `pre_close`, index points |
| `idx_return_1d` | `pct_chg / 100` |
| `idx_volume_hand_source` | Tushare/DB `vol`, hand |
| `idx_volume_share_equiv` | `vol * 100`, share |
| `idx_amount_cny` | `amount * 1000`, CNY |

`metadata/index_context_manifest.json` is produced only after candidate-local index outputs exist and is independently
read back before component-manifest/signoff. It binds release/source/ArtifactReady/PIT identities, full ordered code table,
roles, starts, weight mapping, coverage, per-code roots, exact H5/bin/file/schema hashes, producer/artifact/validator
fingerprints and `hmm_consumer_activation=not_activated`. A manifest copied from another release or left valid while any
underlying index byte drifts must fail.

Candidate validation checks every required index Qlib field value and exact H5 dtype/unit; sampling is not a substitute.
Index evidence also does not substitute for the separate moneyflow contract: terminal release signoff must independently
prove raw H5/static parity and derived 5/20-observation moneyflow formulas with identical values and NaN masks across chunks.

## HMM boundary

The existing HMM benchmark remains `000300.SH`. This dataset feature supplies unified candidate training/prediction context and explicit metadata only. It does not switch the HMM consumer, change the state model or activate a new release.

Future consumers must explicitly bind release ID, schema/universe version, as-of cutoff, feature builder and required roles. Compute cross-index spreads only when both same-day sides exist; never fill a missing side.

Signoff must assert exact list/order/mapping, provider parity, coverage, H5/bin value/unit parity, `all.txt`/`index.txt` isolation, benchmark unchanged and consumer not activated.

Current source acceptance for this contract is fixture-only. No real index/moneyflow provider rows, candidate bytes or HMM
consumer were read, written or activated in this implementation round.
