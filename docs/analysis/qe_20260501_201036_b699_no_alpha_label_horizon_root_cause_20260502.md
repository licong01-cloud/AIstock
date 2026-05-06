# qe_20260501_201036_b699 no-alpha 10D 实际训练成 1D 的代码级原因定位

生成日期：2026-05-02

分析对象：`qe_20260501_201036_b699`

远端 workspace：`/home/lc999/projects/RD-Agent-main/qe_workspace/qe_20260501_201036_b699`

结论状态：只做分析定位，未修改业务代码，未重跑 QE。

## 1. 结论

`qe_20260501_201036_b699` 中 no-alpha 的 Loop2/Loop4 配置层记录的是 `label_horizon=10`，但实际训练标签由 `DynamicFactorsOnlyLoader` 内部生成，而该 loader 把 label 写死为 1D：

```text
label_expr = "(Ref($close, -2) / Ref($close, -1) - 1)"
```

因此 b699 的 alpha/no-alpha 对比不是严格的“是否加入 Alpha158 基线因子”对照，而是：

```text
Branch       Actual Features                          Actual Label
-----------  ---------------------------------------  ------------
Alpha ON     57 custom factors + 20 Alpha158 factors  10D
Alpha OFF    57 custom factors                        1D
```

这会导致 no-alpha 10D 实验结论失效；其结果不能用于判断 Alpha158 是否应该全量保留或删除。

## 2. 配置层证据：loop 配置没有丢失 label_horizon

API/任务配置中 4 个 loop 都记录了 `label_horizon=10`：

```text
Loop   disable_alpha158  cfg.label_horizon  model_params.label_horizon
-----  ----------------  -----------------  --------------------------
Loop1  False             10                 10
Loop2  True              10                 10
Loop3  False             10                 10
Loop4  True              10                 10
```

所以问题不是前端未传、DB 未存、API 未保存，而是配置进入运行时 loader 后没有生效。

## 3. 远端 workspace 证据

### 3.1 Alpha ON 分支正确使用 10D label

远端文件：

`/home/lc999/projects/RD-Agent-main/qe_workspace/qe_20260501_201036_b699/Loop1/conf.yaml`

关键行：

```text
Line 17  class: NestedDataLoader
Line 24  Ref($close, -11) / Ref($close, -1) - 1
```

对应含义：`Ref($close, -11) / Ref($close, -1) - 1` 是 10D forward return，Loop1/Loop3 的 alpha=ON 路径正确使用了 10D 标签。

### 3.2 Alpha OFF 分支进入 DynamicFactorsOnlyLoader，但 conf 未传 label_horizon

远端文件：

`/home/lc999/projects/RD-Agent-main/qe_workspace/qe_20260501_201036_b699/Loop2/conf.yaml`

关键行：

```text
Line 17  class: DynamicFactorsOnlyLoader
Line 18  module_path: qe_custom_loaders
Line 20  dynamic_path: "combined_factors_df.parquet"
```

该 no-alpha 分支的 `conf.yaml` 没有传：

```text
Missing Key    Expected Purpose
-------------  ----------------------------------
label_horizon  让 loader 生成 10D label
label_type     让 loader 知道 close/open/vwap
label_expr     直接传入 Ref($close,-11)... 公式
```

### 3.3 DynamicFactorsOnlyLoader 内部写死 1D label

远端文件：

`/home/lc999/projects/RD-Agent-main/qe_workspace/qe_20260501_201036_b699/Loop2/qe_custom_loaders.py`

关键行：

```text
Line 133  # Label 定义: Ref($close, -2) / Ref($close, -1) - 1
Line 141  # Ref($close, -2) / Ref($close, -1) - 1 表示未来1日收益率
Line 142  label_expr = "(Ref($close, -2) / Ref($close, -1) - 1)"
```

这证明 Loop2/Loop4 虽然配置为 `label_horizon=10`，实际训练时仍加载了 1D 标签。

## 4. 源代码级定位

### 4.1 ConfigComposer 已经计算出 horizon-aware label 公式

文件：`backend/services/quantevolver/config_composer.py`

```text
Line 2455  _label_horizon = normalize_label_horizon((custom_params or {}).get("label_horizon"))
Line 2457  _label_formula = f"Ref({_label_field}, -{_label_horizon + 1}) / Ref({_label_field}, -1) - 1"
```

说明 composer 具备根据 `label_horizon` 计算 1D/3D/5D/10D/20D label 公式的能力。

### 4.2 Alpha ON 分支把 _label_formula 写入 conf.yaml

文件：`backend/services/quantevolver/config_composer.py`

```text
Line 2477  if has_custom_factors and not disable_alpha158:
Line 2482      class: NestedDataLoader
Line 2489      lines.append(f'                            - ["{_label_formula}"]')
```

所以 alpha=ON 时，`_label_formula` 被写入 Alpha158DL 配置，10D 能生效。

### 4.3 Alpha OFF 分支没有把 _label_formula 或 label_horizon 传给 loader

文件：`backend/services/quantevolver/config_composer.py`

```text
Line 2521  elif has_custom_factors and disable_alpha158:
Line 2527      lines.append("        class: DynamicFactorsOnlyLoader")
Line 2528      lines.append("        module_path: qe_custom_loaders")
Line 2530      lines.append('            dynamic_path: "combined_factors_df.parquet"')
```

这里没有写入：

```text
Expected But Missing
--------------------
label_expr: "Ref($close, -11) / Ref($close, -1) - 1"
label_horizon: 10
label_type: close
```

因此 no-alpha 分支丢失了训练周期语义。

### 4.4 DynamicFactorsOnlyLoader 源文件硬编码 1D

文件：`backend/services/quantevolver/qe_custom_loaders.py`

```text
Line 142  label_expr = "(Ref($close, -2) / Ref($close, -1) - 1)"
```

该文件会被复制到实验目录：

文件：`backend/services/quantevolver/config_composer.py`

```text
Line 4231  qe_loader_source = Path(__file__).parent / "qe_custom_loaders.py"
Line 4239  shutil.copy2(qe_loader_source, qe_loader_dest)
```

所以远端 workspace 中的 `Loop2/qe_custom_loaders.py` 是项目源文件的复制版本，硬编码 1D 是可复现的代码路径。

## 5. 触发条件与影响范围

```text
Condition                  Value / Requirement                     Impact
-------------------------  --------------------------------------  ---------------------------------------------
has_custom_factors         True                                    使用自定义因子 parquet
disable_alpha158           True                                    进入 DynamicFactorsOnlyLoader 分支
label_horizon              3/5/10/20                               配置层记录非 1D，但 loader 不接收
Runtime label expression   hardcoded Ref($close,-2)/Ref($close,-1) 实际训练目标固定为 1D
```

影响范围主要是：

```text
Scenario                                      Affected
--------------------------------------------  --------
自定义因子 + 禁用 Alpha158 + label_horizon=1  No，结果与硬编码一致
自定义因子 + 禁用 Alpha158 + label_horizon>1  Yes，实际会错训成 1D
自定义因子 + 启用 Alpha158 + label_horizon>1  No，NestedDataLoader 写入了正确公式
无自定义因子 + Alpha158DL + label_horizon>1   No，已有公式写入路径
```

## 6. 为什么 b699 结果不能用于判断 Alpha158

b699 的表面对比是：

```text
Loop   Model          Alpha158  ConfigLabelHorizon  ActualLabel  CAGRAbs  Sharpe
-----  -------------  --------  ------------------  -----------  -------  ------
Loop1  conservative   ON        10                  10D          83.18%   2.1894
Loop2  conservative   OFF       10                  1D           37.48%   1.1810
Loop3  golden         ON        10                  10D          75.44%   2.0725
Loop4  golden         OFF       10                  1D           34.27%   1.1084
```

由于 `ActualLabel` 不一致，该实验不能回答：

```text
Question                                  Can b699 Answer?
----------------------------------------  ----------------
Alpha158 全量加入是否更好                 No
no-alpha 10D 是否弱于 alpha 10D           No
10D label 是否比 1D 更适合当前 10D 任务    Partially Yes
DynamicFactorsOnlyLoader 是否存在缺陷      Yes
```

## 7. 为什么已有测试没有覆盖到

已存在测试覆盖了普通 horizon-aware 公式生成：

`backend/tests/unified_engine/test_label_horizon.py`

```text
Line 74  def test_config_composer_uses_horizon_aware_formula():
Line 87  assert 'label: ["Ref($vwap, -21) / Ref($vwap, -1) - 1"]' in yaml_text
```

但缺少以下分支测试：

```text
Missing Test Branch
-----------------------------------------------------
has_custom_factors=True
disable_alpha158=True
custom_params={"label_horizon": 10}
assert DynamicFactorsOnlyLoader receives label_horizon/label_expr
assert generated/copy loader does not hardcode 1D for non-1D tasks
```

## 8. 建议修复方向（未实施）

### 8.1 首选方案：conf 显式传 label_expr

在 `ConfigComposer` 的 no-alpha 分支中写入：

```yaml
kwargs:
  dynamic_path: "combined_factors_df.parquet"
  label_expr: "Ref($close, -11) / Ref($close, -1) - 1"
```

`DynamicFactorsOnlyLoader.__init__` 接收 `label_expr`，并使用该表达式加载 label。

优点：

```text
Advantage                          Reason
---------------------------------  -----------------------------------------
最直接                             与 Alpha158DL 使用同一公式来源
避免重复计算                       composer 已经计算好 _label_formula
便于审计                           conf.yaml 可直接看到真实训练标签
避免 close/open/vwap 逻辑散落      label_type/horizon 解析集中在 composer
```

### 8.2 可选方案：传 label_type + label_horizon

在 conf 中传：

```yaml
kwargs:
  dynamic_path: "combined_factors_df.parquet"
  label_type: close
  label_horizon: 10
```

然后 loader 内部计算公式。

缺点：公式逻辑会在 composer 和 loader 两处重复，需要额外保证一致性。

### 8.3 必须 fail-fast，禁止静默回退

当前 loader 的异常处理是：

```text
warnings.warn(... Continuing without labels ...)
```

对训练标签这类关键数据，不应继续运行。建议改成：

```text
If label loading fails -> raise ValueError
If label_horizon/label_expr missing and task requests non-1D -> raise ValueError
If generated conf uses DynamicFactorsOnlyLoader without explicit label config -> fail fast
```

## 9. 建议补充测试

```text
Priority  Test Name / Scenario                                      Expected
--------  ---------------------------------------------------------  ----------------------------------------------
P0        no_alpha_custom_factors_10d_conf_contains_label_expr       conf.yaml contains Ref($close,-11)/Ref($close,-1)-1
P0        dynamic_loader_uses_passed_label_expr                      no hardcoded Ref($close,-2) for 10D
P0        dynamic_loader_missing_label_expr_non_1d_fails_fast        raises ValueError
P1        alpha_on_and_off_same_label_horizon_generate_same_formula  both branches use equivalent 10D formula
P1        b699-style custom evo payload regression                   Loop config 10D + disable_alpha158=True remains 10D
```

## 10. 后续实验建议

修复后应重跑严格 A/B：

```text
Priority  Experiment                                  Purpose
--------  ------------------------------------------  ---------------------------------------------
P0        conservative alpha ON vs OFF, both 10D       验证 Alpha158 对 conservative LGB 的真实贡献
P0        golden alpha ON vs OFF, both 10D             验证 Alpha158 对 golden LGB 的真实贡献
P1        alpha158_top_only, both 10D                  只保留 CORD/RSQR 等高贡献基线因子
P1        no_alpha correct 10D vs previous no_alpha 1D 验证 label_horizon 修复后的收益变化
P2        5D/10D/20D matrix                            区分不同预测周期的最佳因子集合
```

## 11. 最终判断

`qe_20260501_201036_b699` 中 no-alpha loop 训练成 1D 的原因已经可以代码级定位：

```text
Root Cause
------------------------------------------------------------------------------------------------------
ConfigComposer 的 disable_alpha158=True 分支没有把 _label_formula / label_horizon 传给 DynamicFactorsOnlyLoader，
而 DynamicFactorsOnlyLoader 源文件内部又硬编码了 1D label 表达式 Ref($close,-2)/Ref($close,-1)-1。
```

因此，当前 b699 的 alpha/no-alpha 差异主要混入了 label 周期差异，不能作为 Alpha158 因子有效性的最终依据。
