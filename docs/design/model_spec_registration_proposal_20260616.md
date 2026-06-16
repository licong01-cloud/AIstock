# 模型库 Spec 登记提案（待确认后注册）

> **类型**：设计提案（design）· **本文件只提方案，确认后才 `model_registry_register_confirmed`**
> **日期**：2026-06-16
> **关联**：blueprint 附录 A5
> **目标**：补齐 QE 可选模型 spec，覆盖 10D/20D 训练模式，服务多 Alpha 生产与探针

---

## 1. 现状（MCP 取数）

`qe_selectable=true` 共 50 个 spec，其中 **33 个 legacy rdagent**（拟下线，见 cleanup_plan §3），真正 curated **17 个**：

| 族 | 现有 curated spec | 备注 |
|----|------------------|------|
| 序列(PTNN) | `__seed_LSTM_10D_hs64_d02__` ★主力, `__seed_GRU_10D_hs64_d02/d03/d04__`, `__seed_GRU_10D_hs96_d03__`, `__seed_TCN_10D_d02__`, `__seed_ALSTM_default_v1__`, `__seed_ALSTM_sector_v1__`, `__seed_GRU2_default_v1__` | 全 `_10D_` 命名 |
| 树 | `__seed_LGBModel_conservative_v1__` ★主力, `__seed_LGBModel_golden_v1__`, `__seed_CatBoost_10D__`, `__seed_CatBoost_default_v1__`, `__seed_XGBoost_10D__` | |
| 排序/线性/其他 | `__seed_LambdaMART_10D__`, `__seed_Ridge_default_v1__`, `__seed_TabPFN_10D__` | |

**两个关键缺口：**
1. **无 20D 原生 spec**。所有多 Alpha 工作（R7–R21）都用 `_10D_` spec + loop 级 `label_horizon=20` override 跑出来的。生产用 h20，却没有声明 h20 的一等 spec。
2. spec 的 `label_horizon` 字段均为 `None` → horizon 完全由 loop 控制。

> ⚠️ **需你先澄清一点**：spec 名里的 `10D` 指的是 **(a) 输入回看窗口（10 日序列长度）** 还是 **(b) 预测 label horizon（10 日前瞻收益）**？二者正交。
> - 若是 (a)：20D spec = 20 日回看的更长序列模型（真新模型，捕获更长模式）。
> - 若是 (b)：20D spec = 同模型把 horizon 默认设 20（与现有 override 等价，价值在治理/声明）。
> 本提案默认按 **(b) label horizon** 设计（与多 Alpha 用 h20 对齐），若是 (a) 我再调。

---

## 2. 提案 spec（分三层，确认后注册）

> 注册方式：克隆对应现有 spec 的 `model_config`，仅改 horizon 相关默认 + 命名 + `qe_selectable=true`。`catalog_source="manual_20D"` 或 `manual_seed_multi_alpha`。

### Tier 1 — 20D 生产主力（最高优先，多 Alpha 全靠这两个）
| 提案 spec_id | 克隆自 | 改动 | 用途 |
|-------------|--------|------|------|
| `__seed_LSTM_20D_hs64_d02__` | `__seed_LSTM_10D_hs64_d02__` | label_horizon 默认 20 | α1/α3/α6 序列提取器（生产） |
| `__seed_LGBModel_conservative_20D__` | `__seed_LGBModel_conservative_v1__` | label_horizon 默认 20 | α2/α7 树提取器（生产） |

### Tier 2 — alpha 腿天花板探针（中优先）
| 提案 spec_id | 克隆自 | 改动 | 用途 |
|-------------|--------|------|------|
| `__seed_LSTM_20D_hs128_d02__` | LSTM_10D_hs64_d02 | hidden 64→128, h20 | C_FundVal ICIR0.81 + best_epoch≈1 天花板探针 |
| `__seed_TCN_20D_d02__` | `__seed_TCN_10D_d02__` | h20 | 高 ICIR 序列探针（R12/R18 TCN 排序腿） |
| `__seed_LGBModel_golden_20D__` | `__seed_LGBModel_golden_v1__` | h20 | 树族高广度替代 |

### Tier 3 — 多样性/可选（低优先，按需）
| 提案 spec_id | 克隆自 | 用途 |
|-------------|--------|------|
| `__seed_ALSTM_20D_v1__` | `__seed_ALSTM_default_v1__` | 注意力 LSTM 20D，跨域多样性 |
| `__seed_CatBoost_20D__` | `__seed_CatBoost_10D__` | R17B 发现 CatBoost+Disc25 turnover=14 优势 |
| `__seed_LambdaMART_20D__` | `__seed_LambdaMART_10D__` | 排序原生，advisory rank 腿 |

---

## 3. 不做什么（避免再造垃圾目录）

- **不**为每个 alpha 腿单独建 spec：alpha = spec × 因子集，因子集在 loop/包层定义，spec 只管"模型族 × horizon × 超参"。5 个 alpha 腿共用上述少数 spec。
- **不**无脑补全所有族的 20D 版（GRU 多 dropout 变体、TabPFN、Ridge 等暂不补，除非实验需要）。
- legacy 33 个 **deprecate 而非注册新的**（cleanup_plan §3）。

---

## 4. 与 MLflow Model Registry 的分工（线 B M4，零重复）

- 本提案的 spec = AIstock `model_registry` 的 **"训什么"定义**（类型 + 超参 schema + qe_selectable）。
- MLflow Model Registry = **"训出的权重实例 + stage"**（Production/Staging/Archived，回滚/灰度）。
- 二者不重复：一个管 catalog，一个管 trained weight 治理。

---

## 5. 待确认

- [ ] **先回答** §1 的 `10D` 语义（回看窗口 vs label horizon）—— 决定 20D spec 的本质。
- [ ] 批准 Tier 1（2 个 20D 主力）立即注册?
- [ ] Tier 2/3 是否一并注册，还是按实验需要再逐个加?
- [ ] 确认后我 `model_registry_register_confirmed` 注册（需 `AISTOCK_MODEL_REGISTRY_WRITE_API_ENABLED`，若未开则提醒你开启）。

*本提案只列方案，注册需用户确认。*
