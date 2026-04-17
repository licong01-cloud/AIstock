-- Seed Multi-Alpha 模型配置到 aistock_model_catalog
-- Created: 2026-04-15
-- Purpose: Phase 3 Multi-Alpha 架构需要 ALSTM/GRU/Ridge/CatBoost 作为分组模型
--
-- 每个模型的 model_hyperparameters 已包含 pt_model_uri (PTNN类型)
-- 对应的 default_dataset_type 在 config_composer._BUILTIN_MODELS 中定义

-- ── 1. ALSTM Default (时序 price_volume / sector 组) ──────────────────
INSERT INTO aistock_model_catalog (
    model_id, catalog_version, generated_at_utc, catalog_source,
    task_run_id, loop_id, workspace_id, workspace_path,
    model_type, model_name, display_name, model_description,
    model_config, dataset_config,
    model_hyperparameters, model_training_hyperparameters, is_sota
) VALUES (
    '__seed_ALSTM_default_v1__',
    '1.0', '2026-04-15T00:00:00Z', 'manual_seed_multi_alpha',
    'manual_seed_malpha', 100, 'manual_seed_alstm', 'N/A',
    'PTNN',
    'ALSTM',
    'ALSTM 默认 (Multi-Alpha TS)',
    'Qlib ALSTM attention LSTM 时序模型。推荐用于 price_volume/sector 组。验证配置: hidden=64 layers=1 batch=4096 lr=3e-4。',
    '{"class": "GeneralPTNN", "module_path": "qlib.contrib.model.pytorch_general_nn", "dataset_type": "TSDatasetH"}'::jsonb,
    '{"class": "TSDatasetH", "module_path": "qlib.data.dataset"}'::jsonb,
    '{"pt_model_uri": "qlib.contrib.model.pytorch_alstm_ts.ALSTMModel", "d_feat": 20, "hidden_size": 64, "num_layers": 1, "dropout": 0.0, "n_epochs": 200, "lr": 3e-4, "early_stop": 20, "batch_size": 4096, "weight_decay": 1e-5, "GPU": 0}'::jsonb,
    '{"lr": 3e-4, "batch_size": 4096, "n_epochs": 200}'::jsonb,
    true
) ON CONFLICT (model_id) DO NOTHING;

-- ── 2. GRU2 Default (双层 GRU) ─────────────────────────────────────────
INSERT INTO aistock_model_catalog (
    model_id, catalog_version, generated_at_utc, catalog_source,
    task_run_id, loop_id, workspace_id, workspace_path,
    model_type, model_name, display_name, model_description,
    model_config, dataset_config,
    model_hyperparameters, model_training_hyperparameters, is_sota
) VALUES (
    '__seed_GRU2_default_v1__',
    '1.0', '2026-04-15T00:00:00Z', 'manual_seed_multi_alpha',
    'manual_seed_malpha', 101, 'manual_seed_gru2', 'N/A',
    'PTNN',
    'GRU2',
    '双层 GRU 默认 (Multi-Alpha TS)',
    'Qlib 双层 GRU 时序模型，ALSTM 的备选。hidden=128 layers=2 batch=4096 lr=2e-4 dropout=0.2。',
    '{"class": "GeneralPTNN", "module_path": "qlib.contrib.model.pytorch_general_nn", "dataset_type": "TSDatasetH"}'::jsonb,
    '{"class": "TSDatasetH", "module_path": "qlib.data.dataset"}'::jsonb,
    '{"pt_model_uri": "qlib.contrib.model.pytorch_gru_ts.GRUModel", "d_feat": 20, "hidden_size": 128, "num_layers": 2, "dropout": 0.2, "n_epochs": 200, "lr": 2e-4, "early_stop": 20, "batch_size": 4096, "weight_decay": 1e-4, "GPU": 0}'::jsonb,
    '{"lr": 2e-4, "batch_size": 4096, "n_epochs": 200, "dropout": 0.2}'::jsonb,
    true
) ON CONFLICT (model_id) DO NOTHING;

-- ── 3. ALSTM Sector (行业组专用，更小模型) ─────────────────────────────
INSERT INTO aistock_model_catalog (
    model_id, catalog_version, generated_at_utc, catalog_source,
    task_run_id, loop_id, workspace_id, workspace_path,
    model_type, model_name, display_name, model_description,
    model_config, dataset_config,
    model_hyperparameters, model_training_hyperparameters, is_sota
) VALUES (
    '__seed_ALSTM_sector_v1__',
    '1.0', '2026-04-15T00:00:00Z', 'manual_seed_multi_alpha',
    'manual_seed_malpha', 102, 'manual_seed_alstm_sector', 'N/A',
    'PTNN',
    'ALSTM_Sector',
    'ALSTM 行业组 (Multi-Alpha sw2_*)',
    '为 sector 组优化的 ALSTM，hidden=64 layers=1，捕捉行业动量/资金流时序。',
    '{"class": "GeneralPTNN", "module_path": "qlib.contrib.model.pytorch_general_nn", "dataset_type": "TSDatasetH"}'::jsonb,
    '{"class": "TSDatasetH", "module_path": "qlib.data.dataset"}'::jsonb,
    '{"pt_model_uri": "qlib.contrib.model.pytorch_alstm_ts.ALSTMModel", "d_feat": 20, "hidden_size": 64, "num_layers": 1, "dropout": 0.1, "n_epochs": 200, "lr": 3e-4, "early_stop": 20, "batch_size": 4096, "weight_decay": 1e-5, "GPU": 0}'::jsonb,
    '{"lr": 3e-4, "batch_size": 4096, "dropout": 0.1}'::jsonb,
    false
) ON CONFLICT (model_id) DO NOTHING;

-- ── 4. Ridge Default (fundamental 组) ──────────────────────────────────
INSERT INTO aistock_model_catalog (
    model_id, catalog_version, generated_at_utc, catalog_source,
    task_run_id, loop_id, workspace_id, workspace_path,
    model_type, model_name, display_name, model_description,
    model_config, dataset_config,
    model_hyperparameters, model_training_hyperparameters, is_sota
) VALUES (
    '__seed_Ridge_default_v1__',
    '1.0', '2026-04-15T00:00:00Z', 'manual_seed_multi_alpha',
    'manual_seed_malpha', 103, 'manual_seed_ridge', 'N/A',
    'LINEAR',
    'Ridge',
    'Ridge 回归默认 (Multi-Alpha fundamental)',
    'Qlib Ridge 线性回归，推荐用于基本面因子组(bb_*)。低频季度更新，线性关系为主。',
    '{"class": "LinearModel", "module_path": "qlib.contrib.model.linear", "dataset_type": "DatasetH"}'::jsonb,
    '{"class": "DatasetH", "module_path": "qlib.data.dataset"}'::jsonb,
    '{"estimator": "ridge", "alpha": 0.1}'::jsonb,
    '{"alpha": 0.1}'::jsonb,
    true
) ON CONFLICT (model_id) DO NOTHING;

-- ── 5. CatBoost Default (chip / valuation 备选) ────────────────────────
INSERT INTO aistock_model_catalog (
    model_id, catalog_version, generated_at_utc, catalog_source,
    task_run_id, loop_id, workspace_id, workspace_path,
    model_type, model_name, display_name, model_description,
    model_config, dataset_config,
    model_hyperparameters, model_training_hyperparameters, is_sota
) VALUES (
    '__seed_CatBoost_default_v1__',
    '1.0', '2026-04-15T00:00:00Z', 'manual_seed_multi_alpha',
    'manual_seed_malpha', 104, 'manual_seed_catboost', 'N/A',
    'CATBOOST',
    'CatBoost',
    'CatBoost 默认 (Multi-Alpha 备选)',
    'Qlib CatBoostModel，擅长处理有序分类特征。chip/valuation 组的备选非线性模型。',
    '{"class": "CatBoostModel", "module_path": "qlib.contrib.model.catboost_model", "dataset_type": "DatasetH"}'::jsonb,
    '{"class": "DatasetH", "module_path": "qlib.data.dataset"}'::jsonb,
    '{"iterations": 1000, "depth": 8, "learning_rate": 0.05, "l2_leaf_reg": 3.0, "subsample": 0.8, "verbose": 0, "task_type": "CPU"}'::jsonb,
    '{"iterations": 1000, "depth": 8, "learning_rate": 0.05}'::jsonb,
    false
) ON CONFLICT (model_id) DO NOTHING;

-- 验证
SELECT model_id, model_name, model_type, display_name, is_sota
FROM aistock_model_catalog
WHERE catalog_source = 'manual_seed_multi_alpha'
ORDER BY model_id;
