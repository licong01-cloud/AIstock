-- Seed 10D 多周期训练模型到 aistock_model_catalog
-- Created: 2026-04-29
-- Source: manual_10D (手动创建，区别于 rdagent_task_sync 和 manual_seed)
-- Purpose: 为 QE 多周期(10D)训练实验提供优化的模型种子
--   M1-M4: GRU 变体 (不同 dropout/hidden_size)
--   M5:     LSTM 变体
--   M6:     TCN 多尺度卷积
--   M7:     XGBoost
--   M8:     CatBoost (10D 优化超参)
--   M9:     LambdaMART 排序模型
--   M10:    TabPFN 表格基础模型

-- ═══════════════════════════════════════════════════════════════
-- M1: GRU_10D_hs64_d02 — 核心候选，安全起步
-- ═══════════════════════════════════════════════════════════════
INSERT INTO aistock_model_catalog (
    model_id, catalog_version, generated_at_utc, catalog_source,
    task_run_id, loop_id, workspace_id, workspace_path,
    model_type, model_name, display_name, model_description,
    model_config, dataset_config,
    model_hyperparameters, model_training_hyperparameters, is_sota,
    code_text
) VALUES (
    '__seed_GRU_10D_hs64_d02__',
    '1.0', '2026-04-29T00:00:00Z', 'manual_10D',
    'manual_10D_seed', 1, 'manual_10D_gru_hs64_d02', 'N/A',
    'TimeSeries',
    'GRU_10D_hs64_d02',
    'GRU 10D [hs=64, do=0.2]',
    '单层 GRU，hidden_size=64, dropout=0.2。10D horizon 核心候选，基于 GRU_TimeSeries_64 (IC=0.0573) 架构优化正则化。',
    '{"class": "GeneralPTNN", "module_path": "qlib.contrib.model.pytorch_general_nn", "dataset_type": "TSDatasetH"}'::jsonb,
    '{"class": "TSDatasetH", "module_path": "qlib.data.dataset"}'::jsonb,
    '{"d_feat": 20, "hidden_size": 64, "num_layers": 1, "dropout": 0.2}'::jsonb,
    '{"lr": 1e-3, "batch_size": 4096, "n_epochs": 200, "early_stop": 20, "weight_decay": 1e-3}'::jsonb,
    false,
    E'import torch\nimport torch.nn as nn\n\nclass GRU_10D_hs64_d02(nn.Module):\n    def __init__(self, num_features, num_timesteps):\n        super().__init__()\n        self.num_features = num_features\n        self.num_timesteps = num_timesteps\n        self.gru = nn.GRU(\n            input_size=num_features,\n            hidden_size=64,\n            num_layers=1,\n            batch_first=True,\n            dropout=0.2,\n            bidirectional=False\n        )\n        self.linear = nn.Linear(64, 1)\n\n    def forward(self, x):\n        gru_out, hidden = self.gru(x)\n        last_hidden = gru_out[:, -1, :]\n        output = self.linear(last_hidden)\n        return output\n\nmodel_cls = GRU_10D_hs64_d02'
) ON CONFLICT (model_id) DO UPDATE SET
    catalog_version = EXCLUDED.catalog_version,
    generated_at_utc = EXCLUDED.generated_at_utc,
    catalog_source = EXCLUDED.catalog_source,
    model_type = EXCLUDED.model_type,
    model_name = EXCLUDED.model_name,
    display_name = EXCLUDED.display_name,
    model_description = EXCLUDED.model_description,
    model_config = EXCLUDED.model_config,
    dataset_config = EXCLUDED.dataset_config,
    model_hyperparameters = EXCLUDED.model_hyperparameters,
    model_training_hyperparameters = EXCLUDED.model_training_hyperparameters,
    code_text = EXCLUDED.code_text;

-- ═══════════════════════════════════════════════════════════════
-- M2: GRU_10D_hs64_d03 — 正则化对比 (中等正则化)
-- ═══════════════════════════════════════════════════════════════
INSERT INTO aistock_model_catalog (
    model_id, catalog_version, generated_at_utc, catalog_source,
    task_run_id, loop_id, workspace_id, workspace_path,
    model_type, model_name, display_name, model_description,
    model_config, dataset_config,
    model_hyperparameters, model_training_hyperparameters, is_sota,
    code_text
) VALUES (
    '__seed_GRU_10D_hs64_d03__',
    '1.0', '2026-04-29T00:00:00Z', 'manual_10D',
    'manual_10D_seed', 2, 'manual_10D_gru_hs64_d03', 'N/A',
    'TimeSeries',
    'GRU_10D_hs64_d03',
    'GRU 10D [hs=64, do=0.3]',
    '单层 GRU，hidden_size=64, dropout=0.3。M1 的正则化升级对比。',
    '{"class": "GeneralPTNN", "module_path": "qlib.contrib.model.pytorch_general_nn", "dataset_type": "TSDatasetH"}'::jsonb,
    '{"class": "TSDatasetH", "module_path": "qlib.data.dataset"}'::jsonb,
    '{"d_feat": 20, "hidden_size": 64, "num_layers": 1, "dropout": 0.3}'::jsonb,
    '{"lr": 1e-3, "batch_size": 4096, "n_epochs": 200, "early_stop": 20, "weight_decay": 2e-3}'::jsonb,
    false,
    E'import torch\nimport torch.nn as nn\n\nclass GRU_10D_hs64_d03(nn.Module):\n    def __init__(self, num_features, num_timesteps):\n        super().__init__()\n        self.num_features = num_features\n        self.num_timesteps = num_timesteps\n        self.gru = nn.GRU(\n            input_size=num_features,\n            hidden_size=64,\n            num_layers=1,\n            batch_first=True,\n            dropout=0.3,\n            bidirectional=False\n        )\n        self.linear = nn.Linear(64, 1)\n\n    def forward(self, x):\n        gru_out, hidden = self.gru(x)\n        last_hidden = gru_out[:, -1, :]\n        output = self.linear(last_hidden)\n        return output\n\nmodel_cls = GRU_10D_hs64_d03'
) ON CONFLICT (model_id) DO UPDATE SET
    catalog_version = EXCLUDED.catalog_version,
    generated_at_utc = EXCLUDED.generated_at_utc,
    catalog_source = EXCLUDED.catalog_source,
    model_type = EXCLUDED.model_type,
    model_name = EXCLUDED.model_name,
    display_name = EXCLUDED.display_name,
    model_description = EXCLUDED.model_description,
    model_config = EXCLUDED.model_config,
    dataset_config = EXCLUDED.dataset_config,
    model_hyperparameters = EXCLUDED.model_hyperparameters,
    model_training_hyperparameters = EXCLUDED.model_training_hyperparameters,
    code_text = EXCLUDED.code_text;

-- ═══════════════════════════════════════════════════════════════
-- M3: GRU_10D_hs64_d04 — 强正则化兜底
-- ═══════════════════════════════════════════════════════════════
INSERT INTO aistock_model_catalog (
    model_id, catalog_version, generated_at_utc, catalog_source,
    task_run_id, loop_id, workspace_id, workspace_path,
    model_type, model_name, display_name, model_description,
    model_config, dataset_config,
    model_hyperparameters, model_training_hyperparameters, is_sota,
    code_text
) VALUES (
    '__seed_GRU_10D_hs64_d04__',
    '1.0', '2026-04-29T00:00:00Z', 'manual_10D',
    'manual_10D_seed', 3, 'manual_10D_gru_hs64_d04', 'N/A',
    'TimeSeries',
    'GRU_10D_hs64_d04',
    'GRU 10D [hs=64, do=0.4]',
    '单层 GRU，hidden_size=64, dropout=0.4。强正则化兜底，防止 10D 小样本过拟合。',
    '{"class": "GeneralPTNN", "module_path": "qlib.contrib.model.pytorch_general_nn", "dataset_type": "TSDatasetH"}'::jsonb,
    '{"class": "TSDatasetH", "module_path": "qlib.data.dataset"}'::jsonb,
    '{"d_feat": 20, "hidden_size": 64, "num_layers": 1, "dropout": 0.4}'::jsonb,
    '{"lr": 1e-3, "batch_size": 4096, "n_epochs": 200, "early_stop": 20, "weight_decay": 3e-3}'::jsonb,
    false,
    E'import torch\nimport torch.nn as nn\n\nclass GRU_10D_hs64_d04(nn.Module):\n    def __init__(self, num_features, num_timesteps):\n        super().__init__()\n        self.num_features = num_features\n        self.num_timesteps = num_timesteps\n        self.gru = nn.GRU(\n            input_size=num_features,\n            hidden_size=64,\n            num_layers=1,\n            batch_first=True,\n            dropout=0.4,\n            bidirectional=False\n        )\n        self.linear = nn.Linear(64, 1)\n\n    def forward(self, x):\n        gru_out, hidden = self.gru(x)\n        last_hidden = gru_out[:, -1, :]\n        output = self.linear(last_hidden)\n        return output\n\nmodel_cls = GRU_10D_hs64_d04'
) ON CONFLICT (model_id) DO UPDATE SET
    catalog_version = EXCLUDED.catalog_version,
    generated_at_utc = EXCLUDED.generated_at_utc,
    catalog_source = EXCLUDED.catalog_source,
    model_type = EXCLUDED.model_type,
    model_name = EXCLUDED.model_name,
    display_name = EXCLUDED.display_name,
    model_description = EXCLUDED.model_description,
    model_config = EXCLUDED.model_config,
    dataset_config = EXCLUDED.dataset_config,
    model_hyperparameters = EXCLUDED.model_hyperparameters,
    model_training_hyperparameters = EXCLUDED.model_training_hyperparameters,
    code_text = EXCLUDED.code_text;

-- ═══════════════════════════════════════════════════════════════
-- M4: GRU_10D_hs96_d03 — 容量对比
-- ═══════════════════════════════════════════════════════════════
INSERT INTO aistock_model_catalog (
    model_id, catalog_version, generated_at_utc, catalog_source,
    task_run_id, loop_id, workspace_id, workspace_path,
    model_type, model_name, display_name, model_description,
    model_config, dataset_config,
    model_hyperparameters, model_training_hyperparameters, is_sota,
    code_text
) VALUES (
    '__seed_GRU_10D_hs96_d03__',
    '1.0', '2026-04-29T00:00:00Z', 'manual_10D',
    'manual_10D_seed', 4, 'manual_10D_gru_hs96_d03', 'N/A',
    'TimeSeries',
    'GRU_10D_hs96_d03',
    'GRU 10D [hs=96, do=0.3]',
    '单层 GRU，hidden_size=96, dropout=0.3。与 M2 对比验证增大容量是否有收益。',
    '{"class": "GeneralPTNN", "module_path": "qlib.contrib.model.pytorch_general_nn", "dataset_type": "TSDatasetH"}'::jsonb,
    '{"class": "TSDatasetH", "module_path": "qlib.data.dataset"}'::jsonb,
    '{"d_feat": 20, "hidden_size": 96, "num_layers": 1, "dropout": 0.3}'::jsonb,
    '{"lr": 1e-3, "batch_size": 4096, "n_epochs": 200, "early_stop": 20, "weight_decay": 2e-3}'::jsonb,
    false,
    E'import torch\nimport torch.nn as nn\n\nclass GRU_10D_hs96_d03(nn.Module):\n    def __init__(self, num_features, num_timesteps):\n        super().__init__()\n        self.num_features = num_features\n        self.num_timesteps = num_timesteps\n        self.gru = nn.GRU(\n            input_size=num_features,\n            hidden_size=96,\n            num_layers=1,\n            batch_first=True,\n            dropout=0.3,\n            bidirectional=False\n        )\n        self.linear = nn.Linear(96, 1)\n\n    def forward(self, x):\n        gru_out, hidden = self.gru(x)\n        last_hidden = gru_out[:, -1, :]\n        output = self.linear(last_hidden)\n        return output\n\nmodel_cls = GRU_10D_hs96_d03'
) ON CONFLICT (model_id) DO UPDATE SET
    catalog_version = EXCLUDED.catalog_version,
    generated_at_utc = EXCLUDED.generated_at_utc,
    catalog_source = EXCLUDED.catalog_source,
    model_type = EXCLUDED.model_type,
    model_name = EXCLUDED.model_name,
    display_name = EXCLUDED.display_name,
    model_description = EXCLUDED.model_description,
    model_config = EXCLUDED.model_config,
    dataset_config = EXCLUDED.dataset_config,
    model_hyperparameters = EXCLUDED.model_hyperparameters,
    model_training_hyperparameters = EXCLUDED.model_training_hyperparameters,
    code_text = EXCLUDED.code_text;

-- ═══════════════════════════════════════════════════════════════
-- M5: LSTM_10D_hs64_d02 — 备选架构
-- ═══════════════════════════════════════════════════════════════
INSERT INTO aistock_model_catalog (
    model_id, catalog_version, generated_at_utc, catalog_source,
    task_run_id, loop_id, workspace_id, workspace_path,
    model_type, model_name, display_name, model_description,
    model_config, dataset_config,
    model_hyperparameters, model_training_hyperparameters, is_sota,
    code_text
) VALUES (
    '__seed_LSTM_10D_hs64_d02__',
    '1.0', '2026-04-29T00:00:00Z', 'manual_10D',
    'manual_10D_seed', 5, 'manual_10D_lstm_hs64_d02', 'N/A',
    'TimeSeries',
    'LSTM_10D_hs64_d02',
    'LSTM 10D [hs=64, do=0.2]',
    '单层 LSTM，hidden_size=64, dropout=0.2。备选架构，验证 LSTM 在 10D 是否优于 GRU。',
    '{"class": "GeneralPTNN", "module_path": "qlib.contrib.model.pytorch_general_nn", "dataset_type": "TSDatasetH"}'::jsonb,
    '{"class": "TSDatasetH", "module_path": "qlib.data.dataset"}'::jsonb,
    '{"d_feat": 20, "hidden_size": 64, "num_layers": 1, "dropout": 0.2}'::jsonb,
    '{"lr": 1e-3, "batch_size": 4096, "n_epochs": 200, "early_stop": 20, "weight_decay": 1e-3}'::jsonb,
    false,
    E'import torch\nimport torch.nn as nn\n\nclass LSTM_10D_hs64_d02(nn.Module):\n    def __init__(self, num_features, num_timesteps):\n        super().__init__()\n        self.num_features = num_features\n        self.num_timesteps = num_timesteps\n        self.lstm = nn.LSTM(\n            input_size=num_features,\n            hidden_size=64,\n            num_layers=1,\n            batch_first=True,\n            dropout=0.0\n        )\n        self.dropout = nn.Dropout(0.2)\n        self.linear = nn.Linear(64, 1)\n\n    def forward(self, x):\n        lstm_out, (hidden, cell) = self.lstm(x)\n        last_hidden = self.dropout(lstm_out[:, -1, :])\n        output = self.linear(last_hidden)\n        return output\n\nmodel_cls = LSTM_10D_hs64_d02'
) ON CONFLICT (model_id) DO UPDATE SET
    catalog_version = EXCLUDED.catalog_version,
    generated_at_utc = EXCLUDED.generated_at_utc,
    catalog_source = EXCLUDED.catalog_source,
    model_type = EXCLUDED.model_type,
    model_name = EXCLUDED.model_name,
    display_name = EXCLUDED.display_name,
    model_description = EXCLUDED.model_description,
    model_config = EXCLUDED.model_config,
    dataset_config = EXCLUDED.dataset_config,
    model_hyperparameters = EXCLUDED.model_hyperparameters,
    model_training_hyperparameters = EXCLUDED.model_training_hyperparameters,
    code_text = EXCLUDED.code_text;

-- ═══════════════════════════════════════════════════════════════
-- M6: TCN_10D_d02 — 多尺度时序卷积
-- ═══════════════════════════════════════════════════════════════
INSERT INTO aistock_model_catalog (
    model_id, catalog_version, generated_at_utc, catalog_source,
    task_run_id, loop_id, workspace_id, workspace_path,
    model_type, model_name, display_name, model_description,
    model_config, dataset_config,
    model_hyperparameters, model_training_hyperparameters, is_sota,
    code_text
) VALUES (
    '__seed_TCN_10D_d02__',
    '1.0', '2026-04-29T00:00:00Z', 'manual_10D',
    'manual_10D_seed', 6, 'manual_10D_tcn_d02', 'N/A',
    'TimeSeries',
    'TCN_10D_d02',
    'TCN 10D [3层空洞卷积, do=0.2]',
    'Temporal Convolutional Network，3层空洞卷积 (dilation=1,2,4)，channels=64, dropout=0.2。无 Attention 的多尺度时序建模，备选架构。',
    '{"class": "GeneralPTNN", "module_path": "qlib.contrib.model.pytorch_general_nn", "dataset_type": "TSDatasetH"}'::jsonb,
    '{"class": "TSDatasetH", "module_path": "qlib.data.dataset"}'::jsonb,
    '{"d_feat": 20, "hidden_size": 64, "num_layers": 3, "dropout": 0.2}'::jsonb,
    '{"lr": 1e-3, "batch_size": 4096, "n_epochs": 200, "early_stop": 20, "weight_decay": 1e-3}'::jsonb,
    false,
    E'import torch\nimport torch.nn as nn\n\nclass Chomp1d(nn.Module):\n    def __init__(self, chomp_size):\n        super().__init__()\n        self.chomp_size = chomp_size\n    def forward(self, x):\n        return x[:, :, :-self.chomp_size].contiguous()\n\nclass TCNBlock(nn.Module):\n    def __init__(self, in_ch, out_ch, kernel_size, dilation, dropout):\n        super().__init__()\n        pad = (kernel_size - 1) * dilation\n        self.net = nn.Sequential(\n            nn.Conv1d(in_ch, out_ch, kernel_size, dilation=dilation, padding=pad),\n            Chomp1d(pad),\n            nn.ReLU(),\n            nn.Dropout(dropout),\n            nn.Conv1d(out_ch, out_ch, kernel_size, dilation=dilation, padding=pad),\n            Chomp1d(pad),\n            nn.ReLU(),\n            nn.Dropout(dropout),\n        )\n        self.residual = nn.Conv1d(in_ch, out_ch, 1) if in_ch != out_ch else nn.Identity()\n        self.relu = nn.ReLU()\n\n    def forward(self, x):\n        return self.relu(self.net(x) + self.residual(x))\n\nclass TCN_10D_d02(nn.Module):\n    def __init__(self, num_features, num_timesteps):\n        super().__init__()\n        self.num_features = num_features\n        self.num_timesteps = num_timesteps\n        channels = [num_features, 64, 64, 64]\n        self.tcn = nn.Sequential(\n            TCNBlock(channels[0], channels[1], 3, dilation=1, dropout=0.2),\n            TCNBlock(channels[1], channels[2], 3, dilation=2, dropout=0.2),\n            TCNBlock(channels[2], channels[3], 3, dilation=4, dropout=0.2),\n        )\n        self.linear = nn.Linear(64, 1)\n\n    def forward(self, x):\n        x = x.permute(0, 2, 1)  # (B,T,F) -> (B,F,T) for Conv1d\n        out = self.tcn(x)\n        last = out[:, :, -1]\n        return self.linear(last)\n\nmodel_cls = TCN_10D_d02'
) ON CONFLICT (model_id) DO UPDATE SET
    catalog_version = EXCLUDED.catalog_version,
    generated_at_utc = EXCLUDED.generated_at_utc,
    catalog_source = EXCLUDED.catalog_source,
    model_type = EXCLUDED.model_type,
    model_name = EXCLUDED.model_name,
    display_name = EXCLUDED.display_name,
    model_description = EXCLUDED.model_description,
    model_config = EXCLUDED.model_config,
    dataset_config = EXCLUDED.dataset_config,
    model_hyperparameters = EXCLUDED.model_hyperparameters,
    model_training_hyperparameters = EXCLUDED.model_training_hyperparameters,
    code_text = EXCLUDED.code_text;

-- ═══════════════════════════════════════════════════════════════
-- M7: XGBoost_10D — 第二树模型对比
-- ═══════════════════════════════════════════════════════════════
INSERT INTO aistock_model_catalog (
    model_id, catalog_version, generated_at_utc, catalog_source,
    task_run_id, loop_id, workspace_id, workspace_path,
    model_type, model_name, display_name, model_description,
    model_config, dataset_config,
    model_hyperparameters, model_training_hyperparameters, is_sota
) VALUES (
    '__seed_XGBoost_10D__',
    '1.0', '2026-04-29T00:00:00Z', 'manual_10D',
    'manual_10D_seed', 7, 'manual_10D_xgboost', 'N/A',
    'XGBOOST',
    'XGBoost_10D',
    'XGBoost 10D',
    'XGBoost 梯度提升树，level-wise 分裂策略。与 LGBM (leaf-wise) 对比验证树模型方向的可替换性。',
    '{"class": "XGBModel", "module_path": "qlib.contrib.model.xgboost"}'::jsonb,
    '{"class": "DatasetH", "module_path": "qlib.data.dataset"}'::jsonb,
    '{"n_estimators": 500, "max_depth": 8, "learning_rate": 0.05, "subsample": 0.8, "colsample_bytree": 0.8, "reg_alpha": 0.1, "reg_lambda": 1.0, "n_jobs": -1}'::jsonb,
    '{"n_estimators": 500, "learning_rate": 0.05}'::jsonb,
    false
) ON CONFLICT (model_id) DO UPDATE SET
    catalog_version = EXCLUDED.catalog_version,
    generated_at_utc = EXCLUDED.generated_at_utc,
    catalog_source = EXCLUDED.catalog_source,
    model_type = EXCLUDED.model_type,
    model_name = EXCLUDED.model_name,
    display_name = EXCLUDED.display_name,
    model_description = EXCLUDED.model_description,
    model_config = EXCLUDED.model_config,
    dataset_config = EXCLUDED.dataset_config,
    model_hyperparameters = EXCLUDED.model_hyperparameters,
    model_training_hyperparameters = EXCLUDED.model_training_hyperparameters;

-- ═══════════════════════════════════════════════════════════════
-- M8: CatBoost_10D — ordered boosting 时序防泄漏
-- ═══════════════════════════════════════════════════════════════
INSERT INTO aistock_model_catalog (
    model_id, catalog_version, generated_at_utc, catalog_source,
    task_run_id, loop_id, workspace_id, workspace_path,
    model_type, model_name, display_name, model_description,
    model_config, dataset_config,
    model_hyperparameters, model_training_hyperparameters, is_sota
) VALUES (
    '__seed_CatBoost_10D__',
    '1.0', '2026-04-29T00:00:00Z', 'manual_10D',
    'manual_10D_seed', 8, 'manual_10D_catboost', 'N/A',
    'CATBOOST',
    'CatBoost_10D',
    'CatBoost 10D',
    'CatBoost ordered boosting，天然防止时序信息泄漏。10D优化超参: iterations=500, depth=8, l2_leaf_reg=3.0。',
    '{"class": "CatBoostModel", "module_path": "qlib.contrib.model.catboost_model"}'::jsonb,
    '{"class": "DatasetH", "module_path": "qlib.data.dataset"}'::jsonb,
    '{"iterations": 500, "depth": 8, "learning_rate": 0.05, "l2_leaf_reg": 3.0, "subsample": 0.8, "verbose": 0, "task_type": "CPU"}'::jsonb,
    '{"iterations": 500, "learning_rate": 0.05, "depth": 8}'::jsonb,
    false
) ON CONFLICT (model_id) DO UPDATE SET
    catalog_version = EXCLUDED.catalog_version,
    generated_at_utc = EXCLUDED.generated_at_utc,
    catalog_source = EXCLUDED.catalog_source,
    model_type = EXCLUDED.model_type,
    model_name = EXCLUDED.model_name,
    display_name = EXCLUDED.display_name,
    model_description = EXCLUDED.model_description,
    model_config = EXCLUDED.model_config,
    dataset_config = EXCLUDED.dataset_config,
    model_hyperparameters = EXCLUDED.model_hyperparameters,
    model_training_hyperparameters = EXCLUDED.model_training_hyperparameters;

-- ═══════════════════════════════════════════════════════════════
-- M9: LambdaMART_10D — 排序目标优化
-- ═══════════════════════════════════════════════════════════════
INSERT INTO aistock_model_catalog (
    model_id, catalog_version, generated_at_utc, catalog_source,
    task_run_id, loop_id, workspace_id, workspace_path,
    model_type, model_name, display_name, model_description,
    model_config, dataset_config,
    model_hyperparameters, model_training_hyperparameters, is_sota
) VALUES (
    '__seed_LambdaMART_10D__',
    '1.0', '2026-04-29T00:00:00Z', 'manual_10D',
    'manual_10D_seed', 9, 'manual_10D_lambdamart', 'N/A',
    'LAMBDARANK',
    'LambdaMART_10D',
    'LambdaMART 10D [LightGBM ranking]',
    'LightGBM LambdaMART 排序模型。直接优化 NDCG 排序质量，训练目标与选股任务对齐。与 MSE 回归对比验证训练目标对收益的影响。',
    '{"class": "LambdaRankModel", "module_path": "aistock_models.lambdarank"}'::jsonb,
    '{"class": "DatasetH", "module_path": "qlib.data.dataset"}'::jsonb,
    '{"objective": "lambdarank", "num_leaves": 64, "max_depth": 8, "learning_rate": 0.05, "n_estimators": 300, "min_child_samples": 100, "subsample": 0.8, "colsample_bytree": 0.8, "reg_alpha": 0.1, "reg_lambda": 0.1, "early_stopping_rounds": 20}'::jsonb,
    '{"n_estimators": 300, "learning_rate": 0.05}'::jsonb,
    false
) ON CONFLICT (model_id) DO UPDATE SET
    catalog_version = EXCLUDED.catalog_version,
    generated_at_utc = EXCLUDED.generated_at_utc,
    catalog_source = EXCLUDED.catalog_source,
    model_type = EXCLUDED.model_type,
    model_name = EXCLUDED.model_name,
    display_name = EXCLUDED.display_name,
    model_description = EXCLUDED.model_description,
    model_config = EXCLUDED.model_config,
    dataset_config = EXCLUDED.dataset_config,
    model_hyperparameters = EXCLUDED.model_hyperparameters,
    model_training_hyperparameters = EXCLUDED.model_training_hyperparameters;

-- ═══════════════════════════════════════════════════════════════
-- M10: TabPFN_10D — 表格基础模型
-- ═══════════════════════════════════════════════════════════════
INSERT INTO aistock_model_catalog (
    model_id, catalog_version, generated_at_utc, catalog_source,
    task_run_id, loop_id, workspace_id, workspace_path,
    model_type, model_name, display_name, model_description,
    model_config, dataset_config,
    model_hyperparameters, model_training_hyperparameters, is_sota
) VALUES (
    '__seed_TabPFN_10D__',
    '1.0', '2026-04-29T00:00:00Z', 'manual_10D',
    'manual_10D_seed', 10, 'manual_10D_tabpfn', 'N/A',
    'TABPFN',
    'TabPFN_10D',
    'TabPFN 10D/20D [表格基础模型, Nature 2025]',
    'TabPFN 预训练 Transformer，在 1.3 亿合成表格数据集上训练。in-context learning 零训练推理，小样本场景 (<10000) 超越调参 GBDT。适合 10D/20D 样本稀缺场景。',
    '{"class": "TabPFNModel", "module_path": "aistock_models.tabpfn_model"}'::jsonb,
    '{"class": "DatasetH", "module_path": "qlib.data.dataset"}'::jsonb,
    '{"n_estimators": 8, "device": "cuda", "max_context_size": 2000, "n_bins": 10, "random_state": 42}'::jsonb,
    '{}'::jsonb,
    false
) ON CONFLICT (model_id) DO UPDATE SET
    catalog_version = EXCLUDED.catalog_version,
    generated_at_utc = EXCLUDED.generated_at_utc,
    catalog_source = EXCLUDED.catalog_source,
    model_type = EXCLUDED.model_type,
    model_name = EXCLUDED.model_name,
    display_name = EXCLUDED.display_name,
    model_description = EXCLUDED.model_description,
    model_config = EXCLUDED.model_config,
    dataset_config = EXCLUDED.dataset_config,
    model_hyperparameters = EXCLUDED.model_hyperparameters,
    model_training_hyperparameters = EXCLUDED.model_training_hyperparameters;
