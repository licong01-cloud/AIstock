-- Seed LGBModel golden configuration into aistock_model_catalog
-- Created: 2026-04-06
-- Purpose: T1.2 - Ensure QE has high-quality LGBModel available for selection
-- Golden hyperparameters validated on 2026-03-14

INSERT INTO aistock_model_catalog (
    model_id, catalog_version, generated_at_utc, catalog_source,
    task_run_id, loop_id, workspace_id, workspace_path,
    model_type, model_name, model_description,
    model_config, dataset_config,
    model_hyperparameters, model_training_hyperparameters, is_sota
) VALUES (
    '__seed_LGBModel_golden_v1__',
    '1.0', '2026-04-06T00:00:00Z', 'manual_seed',
    'manual_seed', 0, 'manual_seed', 'N/A',
    'LGBModel',
    'LGBModel Golden Seed (validated 2026-03-14)',
    'Qlib LGBModel with golden hyperparameters. batch_size=4096 avg IC=0.051. See model_hyperparameter_best_practices_20260314.md',
    '{"class": "LGBModel", "module_path": "qlib.contrib.model.gbdt", "kwargs": {"loss": "mse", "device_type": "cpu", "max_bin": 63, "colsample_bytree": 0.8879, "learning_rate": 0.2, "subsample": 0.8789, "lambda_l1": 205.6999, "lambda_l2": 580.9768, "max_depth": 8, "num_leaves": 210, "num_threads": 24}}'::jsonb,
    '{"class": "DatasetH", "module_path": "qlib.data.dataset"}'::jsonb,
    '{"loss": "mse", "device_type": "cpu", "max_bin": 63, "colsample_bytree": 0.8879, "learning_rate": 0.2, "subsample": 0.8789, "lambda_l1": 205.6999, "lambda_l2": 580.9768, "max_depth": 8, "num_leaves": 210, "num_threads": 24}'::jsonb,
    '{"learning_rate": 0.2, "max_depth": 8, "num_leaves": 210}'::jsonb,
    true
) ON CONFLICT (model_id) DO NOTHING;

INSERT INTO aistock_model_catalog (
    model_id, catalog_version, generated_at_utc, catalog_source,
    task_run_id, loop_id, workspace_id, workspace_path,
    model_type, model_name, model_description,
    model_config, dataset_config,
    model_hyperparameters, model_training_hyperparameters, is_sota
) VALUES (
    '__seed_LGBModel_conservative_v1__',
    '1.0', '2026-04-06T00:00:00Z', 'manual_seed',
    'manual_seed', 1, 'manual_seed', 'N/A',
    'LGBModel',
    'LGBModel Conservative Seed (lr=0.05)',
    'Qlib LGBModel with conservative lr=0.05, matching RDAGENT_DEFAULT_LGB_KWARGS in config_composer.py',
    '{"class": "LGBModel", "module_path": "qlib.contrib.model.gbdt", "kwargs": {"loss": "mse", "device_type": "cpu", "max_bin": 63, "colsample_bytree": 0.8879, "learning_rate": 0.05, "subsample": 0.8789, "lambda_l1": 205.6999, "lambda_l2": 580.9768, "max_depth": 8, "num_leaves": 210, "num_threads": 24}}'::jsonb,
    '{"class": "DatasetH", "module_path": "qlib.data.dataset"}'::jsonb,
    '{"loss": "mse", "device_type": "cpu", "max_bin": 63, "colsample_bytree": 0.8879, "learning_rate": 0.05, "subsample": 0.8789, "lambda_l1": 205.6999, "lambda_l2": 580.9768, "max_depth": 8, "num_leaves": 210, "num_threads": 24}'::jsonb,
    '{"learning_rate": 0.05, "max_depth": 8, "num_leaves": 210}'::jsonb,
    true
) ON CONFLICT (model_id) DO NOTHING;
