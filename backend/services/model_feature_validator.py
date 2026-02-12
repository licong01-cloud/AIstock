"""
模型特征严格验证模块

严格要求：
1. 不允许任何推断或猜测
2. 不允许任何兜底方案
3. 所有失败必须立即报错
4. 特征数量和顺序必须与模型权重完全一致
"""

import pickle
import json
from pathlib import Path
from typing import List, Dict, Any, Tuple
import logging

logger = logging.getLogger("aistock.model_validator")


class ModelFeatureValidationError(Exception):
    """模型特征验证错误"""
    pass


class ModelFeatureValidator:
    """模型特征严格验证器"""
    
    @staticmethod
    def extract_model_features(model_path: Path) -> Tuple[int, List[str]]:
        """
        从模型权重文件中严格提取特征信息
        
        Returns:
            (特征数量, 特征名称列表)
        
        Raises:
            ModelFeatureValidationError: 如果无法提取特征信息
        """
        if not model_path.exists():
            raise ModelFeatureValidationError(f"模型文件不存在: {model_path}")
        
        try:
            with open(model_path, "rb") as f:
                model = pickle.load(f)
        except Exception as e:
            raise ModelFeatureValidationError(f"加载模型文件失败: {e}")
        
        # 尝试获取特征数量
        num_features = None
        feature_names = []
        
        # 方法1: 从LightGBM模型获取
        inner_model = getattr(model, "model", None) or getattr(model, "booster_", None)
        if inner_model:
            # 获取特征数量
            if hasattr(inner_model, "num_feature"):
                val = getattr(inner_model, "num_feature")
                num_features = val() if callable(val) else val
            elif hasattr(inner_model, "n_features_"):
                num_features = getattr(inner_model, "n_features_")
            
            # 获取特征名称
            if hasattr(inner_model, "feature_name_"):
                feature_names = list(getattr(inner_model, "feature_name_"))
            elif hasattr(inner_model, "feature_name"):
                val = getattr(inner_model, "feature_name")
                feature_names = list(val() if callable(val) else val)
        
        # 验证结果
        if num_features is None or num_features == 0:
            raise ModelFeatureValidationError(
                f"无法从模型中提取特征数量。模型类型: {type(model)}, "
                f"inner_model类型: {type(inner_model) if inner_model else None}"
            )
        
        if not feature_names:
            raise ModelFeatureValidationError(
                f"无法从模型中提取特征名称列表。模型期望 {num_features} 个特征，"
                f"但未找到 feature_name_ 或 feature_name 属性"
            )
        
        if len(feature_names) != num_features:
            raise ModelFeatureValidationError(
                f"特征名称列表长度 ({len(feature_names)}) 与模型期望特征数量 ({num_features}) 不一致"
            )
        
        logger.info(f"从模型中提取到 {num_features} 个特征")
        return num_features, feature_names
    
    @staticmethod
    def extract_alpha158_from_model_meta(model_meta_path: Path) -> List[str]:
        """
        从model_meta.json中严格提取Alpha158基线因子列表
        
        只从 infer_processors 中的 FilterCol 提取，不使用任何兜底方案
        
        Raises:
            ModelFeatureValidationError: 如果无法提取或提取失败
        """
        if not model_meta_path.exists():
            raise ModelFeatureValidationError(f"model_meta.json文件不存在: {model_meta_path}")
        
        try:
            with open(model_meta_path, "r", encoding="utf-8") as f:
                meta_json = json.load(f)
        except Exception as e:
            raise ModelFeatureValidationError(f"读取model_meta.json失败: {e}")
        
        # 严格从 dataset_conf.kwargs.handler.kwargs.infer_processors 提取
        try:
            dataset_conf = meta_json["dataset_conf"]
            handler_kwargs = dataset_conf["kwargs"]["handler"]["kwargs"]
            infer_processors = handler_kwargs["infer_processors"]
        except KeyError as e:
            raise ModelFeatureValidationError(
                f"model_meta.json结构不正确，缺少必需字段: {e}"
            )
        
        # 查找 FilterCol processor
        alpha158_factors = None
        for proc in infer_processors:
            if proc.get("class") == "FilterCol":
                proc_kwargs = proc.get("kwargs", {})
                if proc_kwargs.get("fields_group") == "feature":
                    col_list = proc_kwargs.get("col_list")
                    if isinstance(col_list, list) and col_list:
                        alpha158_factors = col_list
                        break
        
        if not alpha158_factors:
            raise ModelFeatureValidationError(
                "无法从model_meta.json的infer_processors中找到FilterCol.col_list配置"
            )
        
        logger.info(f"从model_meta.json提取到 {len(alpha158_factors)} 个Alpha158基线因子")
        return alpha158_factors
    
    @staticmethod
    def validate_factor_order(
        factor_order_path: Path,
        model_num_features: int,
        model_feature_names: List[str]
    ) -> Dict[str, Any]:
        """
        严格验证factor_order.json与模型特征的一致性
        
        Raises:
            ModelFeatureValidationError: 如果验证失败
        """
        if not factor_order_path.exists():
            raise ModelFeatureValidationError(f"factor_order.json文件不存在: {factor_order_path}")
        
        try:
            with open(factor_order_path, "r", encoding="utf-8") as f:
                factor_order = json.load(f)
        except Exception as e:
            raise ModelFeatureValidationError(f"读取factor_order.json失败: {e}")
        
        # 提取配置
        alpha158_factors = factor_order.get("alpha158_factors", [])
        dynamic_factor_names = factor_order.get("dynamic_factor_names", [])
        total_factors = factor_order.get("total_factors", 0)
        
        # 验证1: 检查必需字段
        if not alpha158_factors:
            raise ModelFeatureValidationError("factor_order.json中缺少alpha158_factors字段")
        
        if "dynamic_factor_names" not in factor_order:
            raise ModelFeatureValidationError(
                "factor_order.json中缺少dynamic_factor_names字段。"
                "注意：必须是因子名称列表，不能是文件路径列表"
            )
        
        # 验证2: 特征总数必须匹配
        actual_total = len(alpha158_factors) + len(dynamic_factor_names)
        if actual_total != model_num_features:
            raise ModelFeatureValidationError(
                f"特征数量不匹配：\n"
                f"  模型期望: {model_num_features} 个特征\n"
                f"  factor_order.json配置: {actual_total} 个特征\n"
                f"    - Alpha158基线因子: {len(alpha158_factors)} 个\n"
                f"    - SOTA动态因子: {len(dynamic_factor_names)} 个\n"
                f"  差异: {model_num_features - actual_total} 个特征"
            )
        
        if total_factors != actual_total:
            raise ModelFeatureValidationError(
                f"factor_order.json中的total_factors字段 ({total_factors}) "
                f"与实际特征数量 ({actual_total}) 不一致"
            )
        
        # 验证3: 特征顺序必须完全一致
        configured_features = alpha158_factors + dynamic_factor_names
        
        for i, (expected, actual) in enumerate(zip(model_feature_names, configured_features)):
            if expected != actual:
                raise ModelFeatureValidationError(
                    f"特征顺序不匹配（位置 {i}）：\n"
                    f"  模型期望: {expected}\n"
                    f"  factor_order.json配置: {actual}\n"
                    f"  前5个模型特征: {model_feature_names[:5]}\n"
                    f"  前5个配置特征: {configured_features[:5]}"
                )
        
        logger.info(f"factor_order.json验证通过：{model_num_features} 个特征完全匹配")
        
        return {
            "valid": True,
            "num_features": model_num_features,
            "alpha158_count": len(alpha158_factors),
            "dynamic_count": len(dynamic_factor_names),
            "feature_names": configured_features
        }
    
    @staticmethod
    def validate_task_sync(task_dir: Path) -> Dict[str, Any]:
        """
        验证任务同步的完整性和正确性
        
        验证步骤：
        1. 检查必需文件是否存在
        2. 从模型中提取特征信息
        3. 从model_meta.json提取Alpha158基线因子
        4. 验证factor_order.json与模型的一致性
        
        Raises:
            ModelFeatureValidationError: 如果验证失败
        """
        logger.info(f"开始验证任务同步: {task_dir}")
        
        # 检查必需文件
        model_path = task_dir / "model.pkl"
        model_meta_path = task_dir / "model_meta.json"
        factor_order_path = task_dir / "factor_order.json"
        
        if not model_path.exists():
            raise ModelFeatureValidationError(f"缺少模型文件: model.pkl")
        
        if not model_meta_path.exists():
            raise ModelFeatureValidationError(f"缺少模型元数据文件: model_meta.json")
        
        if not factor_order_path.exists():
            raise ModelFeatureValidationError(f"缺少因子顺序配置文件: factor_order.json")
        
        # 从模型中提取特征信息
        num_features, feature_names = ModelFeatureValidator.extract_model_features(model_path)
        
        # 从model_meta.json提取Alpha158基线因子
        alpha158_factors = ModelFeatureValidator.extract_alpha158_from_model_meta(model_meta_path)
        
        # 验证factor_order.json
        validation_result = ModelFeatureValidator.validate_factor_order(
            factor_order_path,
            num_features,
            feature_names
        )
        
        # 验证Alpha158因子数量
        factor_order_alpha158 = json.loads(factor_order_path.read_text(encoding="utf-8")).get("alpha158_factors", [])
        if len(factor_order_alpha158) != len(alpha158_factors):
            raise ModelFeatureValidationError(
                f"factor_order.json中的Alpha158因子数量 ({len(factor_order_alpha158)}) "
                f"与model_meta.json中的数量 ({len(alpha158_factors)}) 不一致"
            )
        
        logger.info(f"任务同步验证通过: {task_dir.name}")
        
        return {
            "task_dir": str(task_dir),
            "validation_passed": True,
            "model_features": num_features,
            "alpha158_count": len(alpha158_factors),
            "dynamic_count": num_features - len(alpha158_factors),
            "feature_names": feature_names
        }
