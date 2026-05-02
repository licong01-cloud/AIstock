"""Asset Bundle Management Service for RD-Agent.

Handles downloading, unzipping, and local path management for solidified asset bundles.
Strictly follows Section 15 of Phase3_Detail_Design_RD-Agent_AIstock_Final.md.
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import zipfile
from pathlib import Path
from typing import Dict, Optional

from .rdagent_results_api_client import RDAgentResultsApiClient
from .strategy_package.workspace_policy import ensure_aistock_artifact_path, ensure_not_forbidden_worker_workspace_path

logger = logging.getLogger("aistock.rdagent_asset")

class RDAgentAssetService:
    def __init__(self, base_dir: Optional[str] = None):
        # 默认存储路径: <repo_root>/rdagent_assets (或环境变量指定)
        default_base_dir = Path(__file__).resolve().parents[2] / "rdagent_assets"
        self.base_dir = Path(base_dir or os.getenv("RDAGENT_ASSETS_DIR") or default_base_dir).resolve()
        self.bundles_dir = self.base_dir / "production_bundles"
        ensure_not_forbidden_worker_workspace_path(self.base_dir, purpose="RD-Agent asset bundle base_dir")
        ensure_aistock_artifact_path(self.bundles_dir, purpose="RD-Agent asset bundle cache")
        self._ensure_dirs()
        self.client = RDAgentResultsApiClient()

    def _ensure_dirs(self):
        self.bundles_dir.mkdir(parents=True, exist_ok=True)

    def get_bundle_path(self, asset_bundle_id: str) -> Path:
        """获取资产包本地存放路径"""
        return self.bundles_dir / asset_bundle_id

    def is_bundle_available(self, asset_bundle_id: str) -> bool:
        """检查资产包是否已在本地解压可用"""
        bundle_path = self.get_bundle_path(asset_bundle_id)
        # 简单检查是否存在目录且不为空
        return bundle_path.exists() and any(bundle_path.iterdir())

    def download_and_extract_bundle(self, asset_bundle_id: str) -> bool:
        """下载并解压资产包 (REQ-ASSET-P3-010)"""
        if self.is_bundle_available(asset_bundle_id):
            logger.info(f"资产包 {asset_bundle_id} 已存在，跳过下载。")
            return True

        target_zip = self.base_dir / f"{asset_bundle_id}.zip"
        target_dir = self.get_bundle_path(asset_bundle_id)

        try:
            # 1. 下载
            success = self.client.download_asset_bundle(asset_bundle_id, str(target_zip))
            if not success:
                logger.warning(f"下载资产包 {asset_bundle_id} 失败：Results API 返回 success=False（未生成 zip 文件）。")
                return False

            # 2. 解压
            with zipfile.ZipFile(target_zip, 'r') as zip_ref:
                zip_ref.extractall(target_dir)
            
            logger.info(f"成功同步并解压资产包: {asset_bundle_id}")
            return True
        except Exception as e:
            logger.error(f"处理资产包 {asset_bundle_id} 失败: {e}")
            if target_dir.exists():
                shutil.rmtree(target_dir)
            return False
        finally:
            if target_zip.exists():
                os.remove(target_zip)

    def get_strategy_files(self, asset_bundle_id: str, workspace_id: str) -> Dict[str, str]:
        """获取资产包内指定 Workspace 的物理文件路径映射
        
        支持多种资产包结构：
        1. bundle/workspace_id/{factor.py,model.py,config.yaml,weights/model.pkl}
        2. bundle/{factor.py,model.py,config.yaml,weights/model.pkl} (无 workspace_id 子目录)
        3. bundle/{model.py,conf_*.yaml} (Phase3 新结构，直接在根目录)
        """
        bundle_path = self.get_bundle_path(asset_bundle_id)
        manifest_path = bundle_path / "manifest.json"
        if manifest_path.exists():
            try:
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            except Exception as e:
                raise RuntimeError(f"资产包 {asset_bundle_id} manifest.json 解析失败: {e}")

            schema_version = manifest.get("schema_version")
            if schema_version != 1:
                raise RuntimeError(
                    f"资产包 {asset_bundle_id} manifest schema_version={schema_version} 不受支持（仅支持 1）"
                )

            primary_ws = manifest.get("primary_workspace_id")
            if primary_ws and str(primary_ws) != str(workspace_id):
                logger.warning(
                    f"资产包 {asset_bundle_id} manifest.primary_workspace_id={primary_ws} 与 loop workspace_id={workspace_id} 不一致，将以 manifest 为准"
                )

            primary_assets = manifest.get("primary_assets") or {}
            factor_rel = primary_assets.get("factor_entry_relpath")
            model_rel = primary_assets.get("model_weight_relpath")
            config_rel = primary_assets.get("config_relpath")

            if not factor_rel or not model_rel:
                raise RuntimeError(
                    f"资产包 {asset_bundle_id} manifest.primary_assets 缺少 factor_entry_relpath 或 model_weight_relpath"
                )

            factor_path = (bundle_path / str(factor_rel)).resolve()
            model_path = (bundle_path / str(model_rel)).resolve()
            config_path = (bundle_path / str(config_rel)).resolve() if config_rel else None

            # manifest 是权威索引，但在某些本地/迁移场景下，bundle 可能缺失 workspaces 目录。
            # 为避免直接中断，若 manifest 指向文件缺失，则回退到兼容性扫描逻辑。
            if factor_path.exists() and model_path.exists():
                if config_path is not None and not config_path.exists():
                    logger.warning(
                        f"资产包 {asset_bundle_id} manifest 指向的配置不存在: {config_rel}（将回退扫描或返回空）"
                    )
                    config_path = None
                return {
                    "factor_py": str(factor_path),
                    "config_yaml": str(config_path) if config_path else None,
                    "model_pkl": str(model_path),
                }

            logger.warning(
                "manifest_assets_missing"
                f" bundle_id={asset_bundle_id}"
                f" factor_rel={factor_rel} factor_exists={factor_path.exists()}"
                f" model_rel={model_rel} model_exists={model_path.exists()}"
                f" config_rel={config_rel} config_exists={(config_path.exists() if config_path is not None else None)}"
                " ; fallback to heuristic scan"
            )

        ws_path = bundle_path / workspace_id

        if not ws_path.exists():
            # 兼容性检查 1: 如果资产包结构不含 workspace_id，则直接使用根目录
            if (bundle_path / "factor.py").exists() or (bundle_path / "model.py").exists():
                ws_path = bundle_path
                logger.info(f"资产包 {asset_bundle_id} 使用根目录作为工作区（无 workspace_id 子目录）")
            else:
                # 兼容性检查 2: 尝试查找资产包内唯一的工作区目录
                subdirs = [d for d in bundle_path.iterdir() if d.is_dir() and d.name != "weights"]
                if len(subdirs) == 1:
                    ws_path = subdirs[0]
                    logger.warning(f"资产包 {asset_bundle_id} 中未找到工作区 {workspace_id}，自动使用唯一子目录: {ws_path.name}")
                else:
                    logger.error(f"资产包 {asset_bundle_id} 中未找到工作区 {workspace_id}，且无法确定唯一工作区目录（找到 {len(subdirs)} 个子目录）")
                    return {}

        # 查找因子/模型文件（优先 factor.py，其次 model.py）
        factor_py = None
        if (ws_path / "factor.py").exists():
            factor_py = str(ws_path / "factor.py")
        elif (ws_path / "model.py").exists():
            factor_py = str(ws_path / "model.py")
            logger.info(f"在工作区 {ws_path.name} 中使用 model.py 作为因子实现")
        else:
            # 查找任意 .py 文件
            for py_file in ws_path.glob("*.py"):
                if py_file.name != "__pycache__":
                    factor_py = str(py_file)
                    logger.info(f"在工作区 {ws_path.name} 中找到 Python 文件: {py_file.name}")
                    break

        # 查找配置文件（支持 config.yaml 或 conf_*.yaml）
        config_yaml = None
        if (ws_path / "config.yaml").exists():
            config_yaml = str(ws_path / "config.yaml")
        else:
            # 查找 conf_*.yaml 文件
            conf_files = list(ws_path.glob("conf_*.yaml"))
            if conf_files:
                config_yaml = str(conf_files[0])
                logger.info(f"在工作区 {ws_path.name} 中找到配置文件: {conf_files[0].name}")

        # 查找模型文件（支持多种位置）
        model_pkl = None
        # 1. 优先查找 weights/model.pkl
        if (bundle_path / "weights" / "model.pkl").exists():
            model_pkl = str(bundle_path / "weights" / "model.pkl")
        # 2. 查找 mlruns 目录下的 params.pkl
        elif (bundle_path / "mlruns").exists():
            params_pkl_files = list((bundle_path / "mlruns").rglob("params.pkl"))
            if params_pkl_files:
                model_pkl = str(params_pkl_files[0])
                logger.info(f"在资产包 mlruns 目录中找到模型文件: {params_pkl_files[0].relative_to(bundle_path)}")
        # 3. 查找资产包根目录下的 *.pkl 文件
        else:
            pkl_files = list(bundle_path.glob("*.pkl"))
            if pkl_files:
                # 优先选择可能包含模型的文件（排除 *_pred.pkl, *_label.pkl 等结果文件）
                result_pkl_files = [f for f in pkl_files if not any(x in f.name for x in ['_pred', '_label', '_ic', '_ric', '_indicator', '_port', '_position', '_report', 'ret.pkl'])]
                if result_pkl_files:
                    model_pkl = str(result_pkl_files[0])
                    logger.info(f"在资产包根目录中找到模型文件: {result_pkl_files[0].name}")
                else:
                    # 如果没有明确的模型文件，使用第一个 .pkl 文件
                    model_pkl = str(pkl_files[0])
                    logger.warning(f"在资产包根目录中未找到明确的模型文件，使用: {pkl_files[0].name}")

        result = {
            "factor_py": factor_py,
            "config_yaml": config_yaml,
            "model_pkl": model_pkl
        }

        return result

# 单例
rdagent_asset_service = RDAgentAssetService()
