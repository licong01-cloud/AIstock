"""
QE Artifact 缓存管理器

负责 QE workspace artifact 的本地缓存管理：
- 下载 artifact 到本地
- SHA256 完整性校验
- 元数据管理（metadata.json）
- 缓存清理和查询
"""

import hashlib
import json
import pickle
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from .exceptions import CacheError


class ArtifactCacheManager:
    """
    Artifact 缓存管理器

    管理 QE workspace artifact 的本地缓存，包括：
    - pred.pkl: 预测分数
    - label.pkl: 未来收益标签
    - 其他 artifact 文件

    缓存结构:
        cache_dir/
        ├─ {loop_ref}/
        │  ├─ pred.pkl
        │  ├─ label.pkl
        │  └─ metadata.json
        └─ ...
    """

    def __init__(self, cache_dir: str = "tmp/hmm_evolution_cache/"):
        """
        Args:
            cache_dir: 缓存根目录
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def get_artifact_path(self, loop_ref: str, artifact_name: str) -> Path:
        """
        获取 artifact 的缓存路径

        Args:
            loop_ref: QE loop 引用（如 "qe_20260502_131502_9b54/Loop1"）
            artifact_name: artifact 名称（如 "pred.pkl"）

        Returns:
            缓存文件的完整路径
        """
        # 将 loop_ref 中的 / 替换为 _
        safe_loop_ref = loop_ref.replace("/", "_")
        loop_cache_dir = self.cache_dir / safe_loop_ref
        loop_cache_dir.mkdir(parents=True, exist_ok=True)
        return loop_cache_dir / artifact_name

    def save_artifact(
        self,
        loop_ref: str,
        artifact_name: str,
        artifact_bytes: bytes,
        metadata: Optional[dict] = None,
    ) -> Path:
        """
        保存 artifact 到缓存

        Args:
            loop_ref: QE loop 引用
            artifact_name: artifact 名称
            artifact_bytes: artifact 内容（bytes）
            metadata: 可选的元数据

        Returns:
            缓存文件路径

        Raises:
            CacheError: 保存失败
        """
        try:
            artifact_path = self.get_artifact_path(loop_ref, artifact_name)

            # 写入文件
            artifact_path.write_bytes(artifact_bytes)

            # 计算 SHA256
            sha256 = hashlib.sha256(artifact_bytes).hexdigest()

            # 保存元数据
            meta = {
                "loop_ref": loop_ref,
                "artifact_name": artifact_name,
                "file_size": len(artifact_bytes),
                "sha256": sha256,
                "cached_at": datetime.now().isoformat(),
            }
            if metadata:
                meta.update(metadata)

            self._save_metadata(loop_ref, artifact_name, meta)

            return artifact_path

        except Exception as e:
            raise CacheError(f"Failed to save artifact {artifact_name}: {e}")

    def load_artifact(
        self,
        loop_ref: str,
        artifact_name: str,
        verify_checksum: bool = True,
    ) -> bytes:
        """
        从缓存加载 artifact

        Args:
            loop_ref: QE loop 引用
            artifact_name: artifact 名称
            verify_checksum: 是否验证 SHA256

        Returns:
            artifact 内容（bytes）

        Raises:
            CacheError: 加载失败或校验失败
        """
        artifact_path = self.get_artifact_path(loop_ref, artifact_name)

        if not artifact_path.exists():
            raise CacheError(
                f"Artifact not found in cache: {artifact_name} for {loop_ref}"
            )

        try:
            artifact_bytes = artifact_path.read_bytes()

            if verify_checksum:
                metadata = self._load_metadata(loop_ref, artifact_name)
                if metadata:
                    expected_sha256 = metadata.get("sha256")
                    if expected_sha256:
                        actual_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
                        if actual_sha256 != expected_sha256:
                            raise CacheError(
                                f"Checksum mismatch for {artifact_name}: "
                                f"expected {expected_sha256}, got {actual_sha256}"
                            )

            return artifact_bytes

        except CacheError:
            raise
        except Exception as e:
            raise CacheError(f"Failed to load artifact {artifact_name}: {e}")

    def load_pickle(self, loop_ref: str, artifact_name: str) -> Any:
        """
        从缓存加载 pickle artifact 并反序列化

        Args:
            loop_ref: QE loop 引用
            artifact_name: artifact 名称

        Returns:
            反序列化后的对象

        Raises:
            CacheError: 加载或反序列化失败
        """
        try:
            artifact_bytes = self.load_artifact(loop_ref, artifact_name)
            return pickle.loads(artifact_bytes)
        except Exception as e:
            raise CacheError(f"Failed to load pickle {artifact_name}: {e}")

    def is_cached(self, loop_ref: str, artifact_name: str) -> bool:
        """
        检查 artifact 是否已缓存

        Args:
            loop_ref: QE loop 引用
            artifact_name: artifact 名称

        Returns:
            True if cached, False otherwise
        """
        artifact_path = self.get_artifact_path(loop_ref, artifact_name)
        return artifact_path.exists()

    def clear_cache(self, loop_ref: Optional[str] = None):
        """
        清理缓存

        Args:
            loop_ref: 如果指定，只清理该 loop 的缓存；否则清理所有
        """
        if loop_ref:
            safe_loop_ref = loop_ref.replace("/", "_")
            loop_cache_dir = self.cache_dir / safe_loop_ref
            if loop_cache_dir.exists():
                import shutil
                shutil.rmtree(loop_cache_dir)
        else:
            if self.cache_dir.exists():
                import shutil
                shutil.rmtree(self.cache_dir)
                self.cache_dir.mkdir(parents=True, exist_ok=True)

    def get_cache_info(self, loop_ref: Optional[str] = None) -> dict:
        """
        获取缓存信息

        Args:
            loop_ref: 如果指定，只返回该 loop 的信息；否则返回所有

        Returns:
            缓存信息字典
        """
        if loop_ref:
            safe_loop_ref = loop_ref.replace("/", "_")
            loop_cache_dir = self.cache_dir / safe_loop_ref
            if not loop_cache_dir.exists():
                return {"loop_ref": loop_ref, "cached": False}

            artifacts = {}
            total_size = 0
            for artifact_path in loop_cache_dir.glob("*.pkl"):
                size = artifact_path.stat().st_size
                total_size += size
                artifacts[artifact_path.name] = {
                    "size": size,
                    "cached_at": datetime.fromtimestamp(
                        artifact_path.stat().st_mtime
                    ).isoformat(),
                }

            return {
                "loop_ref": loop_ref,
                "cached": True,
                "artifacts": artifacts,
                "total_size": total_size,
            }
        else:
            all_info = {}
            for loop_dir in self.cache_dir.iterdir():
                if loop_dir.is_dir():
                    loop_ref = loop_dir.name.replace("_", "/", 1)
                    all_info[loop_ref] = self.get_cache_info(loop_ref)
            return all_info

    def _get_metadata_path(self, loop_ref: str) -> Path:
        """获取元数据文件路径"""
        safe_loop_ref = loop_ref.replace("/", "_")
        return self.cache_dir / safe_loop_ref / "metadata.json"

    def _save_metadata(self, loop_ref: str, artifact_name: str, metadata: dict):
        """保存元数据到 metadata.json"""
        metadata_path = self._get_metadata_path(loop_ref)

        # 读取现有元数据
        if metadata_path.exists():
            try:
                all_metadata = json.loads(metadata_path.read_text())
            except:
                all_metadata = {}
        else:
            all_metadata = {}

        # 更新
        all_metadata[artifact_name] = metadata

        # 写回
        metadata_path.write_text(json.dumps(all_metadata, indent=2))

    def _load_metadata(self, loop_ref: str, artifact_name: str) -> Optional[dict]:
        """从 metadata.json 加载元数据"""
        metadata_path = self._get_metadata_path(loop_ref)

        if not metadata_path.exists():
            return None

        try:
            all_metadata = json.loads(metadata_path.read_text())
            return all_metadata.get(artifact_name)
        except:
            return None
