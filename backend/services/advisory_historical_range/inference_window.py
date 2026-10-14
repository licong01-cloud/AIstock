"""Read frozen sequence metadata through the existing native inference loader.

No training, prediction, market data, package admission or database access.
The node uses X-owned symlinks to the existing protected blobs, not copies.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import shlex
import subprocess

from backend.infra.wsl_qlib_runner import win_to_wsl_path
from backend.services.strategy_package.models import AlphaMode, ModelAsset, StrategyPackageManifest
from backend.services.strategy_package.package_asset_store import LocalPackageAssetStore
from backend.services.trading_core.errors import DataUnavailableError, RuntimeConfigInvalidError

_RESULT_PREFIX = "ADVISORY_FROZEN_SEQUENCE_METADATA="
_PROBE = """
import json, os, sys, tempfile
from pathlib import Path
from backend.inference_engine import load_model_from_pkl, _saved_qe_step_len
from backend.services.advisory_historical_range.inference_session import frozen_model_code_environment
bindings = json.loads(sys.argv[2])
with tempfile.TemporaryDirectory(prefix='frozen-window-', dir=sys.argv[1]) as directory:
    workspace = Path(directory)
    for name, source in bindings.items():
        target = workspace / name
        target.parent.mkdir(parents=True, exist_ok=True)
        os.symlink(source, target)
    with frozen_model_code_environment(workspace):
        _, kind, _, _ = load_model_from_pkl(workspace / 'params.pkl')
        length = _saved_qe_step_len(workspace, {'dataset_processor_relpath': 'dataset'}) if kind == 'pytorch' else 1
    print('ADVISORY_FROZEN_SEQUENCE_METADATA=' + json.dumps({'model_kind': kind, 'sequence_length': length}))
"""


class FrozenSequenceLengthReader:
    """Read the native primary seed's exact window lineage for each component."""

    def __init__(self, *, asset_store: LocalPackageAssetStore, repository_root: Path,
                 temporary_root: Path, timeout_seconds: int = 120) -> None:
        self.asset_store = asset_store
        self.repository_root = repository_root.resolve()
        self.temporary_root = temporary_root.resolve()
        if timeout_seconds <= 0:
            raise ValueError("frozen-window metadata requires a positive timeout")
        self.timeout_seconds = timeout_seconds

    def __call__(self, manifest: StrategyPackageManifest) -> dict[str, int]:
        if manifest.alpha_mode is AlphaMode.MULTI_ALPHA:
            from backend.services.strategy_package.multi_alpha_live import (
                _multi_alpha_evidence, _parent_leg_runtime_slices, _seed_leg_slices,
            )
            legs = _parent_leg_runtime_slices(
                manifest, evidence=_multi_alpha_evidence(manifest), package_id=manifest.package_id,
            )
            # Native seed aggregation exposes the first seed's per-leg window
            # and separately retains every seed's read receipts. Mirror that
            # existing contract; do not impose a new ensemble admission rule.
            models = {leg.leg_id: _seed_leg_slices(leg)[0].model_asset for leg in legs}
        else:
            model = manifest.model_asset
            if isinstance(model, list):
                if len(model) != 1:
                    raise RuntimeConfigInvalidError("single-alpha frozen model mapping is ambiguous")
                model = model[0]
            models = {manifest.alpha_components[0].alpha_id: model}
        return {component_id: self._read(model) for component_id, model in models.items()}

    def _read(self, model: ModelAsset) -> int:
        if model.preprocessor_asset is None:
            # The native loader uses step_len=1 when no fitted dataset is declared.
            return 1
        temporary = win_to_wsl_path(str(self.temporary_root))
        if not (temporary == "/mnt/x" or temporary.startswith("/mnt/x/")):
            raise ValueError("frozen-window metadata temporary root must be on X")
        processor = model.preprocessor_asset
        identities = [(model.asset_ref, model.sha256, model.size_bytes),
                      (processor.asset_ref, processor.sha256, processor.size_bytes)]
        bindings = {
            "params.pkl": self._asset_path(model.asset_ref, model.sha256, model.size_bytes),
            "dataset": self._asset_path(processor.asset_ref, processor.sha256, processor.size_bytes),
        }
        for asset in model.model_code_assets:
            relative = Path(asset.relative_path)
            if (relative.is_absolute() or relative.drive or ".." in relative.parts
                    or relative.as_posix() in {".", "params.pkl", "dataset"}
                    or relative.as_posix() in bindings):
                raise RuntimeConfigInvalidError("frozen model code path escapes its metadata workspace")
            bindings[relative.as_posix()] = self._asset_path(asset.asset_ref, asset.sha256, asset.size_bytes)
            identities.append((asset.asset_ref, asset.sha256, asset.size_bytes))
        self.temporary_root.mkdir(parents=True, exist_ok=True)
        conda_sh = os.getenv("QLIB_WSL_CONDA_SH", "~/miniconda3/etc/profile.d/conda.sh")
        conda_source = '$HOME/' + shlex.quote(conda_sh[2:]) if conda_sh.startswith("~/") else shlex.quote(conda_sh)
        arguments = ["python", "-c", _PROBE, temporary, json.dumps(bindings, sort_keys=True)]
        command = (
            f"source {conda_source} && conda activate {shlex.quote(os.getenv('QLIB_WSL_CONDA_ENV', 'rdagent-gpu'))} && "
            f"cd {shlex.quote(win_to_wsl_path(str(self.repository_root)))} && "
            f"CUDA_VISIBLE_DEVICES= OMP_NUM_THREADS=2 PYTHONDONTWRITEBYTECODE=1 "
            f"TMPDIR={shlex.quote(temporary)} TMP={shlex.quote(temporary)} TEMP={shlex.quote(temporary)} "
            + " ".join(shlex.quote(arg) for arg in arguments)
        )
        try:
            completed = subprocess.run(
                ["wsl", "-d", os.getenv("QLIB_WSL_DISTRO", "Ubuntu"), "bash", "-lc", command],
                capture_output=True, text=True, encoding="utf-8", errors="replace", check=False,
                timeout=self.timeout_seconds, creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise DataUnavailableError("frozen model sequence metadata is temporarily unavailable") from exc
        lines = [line[len(_RESULT_PREFIX):] for line in completed.stdout.splitlines() if line.startswith(_RESULT_PREFIX)]
        if completed.returncode != 0 or len(lines) != 1:
            raise DataUnavailableError(
                "native frozen sequence metadata read did not complete",
                context={"model_id": model.model_id, "returncode": completed.returncode},
            )
        try:
            payload = json.loads(lines[0])
            length = payload["sequence_length"]
            if isinstance(length, bool) or not isinstance(length, int) or length < 1:
                raise ValueError("invalid sequence length")
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeConfigInvalidError("native frozen sequence metadata is invalid") from exc
        for uri, sha, size in identities:
            self.asset_store.verify(uri, sha256=sha, size_bytes=size)
        return length

    def _asset_path(self, uri: str | None, sha: str | None, size: int | None) -> str:
        if not uri or not sha or size is None:
            raise RuntimeConfigInvalidError("frozen sequence metadata requires the existing declared asset identity")
        self.asset_store.verify(uri, sha256=sha, size_bytes=size)
        # Resolve the already-verified blob; no materialize_file/copy/export.
        return win_to_wsl_path(str(self.asset_store._path_from_uri(uri).resolve(strict=True)))
