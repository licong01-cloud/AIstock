"""Persist QE sector-risk runtime evidence into the authoritative Qlib Recorder."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path


def _strategy_kwargs(config):
    def collect_matches(value):
        matches = []

        def visit(nested_value):
            if isinstance(nested_value, dict):
                class_name = str(nested_value.get("class") or "")
                kwargs = nested_value.get("kwargs")
                if class_name.startswith("QESectorRiskOverlay") and isinstance(kwargs, dict):
                    matches.append(kwargs)
                for child in nested_value.values():
                    visit(child)
            elif isinstance(nested_value, list):
                for child in nested_value:
                    visit(child)

        visit(value)
        return matches

    task = config.get("task") if isinstance(config, dict) else None
    records = task.get("record") if isinstance(task, dict) else None
    if isinstance(records, dict):
        records = [records]
    executable_matches = collect_matches(records) if isinstance(records, list) else []

    # Qlib configs commonly define ``port_analysis_config`` once as a YAML
    # anchor and reference it from ``task.record[].kwargs.config``.  A global
    # recursive scan sees both object paths, even though only the record path
    # is executable.  Prefer that authoritative execution subtree.  The
    # fallback preserves compatibility with older direct strategy configs.
    matches = executable_matches or collect_matches(config)

    if len(matches) > 1:
        raise RuntimeError(
            f"QE sector-risk config contains multiple executable overlay strategies: {len(matches)}"
        )
    return matches[0] if matches else None


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def persist_sector_risk_overlay_artifacts(recorder, config):
    """Save normalized actions, manifest, and a deterministic receipt as Recorder objects."""
    kwargs = _strategy_kwargs(config)
    if kwargs is None or not bool(kwargs.get("sector_risk_overlay_enabled", False)):
        return None

    action_path = Path(str(kwargs.get("sector_risk_overlay_action_log") or "")).resolve()
    manifest_path = Path(str(kwargs.get("sector_risk_overlay_manifest_file") or "")).resolve()
    data_path = Path(str(kwargs.get("sector_risk_overlay_data_file") or "")).resolve()
    missing = [str(path) for path in (action_path, manifest_path, data_path) if not path.is_file()]
    if missing:
        raise RuntimeError(f"QE sector-risk Recorder persistence missing files: {missing}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    actions = []
    for line_no, raw_line in enumerate(action_path.read_text(encoding="utf-8").splitlines(), start=1):
        if not raw_line.strip():
            continue
        try:
            action = json.loads(raw_line)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"QE sector-risk action ledger contains invalid JSON at line {line_no}"
            ) from exc
        if not isinstance(action, dict):
            raise RuntimeError(
                f"QE sector-risk action ledger line {line_no} must be a JSON object"
            )
        actions.append(action)

    identities = [
        (
            str(item.get("trade_date")),
            str(item.get("instrument")),
            str(item.get("action_type")),
            str(item.get("policy_hash")),
        )
        for item in actions
    ]
    if len(set(identities)) != len(identities):
        raise RuntimeError("QE sector-risk action ledger contains duplicate action identities")

    receipt = {
        "schema_version": "qe_sector_risk_overlay_recorder_receipt_v1",
        "mode": str(kwargs.get("sector_risk_overlay_mode")),
        "dataset_identity": str(manifest.get("dataset_identity") or ""),
        "manifest_payload_sha256": str(manifest.get("manifest_payload_sha256") or ""),
        "runtime_sha256": _sha256(data_path),
        "action_log_sha256": _sha256(action_path),
        "action_count": len(actions),
        "action_type_counts": {
            action_type: sum(1 for item in actions if str(item.get("action_type")) == action_type)
            for action_type in sorted({str(item.get("action_type")) for item in actions})
        },
    }
    recorder.save_objects(
        **{
            "qe_sector_risk_overlay_manifest.pkl": manifest,
            "qe_sector_risk_overlay_actions.pkl": actions,
            "qe_sector_risk_overlay_receipt.pkl": receipt,
        }
    )
    return receipt
