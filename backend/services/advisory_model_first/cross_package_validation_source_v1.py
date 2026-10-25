"""Consumer-only historical signal preparation, with X-only temporary state."""
from __future__ import annotations

from datetime import date, datetime, timezone
import json
import math
from pathlib import Path
import re
import time

from backend.services.advisory_model_first.cross_package_validation_contracts_v1 import (
    SCHEMA, canonical_sha, file_sha, publish_json, readonly_connection, real_root,
)


class NoArtifactWrites:
    def save(self, *_args, **_kwargs):
        raise ValueError("cross-package consumer forbids artifact repository writes")


class ReadonlyCopyAssetConsumer:
    """Public store adapter: immutable bytes into X, no cross-drive hardlinks."""
    def __init__(self, store, *, temp_root):
        self.store = store
        self.temp_root = real_root(temp_root, required_drive="X:")

    def get(self, uri):
        return self.store.get(uri)

    def exists(self, uri):
        return self.store.exists(uri)

    def verify(self, uri, *, sha256, size_bytes):
        return self.store.verify(uri, sha256=sha256, size_bytes=size_bytes)

    def materialize_file(self, uri, target, *, sha256, size_bytes):
        target = Path(target)
        if self.temp_root not in target.resolve().parents:
            raise ValueError("asset consumer target escapes X temporary root")
        self.verify(uri, sha256=sha256, size_bytes=size_bytes)
        payload = self.get(uri)
        if len(payload) != size_bytes:
            raise ValueError("asset consumer source size differs")
        import hashlib
        if hashlib.sha256(payload).hexdigest() != sha256:
            raise ValueError("asset consumer source hash differs")
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("xb") as stream:
            stream.write(payload)
        if file_sha(target) != sha256 or target.stat().st_size != size_bytes:
            raise ValueError("asset consumer X readback differs")


def make_readonly_signal_service(*, temp_root, repo_root, local_cpu_only=False):
    from backend.services.strategy_package.live_inference import (
        QEExperimentRuntimeAssetResolver, WslStrategyPackageInferenceProvider, win_to_wsl_path,
    )
    from backend.services.strategy_package.selection_artifact import StrategyPackageSelectionArtifactService
    from backend.services.strategy_package.package_asset_store import LocalPackageAssetStore

    root = real_root(temp_root, required_drive="X:")
    root.mkdir(parents=True, exist_ok=True)

    class XTemporaryProvider(WslStrategyPackageInferenceProvider):
        def _build_env_exports(self, *, historical_read_only=False):
            if not historical_read_only:
                raise ValueError("cross-package provider is readonly only")
            return super()._build_env_exports(historical_read_only=True) + (
                f" TMPDIR={self._quote(win_to_wsl_path(str(root)))} TEMP={self._quote(win_to_wsl_path(str(root)))}"
                f" TMP={self._quote(win_to_wsl_path(str(root)))} OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1"
                + (" CUDA_VISIBLE_DEVICES=''" if local_cpu_only else "")
            )

    assets = ReadonlyCopyAssetConsumer(LocalPackageAssetStore(), temp_root=root)
    resolver = QEExperimentRuntimeAssetResolver(conn_factory=readonly_connection, cache_root=root/"package-runtime", asset_store=assets)
    provider = XTemporaryProvider(repo_root=repo_root, safe_artifact_roots=(root,), timeout_seconds=3600)
    service = StrategyPackageSelectionArtifactService(conn_factory=readonly_connection,
        artifact_repository=NoArtifactWrites(), runtime_asset_resolver=resolver, live_inference_provider=provider)
    service._advisory_local_cpu_only = local_cpu_only
    return service


def local_cpu_resource_observation(*, api_base, temp_root, distro="Ubuntu"):
    """Public metadata and local capacity only; no scheduler or process control."""
    import ipaddress
    import shutil
    import subprocess
    from urllib.parse import urlparse
    import psutil
    from backend.mcp.common import AIstockApiClient
    from backend.services.advisory_model_first.generic_population_price_5td_cli_v1 import public_qe_observation_v1
    api = AIstockApiClient(api_base, timeout=8, max_response_bytes=1048576)
    observation = public_qe_observation_v1(api_base, get=api.get)
    local_hosts = {"localhost", "127.0.0.1", "::1"}
    for rows in psutil.net_if_addrs().values():
        local_hosts.update(row.address for row in rows if row.family.name in {"AF_INET", "AF_INET6"})
    nodes = []
    counts = dict(observation["active_counts"])
    disjoint = counts["single"] == 0 and counts["custom_evo"] < 10 and counts["multi_alpha"] < 10
    tasks, node_ids = [], set()
    if disjoint and counts["custom_evo"]:
        for status in ("running", "pending"):
            result = api.get("/quantevolver/evolution/tasks", params=dict(status=status, detail="summary", limit=10))
            if result.get("status") != "success" or not isinstance(result.get("data"), list):
                raise ValueError("local CPU observation has no complete QE node list")
            if len(result["data"]) >= 10:
                raise ValueError("local CPU QE node list may be truncated")
            tasks.extend(result["data"])
        # These two list APIs report returned row counts, not total counts.
        # The limit=1 observation is only a busy hint; the bounded full list is
        # authoritative for resource proof, and saturation is never complete.
        counts["custom_evo"] = len(tasks)
        disjoint = len({task["task_id"] for task in tasks}) == len(tasks)
        disjoint &= all(isinstance(task.get("node_id"), str) and task["node_id"] for task in tasks)
        node_ids.update(task["node_id"] for task in tasks if isinstance(task.get("node_id"), str) and task["node_id"])
    if counts["single"] == 0 and counts["multi_alpha"] < 10 and counts["multi_alpha"]:
        runs = []
        for status in ("running", "pending"):
            result = api.get("/multi-alpha/combine-backtest/runs", params=dict(status=status, limit=10))
            data = result.get("data", {})
            if (result.get("status") != "success" or not isinstance(data.get("runs"), list)
                    or data.get("count") != len(data["runs"]) or len(data["runs"]) >= 10):
                raise ValueError("local CPU observation has no complete multi-alpha node list")
            runs.extend(data["runs"])
        counts["multi_alpha"] = len(runs)
        disjoint &= len({run["id"] for run in runs}) == len(runs)
        for run in runs:
            config = run.get("backtest_config_json", {})
            dataset = run.get("execution_identity_json", {}).get("dataset", {})
            targets = set(run.get("node_parallelism_json", {})) | set(config.get("node_parallelism", {}))
            targets.update(value for value in (config.get("node_id"), dataset.get("resolved_node_id")) if value)
            disjoint &= bool(targets) and all(isinstance(target, str) and target for target in targets)
            node_ids.update(target for target in targets if isinstance(target, str) and target)
    if node_ids:
        for node_id in sorted(node_ids):
            node = api.get("/dispatch/nodes/"+node_id)
            host = urlparse(node.get("api_base_url", "")).hostname
            # No DNS assumptions: unrecognized/hostname-only nodes stay shared.
            try:
                remote = not ipaddress.ip_address(host).is_loopback and host not in local_hosts
            except (TypeError, ValueError):
                remote = False
            nodes.append(dict(node_id=node_id, disjoint_host=remote))
            disjoint &= remote
    free = subprocess.run(["wsl", "-d", distro, "--", "free", "-b"],capture_output=True,text=True,check=True,timeout=15)
    memory_line = next(line for line in free.stdout.splitlines() if line.startswith("Mem:"))
    wsl_available = int(memory_line.split()[-1])
    windows_available, x_available = psutil.virtual_memory().available, shutil.disk_usage(real_root(temp_root, required_drive="X:")).free
    observation.update(active_counts=counts,readonly_local_cpu_safe=bool(disjoint and windows_available>=16*1024**3
        and wsl_available>=8*1024**3 and x_available>=2*1024**3), consumer_runtime="LOCAL_WSL_CPU_1_NO_GPU",
        qe_nodes=nodes, windows_available_bytes=windows_available, wsl_available_bytes=wsl_available,
        temp_available_bytes=x_available, physical_fit_count=0)
    return observation


def validate_signal_artifact(artifact, *, package_id, manifest_sha256, decision_date):
    packet = artifact.model_dump(mode="json") if hasattr(artifact, "model_dump") else dict(artifact)
    if (packet["package_id"] != package_id or packet["manifest_sha256"] != manifest_sha256
            or packet["trade_date"] != decision_date.isoformat() or packet["data_source"] != "DB_HISTORICAL"
            or packet["metadata"].get("score_trade_date") != decision_date.isoformat()
            or packet["metadata"].get("cutoff_date") != decision_date.isoformat()):
        raise ValueError("prepared package / manifest / D cutoff differs")
    rows = packet["scores_json"]
    if packet["score_count"] != len(rows) or packet["universe_count"] < len(rows):
        raise ValueError("prepared raw score count differs from universe")
    symbols = [row["symbol"] for row in rows]
    if (len(set(symbols)) != len(symbols) or any(re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", value) is None for value in symbols)
            or [row["rank"] for row in rows] != list(range(1, len(rows)+1))
            or any(isinstance(row["score"], bool) or not math.isfinite(float(row["score"])) for row in rows)):
        raise ValueError("prepared signal keys / rank / scores differ")
    if rows != sorted(rows, key=lambda row: (-float(row["score"]), row["symbol"])):
        raise ValueError("prepared signal order differs")
    return packet


def prepare_package_dates(*, service, package, decision_dates, output_root, observe_resources, progress=None):
    root = real_root(output_root)
    packages_id = package["package_id"]
    dates = list(decision_dates)
    if dates != sorted(set(dates)) or any(not isinstance(d, date) for d in dates):
        raise ValueError("prepared original dates must be unique and ordered")
    results = []
    config = dict(runtime_profile=dict(selection=dict(top_k=50)),
                  selection_artifact_config=dict(inference_backend="wsl"))
    for d in dates:
        target = root/packages_id/f"{d.isoformat()}.json"
        if target.exists():
            checkpoint = json.loads(target.read_text(encoding="utf-8"))
            if (checkpoint["package_id"] != packages_id or checkpoint["decision_date"] != d.isoformat()
                    or checkpoint["manifest_sha256"] != package["manifest_sha256"]):
                raise ValueError("checkpoint identity differs")
            if checkpoint["status"] == "PREPARED":
                if (checkpoint["artifact_payload_sha256"] != canonical_sha(checkpoint["artifact"])
                        or checkpoint["candidate_view_sha256"] != canonical_sha(checkpoint["candidate_view"])
                        or checkpoint["candidate_view"] != checkpoint["artifact"]["scores_json"][:50]):
                    raise ValueError("checkpoint original scores/candidate projection changed")
                validate_signal_artifact(checkpoint["artifact"], package_id=packages_id,
                    manifest_sha256=package["manifest_sha256"], decision_date=d)
                results.append(checkpoint)
                continue
            # Failed exact attempts stay immutable; caller must explicitly resolve them.
            results.append(checkpoint)
            continue
        resources = observe_resources()
        local_proof = "readonly_local_cpu_safe" in resources
        if (local_proof and resources["readonly_local_cpu_safe"] is not True
                or not local_proof and any(resources["active_counts"].values())):
            if progress:
                progress(dict(event="WAITING_SHARED_INFERENCE_RESOURCE", package_id=packages_id, decision_date=d.isoformat(), resources=resources))
            break
        if local_proof and not getattr(service, "_advisory_local_cpu_only", False):
            raise ValueError("local CPU capacity proof requires a CPU-only consumer provider")
        began = time.monotonic()
        checkpoint = dict(schema_version=SCHEMA, package_id=packages_id, manifest_sha256=package["manifest_sha256"],
            decision_date=d.isoformat(), candidate_top_k=50, portfolio_topk=package.get("portfolio_topk"),
            source_evidence="CURRENT_DATABASE_NON_VINTAGE", historical_original_receipt=False,
            captured_at=datetime.now(timezone.utc).isoformat(), resource_observation=resources, physical_fit_count=0,
            database_written=False, sealed_read=False, outcomes_read=False)
        try:
            artifacts = service.prepare_from_live_inference_dates(package_id=packages_id, trade_dates=[d],
                cutoff_date=d, data_source="DB_HISTORICAL", runtime_config=config,
                include_reference_price=False, historical_read_only=True)
            if len(artifacts) != 1:
                raise ValueError("public prepare did not return exactly the original D")
            packet = validate_signal_artifact(artifacts[0], package_id=packages_id,
                manifest_sha256=package["manifest_sha256"], decision_date=d)
            candidate_view = packet["scores_json"][:50]
            checkpoint.update(status="PREPARED", artifact=packet, artifact_payload_sha256=canonical_sha(packet),
                candidate_view=candidate_view, candidate_view_sha256=canonical_sha(candidate_view))
        except Exception as exc:
            context = getattr(exc, "context", {})
            checkpoint.update(status="INPUT_UNAVAILABLE", error_type=type(exc).__name__,
                reason_code=context.get("reason_code", getattr(exc, "reason_code", "CONSUMER_PREPARE_FAILED")),
                detail={key: context[key] for key in ("missing_fields", "missing_source_roles", "trade_date", "cutoff_date",
                    "inference_backend", "factor_names", "symbol", "file", "workspace_path") if key in context})
            if isinstance(exc, OSError):
                checkpoint["os_error"] = dict(errno=exc.errno, winerror=getattr(exc, "winerror", None), filename=exc.filename)
        checkpoint["elapsed_seconds"] = time.monotonic()-began
        publish_json(target, checkpoint)
        results.append(checkpoint)
        if progress:
            progress(dict(event="SOURCE_DATE_FINISHED", package_id=packages_id, decision_date=d.isoformat(),
                status=checkpoint["status"], elapsed_seconds=checkpoint["elapsed_seconds"],
                reason_code=checkpoint.get("reason_code"), score_count=len(checkpoint.get("artifact", {}).get("scores_json", []))))
    return results


def frozen_prediction_metadata(*, api, package):
    """Reuse an approved package's public original QE score artifact, no Archive gate."""
    run_id = package.get("run_id")
    if not run_id:
        return dict(package_id=package["package_id"], status="INPUT_UNAVAILABLE", reason="PACKAGE_HAS_NO_RUN_ID")
    result = api.get(f"/prediction-store/pointers/{run_id}")
    data = result.get("data", {})
    manifest = data.get("prediction_store_manifest")
    if not isinstance(manifest, dict):
        return dict(package_id=package["package_id"], run_id=run_id, status="INPUT_UNAVAILABLE",
                    reason="PUBLIC_PREDICTION_MANIFEST_UNAVAILABLE", pointer_status=data.get("pointer_status"))
    if manifest.get("run_key") != run_id:
        raise ValueError("public frozen prediction run differs from approved package")
    descriptors = [a for a in manifest.get("artifacts", []) if a.get("artifact_type") == "prediction"]
    if len(descriptors) != 1:
        raise ValueError("public frozen prediction descriptor not unique")
    descriptor = descriptors[0]
    metadata = manifest.get("metadata", {})
    if metadata.get("task_id") and metadata["task_id"] != package["source_id"]:
        raise ValueError("public frozen prediction task differs from approved package")
    if type(descriptor["size_bytes"]) is not int or not 0 < descriptor["size_bytes"] <= 128*1024*1024:
        raise ValueError("frozen prediction size exceeds consumer budget")
    return dict(package_id=package["package_id"], manifest_sha256=package["manifest_sha256"], run_id=run_id,
        status="FROZEN_SCORE_AVAILABLE", pointer_status=data.get("pointer_status"), descriptor=descriptor,
        prediction_manifest_sha256=canonical_sha(manifest), source_task=metadata.get("task_id"),
        source_loop=metadata.get("loop_id"), source_node=metadata.get("source_node_id"),
        original_source="QE_FROZEN_BACKTEST_SCORE", returns_read=False, database_written=False, new_fit_count=0)


def read_current_canary_view(*, package, source_root, decision_dates):
    """Current read-only inference is a new source, never an old native capture."""
    import pandas as pd
    dates = list(decision_dates)
    if dates != sorted(set(dates)) or any(type(d) is not date for d in dates):
        raise ValueError("current source must keep the full original ordered D axis")
    rows, checkpoints, missing = [], [], []
    for d in dates:
        path = Path(source_root)/package["package_id"]/(d.isoformat()+".json")
        if not path.is_file():
            missing.append(d.isoformat())
            continue
        body = json.loads(path.read_text(encoding="utf-8"))
        if (body["package_id"] != package["package_id"] or body["manifest_sha256"] != package["manifest_sha256"]
                or body["decision_date"] != d.isoformat() or body["candidate_top_k"] != 50
                or body["source_evidence"] != "CURRENT_DATABASE_NON_VINTAGE" or body["sealed_read"] is not False):
            raise ValueError("current canary package/clock checkpoint changed")
        if body["status"] != "PREPARED":
            missing.append(d.isoformat())
            checkpoints.append(dict(path=str(path),sha256=file_sha(path),status=body["status"],reason=body.get("reason_code")))
            continue
        packet = validate_signal_artifact(body["artifact"],package_id=package["package_id"],manifest_sha256=package["manifest_sha256"],decision_date=d)
        if (body["artifact_payload_sha256"] != canonical_sha(packet) or body["candidate_view_sha256"] != canonical_sha(body["candidate_view"])
                or body["candidate_view"] != packet["scores_json"][:50] or body["physical_fit_count"] != 0
                or body["database_written"] is not False or body["outcomes_read"] is not False
                or body["historical_original_receipt"] is not False):
            raise ValueError("current canary score/provenance checkpoint changed")
        for row in body["candidate_view"]:
            rows.append(dict(decision_date=d,instrument=row["symbol"],score=row["score"],rank=row["rank"]))
        if not body["candidate_view"]:
            missing.append(d.isoformat())  # Retain empty source evidence, never manufacture five cash slots.
        checkpoints.append(dict(path=str(path),sha256=file_sha(path),status="PREPARED",raw_score_count=packet["score_count"]))
    frame = pd.DataFrame(rows,columns=["decision_date","instrument","score","rank"])
    descriptor = dict(artifact_type="CONSUMER_READONLY_CURRENT_SCORE_VIEW",checkpoints=checkpoints)
    receipt = dict(package_id=package["package_id"],manifest_sha256=package["manifest_sha256"],run_id=package.get("run_id"),
        descriptor=descriptor,candidate_top_k=50,declared_decision_days=len(dates),original_roster_rows=len(frame),
        known_decision_days=int(frame.decision_date.nunique()),missing_decision_dates=missing,
        original_source="CURRENT_DATABASE_NON_VINTAGE_READONLY_CANARY_NOT_HISTORICAL_CAPTURE",
        historical_original_receipt=False,native_receipt_created=False,database_written=False,outcomes_read=False,physical_fit_count=0)
    return frame,receipt


def read_frozen_prediction_view(*, api, source, decision_dates):
    """Read owned hash-bound scores only; never label/return artifacts."""
    import pandas as pd
    from backend.services.advisory_model_first.cross_package_validation_contracts_v1 import file_sha
    if source["status"] != "FROZEN_SCORE_AVAILABLE":
        raise ValueError("frozen package score source unavailable")
    result = api.get(f"/prediction-store/pred/{source['run_id']}", params=dict(head=0))
    body = result["data"]
    descriptor = source["descriptor"]
    path = Path(body["artifact_path"])
    if (not path.is_absolute() or path.resolve() != path or path.drive.upper() == "C:"
            or path.stat().st_size != descriptor["size_bytes"] or file_sha(path) != descriptor["sha256"]
            or body.get("run_id") != source["run_id"] or body.get("artifact_type") != "prediction"):
        raise ValueError("public owned prediction file identity differs")
    frame = pd.read_pickle(path)  # Only explicitly approved AIstock-owned, SHA-bound DataFrame artifacts.
    if isinstance(frame, pd.Series):
        frame = frame.to_frame(name="score")
    if (not isinstance(frame, pd.DataFrame) or not isinstance(frame.index, pd.MultiIndex)
            or frame.index.names != ["datetime", "instrument"] or len(frame) > 5000000
            or list(frame.columns) not in [["score"], [0]]):
        raise ValueError("frozen prediction is not the original score-only schema")
    frame.columns = ["score"]
    rows = frame.reset_index()
    rows["decision_date"] = pd.to_datetime(rows.datetime).dt.date
    dates = list(decision_dates)
    if dates != sorted(set(dates)):
        raise ValueError("frozen source declared original dates differ")
    selected = rows.loc[rows.decision_date.isin(dates), ["decision_date", "instrument", "score"]].copy()
    if (selected.duplicated(["decision_date", "instrument"]).any()
            or not selected.instrument.str.fullmatch(r"\d{6}\.(SH|SZ|BJ)").all()
            or not selected.score.map(lambda value: math.isfinite(float(value))).all()):
        raise ValueError("original frozen scores contain invalid / duplicate keys")
    selected = selected.sort_values(["decision_date", "score", "instrument"], ascending=[True, False, True], kind="stable")
    selected["rank"] = selected.groupby("decision_date", sort=False).cumcount()+1
    raw_counts = selected.groupby("decision_date", sort=False).size().to_dict()
    selected = selected.loc[selected["rank"].le(50)].copy()
    if file_sha(path) != descriptor["sha256"]:
        raise ValueError("frozen score file changed while reading")
    receipt = {**source, "declared_decision_days": len(dates), "known_decision_days": len(raw_counts),
        "missing_decision_dates": [d.isoformat() for d in dates if d not in raw_counts],
        "raw_scored_counts": {d.isoformat(): int(n) for d, n in raw_counts.items()},
        "original_roster_rows": len(selected), "candidate_top_k": 50, "portfolio_topk_is_not_signal_limit": True,
        "sealed_returns_read": False, "historical_original_receipt": False}
    return selected.reset_index(drop=True), receipt


def selection_source_catalog(*, packages, decision_dates, calendar):
    """Metadata only: Selection artifact date is T, score metadata supplies D."""
    from backend.services.advisory_model_first.cross_package_validation_contracts_v1 import readonly_connection
    import psycopg2.extras
    expected = {str(d): str(calendar[calendar.index(d)+1]) for d in decision_dates}
    identities = {p["package_id"]: p for p in packages}
    with readonly_connection() as connection:
        with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute("""SELECT artifact_id,package_id,manifest_sha256,trade_date,data_source,
                runtime_config_hash,artifact_sha256,score_count,universe_count,status,metadata,created_at
                FROM strategy_pkg.selection_score_artifact WHERE package_id=ANY(%s)
                AND trade_date BETWEEN %s AND %s ORDER BY package_id,trade_date,created_at,artifact_id LIMIT 4097""",
                (list(identities), min(expected.values()), max(expected.values())))
            rows = [dict(row) for row in cursor.fetchall()]
    if len(rows) >= 4097:
        raise ValueError("bounded Selection source catalog may be truncated")
    groups, rejected = {}, []
    for original in rows:
        row = {**original, "trade_date": str(original["trade_date"]), "created_at": original["created_at"].isoformat()}
        metadata = row["metadata"]
        d = metadata.get("score_trade_date")
        row["decision_date"] = d
        if row["manifest_sha256"] != identities[row["package_id"]]["manifest_sha256"]:
            rejected.append({**row, "reason": "OTHER_PACKAGE_MANIFEST_VERSION"})
        elif (d not in expected or row["trade_date"] != expected[d] or metadata.get("cutoff_date") != d
                or row["status"] != "SUCCEEDED" or row["data_source"] != "DB_HISTORICAL"):
            rejected.append({**row, "reason": "NO_MATCHING_FROZEN_D_T_SOURCE_ON_DECLARED_AXIS"})
        else:
            groups.setdefault((row["package_id"], d), []).append(row)
    selected, variants = [], []
    for (package_id, day), members in sorted(groups.items()):
        ordered = sorted(members, key=lambda r: (-r["score_count"], r["created_at"], r["artifact_id"]))
        selected.append(ordered[0])
        variants.append(dict(package_id=package_id, decision_date=day, selected_artifact_id=ordered[0]["artifact_id"],
            all_versions=[r["artifact_id"] for r in ordered]))
    return dict(selected=selected, variants=variants, rejected=rejected,
        selection_rule="MAX_EXISTING_SCORE_COUNT_THEN_EARLIEST_CREATED_AT_THEN_ARTIFACT_ID_TOP50_VIEW_ONLY",
        declared_decision_dates=[str(d) for d in decision_dates], outcomes_read=False, database_written=False)


def read_selection_source_view(*, package, selected, decision_dates, repository):
    """Original stored ordering/legacy provenance, not regenerated Selection."""
    import pandas as pd
    from datetime import date
    from backend.services.strategy_package.runtime_variant import canonical_json_sha256
    rows, bindings = [], []
    for metadata in selected:
        if metadata["package_id"] != package["package_id"] or metadata["manifest_sha256"] != package["manifest_sha256"]:
            raise ValueError("frozen Selection source package identity differs")
        artifact = repository.get(package_id=package["package_id"], manifest_sha256=package["manifest_sha256"],
            trade_date=date.fromisoformat(metadata["trade_date"]), data_source=metadata["data_source"],
            runtime_config_hash=metadata["runtime_config_hash"])
        scores = artifact.scores_json
        if (artifact.artifact_id != metadata["artifact_id"] or artifact.artifact_sha256 != metadata["artifact_sha256"]
                or canonical_json_sha256(scores) != metadata["artifact_sha256"]
                or artifact.score_count != len(scores) or artifact.score_count != metadata["score_count"]
                or artifact.metadata.get("score_trade_date") != metadata["decision_date"]
                or artifact.metadata.get("cutoff_date") != metadata["decision_date"]):
            raise ValueError("frozen Selection original scores/D provenance changed")
        symbols = set()
        for position, score in enumerate(scores, 1):
            if (type(score["rank"]) is not int or score["rank"] != position or score["symbol"] in symbols
                    or not isinstance(score["score"], (int, float)) or isinstance(score["score"], bool)
                    or not math.isfinite(score["score"])):
                raise ValueError("frozen Selection rank/score/unique candidate differs")
            symbols.add(score["symbol"])
            if position <= 50:
                rows.append(dict(decision_date=date.fromisoformat(metadata["decision_date"]),
                    instrument=score["symbol"], score=float(score["score"]), rank=position))
        bindings.append(dict(artifact_id=artifact.artifact_id, artifact_sha256=artifact.artifact_sha256,
            execution_date=str(artifact.trade_date), decision_date=metadata["decision_date"],
            original_score_count=len(scores), runtime_config_hash=artifact.runtime_config_hash))
    frame = pd.DataFrame(rows, columns=["decision_date", "instrument", "score", "rank"])
    if not frame.empty and not frame.instrument.str.fullmatch(r"\d{6}\.(SH|SZ|BJ)").all():
        raise ValueError("frozen Selection original instrument differs")
    known = sorted(set(frame.decision_date))
    receipt = dict(package_id=package["package_id"], manifest_sha256=package["manifest_sha256"],
        run_id=package.get("run_id"), declared_decision_days=len(decision_dates), known_decision_days=len(known),
        missing_decision_dates=[str(d) for d in decision_dates if d not in known], original_roster_rows=len(frame),
        candidate_top_k=50, original_source="FROZEN_SELECTION_SCORE_ARTIFACT", native_receipt_created=False,
        original_source_bindings=bindings, original_N_preserved=True, physical_fit_count=0, outcomes_read=False)
    return frame, receipt
