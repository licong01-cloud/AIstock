from __future__ import annotations

from io import BytesIO
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
from typing import Any, Mapping

import pytest

from backend.services.dataset_release.canonical import canonical_json_bytes
from backend.services.dataset_release.monthly_immutable_deploy import (
    ImmutableStreamingNodeTransport,
    MonthlyImmutableDeployError,
    ReleaseFile,
)
from backend.services.dataset_release.monthly_remote_deploy import (
    MonthlyRemoteDeployError,
    REMOTE_DEPLOY_REQUEST_SCHEMA,
    REMOTE_DEPLOY_RESULT_SCHEMA,
    deploy,
    readback,
)
from backend.services.dataset_release.monthly_worker import ProducerContext


MANIFEST = "a" * 64


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _files(tmp_path: Path) -> tuple[ReleaseFile, ...]:
    source = tmp_path / "source"
    source.mkdir()
    data = source / "data.bin"
    data.write_bytes(b"shared-data")
    alias = source / "alias.bin"
    os.link(data, alias)
    manifest = source / "qe_dataset_manifest.json"
    manifest.write_bytes(
        canonical_json_bytes({"dataset_manifest_sha256": MANIFEST}) + b"\n"
    )
    return (
        ReleaseFile("data.bin", data, _sha(data), data.stat().st_size),
        ReleaseFile("alias.bin", alias, _sha(alias), alias.stat().st_size, "data.bin"),
        ReleaseFile(
            "qe_dataset_manifest.json",
            manifest,
            _sha(manifest),
            manifest.stat().st_size,
        ),
    )


def _request(tmp_path: Path, files: tuple[ReleaseFile, ...]) -> dict[str, Any]:
    parent = tmp_path / "releases"
    parent.mkdir()
    return {
        "schema_version": REMOTE_DEPLOY_REQUEST_SCHEMA,
        "operation_id": "dmr_" + "1" * 32,
        "attempt": 1,
        "node_id": "rdagent-node1",
        "allowed_parent": str(parent.resolve()),
        "candidate_root": str((parent / "candidate").resolve()),
        "dataset_manifest_sha256": MANIFEST,
        "files": [
            {
                "path": item.relative_path,
                "sha256": item.sha256,
                "size": item.size,
                "hardlink_source": item.hardlink_source,
            }
            for item in files
        ],
    }


def _archive(files: tuple[ReleaseFile, ...]) -> BytesIO:
    stream = BytesIO()
    with tarfile.open(fileobj=stream, mode="w") as archive:
        for item in files:
            if item.hardlink_source is not None:
                continue
            info = tarfile.TarInfo(item.relative_path)
            info.size = item.size
            with item.source_path.open("rb") as handle:
                archive.addfile(info, handle)
    stream.seek(0)
    return stream


def test_remote_endpoint_streams_verifies_and_resumes_exact_tree(tmp_path: Path) -> None:
    source_files = _files(tmp_path)
    files = (source_files[1], source_files[0], source_files[2])
    request = _request(tmp_path, files)

    before = readback(request)
    result = deploy(request, _archive(files))
    resumed = readback(request)

    target = Path(request["candidate_root"])
    assert before["status"] == "ABSENT"
    assert result["status"] == resumed["status"] == "PASS"
    assert result["bytes_transferred"] == sum(
        item.size for item in files if item.hardlink_source is None
    )
    assert resumed["bytes_transferred"] == 0
    assert os.path.samefile(target / "data.bin", target / "alias.bin")
    assert [item["path"] for item in result["files"]] == [
        item.relative_path for item in files
    ]


def test_remote_endpoint_rejects_existing_drift_and_archive_mismatch(tmp_path: Path) -> None:
    files = _files(tmp_path)
    request = _request(tmp_path, files)
    target = Path(request["candidate_root"])
    target.mkdir()
    (target / "unexpected").write_bytes(b"x")
    with pytest.raises(MonthlyRemoteDeployError, match="file set differs"):
        readback(request)

    target.rename(target.with_name("drift"))
    stream = BytesIO()
    with tarfile.open(fileobj=stream, mode="w") as archive:
        info = tarfile.TarInfo("unexpected")
        info.size = 1
        archive.addfile(info, BytesIO(b"x"))
    stream.seek(0)
    with pytest.raises(MonthlyRemoteDeployError, match="archive differs"):
        deploy(request, stream)
    assert not Path(request["candidate_root"]).exists()
    assert not list(Path(request["allowed_parent"]).glob("*.deploying"))


def test_remote_endpoint_rejects_copied_bytes_in_place_of_required_hardlink(
    tmp_path: Path,
) -> None:
    files = _files(tmp_path)
    request = _request(tmp_path, files)
    deploy(request, _archive(files))
    target = Path(request["candidate_root"])
    alias = target / "alias.bin"
    payload = alias.read_bytes()
    alias.unlink()
    alias.write_bytes(payload)

    with pytest.raises(MonthlyRemoteDeployError, match="hardlink topology differs"):
        readback(request)


def _result(request: Mapping[str, Any], *, status: str, files: list[dict[str, Any]], transferred: int):
    value = {
        "schema_version": REMOTE_DEPLOY_RESULT_SCHEMA,
        "status": status,
        "node_id": request["node_id"],
        "candidate_root": request["candidate_root"],
        "dataset_manifest_sha256": request["dataset_manifest_sha256"],
        "request_sha256": hashlib.sha256(canonical_json_bytes(request)).hexdigest(),
        "files": files,
        "bytes_transferred": transferred,
    }
    return canonical_json_bytes(value) + b"\n"


def test_streaming_transport_probes_then_deploys_without_shell(tmp_path: Path) -> None:
    files = _files(tmp_path)
    parent = "/home/lc999/data/releases"
    target = parent + "/candidate"
    observed: dict[str, Any] = {}

    def command_runner(command, *, payload, timeout_seconds):  # type: ignore[no-untyped-def]
        request = json.loads(payload.decode("utf-8"))
        observed["probe_command"] = command
        observed["probe_timeout"] = timeout_seconds
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=_result(request, status="ABSENT", files=[], transferred=0),
            stderr=b"",
        )

    def stream_runner(command, *, request, files, timeout_seconds):  # type: ignore[no-untyped-def]
        value = json.loads(request.decode("utf-8"))
        observed["stream_command"] = command
        observed["stream_timeout"] = timeout_seconds
        rows = [
            {"path": item.relative_path, "sha256": item.sha256, "size": item.size}
            for item in files
        ]
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=_result(
                value,
                status="PASS",
                files=rows,
                transferred=sum(item.size for item in files if item.hardlink_source is None),
            ),
            stderr=b"",
        )

    transport = ImmutableStreamingNodeTransport(
        node_id="rdagent-node1",
        allowed_parent=parent,
        command_prefix=(
            "ssh",
            "rdagent-node1",
            "/opt/conda/bin/python",
            "-m",
            "backend.services.dataset_release.monthly_remote_deploy",
        ),
        timeout_seconds=3_600,
        command_runner=command_runner,
        stream_runner=stream_runner,
    )
    context = ProducerContext(
        stage="DEPLOY",
        operation_id="dmr_" + "1" * 32,
        attempt=1,
        request={},
        plan={},
        prior_receipts={},
    )

    result = transport.deploy(context, candidate_root=target, files=files)

    assert result.node_id == "rdagent-node1"
    assert result.candidate_root == target
    assert result.files == tuple(
        (item.relative_path, item.sha256, item.size) for item in files
    )
    assert observed["probe_command"][-1] == "readback"
    assert observed["stream_command"][-1] == "deploy"


@pytest.mark.skipif(os.name == "nt", reason="remote deployment endpoint is POSIX-only")
def test_streaming_transport_default_binary_protocol_is_end_to_end(tmp_path: Path) -> None:
    files = _files(tmp_path)
    parent = tmp_path / "node-releases"
    parent.mkdir()
    target = parent / "candidate"
    transport = ImmutableStreamingNodeTransport(
        node_id="wsl2-5080",
        allowed_parent=str(parent.resolve()),
        command_prefix=(
            sys.executable,
            "-m",
            "backend.services.dataset_release.monthly_remote_deploy",
        ),
        timeout_seconds=300,
    )
    context = ProducerContext(
        stage="DEPLOY",
        operation_id="dmr_" + "2" * 32,
        attempt=1,
        request={},
        plan={},
        prior_receipts={},
    )

    first = transport.deploy(context, candidate_root=str(target.resolve()), files=files)
    resumed = transport.deploy(context, candidate_root=str(target.resolve()), files=files)

    assert first.bytes_transferred > 0
    assert resumed.bytes_transferred == 0
    assert first.files == resumed.files
    assert os.path.samefile(target / "data.bin", target / "alias.bin")


def test_streaming_transport_rejects_remote_identity_drift(tmp_path: Path) -> None:
    files = _files(tmp_path)

    def command_runner(command, *, payload, timeout_seconds):  # type: ignore[no-untyped-def]
        del timeout_seconds
        request = json.loads(payload.decode("utf-8"))
        raw = json.loads(_result(request, status="ABSENT", files=[], transferred=0))
        raw["node_id"] = "wsl2-5080"
        return subprocess.CompletedProcess(
            command, 0, stdout=canonical_json_bytes(raw) + b"\n", stderr=b""
        )

    transport = ImmutableStreamingNodeTransport(
        node_id="rdagent-node1",
        allowed_parent="/home/lc999/data/releases",
        command_prefix=("ssh", "node", "python", "-m", "module"),
        command_runner=command_runner,
    )
    context = ProducerContext(
        stage="DEPLOY",
        operation_id="dmr_" + "1" * 32,
        attempt=1,
        request={},
        plan={},
        prior_receipts={},
    )
    with pytest.raises(MonthlyImmutableDeployError, match="identity differs"):
        transport.deploy(
            context,
            candidate_root="/home/lc999/data/releases/candidate",
            files=files,
        )
