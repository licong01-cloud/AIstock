"""Publish committed monthly tooling separately from legacy node projects.

No services, data releases, dotenv, dependency installation or active pointers
are involved. Preparation is lazy, at DEPLOY/probe execution, never preflight.
"""

from __future__ import annotations

import base64
import ast
from dataclasses import dataclass
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import re
import shlex
import subprocess
import tarfile
from typing import Callable

from .canonical import canonical_json_bytes
from .monthly_subprocess import headless_process_options, run_headless

_REQUIRED = (
    "backend/services/dataset_release/monthly_remote_deploy.py",
    "backend/services/dataset_release/monthly_node_probe.py",
    "backend/services/dataset_release/monthly_shared_consumer_probe.py",
    "backend/services/dataset_release/monthly_hmm_consumer_probe.py",
    "backend/services/dataset_release/monthly_qe_consumer_probe.py",
    "scripts/precompute_hmm_coefficients.py",
)


def _install_node_tools(payload):
    """Standalone stdlib bootstrap, also executed directly by offline tests."""
    import base64
    import hashlib
    import io
    import json
    import os
    from pathlib import Path, PurePosixPath
    import re
    import tarfile
    import tempfile

    def canonical(value):
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode(
            "utf-8"
        )

    manifest = payload["manifest"]
    if (
        set(payload) != {"manifest", "archive", "tools_parent"}
        or set(manifest) != {"schema_version", "source_commit", "archive_sha256", "files"}
        or manifest["schema_version"] != "aistock_monthly_node_tools_v1"
    ):
        raise ValueError("monthly tooling schema differs")
    commit = manifest["source_commit"]
    if re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        raise ValueError("monthly tooling commit is invalid")
    parent = Path(payload["tools_parent"])
    if not parent.is_absolute() or ".." in parent.parts or parent.name != ".aistock-monthly-tools":
        raise ValueError("monthly tooling parent is invalid")
    for item in (parent, *parent.parents):
        if item.is_symlink():
            raise ValueError("monthly tooling parent must be plain")
    archive = base64.b64decode(payload["archive"], validate=True)
    if hashlib.sha256(archive).hexdigest() != manifest["archive_sha256"]:
        raise ValueError("monthly tooling archive hash differs")
    rows = manifest["files"]
    expected = {}
    for row in rows:
        if (
            set(row) != {"path", "size", "sha256"}
            or type(row["size"]) is not int
            or row["size"] < 0
            or re.fullmatch(r"[0-9a-f]{64}", str(row["sha256"])) is None
        ):
            raise ValueError("monthly tooling file identity is invalid")
        relative = PurePosixPath(row["path"])
        if (
            not relative.parts
            or relative.is_absolute()
            or ".." in relative.parts
            or relative.as_posix() != row["path"]
            or relative.parts[0] not in {"backend", "scripts", "config", "configs"}
            or any(part.startswith(".env") for part in relative.parts)
            or row["path"] in expected
        ):
            raise ValueError("monthly tooling inventory path is invalid")
        expected[row["path"]] = row
    if not expected:
        raise ValueError("monthly tooling inventory is empty")
    target = parent / commit
    raw_manifest = canonical(manifest) + b"\n"

    def verify(root):
        if root.is_symlink() or not root.is_dir():
            raise ValueError("monthly tooling release must be a plain directory")
        names = set()
        for base, dirs, files in os.walk(root):
            for name in dirs:
                if (Path(base) / name).is_symlink():
                    raise ValueError("monthly tooling contains a linked directory")
            for name in files:
                item = Path(base) / name
                if item.is_symlink() or not item.is_file():
                    raise ValueError("monthly tooling contains a nonregular file")
                relative = item.relative_to(root).as_posix()
                names.add(relative)
                if relative == "node_tools_manifest.json":
                    if item.read_bytes() != raw_manifest:
                        raise ValueError("monthly tooling manifest differs")
                else:
                    row = expected.get(relative)
                    if (
                        row is None
                        or item.stat().st_size != row["size"]
                        or hashlib.sha256(item.read_bytes()).hexdigest() != row["sha256"]
                    ):
                        raise ValueError("monthly tooling code bytes differ")
        if names != {*expected, "node_tools_manifest.json"}:
            raise ValueError("monthly tooling release inventory differs")

    if not target.exists():
        parent.mkdir(parents=True, exist_ok=True)
        staging = Path(tempfile.mkdtemp(prefix=commit + "-", dir=parent))
        with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as handle:
            members = handle.getmembers()
            if len(members) != len(expected) or {item.name for item in members} != set(expected):
                raise ValueError("monthly tooling archive inventory differs")
            for member in members:
                if not member.isfile():
                    raise ValueError("monthly tooling archive contains links or special files")
                row = expected[member.name]
                content = handle.extractfile(member).read()
                if len(content) != row["size"] or hashlib.sha256(content).hexdigest() != row["sha256"]:
                    raise ValueError("monthly tooling source bytes differ")
                item = staging / member.name
                item.parent.mkdir(parents=True, exist_ok=True)
                with item.open("xb") as output:
                    output.write(content)
                item.chmod(0o444)
        with (staging / "node_tools_manifest.json").open("xb") as output:
            output.write(raw_manifest)
        verify(staging)
        for base, dirs, files in os.walk(staging, topdown=False):
            for name in files:
                (Path(base) / name).chmod(0o444)
            Path(base).chmod(0o555)
        if os.name == "posix":
            # Linux no-replace rename: a concurrent publisher must never be overlaid.
            import ctypes

            native = ctypes.CDLL(None, use_errno=True).renameat2
            native.argtypes = (ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint)
            if native(-100, os.fsencode(staging), -100, os.fsencode(target), 1) != 0:
                error = ctypes.get_errno()
                if error != 17:  # EEXIST: compare the other publisher's exact bytes below.
                    raise OSError(error, os.strerror(error))
        else:
            os.rename(staging, target)  # Windows rename already refuses an existing destination.
    verify(target)
    return {
        "status": "PASS",
        "source_commit": commit,
        "project_root": str(target),
        "manifest_sha256": hashlib.sha256(raw_manifest).hexdigest(),
        "file_count": len(expected),
    }


def _bundle(source_root: Path, commit: str) -> tuple[dict, str]:
    def git(*args):
        try:
            return subprocess.run(
                ("git", "-C", str(source_root), *args),
                check=True,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=180,
                **headless_process_options(),
            ).stdout
        except subprocess.CalledProcessError as error:
            raise ValueError("monthly tooling pinned Git source is unavailable") from error

    if re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        raise ValueError("monthly tooling source commit is invalid")
    roots = set(git("ls-tree", "--name-only", commit).decode().splitlines())
    selected = sorted(roots & {"backend", "scripts", "config", "configs"})
    if not {"backend", "scripts"} <= set(selected):
        raise ValueError("monthly tooling source roots are incomplete")
    source = git("archive", "--format=tar", commit, *selected)
    buffer = io.BytesIO()
    rows = []
    with (
        tarfile.open(fileobj=io.BytesIO(source), mode="r:") as archive,
        tarfile.open(fileobj=buffer, mode="w:") as output,
    ):
        for member in archive.getmembers():
            if member.isdir():
                continue
            path = PurePosixPath(member.name)
            if any(part.startswith(".env") or part in {"tests", "__pycache__"} for part in path.parts):
                continue
            if path.suffix not in {".py", ".json", ".yaml", ".yml", ".sql"}:
                continue
            if not member.isfile():
                raise ValueError("monthly tooling Git source contains a nonregular file")
            content = archive.extractfile(member).read()
            info = tarfile.TarInfo(member.name)
            info.size, info.mode = len(content), 0o444
            output.addfile(info, io.BytesIO(content))
            rows.append({"path": member.name, "sha256": hashlib.sha256(content).hexdigest(), "size": len(content)})
    if not set(_REQUIRED) <= {row["path"] for row in rows}:
        raise ValueError("monthly tooling committed source modules are incomplete")
    raw = buffer.getvalue()
    manifest = {
        "schema_version": "aistock_monthly_node_tools_v1",
        "source_commit": commit,
        "archive_sha256": hashlib.sha256(raw).hexdigest(),
        "files": rows,
    }
    return manifest, base64.b64encode(raw).decode("ascii")


def _bootstrap(manifest: dict, encoded: str) -> str:
    name = "backend/services/dataset_release/monthly_node_tools.py"
    raw = base64.b64decode(encoded, validate=True)
    if hashlib.sha256(raw).hexdigest() != manifest["archive_sha256"]:
        raise ValueError("monthly tooling bootstrap archive identity differs")
    rows = {row["path"]: row for row in manifest["files"]}
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as handle:
        member = handle.getmember(name)
        if not member.isfile():
            raise ValueError("monthly tooling bootstrap is not regular source")
        content = handle.extractfile(member).read()
    expected = rows.get(name)
    if (
        expected is None
        or expected["size"] != len(content)
        or expected["sha256"] != hashlib.sha256(content).hexdigest()
    ):
        raise ValueError("monthly tooling bootstrap identity differs")
    source = content.decode("utf-8")
    tree = ast.parse(source)
    functions = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "_install_node_tools"]
    if len(functions) != 1:
        raise ValueError("monthly tooling frozen bootstrap is ambiguous")
    return ast.get_source_segment(source, functions[0])


@dataclass
class MonthlyNodeTools:
    host: str
    legacy_project_root: str
    python_executable: str
    source_root: Path
    executor: Callable = run_headless
    _receipt: dict | None = None

    def __post_init__(self):
        if re.fullmatch(r"[A-Za-z0-9_.@-]{1,255}", self.host) is None:
            raise ValueError("monthly tooling host is invalid")
        for value in (self.legacy_project_root, self.python_executable):
            path = PurePosixPath(value)
            if not path.is_absolute() or ".." in path.parts or path.as_posix() != value or "\x00" in value:
                raise ValueError("monthly tooling runtime path is invalid")
        self.source_commit = (
            subprocess.run(
                ("git", "-C", str(self.source_root), "rev-parse", "HEAD"),
                check=True,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=30,
                **headless_process_options(),
            )
            .stdout.decode()
            .strip()
        )

    def command(self, module: str) -> tuple[str, ...]:
        if module not in {"monthly_remote_deploy", "monthly_node_probe"}:
            raise ValueError("monthly tooling module is not registered")
        if self._receipt is None:
            manifest, archive = _bundle(self.source_root, self.source_commit)
            parent = (PurePosixPath(self.legacy_project_root).parent / ".aistock-monthly-tools").as_posix()
            payload = {"manifest": manifest, "archive": archive, "tools_parent": parent}
            bootstrap = (
                _bootstrap(manifest, archive)
                + "\nimport json,sys\nprint(json.dumps(_install_node_tools(json.load(sys.stdin)),sort_keys=True,separators=(',',':')))\n"
            )
            remote = "exec " + shlex.quote(self.python_executable) + " -c " + shlex.quote(bootstrap)
            completed = self.executor(
                self._ssh(remote),
                input=canonical_json_bytes(payload),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
                timeout=300,
            )
            if completed.returncode:
                raise ValueError(
                    "monthly tooling immutable publish failed: " + completed.stderr.decode(errors="replace")[-1000:]
                )
            receipt = json.loads(completed.stdout)
            expected = {
                "status": "PASS",
                "source_commit": self.source_commit,
                "project_root": parent + "/" + self.source_commit,
                "manifest_sha256": hashlib.sha256(canonical_json_bytes(manifest) + b"\n").hexdigest(),
                "file_count": len(manifest["files"]),
            }
            if receipt != expected:
                raise ValueError("monthly tooling publish readback identity differs")
            self._receipt = receipt
        remote = (
            "cd -- "
            + shlex.quote(self._receipt["project_root"])
            + " && exec "
            + shlex.quote(self.python_executable)
            + (" -m backend.services.dataset_release." + module)
        )
        return self._ssh(remote)

    def _ssh(self, remote: str) -> tuple[str, ...]:
        return ("ssh", "-F", "NUL", "-o", "BatchMode=yes", "-o", "ConnectTimeout=30", self.host, remote)
