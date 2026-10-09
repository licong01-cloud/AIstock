"""Offline cross-package validation identities; no deployment or training."""
from __future__ import annotations

from datetime import date
import hashlib
import json
import os
from pathlib import Path
import tempfile


SCHEMA = "advisory_cross_package_frozen_validation_v1"
NEW_PACKAGES = (
    "pkg_cc7eccb6202e4816b9d7c7d6b748bd37",
    "pkg_8dbd396d27184de6aa5fe288011220f6",
)
CANARY_DATES = (date(2026, 3, 11), date(2026, 5, 6), date(2026, 8, 25))


def canonical_sha(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), allow_nan=False).encode("utf-8")).hexdigest()


def file_sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1048576), b""):
            digest.update(chunk)
    return digest.hexdigest()


def real_root(value, *, required_drive=None):
    path = Path(value)
    if not path.is_absolute() or path.resolve() != path or path.drive.upper() == "C:":
        raise ValueError("validation needs an explicit real non-C root")
    if required_drive and path.drive.upper() != required_drive.upper():
        raise ValueError(f"validation root must be on {required_drive}")
    return path


def publish_bytes(path, data):
    """Immutable checkpoint: exact retry is idempotent, changed content rejected."""
    path = Path(path)
    real_root(path.parent)
    if not isinstance(data, bytes):
        raise ValueError("checkpoint requires exact bytes")
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != data:
            raise ValueError(f"checkpoint differs; do not overwrite {path.name}")
        return
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".checkpoint_", delete=False) as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
        temporary = Path(stream.name)
    try:
        # Exclusive final publication, without replacing a concurrently written file.
        os.link(temporary, path)
    finally:
        temporary.unlink()


def publish_json(path, value):
    publish_bytes(path, json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False).encode("utf-8"))


def readonly_connection():
    """Existing DB only, with a connection-local read-only transaction default."""
    from contextlib import contextmanager
    from backend.db.pg_pool import get_conn

    @contextmanager
    def session():
        with get_conn() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SHOW default_transaction_read_only")
                original_default = cursor.fetchone()[0]
                cursor.execute("SET default_transaction_read_only=on")
            connection.commit()
            try:
                yield connection
            finally:
                connection.rollback()
                with connection.cursor() as cursor:
                    cursor.execute("SELECT set_config('default_transaction_read_only', %s, false)", (original_default,))
                connection.commit()
    return session()
