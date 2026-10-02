"""Transactional repository for LocalSIM package-scoped runtime profiles."""

from __future__ import annotations

from contextlib import AbstractContextManager
from copy import deepcopy
from datetime import datetime
from typing import Any, Callable, Protocol

import psycopg2
import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.services.trading_core.errors import DataUnavailableError, InvalidStateTransitionError

from .localsim_runtime_profile import (
    LOCALSIM_RUNTIME_PROFILE_VERSION_SCHEMA,
    LocalSimRuntimeProfileStatus,
    LocalSimRuntimeProfileV1,
    LocalSimRuntimeProfileValidationStatus,
    LocalSimRuntimeProfileVersionV1,
    LocalSimRuntimeProfileConfigV1,
)
from .models import canonical_json_sha256


ProfileConnFactory = Callable[[], AbstractContextManager[Any]]


class LocalSimRuntimeProfileRepositoryProtocol(Protocol):
    def create_profile(self, profile: LocalSimRuntimeProfileV1) -> LocalSimRuntimeProfileV1: ...

    def get_profile(self, profile_id: str) -> LocalSimRuntimeProfileV1: ...

    def list_profiles(
        self,
        *,
        package_id: str | None = None,
        before: tuple[datetime, str] | None = None,
        limit: int = 100,
    ) -> list[LocalSimRuntimeProfileV1]: ...

    def create_version(
        self,
        *,
        profile_id: str,
        expected_profile_version: int,
        config: LocalSimRuntimeProfileConfigV1,
        validation_status: LocalSimRuntimeProfileValidationStatus,
        validation_evidence: dict[str, Any],
        created_by: str,
        created_at: datetime,
    ) -> tuple[LocalSimRuntimeProfileV1, LocalSimRuntimeProfileVersionV1]: ...

    def get_version(self, profile_version_id: str) -> LocalSimRuntimeProfileVersionV1: ...

    def list_versions(
        self,
        *,
        profile_id: str,
        before: tuple[datetime, str] | None = None,
        limit: int = 100,
    ) -> list[LocalSimRuntimeProfileVersionV1]: ...

    def retire_profile(
        self, *, profile_id: str, expected_version: int, updated_at: datetime
    ) -> LocalSimRuntimeProfileV1: ...


def _transaction_conn() -> AbstractContextManager[Any]:
    return get_conn(autocommit=False, manage_transaction=True)


class LocalSimRuntimeProfileRepository:
    def __init__(self, conn_factory: ProfileConnFactory | None = None) -> None:
        self._conn_factory = conn_factory or _transaction_conn

    def create_profile(self, profile: LocalSimRuntimeProfileV1) -> LocalSimRuntimeProfileV1:
        try:
            with self._conn_factory() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        """
                        INSERT INTO paper_v2.localsim_runtime_profile_v1 (
                            profile_id, profile_hash, schema_version, package_id, manifest_sha256,
                            profile_name, status, version, created_by, created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (profile_hash) DO NOTHING
                        """,
                        (
                            profile.profile_id,
                            profile.profile_hash,
                            profile.schema_version,
                            profile.package_id,
                            profile.manifest_sha256,
                            profile.profile_name,
                            profile.status.value,
                            profile.version,
                            profile.created_by,
                            profile.created_at,
                            profile.updated_at,
                        ),
                    )
                    persisted = self._select_profile(cur, profile.profile_id)
                    self._require_profile_identity(profile, persisted)
                    return persisted
        except psycopg2.IntegrityError as exc:
            raise InvalidStateTransitionError(
                "LocalSIM runtime profile conflicts with existing authority",
                context={"reason_code": "LOCALSIM_RUNTIME_PROFILE_CONFLICT", "profile_id": profile.profile_id},
            ) from exc

    def get_profile(self, profile_id: str) -> LocalSimRuntimeProfileV1:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                return self._select_profile(cur, profile_id)

    def list_profiles(
        self,
        *,
        package_id: str | None = None,
        before: tuple[datetime, str] | None = None,
        limit: int = 100,
    ) -> list[LocalSimRuntimeProfileV1]:
        clauses: list[str] = []
        params: list[Any] = []
        if package_id is not None:
            clauses.append("package_id = %s")
            params.append(package_id)
        if before is not None:
            clauses.append("(created_at, profile_id) < (%s, %s)")
            params.extend(before)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"SELECT * FROM paper_v2.localsim_runtime_profile_v1 {where} "
                    "ORDER BY created_at DESC, profile_id DESC LIMIT %s",
                    tuple(params),
                )
                return [self._profile_from_row(dict(row)) for row in cur.fetchall()]

    def create_version(
        self,
        *,
        profile_id: str,
        expected_profile_version: int,
        config: LocalSimRuntimeProfileConfigV1,
        validation_status: LocalSimRuntimeProfileValidationStatus,
        validation_evidence: dict[str, Any],
        created_by: str,
        created_at: datetime,
    ) -> tuple[LocalSimRuntimeProfileV1, LocalSimRuntimeProfileVersionV1]:
        config_json = config.model_dump(mode="json")
        config_sha256 = canonical_json_sha256(config_json)
        try:
            with self._conn_factory() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    profile = self._select_profile(cur, profile_id, for_update=True)
                    if profile.status is LocalSimRuntimeProfileStatus.RETIRED:
                        raise InvalidStateTransitionError(
                            "retired LocalSIM runtime profile cannot receive a new version",
                            context={"reason_code": "LOCALSIM_RUNTIME_PROFILE_RETIRED"},
                        )
                    cur.execute(
                        """
                        SELECT * FROM paper_v2.localsim_runtime_profile_version_v1
                        WHERE profile_id = %s AND config_sha256 = %s
                        """,
                        (profile_id, config_sha256),
                    )
                    duplicate = cur.fetchone()
                    if duplicate is not None:
                        return profile, self._version_from_row(dict(duplicate))
                    if profile.version != expected_profile_version:
                        raise InvalidStateTransitionError(
                            "LocalSIM runtime profile version CAS failed",
                            context={
                                "reason_code": "LOCALSIM_RUNTIME_PROFILE_CAS_CONFLICT",
                                "profile_id": profile_id,
                                "expected_version": expected_profile_version,
                            },
                        )
                    cur.execute(
                        """
                        SELECT COALESCE(MAX(version_no), 0) + 1 AS next_version_no
                        FROM paper_v2.localsim_runtime_profile_version_v1
                        WHERE profile_id = %s
                        """,
                        (profile_id,),
                    )
                    version_no = int(cur.fetchone()["next_version_no"])
                    version = _build_version(
                        profile=profile,
                        version_no=version_no,
                        config_json=config_json,
                        config_sha256=config_sha256,
                        validation_status=validation_status,
                        validation_evidence=validation_evidence,
                        created_by=created_by,
                        created_at=created_at,
                    )
                    self._insert_version(cur, version)
                    cur.execute(
                        """
                        UPDATE paper_v2.localsim_runtime_profile_v1
                        SET version = version + 1, updated_at = %s
                        WHERE profile_id = %s AND version = %s AND status = 'ACTIVE'
                        """,
                        (created_at, profile_id, expected_profile_version),
                    )
                    if cur.rowcount != 1:
                        raise InvalidStateTransitionError(
                            "LocalSIM runtime profile version CAS failed",
                            context={"reason_code": "LOCALSIM_RUNTIME_PROFILE_CAS_CONFLICT"},
                        )
                    persisted_profile = self._select_profile(cur, profile_id, for_update=True)
                    persisted_version = self._select_version(cur, version.profile_version_id)
                    self._require_version_identity(version, persisted_version)
                    return persisted_profile, persisted_version
        except psycopg2.IntegrityError as exc:
            raise InvalidStateTransitionError(
                "LocalSIM runtime profile version conflicts with existing authority",
                context={"reason_code": "LOCALSIM_RUNTIME_PROFILE_VERSION_CONFLICT", "profile_id": profile_id},
            ) from exc

    def get_version(self, profile_version_id: str) -> LocalSimRuntimeProfileVersionV1:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                return self._select_version(cur, profile_version_id)

    def list_versions(
        self,
        *,
        profile_id: str,
        before: tuple[datetime, str] | None = None,
        limit: int = 100,
    ) -> list[LocalSimRuntimeProfileVersionV1]:
        self.get_profile(profile_id)
        clauses = ["profile_id = %s"]
        params: list[Any] = [profile_id]
        if before is not None:
            clauses.append("(created_at, profile_version_id) < (%s, %s)")
            params.extend(before)
        params.append(limit)
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    f"SELECT * FROM paper_v2.localsim_runtime_profile_version_v1 "
                    f"WHERE {' AND '.join(clauses)} "
                    "ORDER BY created_at DESC, profile_version_id DESC LIMIT %s",
                    tuple(params),
                )
                return [self._version_from_row(dict(row)) for row in cur.fetchall()]

    def retire_profile(
        self, *, profile_id: str, expected_version: int, updated_at: datetime
    ) -> LocalSimRuntimeProfileV1:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    UPDATE paper_v2.localsim_runtime_profile_v1
                    SET status = 'RETIRED', version = version + 1, updated_at = %s
                    WHERE profile_id = %s AND version = %s AND status = 'ACTIVE'
                    """,
                    (updated_at, profile_id, expected_version),
                )
                if cur.rowcount != 1:
                    raise InvalidStateTransitionError(
                        "LocalSIM runtime profile retire CAS failed",
                        context={"reason_code": "LOCALSIM_RUNTIME_PROFILE_CAS_CONFLICT"},
                    )
                return self._select_profile(cur, profile_id, for_update=True)

    @staticmethod
    def _insert_version(cur: Any, version: LocalSimRuntimeProfileVersionV1) -> None:
        cur.execute(
            """
            INSERT INTO paper_v2.localsim_runtime_profile_version_v1 (
                profile_version_id, profile_version_hash, schema_version, profile_id,
                package_id, manifest_sha256, version_no, config_json, config_sha256,
                daily_strategy_profile_version_id, validation_status, validation_evidence,
                created_by, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                version.profile_version_id,
                version.profile_version_hash,
                version.schema_version,
                version.profile_id,
                version.package_id,
                version.manifest_sha256,
                version.version_no,
                psycopg2.extras.Json(version.config_json),
                version.config_sha256,
                version.daily_strategy_profile_version_id,
                version.validation_status.value,
                psycopg2.extras.Json(version.validation_evidence),
                version.created_by,
                version.created_at,
            ),
        )

    @classmethod
    def _select_profile(
        cls, cur: Any, profile_id: str, *, for_update: bool = False
    ) -> LocalSimRuntimeProfileV1:
        suffix = " FOR UPDATE" if for_update else ""
        cur.execute(
            f"SELECT * FROM paper_v2.localsim_runtime_profile_v1 WHERE profile_id = %s{suffix}",
            (profile_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise DataUnavailableError("LocalSIM runtime profile does not exist", context={"profile_id": profile_id})
        return cls._profile_from_row(dict(row))

    @classmethod
    def _select_version(cls, cur: Any, profile_version_id: str) -> LocalSimRuntimeProfileVersionV1:
        cur.execute(
            "SELECT * FROM paper_v2.localsim_runtime_profile_version_v1 WHERE profile_version_id = %s",
            (profile_version_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise DataUnavailableError(
                "LocalSIM runtime profile version does not exist",
                context={"profile_version_id": profile_version_id},
            )
        return cls._version_from_row(dict(row))

    @staticmethod
    def _profile_from_row(row: dict[str, Any]) -> LocalSimRuntimeProfileV1:
        return LocalSimRuntimeProfileV1(**row)

    @staticmethod
    def _version_from_row(row: dict[str, Any]) -> LocalSimRuntimeProfileVersionV1:
        return LocalSimRuntimeProfileVersionV1(**row)

    @staticmethod
    def _require_profile_identity(expected: LocalSimRuntimeProfileV1, actual: LocalSimRuntimeProfileV1) -> None:
        if (expected.profile_id, expected.profile_hash) != (actual.profile_id, actual.profile_hash):
            raise InvalidStateTransitionError(
                "LocalSIM runtime profile readback identity differs",
                context={"reason_code": "LOCALSIM_RUNTIME_PROFILE_READBACK_MISMATCH"},
            )

    @staticmethod
    def _require_version_identity(
        expected: LocalSimRuntimeProfileVersionV1, actual: LocalSimRuntimeProfileVersionV1
    ) -> None:
        if (expected.profile_version_id, expected.profile_version_hash, expected.config_sha256) != (
            actual.profile_version_id,
            actual.profile_version_hash,
            actual.config_sha256,
        ):
            raise InvalidStateTransitionError(
                "LocalSIM runtime profile version readback identity differs",
                context={"reason_code": "LOCALSIM_RUNTIME_PROFILE_VERSION_READBACK_MISMATCH"},
            )


class InMemoryLocalSimRuntimeProfileRepository:
    def __init__(self) -> None:
        self.profiles: dict[str, LocalSimRuntimeProfileV1] = {}
        self.profile_hash_index: dict[str, str] = {}
        self.versions: dict[str, LocalSimRuntimeProfileVersionV1] = {}
        self.profile_config_index: dict[tuple[str, str], str] = {}

    def create_profile(self, profile: LocalSimRuntimeProfileV1) -> LocalSimRuntimeProfileV1:
        existing_id = self.profile_hash_index.get(profile.profile_hash)
        if existing_id is not None:
            return self.profiles[existing_id]
        if profile.profile_id in self.profiles:
            raise InvalidStateTransitionError(
                "LocalSIM runtime profile conflicts with existing authority",
                context={"reason_code": "LOCALSIM_RUNTIME_PROFILE_CONFLICT"},
            )
        self.profiles[profile.profile_id] = profile
        self.profile_hash_index[profile.profile_hash] = profile.profile_id
        return profile

    def get_profile(self, profile_id: str) -> LocalSimRuntimeProfileV1:
        try:
            return self.profiles[profile_id]
        except KeyError as exc:
            raise DataUnavailableError("LocalSIM runtime profile does not exist", context={"profile_id": profile_id}) from exc

    def list_profiles(
        self,
        *,
        package_id: str | None = None,
        before: tuple[datetime, str] | None = None,
        limit: int = 100,
    ) -> list[LocalSimRuntimeProfileV1]:
        values = [
            item
            for item in self.profiles.values()
            if (package_id is None or item.package_id == package_id)
            and (before is None or (item.created_at, item.profile_id) < before)
        ]
        return sorted(values, key=lambda item: (item.created_at, item.profile_id), reverse=True)[:limit]

    def create_version(
        self,
        *,
        profile_id: str,
        expected_profile_version: int,
        config: LocalSimRuntimeProfileConfigV1,
        validation_status: LocalSimRuntimeProfileValidationStatus,
        validation_evidence: dict[str, Any],
        created_by: str,
        created_at: datetime,
    ) -> tuple[LocalSimRuntimeProfileV1, LocalSimRuntimeProfileVersionV1]:
        snapshot = deepcopy(self.__dict__)
        try:
            profile = self.get_profile(profile_id)
            if profile.status is LocalSimRuntimeProfileStatus.RETIRED:
                raise InvalidStateTransitionError(
                    "retired LocalSIM runtime profile cannot receive a new version",
                    context={"reason_code": "LOCALSIM_RUNTIME_PROFILE_RETIRED"},
                )
            config_json = config.model_dump(mode="json")
            config_sha256 = canonical_json_sha256(config_json)
            duplicate_id = self.profile_config_index.get((profile_id, config_sha256))
            if duplicate_id is not None:
                return profile, self.versions[duplicate_id]
            if profile.version != expected_profile_version:
                raise InvalidStateTransitionError(
                    "LocalSIM runtime profile version CAS failed",
                    context={"reason_code": "LOCALSIM_RUNTIME_PROFILE_CAS_CONFLICT"},
                )
            version_no = 1 + max(
                (version.version_no for version in self.versions.values() if version.profile_id == profile_id),
                default=0,
            )
            version = _build_version(
                profile=profile,
                version_no=version_no,
                config_json=config_json,
                config_sha256=config_sha256,
                validation_status=validation_status,
                validation_evidence=validation_evidence,
                created_by=created_by,
                created_at=created_at,
            )
            self.versions[version.profile_version_id] = version
            self.profile_config_index[(profile_id, config_sha256)] = version.profile_version_id
            profile = profile.model_copy(update={"version": profile.version + 1, "updated_at": created_at})
            self.profiles[profile_id] = profile
            return profile, version
        except Exception:
            self.__dict__.update(snapshot)
            raise

    def get_version(self, profile_version_id: str) -> LocalSimRuntimeProfileVersionV1:
        try:
            return self.versions[profile_version_id]
        except KeyError as exc:
            raise DataUnavailableError(
                "LocalSIM runtime profile version does not exist",
                context={"profile_version_id": profile_version_id},
            ) from exc

    def list_versions(
        self,
        *,
        profile_id: str,
        before: tuple[datetime, str] | None = None,
        limit: int = 100,
    ) -> list[LocalSimRuntimeProfileVersionV1]:
        self.get_profile(profile_id)
        values = [
            item
            for item in self.versions.values()
            if item.profile_id == profile_id
            and (before is None or (item.created_at, item.profile_version_id) < before)
        ]
        return sorted(values, key=lambda item: (item.created_at, item.profile_version_id), reverse=True)[:limit]

    def retire_profile(
        self, *, profile_id: str, expected_version: int, updated_at: datetime
    ) -> LocalSimRuntimeProfileV1:
        profile = self.get_profile(profile_id)
        if profile.version != expected_version or profile.status is not LocalSimRuntimeProfileStatus.ACTIVE:
            raise InvalidStateTransitionError(
                "LocalSIM runtime profile retire CAS failed",
                context={"reason_code": "LOCALSIM_RUNTIME_PROFILE_CAS_CONFLICT"},
            )
        profile = profile.model_copy(
            update={
                "status": LocalSimRuntimeProfileStatus.RETIRED,
                "version": profile.version + 1,
                "updated_at": updated_at,
            }
        )
        self.profiles[profile_id] = profile
        return profile


def _build_version(
    *,
    profile: LocalSimRuntimeProfileV1,
    version_no: int,
    config_json: dict[str, Any],
    config_sha256: str,
    validation_status: LocalSimRuntimeProfileValidationStatus,
    validation_evidence: dict[str, Any],
    created_by: str,
    created_at: datetime,
) -> LocalSimRuntimeProfileVersionV1:
    identity = {
        "schema_version": LOCALSIM_RUNTIME_PROFILE_VERSION_SCHEMA,
        "profile_id": profile.profile_id,
        "package_id": profile.package_id,
        "manifest_sha256": profile.manifest_sha256,
        "config_sha256": config_sha256,
    }
    version_hash = canonical_json_sha256(identity)
    daily_strategy = LocalSimRuntimeProfileConfigV1.model_validate(config_json).daily_strategy
    daily_hash = canonical_json_sha256(daily_strategy.model_dump(mode="json"))
    return LocalSimRuntimeProfileVersionV1(
        profile_version_id=f"lsrpv_{version_hash[:16]}",
        profile_version_hash=version_hash,
        profile_id=profile.profile_id,
        package_id=profile.package_id,
        manifest_sha256=profile.manifest_sha256,
        version_no=version_no,
        config_json=config_json,
        config_sha256=config_sha256,
        daily_strategy_profile_version_id=f"lsdaily_{daily_hash[:16]}",
        validation_status=validation_status,
        validation_evidence=validation_evidence,
        created_by=str(created_by).strip(),
        created_at=created_at,
    )
