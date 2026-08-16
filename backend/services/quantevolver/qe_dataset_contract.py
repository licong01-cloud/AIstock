"""Immutable dataset identity shared by all QE backtest consumers.

The live selection and paper-trading runtimes intentionally do not import this
module.  Updating these defaults is part of deploying a new QE dataset; normal
QE experiment creation only consumes the resulting immutable contract.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import re
from dataclasses import dataclass
from typing import Any, Mapping

from backend.services.canonical_equity_pit import (
    CANONICAL_PIT_SNAPSHOT_PREFIX,
    PitAuthorityStatus,
    PitConsumerBinding,
    require_canonical_consumer_binding,
)
from backend.services.canonical_pit_dataset_consumer import (
    CanonicalPitDatasetIdentity,
    FormalDatasetUsage,
    require_formal_dataset_pit_identity,
)
from backend.services.dataset_release.canonical import ensure_sha256
from backend.services.dataset_release.cas_store import canonical_json_bytes


_DEFAULT_DATASET_ID = "qlib_st_pit_active_h5_daily_candidate_20180801_20260630_moneyflow_v2"
_DATASET_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,95}$")
_SOURCE_CONTRACT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
QE_FORMAL_DATASET_BINDING_SCHEMA = "qe_formal_canonical_pit_dataset_binding_v1"
QE_FORMAL_DATASET_REQUEST_SCHEMA = "qe_formal_canonical_pit_dataset_request_v1"
QE_FORMAL_DATASET_REQUEST_PARAM = "_qe_formal_dataset_request"
QE_FORMAL_RUNTIME_PINS_SCHEMA = "qe_formal_frozen_runtime_pins_v1"


def _dataset_id() -> str:
    value = os.getenv("QE_DATASET_CONTRACT_ID", _DEFAULT_DATASET_ID).strip().lower()
    if not _DATASET_ID_RE.fullmatch(value):
        raise RuntimeError(
            "QE_DATASET_CONTRACT_ID must match [a-z0-9][a-z0-9_.-]{0,95}; "
            f"got {value!r}"
        )
    return value


QE_DATASET_CONTRACT_ID = _dataset_id()
QE_DATASET_START_DATE = dt.date(2018, 8, 1)
QE_DATASET_SIGNAL_END_DATE = dt.date(2026, 6, 30)
QE_ST_PIT_UNIVERSE_KEY = f"shsz_st_pit_qe_dataset_{QE_DATASET_CONTRACT_ID}"

# Frozen qlib-bin universe pins for the deployed dataset (BUG-989 zero-DB data
# plane).  These values are computed over the frozen bin files at dataset
# deploy time: sha256 of instruments/all.txt, calendars/day.txt and
# meta_export.json plus the snapshot identity they must declare.  The QE
# computation data plane verifies these pins instead of querying market.*
# tables; a mismatch, like any missing frozen input, fails closed.  Deploying
# a new dataset means updating every constant in this module together.
QE_FROZEN_BIN_SNAPSHOT_ID = "qlib_bin_st_pit_active_daily_candidate_20180801_20260630"
QE_FROZEN_BIN_UNIVERSE_KEY = "shsz_st_pit_active_v1"
QE_FROZEN_INSTRUMENTS_SHA256 = "94c9d82de1ba60446d7d6114b39b1066fa3bda3f2a7b9787bb7f0ad4a2a05ca4"
QE_FROZEN_CALENDAR_SHA256 = "6ab71db126fd8c0173831162d5413691c33bfecbbc81db687d8a2de7cc776031"
QE_FROZEN_META_EXPORT_SHA256 = "66c5c070b368ec1352ce4031dce4be982d44894e58968bb3ead0cdc5b65eefb3"
# Universe fingerprint stamped onto QE factor caches: the frozen span file
# digest, not any market.stock_universe_pit_state row.
QE_FROZEN_UNIVERSE_FINGERPRINT_SHA256 = QE_FROZEN_INSTRUMENTS_SHA256

# Frozen suspend_d candidate dataset pins (BUG-989 continuation).  The suspend
# sidecar lives in a versioned candidate directory that is a *sibling* of the
# frozen qlib bin directory on every compute node (same layout on WSL and
# node1): ``<parent of provider_uri_day>/suspend_d_daily_candidate_20180801_20260630``.
# It was exported read-only from market.suspend_d (suspend_type='S', sh/sz
# only, BJ excluded) by scripts/export_suspend_d_candidate.py and carries a
# per-trading-day completeness receipt in manifest.json.  The QE computation
# data plane verifies these pins instead of querying market.suspend_d; a
# mismatch, like any missing frozen input, fails closed.
QE_FROZEN_SUSPEND_DATASET_ID = "suspend_d_daily_candidate_20180801_20260630"
QE_FROZEN_SUSPEND_PARQUET_SHA256 = "493f694312f514d39960aefc275c23ddc6bb60a6a2606cd57e39c428090cc33d"
QE_FROZEN_SUSPEND_MANIFEST_SHA256 = "eea71b92e9098c598db671e6c4bc4f0195516805cb657e3b85a978d60f047ff0"
QE_FROZEN_SUSPEND_SOURCE_CONTRACT = "tushare_suspend_d_shsz_S_v1"


@dataclass(frozen=True, slots=True)
class QEFormalDatasetBinding:
    """Serializable, database-free PIT identity for one formal QE use.

    Instances are created from the W3-A neutral manifest adapter.  The strict
    mapping reader exists for persisted QE configs and validates the complete
    canonical projection again; it never accepts a path, rolling key, or
    online-database fallback.
    """

    usage_mode: str
    authority_id: str
    rule_version: str
    rule_parameters_digest: str
    release_id: str
    cutoff: dt.date
    frozen_snapshot_digest: str
    manifest_digest: str
    schema_version: str = QE_FORMAL_DATASET_BINDING_SCHEMA

    @property
    def frozen_universe_key(self) -> str:
        return f"{CANONICAL_PIT_SNAPSHOT_PREFIX}{self.release_id}"

    @property
    def qe_runtime_universe_key(self) -> str:
        """Compatibility namespace for the existing QE-only risk profile type."""

        return f"shsz_st_pit_qe_dataset_{self.release_id}"

    @classmethod
    def from_identity(
        cls,
        identity: CanonicalPitDatasetIdentity,
        *,
        usage_mode: FormalDatasetUsage | str,
    ) -> "QEFormalDatasetBinding":
        usage = FormalDatasetUsage(usage_mode)
        return cls(
            usage_mode=usage.value,
            authority_id=identity.authority_id,
            rule_version=identity.rule_version,
            rule_parameters_digest=identity.rule_parameters_digest,
            release_id=identity.release_id,
            cutoff=identity.cutoff,
            frozen_snapshot_digest=identity.frozen_snapshot_digest,
            manifest_digest=identity.manifest_digest,
        ).validated()

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "QEFormalDatasetBinding":
        required = {
            "schema_version",
            "usage_mode",
            "authority_id",
            "rule_version",
            "rule_parameters_digest",
            "release_id",
            "cutoff",
            "frozen_snapshot_digest",
            "manifest_digest",
        }
        if not isinstance(value, Mapping) or set(value) != required:
            raise ValueError("QE formal dataset binding schema/fields are invalid")
        if value.get("schema_version") != QE_FORMAL_DATASET_BINDING_SCHEMA:
            raise ValueError("QE formal dataset binding schema_version is invalid")
        try:
            cutoff = dt.date.fromisoformat(str(value["cutoff"]))
        except ValueError as exc:
            raise ValueError("QE formal dataset binding cutoff is invalid") from exc
        return cls(
            usage_mode=str(value["usage_mode"]),
            authority_id=str(value["authority_id"]),
            rule_version=str(value["rule_version"]),
            rule_parameters_digest=str(value["rule_parameters_digest"]),
            release_id=str(value["release_id"]),
            cutoff=cutoff,
            frozen_snapshot_digest=str(value["frozen_snapshot_digest"]),
            manifest_digest=str(value["manifest_digest"]),
        ).validated()

    def validated(self) -> "QEFormalDatasetBinding":
        if self.schema_version != QE_FORMAL_DATASET_BINDING_SCHEMA:
            raise ValueError("QE formal dataset binding schema_version is invalid")
        try:
            usage = FormalDatasetUsage(self.usage_mode)
        except ValueError as exc:
            raise ValueError("QE formal dataset binding usage_mode is invalid") from exc
        release_id = str(self.release_id or "").strip()
        if not release_id:
            raise ValueError("QE formal dataset binding release_id is empty")
        if release_id != release_id.lower() or not _DATASET_ID_RE.fullmatch(release_id):
            raise ValueError("QE formal dataset binding release_id is not canonical")
        snapshot_digest = ensure_sha256(
            self.frozen_snapshot_digest,
            field="frozen_snapshot_digest",
        )
        manifest_digest = ensure_sha256(self.manifest_digest, field="manifest_digest")
        binding = PitConsumerBinding(
            authority_id=self.authority_id,
            authority_status=PitAuthorityStatus.ACTIVE_CANONICAL,
            universe_key=f"{CANONICAL_PIT_SNAPSHOT_PREFIX}{release_id}",
            rule_version=self.rule_version,
            rule_parameters_digest=self.rule_parameters_digest,
            snapshot_digest=snapshot_digest,
            cutoff=self.cutoff,
            release_id=release_id,
        )
        require_canonical_consumer_binding(
            binding,
            consumer=usage.value,
            immutable_snapshot_required=True,
        )
        if (
            usage.value != self.usage_mode
            or release_id != self.release_id
            or snapshot_digest != self.frozen_snapshot_digest
            or manifest_digest != self.manifest_digest
        ):
            raise ValueError("QE formal dataset binding values are not canonical")
        return self

    def as_dict(self) -> dict[str, str]:
        return {
            "schema_version": self.schema_version,
            "usage_mode": self.usage_mode,
            "authority_id": self.authority_id,
            "rule_version": self.rule_version,
            "rule_parameters_digest": self.rule_parameters_digest,
            "release_id": self.release_id,
            "cutoff": self.cutoff.isoformat(),
            "frozen_snapshot_digest": self.frozen_snapshot_digest,
            "manifest_digest": self.manifest_digest,
        }


@dataclass(frozen=True, slots=True)
class QEFormalRuntimePins:
    """Hash pins resolved from the candidate file graph for QE runtime use."""

    artifact_root: str
    qlib_bin_snapshot_id: str
    qlib_instruments_sha256: str
    qlib_calendar_sha256: str
    qlib_meta_export_sha256: str
    suspend_dataset_id: str
    suspend_parquet_sha256: str
    suspend_manifest_sha256: str
    suspend_source_contract: str
    schema_version: str = QE_FORMAL_RUNTIME_PINS_SCHEMA

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "QEFormalRuntimePins":
        required = {
            "schema_version",
            "artifact_root",
            "qlib_bin_snapshot_id",
            "qlib_instruments_sha256",
            "qlib_calendar_sha256",
            "qlib_meta_export_sha256",
            "suspend_dataset_id",
            "suspend_parquet_sha256",
            "suspend_manifest_sha256",
            "suspend_source_contract",
        }
        if not isinstance(value, Mapping) or set(value) != required:
            raise ValueError("QE formal runtime pins schema/fields are invalid")
        if value.get("schema_version") != QE_FORMAL_RUNTIME_PINS_SCHEMA:
            raise ValueError("QE formal runtime pins schema_version is invalid")
        return cls(**{key: str(value[key]) for key in required if key != "schema_version"}).validated()

    def validated(self) -> "QEFormalRuntimePins":
        if self.schema_version != QE_FORMAL_RUNTIME_PINS_SCHEMA:
            raise ValueError("QE formal runtime pins schema_version is invalid")
        for field in (
            "artifact_root",
            "qlib_instruments_sha256",
            "qlib_calendar_sha256",
            "qlib_meta_export_sha256",
            "suspend_parquet_sha256",
            "suspend_manifest_sha256",
        ):
            raw = str(getattr(self, field))
            if ensure_sha256(raw, field=field) != raw:
                raise ValueError(f"QE formal runtime pins {field} is not canonical")
        for field in ("qlib_bin_snapshot_id", "suspend_dataset_id"):
            raw = str(getattr(self, field) or "")
            if raw != raw.strip() or not _DATASET_ID_RE.fullmatch(raw):
                raise ValueError(f"QE formal runtime pins {field} is not canonical")
        source_contract = str(self.suspend_source_contract or "")
        if (
            source_contract != source_contract.strip()
            or not _SOURCE_CONTRACT_RE.fullmatch(source_contract)
        ):
            raise ValueError(
                "QE formal runtime pins suspend_source_contract is not canonical"
            )
        return self

    def as_dict(self) -> dict[str, str]:
        return {
            "schema_version": self.schema_version,
            "artifact_root": self.artifact_root,
            "qlib_bin_snapshot_id": self.qlib_bin_snapshot_id,
            "qlib_instruments_sha256": self.qlib_instruments_sha256,
            "qlib_calendar_sha256": self.qlib_calendar_sha256,
            "qlib_meta_export_sha256": self.qlib_meta_export_sha256,
            "suspend_dataset_id": self.suspend_dataset_id,
            "suspend_parquet_sha256": self.suspend_parquet_sha256,
            "suspend_manifest_sha256": self.suspend_manifest_sha256,
            "suspend_source_contract": self.suspend_source_contract,
        }


@dataclass(frozen=True, slots=True)
class QEFormalDatasetRequest:
    """Detached sealed-manifest request persisted by formal QE configs."""

    usage_mode: str
    expected_manifest_digest: str
    release_manifest: Mapping[str, Any]
    runtime_pins: QEFormalRuntimePins
    schema_version: str = QE_FORMAL_DATASET_REQUEST_SCHEMA

    @classmethod
    def from_release_manifest(
        cls,
        release_manifest: Mapping[str, Any],
        *,
        usage_mode: FormalDatasetUsage | str,
        expected_manifest_digest: str,
        runtime_pins: QEFormalRuntimePins | Mapping[str, Any],
    ) -> "QEFormalDatasetRequest":
        encoded = canonical_json_bytes(dict(release_manifest))
        detached = json.loads(encoded)
        request = cls(
            usage_mode=FormalDatasetUsage(usage_mode).value,
            expected_manifest_digest=expected_manifest_digest,
            release_manifest=detached,
            runtime_pins=(
                runtime_pins.validated()
                if isinstance(runtime_pins, QEFormalRuntimePins)
                else QEFormalRuntimePins.from_mapping(runtime_pins)
            ),
        )
        request.binding()
        return request

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "QEFormalDatasetRequest":
        required = {
            "schema_version",
            "usage_mode",
            "expected_manifest_digest",
            "release_manifest",
            "runtime_pins",
        }
        if not isinstance(value, Mapping) or set(value) != required:
            raise ValueError("QE formal dataset request schema/fields are invalid")
        if value.get("schema_version") != QE_FORMAL_DATASET_REQUEST_SCHEMA:
            raise ValueError("QE formal dataset request schema_version is invalid")
        manifest = value.get("release_manifest")
        if not isinstance(manifest, Mapping):
            raise ValueError("QE formal dataset request release_manifest is invalid")
        runtime_pins = value.get("runtime_pins")
        if not isinstance(runtime_pins, Mapping):
            raise ValueError("QE formal dataset request runtime_pins is invalid")
        return cls.from_release_manifest(
            manifest,
            usage_mode=str(value["usage_mode"]),
            expected_manifest_digest=str(value["expected_manifest_digest"]),
            runtime_pins=runtime_pins,
        )

    def binding(self) -> QEFormalDatasetBinding:
        if self.schema_version != QE_FORMAL_DATASET_REQUEST_SCHEMA:
            raise ValueError("QE formal dataset request schema_version is invalid")
        expected_digest = ensure_sha256(
            self.expected_manifest_digest,
            field="expected_manifest_digest",
        )
        binding = require_qe_formal_dataset_binding(
            self.release_manifest,
            usage_mode=self.usage_mode,
            expected_manifest_digest=expected_digest,
        )
        pins = (
            self.runtime_pins.validated()
            if isinstance(self.runtime_pins, QEFormalRuntimePins)
            else QEFormalRuntimePins.from_mapping(self.runtime_pins)
        )
        manifest_artifact_root = ensure_sha256(
            str(self.release_manifest.get("artifact_root") or ""),
            field="release_manifest.artifact_root",
        )
        if pins.artifact_root != manifest_artifact_root:
            raise ValueError(
                "QE formal runtime pins artifact_root differs from release manifest"
            )
        return binding

    def as_dict(self) -> dict[str, Any]:
        detached = json.loads(canonical_json_bytes(dict(self.release_manifest)))
        pins = (
            self.runtime_pins.validated()
            if isinstance(self.runtime_pins, QEFormalRuntimePins)
            else QEFormalRuntimePins.from_mapping(self.runtime_pins)
        )
        return {
            "schema_version": self.schema_version,
            "usage_mode": self.usage_mode,
            "expected_manifest_digest": self.expected_manifest_digest,
            "release_manifest": detached,
            "runtime_pins": pins.as_dict(),
        }


def require_qe_formal_dataset_binding(
    release_manifest: Mapping[str, Any],
    *,
    usage_mode: FormalDatasetUsage | str,
    expected_manifest_digest: str,
) -> QEFormalDatasetBinding:
    """Create the only accepted formal QE binding from a sealed manifest."""

    identity = require_formal_dataset_pit_identity(
        release_manifest,
        usage_mode=usage_mode,
        expected_manifest_digest=expected_manifest_digest,
    )
    return QEFormalDatasetBinding.from_identity(identity, usage_mode=usage_mode)


def require_qe_formal_dataset_binding_projection(
    value: QEFormalDatasetBinding | Mapping[str, Any],
) -> QEFormalDatasetBinding:
    if isinstance(value, QEFormalDatasetBinding):
        return value.validated()
    return QEFormalDatasetBinding.from_mapping(value)


def require_qe_formal_dataset_request(
    value: QEFormalDatasetRequest | Mapping[str, Any],
) -> QEFormalDatasetRequest:
    request = (
        value
        if isinstance(value, QEFormalDatasetRequest)
        else QEFormalDatasetRequest.from_mapping(value)
    )
    request.binding()
    return request


def require_same_qe_formal_dataset_binding(
    first: QEFormalDatasetBinding | Mapping[str, Any],
    second: QEFormalDatasetBinding | Mapping[str, Any],
) -> QEFormalDatasetBinding:
    left = require_qe_formal_dataset_binding_projection(first)
    right = require_qe_formal_dataset_binding_projection(second)
    if left.as_dict() != right.as_dict():
        raise ValueError("QE formal dataset bindings differ")
    return left


def require_qe_dataset_window(*, start_date: dt.date, end_date: dt.date) -> None:
    """Fail fast when a QE consumer escapes the deployed dataset contract."""

    if start_date < QE_DATASET_START_DATE or end_date > QE_DATASET_SIGNAL_END_DATE:
        raise ValueError(
            "QE request window is outside the deployed immutable dataset contract: "
            f"requested={start_date}..{end_date}, "
            f"dataset={QE_DATASET_START_DATE}..{QE_DATASET_SIGNAL_END_DATE}, "
            f"dataset_id={QE_DATASET_CONTRACT_ID}"
        )
    if end_date < start_date:
        raise ValueError(f"QE request end date {end_date} is earlier than start date {start_date}")


def require_qe_formal_dataset_window(
    binding: QEFormalDatasetBinding | Mapping[str, Any],
    *,
    start_date: dt.date,
    end_date: dt.date,
) -> QEFormalDatasetBinding:
    formal = require_qe_formal_dataset_binding_projection(binding)
    if end_date < start_date:
        raise ValueError(f"QE request end date {end_date} is earlier than start date {start_date}")
    if end_date > formal.cutoff:
        raise ValueError(
            "QE request window exceeds the formal frozen dataset cutoff: "
            f"requested={start_date}..{end_date}, cutoff={formal.cutoff}, release_id={formal.release_id}"
        )
    return formal
