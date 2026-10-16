import hashlib
import stat
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.services.dataset_release import sw_l2_quote_policy as policy
from backend.services.dataset_release.canonical import canonical_json_bytes, digest_named_fields
from backend.services.dataset_release.monthly_build_recovery import LEGACY_POLICY_ID, recover_unpublished_quote_policy
from backend.services.dataset_release.monthly_build_resume import MonthlyBuildResumeError, _signature
from backend.services.dataset_release.monthly_unified import _digest
from backend.tests.dataset_release.test_sw_l2_quote_policy import _code_map


def _write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes(value) + b"\n")


def _fixture(tmp_path, monkeypatch):
    op = "dmr_" + "1" * 32
    operation = tmp_path / "state/monthly" / op
    target = tmp_path / "releases/candidate"
    staging = target.parent / f".staging/.candidate.{op}.attempt-2.building"
    staging.parent.mkdir(parents=True)
    plan = {"candidate_root": str(target), "generation": "new", "release_id": "release",
            "target_cutoff": "2026-09-30", "predecessor": {"identity": "old"},
            "profile_candidate": str(tmp_path / "new-profile.json"),
            "monthly_industry_quote_source": {"quote_source": {"source_dataset": "market.sw_daily",
                "partitions": [{"partition_key": "2024-07", "ref": {"sha256": "a" * 64, "size": 1}}]}}}
    codes = [f"801{i:03d}.SI" for i in range(131)]
    monkeypatch.setattr(policy, "SW2021_L2_TAXONOMY_DIGEST", policy.taxonomy_digest(codes))
    monkeypatch.setattr(policy, "_LAST_PUBLISHED", {})
    mapping = _code_map(codes)
    old = policy.build_quote_availability_payload(code_map=mapping, required_start=date(2024, 7, 1), cutoff=date(2026, 9, 30))
    next(r for r in old["entries"] if r["canonical_l2_code"] == "801011.SI")["availability_spans"] = []
    component = target / "components/sector_context_candidate_v1"
    payloads = {"qe_dataset_manifest.json": {"generation": "new"},
        "components/sector_context_candidate_v1/sector_code_map.json": dict(mapping_payload(mapping)),
        "components/sector_context_candidate_v1/sector_quote_availability.json": old,
        "components/sector_context_candidate_v1/component_receipt.json": {
            "quote_availability": {"publication_policy_identity": LEGACY_POLICY_ID}}}
    pins = []
    for relative, value in payloads.items():
        path = target / relative
        _write(path, value)
        pins.append({"id": f"candidate/{relative}", "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "size": path.stat().st_size})
    price = target / "minute_bin/close.bin"
    price.parent.mkdir()
    price.write_bytes(b"accurate-export")
    binding = {"operation_id": op, "candidate_name": target.name, "release_id": plan["release_id"],
               "target_cutoff": plan["target_cutoff"], "predecessor": plan["predecessor"]}
    digest = digest_named_fields("aistock_monthly_build_data_binding_v1", binding)
    _write(staging.parent / f".candidate.{op}.resume-{digest}.json", {
        "schema_version": "aistock_monthly_build_resume_v1", "binding": binding, "binding_digest": digest,
        "materialization_attempt": 2, "staging_relative_path": staging.relative_to(target.parent).as_posix(),
        "finalization_started": True, "steps": {s: {} for s in ("prepare", "qlib_dump_daily", "qlib_dump_minute", "finalize-bins")},
        "files": {"minute_bin/close.bin": _signature(price)}})
    build = {"output_refs": pins, "scope": {"dataset_manifest_sha256": "b" * 64}}
    _write(operation / "receipts/build.json", build)
    _write(operation / "checkpoints/build.json", {"plan_digest": _digest(plan), "request_digest": "request",
        "receipt": {"path": "receipts/build.json",
        "sha256": hashlib.sha256((operation / "receipts/build.json").read_bytes()).hexdigest()}})
    state = {"status": "FAILED", "current_stage": "DERIVE", "attempt": 3}
    downstream = {}
    store = SimpleNamespace(operation_root=lambda _: operation, read_state=lambda _: state, read_plan=lambda _: plan,
        read_request=lambda _: {"semantic_digest": "request"},
        read_checkpoint=lambda _, stage: (build if stage == "BUILD" and (operation / "checkpoints/build.json").exists()
                                          else {} if stage == "SOURCE" else downstream.get(stage)))
    active = tmp_path / "active.json"
    _write(active, {"generation": "old"})
    cas = SimpleNamespace(validate_reference_metadata=lambda r: SimpleNamespace(**r),
        get_json_bounded=lambda *a, **kw: {"rows": [{"index_code": "801011.SI", "trade_date": "2024-07-01",
            "pct_change": "3.48", "vol": "8175.6", "amount": "16559.9817"}]})
    return dict(store=store, operation_id=op, cas=cas, active_profile=active), target, staging, state, downstream, component


def mapping_payload(mapping):
    from backend.services.dataset_release.shared_sector_context import build_release_sw_l2_code_map_payload
    return build_release_sw_l2_code_map_payload(code_to_id=mapping.code_to_id, member_backed_codes=mapping.member_backed_codes,
        authority_id=mapping.mapping_authority["authority_id"], authority_sha256=mapping.mapping_authority["authority_sha256"])


def test_repack_dry_run_and_apply_keep_exports_and_old_seals(tmp_path, monkeypatch):
    args, target, staging, *_ = _fixture(tmp_path, monkeypatch)
    price = target / "minute_bin/close.bin"
    before = _signature(price)
    active = args["active_profile"].read_bytes()
    result = recover_unpublished_quote_policy(**args)
    assert result["applied"] is False and target.exists() and not staging.exists()
    assert result["exports_repeated"] is False
    result = recover_unpublished_quote_policy(**args, apply=True)
    assert result["applied"] and not target.exists() and staging.exists()
    assert _signature(staging / "minute_bin/close.bin") == before
    assert (staging / "minute_bin/close.bin").read_bytes() == b"accurate-export"
    assert args["active_profile"].read_bytes() == active
    assert (args["store"].operation_root("") / "receipts/build.json").exists()
    assert recover_unpublished_quote_policy(**args, apply=True) == result


def test_repack_recovers_crash_between_candidate_and_checkpoint_moves(tmp_path, monkeypatch):
    args, target, staging, *_ = _fixture(tmp_path, monkeypatch)
    rename = Path.rename
    def interrupt(path, destination):
        if path.name == "build.json":
            raise OSError("interrupted")
        return rename(path, destination)
    monkeypatch.setattr(Path, "rename", interrupt)
    with pytest.raises(OSError, match="interrupted"):
        recover_unpublished_quote_policy(**args, apply=True)
    assert not target.exists() and staging.exists()
    monkeypatch.setattr(Path, "rename", rename)
    assert recover_unpublished_quote_policy(**args, apply=True)["applied"]


@pytest.mark.parametrize("case", ["running", "deployed", "active", "changed-price", "bad-proof", "linked", "wrong-policy"])
def test_repack_rejects_unsafe_or_unexplained_candidates(tmp_path, monkeypatch, case):
    args, target, staging, state, downstream, component = _fixture(tmp_path, monkeypatch)
    if case == "running":
        state["status"] = "BUILDING"
    if case == "deployed":
        downstream["DEPLOY"] = {}
    if case == "active":
        _write(args["active_profile"], {"generation": "new"})
    if case == "changed-price":
        (target / "minute_bin/close.bin").write_bytes(b"different")
    if case == "bad-proof":
        args["cas"].get_json_bounded = lambda *a, **kw: {"rows": []}
    if case == "linked":
        lstat = Path.lstat
        monkeypatch.setattr(Path, "lstat", lambda path: SimpleNamespace(st_mode=stat.S_IFLNK) if path == component else lstat(path))
    if case == "wrong-policy":
        _write(component / "component_receipt.json", {"quote_availability": {"publication_policy_identity": "c" * 64}})
    with pytest.raises((ValueError, MonthlyBuildResumeError)):
        recover_unpublished_quote_policy(**args, apply=True)
    assert target.exists() and not staging.exists()
