from __future__ import annotations

import hashlib
import csv
import os
from datetime import date
from pathlib import Path

import pytest

from backend.services.dataset_release.daily_minute_materializer import (
    DAILY_FIELDS,
    MINUTE_FIELDS,
    DailyMinuteBinFinalizer,
    DailyMinuteCsvPreparer,
    DailyMinuteMaterializationError,
    DailyMinuteMaterializationSpec,
    DailyMinuteMaterializer,
    QlibDumpToolchain,
    SupervisedDumpFailed,
    build_composite_canonical_rows,
    build_selective_override_canonical_rows,
    _write_ordered_stock_csvs,
)
from backend.services.dataset_release.index_contract import DOMESTIC_INDEX_DEFINITIONS
from backend.services.dataset_release.pit import freeze_pit_snapshot
from backend.services.dataset_release.monthly_csv_resume import MonthlyCsvResume


def _resume_spec(tmp_path):
    return DailyMinuteMaterializationSpec(dataset='minute_bin', staging_root=tmp_path,
        project_root=tmp_path, cutoff=date(2026,7,31), effective_start=date(2026,7,1),
        pit_snapshot=_pit(date(2026,7,31)), dump_workers=1, toolchain=_toolchain())


def test_partial_month_csv_failure_reuses_only_closed_stock_files(tmp_path):
    spec = _resume_spec(tmp_path)
    root = tmp_path/'csv'
    def session():
        return MonthlyCsvResume(root, binding={'source_bundle_sha256':'a'*64}, dataset='minute_bin',
                                stock_codes=('000001.SZ','600000.SH'),index_codes=(),cutoff=spec.cutoff)
    source = list(_rows('minute_bin'))
    def failure():
        yield from source[:3]
        raise RuntimeError('next source stock failed')
    with pytest.raises(RuntimeError,match='next source stock'):
        session().prepare(spec, rows=failure())
    closed = root/'000001.SZ.csv'
    before = closed.stat()
    assert closed.is_file() and not (root/'600000.SH.csv').exists()
    receipt = session().prepare(spec, rows=source[2:]).receipt
    assert receipt['csv']['rows'] == 4
    assert receipt['actual_work']['stock_rows_reused'] == 2
    assert receipt['actual_work']['stock_rows_transformed'] == 2
    assert closed.stat().st_mtime_ns == before.st_mtime_ns


@pytest.mark.parametrize('fault',['source','changed','unknown','nonfinite','truncated'])
def test_month_csv_recovery_rejects_real_identity_and_content_errors(tmp_path, fault):
    spec = _resume_spec(tmp_path)
    root = tmp_path/'csv'
    root.mkdir()
    _write_ordered_stock_csvs(_rows('minute_bin'),csv_root=root,dataset='minute_bin',cutoff=spec.cutoff,checkpoint=lambda:None)
    kwargs = dict(dataset='minute_bin',stock_codes=('000001.SZ','600000.SH'),index_codes=(),cutoff=spec.cutoff)
    binding={'source_bundle_sha256':'a'*64}
    path=root/'000001.SZ.csv'
    if fault in {'source','changed'}:
        MonthlyCsvResume(root,binding=binding,**kwargs)
        if fault=='source':
            binding={'source_bundle_sha256':'b'*64}
        else:
            path.write_bytes(path.read_bytes()+b'corrupt')
    elif fault=='unknown':
        path.rename(root/'000002.SZ.csv')
    elif fault=='nonfinite':
        path.write_bytes(path.read_bytes().replace(b',1,',b',NaN,',1))
    else:
        path.write_bytes(path.read_bytes()[:-1])
    with pytest.raises(DailyMinuteMaterializationError):
        MonthlyCsvResume(root,binding=binding,**kwargs)


def test_completed_month_csv_retry_does_not_reparse_or_rehash_data(tmp_path, monkeypatch):
    spec = _resume_spec(tmp_path)
    root = tmp_path/'csv'
    root.mkdir()
    _write_ordered_stock_csvs(_rows('minute_bin'),csv_root=root,dataset='minute_bin',cutoff=spec.cutoff,checkpoint=lambda:None)
    kwargs=dict(binding={'source_bundle_sha256':'a'*64},dataset='minute_bin',
                stock_codes=('000001.SZ','600000.SH'),index_codes=(),cutoff=spec.cutoff)
    first=MonthlyCsvResume(root,**kwargs)
    def forbidden(*args,**kw):
        raise AssertionError('closed CSV must use its native recorded signature')
    monkeypatch.setattr('backend.services.dataset_release.monthly_csv_resume._inspect',forbidden)
    assert MonthlyCsvResume(root,**kwargs).files == first.files


@pytest.mark.parametrize('change',['metadata','missing'])
def test_completed_month_csv_handles_metadata_only_change_and_exact_file_recovery(tmp_path,change):
    spec=_resume_spec(tmp_path)
    root=tmp_path/'csv'
    kwargs=dict(binding={'source_bundle_sha256':'a'*64},dataset='minute_bin',
                stock_codes=('000001.SZ','600000.SH'),index_codes=(),cutoff=spec.cutoff)
    first=MonthlyCsvResume(root,**kwargs)
    expected=first.prepare(spec,rows=_rows('minute_bin')).receipt
    path=root/'000001.SZ.csv'
    if change=='metadata':
        value=path.stat()
        os.utime(path,ns=(value.st_atime_ns,value.st_mtime_ns+1000000))
        current=MonthlyCsvResume(root,**kwargs)
        receipt=current.prepare(spec,rows=()).receipt
        assert receipt['actual_work']['stock_rows_transformed']==0
    else:
        path.unlink()
        current=MonthlyCsvResume(root,**kwargs)
        receipt=current.prepare(spec,rows=(row for row in _rows('minute_bin') if row['instrument']=='000001.SZ')).receipt
        assert receipt['actual_work']['stock_rows_transformed']==2
    assert receipt['csv']==expected['csv']


def test_native_csv_and_metadata_crash_fragments_are_retained_not_published(tmp_path):
    spec=_resume_spec(tmp_path)
    root=tmp_path/'csv'
    metadata=root/'.completed'
    metadata.mkdir(parents=True)
    fragments=(root/'.000001.SZ.abcdefgh.partial.csv', metadata/'.binding.json.abcdef12.partial')
    for path in fragments:
        path.write_bytes(b'unfinished native bytes')
    resume=MonthlyCsvResume(root,binding={'source_bundle_sha256':'a'*64},dataset='minute_bin',
                            stock_codes=('000001.SZ','600000.SH'),index_codes=(),cutoff=spec.cutoff)
    receipt=resume.prepare(spec,rows=_rows('minute_bin')).receipt
    assert receipt['csv']['rows']==4
    assert not any(path.exists() for path in fragments)
    preserved=list((root/'.unfinished').iterdir())
    assert len(preserved)==2 and all(path.read_bytes()==b'unfinished native bytes' for path in preserved)
    assert not any('.partial' in path.name for path in preserved)


def test_month_csv_resume_rejects_linked_work_root(tmp_path):
    from backend.services.dataset_release.monthly_build_resume import MonthlyBuildResumeError
    outside=tmp_path/'outside'
    outside.mkdir()
    link=tmp_path/'linked'
    try:
        link.symlink_to(outside,target_is_directory=True)
    except OSError:
        pytest.skip('symbolic links are unavailable in this environment')
    with pytest.raises(MonthlyBuildResumeError,match='contains a link'):
        MonthlyCsvResume(link,binding={'source_bundle_sha256':'a'*64},dataset='minute_bin',
                         stock_codes=('000001.SZ',),index_codes=(),cutoff=date(2026,7,31))
    assert not list(outside.iterdir())


@pytest.mark.parametrize("dataset", ["daily_bin", "minute_bin"])
def test_stock_csv_receipt_dates_match_exact_serialized_boundaries(tmp_path: Path, dataset: str) -> None:
    receipt = _write_ordered_stock_csvs(
        _rows(dataset), csv_root=tmp_path, dataset=dataset,
        cutoff=date(2026, 7, 31), checkpoint=lambda: None,
    )
    for item in receipt["files"]:
        with (tmp_path / f'{item["instrument"]}.csv').open(encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
        assert item["start"] == rows[0]["date"]
        assert item["end"] == rows[-1]["date"]


def _pit(cutoff: date):
    return freeze_pit_snapshot(
        [
            {
                "ts_code": code,
                "eligible_start": date(2026, 7, 1),
                "eligible_end": cutoff,
                "entry_reason": None,
                "exit_reason": None,
            }
            for code in ("000001.SZ", "600000.SH")
        ],
        universe_key="shsz_st_pit_active_v1",
        rule_version="st_pub_next_trade_restore_active_l_v1",
        scope_start=date(2026, 7, 1),
        cutoff=cutoff,
        state_identity="pit-fixture",
        source_fingerprint_sha256="a" * 64,
        parameter_hash="b" * 64,
    )


def _toolchain() -> QlibDumpToolchain:
    fixture_file = Path(__file__).resolve()
    digest = hashlib.sha256(fixture_file.read_bytes()).hexdigest()
    return QlibDumpToolchain(
        distro="Ubuntu",
        conda_sh="/opt/conda.sh",
        conda_env="rdagent-gpu",
        dump_script_wsl="/opt/rdagent/scripts/dump_bin.py",
        dump_script_windows=fixture_file,
        dump_script_sha256=digest,
        guardian_python="/usr/bin/python3",
        guardian_script_wsl="/opt/aistock/wsl_resource_guardian.py",
        guardian_script_windows=fixture_file,
        guardian_script_sha256=digest,
        heartbeat_path_wsl="/mnt/x/control/heartbeat.json",
        runner_python_wsl="/usr/bin/python3",
        runner_script_wsl="/opt/aistock/subprocess_runner.py",
        runner_script_windows=fixture_file,
        runner_script_sha256=digest,
    )


def _rows(dataset: str):
    fields = DAILY_FIELDS if dataset == "daily_bin" else MINUTE_FIELDS
    timestamps = (
        ("2026-07-30", "2026-07-31") if dataset == "daily_bin" else ("2026-07-30 09:31:00", "2026-07-31 15:00:00")
    )
    for code in ("000001.SZ", "600000.SH"):
        for ordinal, timestamp in enumerate(timestamps):
            yield {
                "datetime": timestamp,
                "instrument": code,
                **{field: float(ordinal + 1) for field in fields},
            }


def _index_csvs(root: Path) -> None:
    root.mkdir()
    for definition in DOMESTIC_INDEX_DEFINITIONS:
        values = ["1"] * len(DAILY_FIELDS)
        (root / f"{definition.daily_code}.csv").write_text(
            "date,symbol," + ",".join(DAILY_FIELDS) + "\n"
            f"2026-07-31,{definition.daily_code}," + ",".join(values) + "\n",
            encoding="utf-8",
        )


class FakeExecutor:
    def __init__(self, working: Path, *, dataset: str, returncode: int = 0) -> None:
        self.working = working
        self.dataset = dataset
        self.returncode = returncode
        self.calls = []

    def run_supervised(self, command, **kwargs):
        self.calls.append((tuple(command), kwargs))
        if self.returncode == 0:
            frequency = "day" if self.dataset == "daily_bin" else "1min"
            (self.working / "calendars").mkdir(parents=True)
            (self.working / "instruments").mkdir()
            (self.working / "features").mkdir()
            calendar = (
                "2026-07-30\n2026-07-31\n"
                if self.dataset == "daily_bin"
                else "2026-07-30 09:31:00\n2026-07-31 15:00:00\n"
            )
            (self.working / "calendars" / f"{frequency}.txt").write_text(calendar, encoding="utf-8")
            codes = {"000001.SZ", "600000.SH"}
            if self.dataset == "daily_bin":
                codes.update(item.daily_code for item in DOMESTIC_INDEX_DEFINITIONS)
            suffix = "day" if self.dataset == "daily_bin" else "1min"
            for code in codes:
                feature = self.working / "features" / code.lower()
                feature.mkdir()
                (feature / f"close.{suffix}.bin").write_bytes(b"fixture-bin")
        return {
            "returncode": self.returncode,
            "pid": 123,
            "elapsed_seconds": 0.01,
            "log_segments": [
                {
                    "stream": "stdout",
                    "generation": 0,
                    "path": "temporary/log",
                    "size_bytes": 16,
                    "sha256": "a" * 64,
                    "cas_ref": {
                        "sha256": "a" * 64,
                        "size": 16,
                        "relative_path": "cas/aa/" + "a" * 64,
                    },
                }
            ],
            "active_processes": 0,
            "job_peak_commit_bytes": 1024,
            "wsl_readback": {"memory.max": 2048, "memory.swap.max": 0},
            "result_path": "temporary/result.json",
            "log_root": "temporary/logs",
        }


def test_daily_materializer_uses_supervised_wsl_and_keeps_index_authority_private(
    tmp_path: Path,
) -> None:
    staging = tmp_path / "staging"
    project = tmp_path / "project"
    index = tmp_path / "index"
    staging.mkdir()
    project.mkdir()
    _index_csvs(index)
    source_bytes = (index / "000300.SH.csv").read_bytes()
    spec = DailyMinuteMaterializationSpec(
        dataset="daily_bin",
        staging_root=staging,
        project_root=project,
        cutoff=date(2026, 7, 31),
        effective_start=date(2026, 7, 30),
        pit_snapshot=_pit(date(2026, 7, 31)),
        dump_workers=2,
        toolchain=_toolchain(),
        index_csv_root=index,
    )
    executor = FakeExecutor(staging / "daily_bin" / ".qlib.working", dataset="daily_bin")

    first = DailyMinuteMaterializer().materialize(spec, rows=_rows("daily_bin"), executor=executor)
    second = DailyMinuteMaterializer().materialize(spec, rows=_rows("daily_bin"), executor=executor)

    assert first.receipt["status"] == "PASS"
    assert second.receipt == first.receipt
    assert len(executor.calls) == 1
    command, kwargs = executor.calls[0]
    assert command[:2] == ("bash", "-lc")
    assert kwargs["runtime"] == "wsl"
    assert kwargs["execution_id"] == "daily_bin-qlib-dump"
    assert kwargs["wsl"].runner_script_wsl == "/opt/aistock/subprocess_runner.py"
    assert "dump_all" in command[2]
    assert first.receipt["bin"]["index_codes"] == [item.daily_code for item in DOMESTIC_INDEX_DEFINITIONS]
    assert "result_path" not in first.receipt["supervised_child"]
    assert "path" not in first.receipt["supervised_child"]["log_segments"][0]
    assert first.receipt["supervised_child"]["log_segments"][0]["cas_ref"] == {
        "sha256": "a" * 64,
        "size": 16,
        "relative_path": "cas/aa/" + "a" * 64,
    }
    copied = staging / "daily_bin" / "csv" / "000300.SH.csv"
    (index / "000300.SH.csv").write_bytes(b"changed-source")
    assert copied.read_bytes() == source_bytes


def test_minute_materializer_has_no_index_and_never_runs_unsupervised(tmp_path: Path) -> None:
    staging = tmp_path / "staging"
    project = tmp_path / "project"
    staging.mkdir()
    project.mkdir()
    spec = DailyMinuteMaterializationSpec(
        dataset="minute_bin",
        staging_root=staging,
        project_root=project,
        cutoff=date(2026, 7, 31),
        effective_start=date(2026, 7, 30),
        pit_snapshot=_pit(date(2026, 7, 31)),
        dump_workers=2,
        toolchain=_toolchain(),
    )
    executor = FakeExecutor(staging / "minute_bin" / ".qlib.working", dataset="minute_bin")

    receipt = DailyMinuteMaterializer().materialize(spec, rows=_rows("minute_bin"), executor=executor).receipt

    assert receipt["indices"] == {"codes": [], "files": []}
    assert receipt["bin"]["index_codes"] == []
    assert receipt["memory_contract"]["cross_instrument_frames_retained"] == 0


def test_failed_or_unordered_input_never_publishes_bin(tmp_path: Path) -> None:
    staging = tmp_path / "staging"
    project = tmp_path / "project"
    staging.mkdir()
    project.mkdir()
    spec = DailyMinuteMaterializationSpec(
        dataset="minute_bin",
        staging_root=staging,
        project_root=project,
        cutoff=date(2026, 7, 31),
        effective_start=date(2026, 7, 30),
        pit_snapshot=_pit(date(2026, 7, 31)),
        dump_workers=1,
        toolchain=_toolchain(),
    )
    failed = FakeExecutor(
        staging / "minute_bin" / ".qlib.working",
        dataset="minute_bin",
        returncode=17,
    )
    with pytest.raises(SupervisedDumpFailed):
        DailyMinuteMaterializer().materialize(spec, rows=_rows("minute_bin"), executor=failed)
    assert not (staging / "minute_bin" / "qlib").exists()

    another = tmp_path / "another"
    another.mkdir()
    unordered_spec = DailyMinuteMaterializationSpec(
        dataset="minute_bin",
        staging_root=another,
        project_root=project,
        cutoff=date(2026, 7, 31),
        effective_start=date(2026, 7, 30),
        pit_snapshot=_pit(date(2026, 7, 31)),
        dump_workers=1,
        toolchain=_toolchain(),
    )
    unordered = list(_rows("minute_bin"))[::-1]
    never = FakeExecutor(another / "minute_bin" / ".qlib.working", dataset="minute_bin")
    with pytest.raises(DailyMinuteMaterializationError, match="globally ordered"):
        DailyMinuteMaterializer().materialize(unordered_spec, rows=unordered, executor=never)
    assert never.calls == []


def test_prepare_and_finalize_are_separate_supervised_stage_boundaries(
    tmp_path: Path,
) -> None:
    staging = tmp_path / "staging"
    project = tmp_path / "project"
    staging.mkdir()
    project.mkdir()
    spec = DailyMinuteMaterializationSpec(
        dataset="minute_bin",
        staging_root=staging,
        project_root=project,
        cutoff=date(2026, 7, 31),
        effective_start=date(2026, 7, 30),
        pit_snapshot=_pit(date(2026, 7, 31)),
        dump_workers=2,
        toolchain=_toolchain(),
    )

    preparation = DailyMinuteCsvPreparer().prepare(spec, rows=_rows("minute_bin")).receipt
    assert not (staging / "minute_bin" / "qlib").exists()
    fake = FakeExecutor(staging / "minute_bin" / ".qlib.working", dataset="minute_bin")
    child = fake.run_supervised(("fixture",))
    final = (
        DailyMinuteBinFinalizer()
        .finalize(
            spec,
            preparation=preparation,
            supervised_child=child,
        )
        .receipt
    )

    assert final["status"] == "PASS"
    assert final["sealed_canonical_rows"] == preparation["sealed_canonical_rows"]
    assert (staging / "minute_bin" / "qlib").is_dir()


def test_dump_command_supports_only_explicit_full_or_incremental_modes(tmp_path: Path) -> None:
    from backend.services.dataset_release.daily_minute_materializer import (
        build_qlib_dump_command,
    )

    command = build_qlib_dump_command(
        dataset="daily_bin",
        csv_root=tmp_path / "csv",
        working_root=tmp_path / "qlib",
        dump_workers=2,
        toolchain=_toolchain(),
        mode="dump_update",
    )
    assert "dump_update" in command[2]
    csv_root = tmp_path / "batched-csv"
    csv_root.mkdir()
    (csv_root / "batch_manifest.json").write_text("{}\n", encoding="utf-8")
    batched_command = build_qlib_dump_command(
        dataset="daily_bin",
        csv_root=csv_root,
        working_root=tmp_path / "qlib-batched",
        dump_workers=2,
        toolchain=_toolchain(),
        mode="batched_patch",
    )
    assert "dataset_release_qlib_batched_dump.py" in batched_command[2]
    assert "--manifest" in batched_command[2]
    assert "dump_update" not in batched_command[2]
    batched_full_command = build_qlib_dump_command(
        dataset="daily_bin",
        csv_root=csv_root,
        working_root=tmp_path / "qlib-batched-full",
        dump_workers=2,
        toolchain=_toolchain(),
        mode="batched_full",
    )
    assert "dataset_release_qlib_batched_dump.py" in batched_full_command[2]
    assert "--manifest" in batched_full_command[2]
    with pytest.raises(DailyMinuteMaterializationError, match="not allowlisted"):
        build_qlib_dump_command(
            dataset="daily_bin",
            csv_root=tmp_path / "csv",
            working_root=tmp_path / "qlib",
            dump_workers=2,
            toolchain=_toolchain(),
            mode="unsafe",
        )


def _canonical_file(*, start: str, end: str, digest: str, rows: int = 1) -> dict:
    return {
        "instrument": "000001.SZ",
        "rows": rows,
        "sha256": digest * 64,
        "size_bytes": rows * 100,
        "start": start,
        "end": end,
    }


def test_composite_preserves_override_lineage_across_later_month_delta() -> None:
    baseline = {
        "schema_version": "dataset_release_sealed_qlib_csv_rows_v1",
        "dataset": "daily_bin",
        "root_relative_path": "daily_bin/csv",
        "ordered_fields": ["date", "symbol", *DAILY_FIELDS],
        "rows": 1,
        "files": [
            {
                **_canonical_file(start="2026-06-30", end="2026-06-30", digest="a"),
                "relative_path": "000001.SZ.csv",
            }
        ],
    }
    july = build_composite_canonical_rows(
        dataset="daily_bin",
        baseline=baseline,
        patch_preparation={
            "csv": {
                "rows": 1,
                "files": [_canonical_file(start="2026-07-31", end="2026-07-31", digest="b")],
            }
        },
        delta_root_relative_path="daily_bin/csv_deltas/202607",
    )
    override = build_selective_override_canonical_rows(
        dataset="daily_bin",
        baseline=july,
        patch_preparation={
            "csv": {
                "rows": 2,
                "files": [
                    _canonical_file(
                        start="2026-06-30",
                        end="2026-07-31",
                        digest="c",
                        rows=2,
                    )
                ],
            }
        },
        override_root_relative_path="daily_bin/csv_overrides/revision-1",
        invalidation_scopes=(
            {
                "kind": "qfq_denominator_change",
                "instrument": "000001.SZ",
            },
        ),
    )
    august = build_composite_canonical_rows(
        dataset="daily_bin",
        baseline=override,
        patch_preparation={
            "csv": {
                "rows": 1,
                "files": [_canonical_file(start="2026-08-31", end="2026-08-31", digest="d")],
            }
        },
        delta_root_relative_path="daily_bin/csv_deltas/202608",
    )

    assert august["merge_contract"] == ("instrument_active_segments_with_explicit_overrides_v1")
    assert august["overrides"] == override["overrides"]
    assert [item["root_relative_path"] for item in august["segments"]] == [
        "daily_bin/csv_overrides/revision-1",
        "daily_bin/csv_deltas/202608",
    ]
    assert {item["root_relative_path"] for item in august["overrides"][0]["superseded_segments"]} == {
        "daily_bin/csv",
        "daily_bin/csv_deltas/202607",
    }


def test_selective_override_requires_code_local_scope() -> None:
    baseline = {
        "schema_version": "dataset_release_sealed_qlib_csv_rows_v1",
        "dataset": "daily_bin",
        "root_relative_path": "daily_bin/csv",
        "ordered_fields": ["date", "symbol", *DAILY_FIELDS],
        "rows": 1,
        "files": [
            {
                **_canonical_file(start="2026-07-31", end="2026-07-31", digest="a"),
                "relative_path": "000001.SZ.csv",
            }
        ],
    }
    with pytest.raises(
        DailyMinuteMaterializationError,
        match="code-local invalidation authority",
    ):
        build_selective_override_canonical_rows(
            dataset="daily_bin",
            baseline=baseline,
            patch_preparation={
                "csv": {
                    "rows": 1,
                    "files": [_canonical_file(start="2026-07-31", end="2026-07-31", digest="b")],
                }
            },
            override_root_relative_path="daily_bin/csv_overrides/revision",
            invalidation_scopes=({"kind": "qfq_series_tail", "instrument": "000001.SZ"},),
        )
