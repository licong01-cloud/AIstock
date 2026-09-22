from __future__ import annotations

from scripts.hmm_risk import run_rotation_l2 as subject


def test_preflight_cli_requires_and_forwards_explicit_source_authorities(tmp_path, monkeypatch) -> None:
    captured = {}

    def build(**kwargs):
        captured.update(kwargs)
        return {"input_hash": "a" * 64}

    monkeypatch.setattr(subject, "build_rotation_l2_input_bundle", build)
    profile = (tmp_path / "profile.json").resolve()
    root = (tmp_path / "dataset").resolve()
    security = (tmp_path / "security.json").resolve()
    absence = (tmp_path / "absence.json").resolve()
    output = tmp_path / "output.json"

    result = subject.main(
        [
            "preflight",
            "--active-profile",
            str(profile),
            "--dataset-root",
            str(root),
            "--source-commit",
            "b" * 40,
            "--security-identity-manifest",
            str(security),
            "--security-identity-sha256",
            "c" * 64,
            "--provider-absence-manifest",
            str(absence),
            "--provider-absence-sha256",
            "d" * 64,
            "--output",
            str(output),
        ]
    )

    assert result == 0
    assert output.is_file()
    assert captured == {
        "active_profile_path": profile,
        "dataset_root": root,
        "source_commit": "b" * 40,
        "security_identity_manifest_path": security,
        "security_identity_sha256": "c" * 64,
        "provider_absence_manifest_path": absence,
        "provider_absence_sha256": "d" * 64,
    }


def test_preflight_cli_does_not_overwrite_existing_artifact(tmp_path, monkeypatch, capsys) -> None:
    monkeypatch.setattr(subject, "build_rotation_l2_input_bundle", lambda **_kwargs: {"input_hash": "a" * 64})
    output = tmp_path / "output.json"
    output.write_text("existing", encoding="utf-8")
    values = [
        "preflight",
        "--active-profile",
        str((tmp_path / "profile").resolve()),
        "--dataset-root",
        str((tmp_path / "dataset").resolve()),
        "--source-commit",
        "b" * 40,
        "--security-identity-manifest",
        str((tmp_path / "security").resolve()),
        "--security-identity-sha256",
        "c" * 64,
        "--provider-absence-manifest",
        str((tmp_path / "absence").resolve()),
        "--provider-absence-sha256",
        "d" * 64,
        "--output",
        str(output),
    ]

    assert subject.main(values) == 1
    assert "refusing to overwrite artifact" in capsys.readouterr().err
    assert output.read_text(encoding="utf-8") == "existing"
