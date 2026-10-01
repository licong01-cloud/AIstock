from datetime import date

from backend.services.advisory_model_first import historical_price_replay_cli as cli


def test_cli_runs_batch_replay_without_prospective_clock(monkeypatch, tmp_path, capsys):
    (tmp_path / "runtime.env").write_text("", encoding="utf-8")

    class Request:
        pass

    class Receipt:
        def model_dump(self, *, mode):
            assert mode == "json"
            return {
                "status": "PUBLISHED",
                "evidence_level": "HISTORICAL_REPLAY",
                "decision_use": "NAVIGATION_ONLY",
            }

    captured = {}

    monkeypatch.setattr(
        cli,
        "prepare_historical_price_replay_request",
        lambda **kwargs: (captured.update(kwargs) or Request(), tmp_path / "source.parquet"),
    )

    class Service:
        def run(self, **kwargs):
            captured.update(kwargs)
            return Receipt()

    monkeypatch.setattr(cli, "AdvisoryHistoricalPriceReplayService", Service)
    result = cli.main(
        [
            "--env-file",
            str(tmp_path / "runtime.env"),
            "--model-root",
            str(tmp_path / "models"),
            "--output-root",
            str(tmp_path / "out"),
            "--price-range-bundle-id",
            "a" * 64,
            "--decision-start",
            "2026-01-05",
            "--decision-end",
            "2026-01-06",
            "--replay-as-of",
            "2026-01-08",
        ]
    )
    assert result == 0
    assert captured["decision_start_trade_date"] == date(2026, 1, 5)
    assert captured["replay_as_of_date"] == date(2026, 1, 8)
    assert "HISTORICAL_REPLAY" in capsys.readouterr().out
