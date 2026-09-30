import pytest
import subprocess
import sys

from scripts import prepare_etf_share_size_deployment as deploy


def test_cli_loads_in_fresh_process_without_pythonpath(tmp_path):
    result = subprocess.run([sys.executable, str(deploy.ROOT / "scripts/prepare_etf_share_size_deployment.py"), "--help"],
                            cwd=tmp_path, capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr


def test_exact_schedule_plan_preserves_existing_operator_configuration():
    plan = deploy.schedule_plan()
    assert [(r["dataset"], r["mode"]) for r in plan] == [
        ("etf_share_size", "incremental"), ("etf_basic_snapshots", "init")]
    assert "DO NOTHING" in deploy.SCHEDULE_SQL
    assert "DO UPDATE" not in deploy.SCHEDULE_SQL


def test_production_apply_refused_before_migration():
    class Cursor:
        def __enter__(self):
            return self
        def __exit__(self, *args):
            return False
        def execute(self, sql):
            assert sql == "SELECT current_database()"
        def fetchone(self):
            return ("aistock",)
    class Connection:
        def cursor(self):
            return Cursor()
    with pytest.raises(ValueError, match="DEV apply requires"):
        deploy.apply_dev(Connection())


@pytest.mark.parametrize("deployed", [False, True])
def test_readback_distinguishes_deployment_from_provider_failure(deployed):
    class Cursor:
        def __enter__(self):
            return self
        def __exit__(self, *args):
            return False
        def execute(self, sql, params=()):
            self.sql = sql
            assert not any(word in sql for word in ("INSERT", "UPDATE", "DELETE", "CREATE"))
        def fetchone(self):
            return ("aistock_dev",)
        def fetchall(self):
            if not deployed:
                return []
            if "information_schema" in self.sql:
                return [("etf_share_size", k, v) for k, v in {
                    "trade_date": "date", "ts_code": "text", "total_share": "numeric",
                    "total_size": "numeric", "nav": "numeric", "close": "numeric"}.items()] + [
                    ("etf_basic_snapshots", "snapshot_date", "date"),
                    ("etf_basic_snapshots", "ts_code", "text")]
            if "hypertables" in self.sql:
                return [(d,) for d in deploy.DATASETS]
            if "stats_config" in self.sql:
                return [(d, f"market.{d}", True) for d in deploy.DATASETS]
            return [(r["dataset"], r["mode"], True) for r in deploy.schedule_plan()]
    class Connection:
        def cursor(self):
            return Cursor()
    result = deploy.preflight(Connection())
    assert result["complete"] is deployed
    assert result["production_write"] is False
    assert len(result["migration_sha256"]) == 64
    if not deployed:
        assert "etf_share_size:deployment_table_missing" in result["issues"]
