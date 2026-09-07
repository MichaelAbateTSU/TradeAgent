from datetime import UTC, date, datetime

import pytest
from alembic import command
from alembic.config import Config
from pydantic import ValidationError
from sqlalchemy import Text, inspect
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from tradeagent.experimental_policy import ExperimentalSettings, OperationalCertificate, certificate
from tradeagent.persistence import Database, ProductionRepository, controls, events


def test_full_operational_certificate_survives_migration_and_cannot_be_truncated(
    tmp_path, monkeypatch
):
    url = f"sqlite:///{tmp_path / 'control-migration.db'}"
    monkeypatch.setenv("TRADEAGENT_DATABASE_URL", url)
    config = Config("alembic.ini")
    command.upgrade(config, "head")
    command.downgrade(config, "0007_daily_status_email")
    with Database(url) as database:
        old = {
            column["name"]: column for column in inspect(database.engine).get_columns("controls_v2")
        }
        assert old["control_value"]["type"].length == 500
        ProductionRepository(database).set_control("kill_switch", "active")
    command.upgrade(config, "head")
    settings = ExperimentalSettings(
        purpose="iex-practice",
        practice_start_date=date(2026, 9, 8),
        cohort_id="v20-iex-practice-20260908-r2",
        _env_file=None,
    )
    proof = certificate(
        settings,
        config_hash="a" * 64,
        code_sha="b" * 40,
        account_id="synthetic-paper-account",
        checks={
            "operator_confirmation": True,
            "paper_account_verified": True,
            "configured_execution_feed": True,
            "no_live_credential_environment": True,
        },
        now=datetime(2026, 9, 7, tzinfo=UTC),
        limitations=(
            "IEX practice only; excluded from research and the 60-session/60-round-trip floor",
            "synthetic mechanics do not establish live exchange execution or profitability",
            "every entry rechecks quote freshness, quantities, account, calendar and limits",
        ),
    )
    payload = proof.model_dump_json()
    assert len(payload) > 500
    key = f"{settings.cohort_id}:certificate"
    with Database(url) as database:
        current = {
            column["name"]: column for column in inspect(database.engine).get_columns("controls_v2")
        }
        assert isinstance(current["control_value"]["type"], Text)
        repo = ProductionRepository(database)
        assert repo.get_control("kill_switch") == "active"
        repo.set_control(key, payload)
        assert OperationalCertificate.model_validate_json(repo.get_control(key)) == proof
    with pytest.raises(ValueError, match="archive them explicitly"):
        command.downgrade(config, "0007_daily_status_email")
    with Database(url) as database:
        repo = ProductionRepository(database)
        assert repo.get_control(key) == payload
        assert repo.get_control("kill_switch") == "active"


def test_fresh_postgres_schema_does_not_limit_certificate_length():
    ddl = str(CreateTable(controls).compile(dialect=postgresql.dialect()))
    assert "control_value TEXT NOT NULL" in ddl
    assert isinstance(controls.c.control_value.type, Text)


@pytest.mark.parametrize("cohort", ["", "x" * 52])
def test_cohort_ids_fail_before_exceeding_persistent_key_width(cohort):
    with pytest.raises(ValidationError):
        ExperimentalSettings(cohort_id=cohort, _env_file=None)


def test_largest_accepted_cohort_fits_evidence_and_control_keys():
    cohort = ExperimentalSettings(cohort_id="x" * 51, _env_file=None).cohort_id
    evidence_id = "a" * 64
    for suffix in ("pre_context", "extraction"):
        assert len(f"{cohort}:{evidence_id}:{suffix}") <= events.c.trace_id.type.length
    for suffix in ("certificate", "calibration_status", "source_watermark"):
        assert len(f"{cohort}:{suffix}") <= controls.c.control_key.type.length
