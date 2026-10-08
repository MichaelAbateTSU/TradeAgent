"""Real PG only, opt-in. No SQLite replacement or default production credentials.

Set CONFIRMATION_PG_VALIDATION_OPT_IN=yes, CONFIRMATION_PG_SCRATCH_URL and
CONFIRMATION_PG_SCRATCH_DATABASE and CONFIRMATION_PG_SCRATCH_MAJOR_VERSION for an
already-existing dedicated loopback scratch DB. The CLI supervisor bounds execution.
Default full-suite behavior is an explicit skip; it does not read URL credentials.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import pytest

from tradeagent.kraken_confirmation_pg_validation import supervise


def test_real_pg_rows_fencing_report_pool_and_cadence() -> None:
    if os.getenv("CONFIRMATION_PG_VALIDATION_OPT_IN") != "yes":
        pytest.skip("real PostgreSQL scratch validation explicitly not opted in")
    address = os.getenv("CONFIRMATION_PG_SCRATCH_URL")
    database = os.getenv("CONFIRMATION_PG_SCRATCH_DATABASE")
    major = os.getenv("CONFIRMATION_PG_SCRATCH_MAJOR_VERSION")
    if not address or not database or not major or not major.isdecimal():
        pytest.fail("PG opt-in requires explicit scratch URL, expected database and major version")
    path = (
        Path(__file__).parents[1]
        / "docs"
        / "experiments"
        / ("2026-10-book-confirmation-72h-v2")
        / "frozen-protocol.json"
    )
    result = supervise(
        argparse.Namespace(
            scratch_url_env="CONFIRMATION_PG_SCRATCH_URL",
            expected_database=database,
            expected_major_version=int(major),
            archived_protocol=path,
        )
    )
    assert result.status == "passed", result.model_dump_json()
    assert result.cleanup_verified is True
    assert result.pg_lock_pool_concurrency_validated is True
    assert result.formal_claim_created is False and result.existing_table_writes == 0
    assert result.existing_tables_unchanged is True
    assert result.server_version_num is not None and result.server_version_num // 10000 == int(
        major
    )
