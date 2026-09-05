from __future__ import annotations

import json
from pathlib import Path

import pytest

from tradeagent.persistence import Database
from tradeagent.promotion import (
    PROMOTION_AUTHORIZATION,
    StrategyPromotionService,
)


def test_only_qualified_audited_strategy_can_be_promoted(tmp_path: Path) -> None:
    database = Database(f"sqlite:///{tmp_path / 'promotions.db'}")
    database.initialize()
    service = StrategyPromotionService(database)
    audit = tmp_path / "holdout-opened.json"
    audit.write_text(json.dumps({"holdout_hash": "h" * 64}), encoding="utf-8")

    with pytest.raises(ValueError, match="unqualified"):
        service.promote(
            strategy_id="strategy-v1",
            dataset_hash="d" * 64,
            config_hash="c" * 64,
            holdout_hash="h" * 64,
            git_sha="abc123",
            qualified=False,
            holdout_audit_path=audit,
            approved_by="owner",
            authorization=PROMOTION_AUTHORIZATION,
        )
    promotion_id = service.promote(
        strategy_id="strategy-v1",
        dataset_hash="d" * 64,
        config_hash="c" * 64,
        holdout_hash="h" * 64,
        git_sha="abc123",
        qualified=True,
        holdout_audit_path=audit,
        approved_by="owner",
        authorization=PROMOTION_AUTHORIZATION,
    )

    assert promotion_id
    assert service.is_strategy_qualified("strategy-v1")
    assert not service.is_strategy_qualified("missing")
    assert service.revoke("strategy-v1")
    assert not service.is_strategy_qualified("strategy-v1")
    assert not service.revoke("strategy-v1")
    database.dispose()


def test_promotion_requires_exact_authorization_and_matching_holdout(
    tmp_path: Path,
) -> None:
    database = Database(f"sqlite:///{tmp_path / 'promotions.db'}")
    database.initialize()
    service = StrategyPromotionService(database)
    audit = tmp_path / "holdout-opened.json"
    audit.write_text(json.dumps({"holdout_hash": "x" * 64}), encoding="utf-8")
    arguments = {
        "strategy_id": "strategy-v1",
        "dataset_hash": "d" * 64,
        "config_hash": "c" * 64,
        "holdout_hash": "h" * 64,
        "git_sha": "abc123",
        "qualified": True,
        "holdout_audit_path": audit,
        "approved_by": "owner",
    }

    with pytest.raises(ValueError, match="authorization"):
        service.promote(**arguments, authorization="yes")
    with pytest.raises(ValueError, match="does not match"):
        service.promote(**arguments, authorization=PROMOTION_AUTHORIZATION)
    audit.unlink()
    with pytest.raises(ValueError, match="audit"):
        service.promote(**arguments, authorization=PROMOTION_AUTHORIZATION)
    database.dispose()
