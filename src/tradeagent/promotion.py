from __future__ import annotations

import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from uuid import UUID, uuid4

from sqlalchemy import insert, select, update

from tradeagent.persistence import Database, strategy_promotions

PROMOTION_AUTHORIZATION = "I approve this qualified strategy for autonomous paper trading"


class StrategyPromotionService:
    def __init__(self, database: Database) -> None:
        self._database = database

    def promote(
        self,
        *,
        strategy_id: str,
        dataset_hash: str,
        config_hash: str,
        holdout_hash: str,
        git_sha: str,
        qualified: bool,
        holdout_audit_path: Path,
        approved_by: str,
        authorization: str,
    ) -> UUID:
        if not qualified:
            raise ValueError("unqualified strategy cannot be promoted")
        if authorization != PROMOTION_AUTHORIZATION:
            raise ValueError("exact paper promotion authorization is required")
        if not holdout_audit_path.exists():
            raise ValueError("terminal holdout audit is required")
        audit = json.loads(holdout_audit_path.read_text(encoding="utf-8"))
        if audit.get("holdout_hash") != holdout_hash:
            raise ValueError("holdout audit hash does not match promotion evidence")
        approved_at = datetime.now(UTC)
        digest_payload = "|".join(
            [
                strategy_id,
                dataset_hash,
                config_hash,
                holdout_hash,
                git_sha,
                approved_by,
                approved_at.isoformat(),
            ]
        )
        digest = sha256(digest_payload.encode()).hexdigest()
        promotion_id = uuid4()
        with self._database.begin() as connection:
            connection.execute(
                update(strategy_promotions)
                .where(
                    strategy_promotions.c.strategy_id == strategy_id,
                    strategy_promotions.c.status == "active",
                )
                .values(status="superseded")
            )
            connection.execute(
                insert(strategy_promotions).values(
                    promotion_id=str(promotion_id),
                    strategy_id=strategy_id,
                    dataset_hash=dataset_hash,
                    config_hash=config_hash,
                    holdout_hash=holdout_hash,
                    git_sha=git_sha,
                    approved_by=approved_by,
                    approved_at=approved_at,
                    promotion_digest=digest,
                    status="active",
                )
            )
        return promotion_id

    def revoke(self, strategy_id: str) -> bool:
        with self._database.begin() as connection:
            result = connection.execute(
                update(strategy_promotions)
                .where(
                    strategy_promotions.c.strategy_id == strategy_id,
                    strategy_promotions.c.status == "active",
                )
                .values(status="revoked")
            )
            return bool(result.rowcount)

    def is_strategy_qualified(self, strategy_id: str) -> bool:
        with self._database.begin() as connection:
            promotion = connection.scalar(
                select(strategy_promotions.c.promotion_id)
                .where(
                    strategy_promotions.c.strategy_id == strategy_id,
                    strategy_promotions.c.status == "active",
                )
                .limit(1)
            )
            return promotion is not None
