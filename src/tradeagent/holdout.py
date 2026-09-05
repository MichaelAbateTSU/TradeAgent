from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from tradeagent.universe import UniverseFrame

HOLDOUT_AUTHORIZATION = "I acknowledge this is the one-time terminal holdout evaluation"


class HoldoutManifest(BaseModel):
    model_config = ConfigDict(frozen=True)

    dataset_hash: str = Field(min_length=64, max_length=64)
    development_hash: str = Field(min_length=64, max_length=64)
    holdout_hash: str = Field(min_length=64, max_length=64)
    symbols: tuple[str, ...]
    total_frames: int = Field(gt=1)
    development_frames: int = Field(gt=0)
    holdout_frames: int = Field(gt=0)
    holdout_started_at: datetime
    sealed_at: datetime


def seal_holdout(
    frames: Sequence[UniverseFrame],
    manifest_path: Path,
    *,
    holdout_fraction: float = 0.20,
) -> HoldoutManifest:
    if not 0.10 <= holdout_fraction <= 0.50:
        raise ValueError("holdout_fraction must be between 0.10 and 0.50")
    if len(frames) < 20:
        raise ValueError("holdout sealing requires at least 20 frames")
    holdout_count = max(1, int(len(frames) * holdout_fraction))
    development_count = len(frames) - holdout_count
    dataset_hash = _frame_hash(frames)
    if manifest_path.exists():
        existing = load_holdout_manifest(manifest_path)
        if existing.dataset_hash != dataset_hash:
            raise ValueError("sealed holdout dataset hash does not match current data")
        return existing

    symbols = tuple(sorted(bar.symbol for bar in frames[0].bars))
    if any(tuple(sorted(bar.symbol for bar in frame.bars)) != symbols for frame in frames):
        raise ValueError("holdout frames must use one stable universe")
    manifest = HoldoutManifest(
        dataset_hash=dataset_hash,
        development_hash=_frame_hash(frames[:development_count]),
        holdout_hash=_frame_hash(frames[development_count:]),
        symbols=symbols,
        total_frames=len(frames),
        development_frames=development_count,
        holdout_frames=holdout_count,
        holdout_started_at=frames[development_count].timestamp,
        sealed_at=datetime.now(UTC),
    )
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        manifest.model_dump_json(indent=2),
        encoding="utf-8",
    )
    return manifest


def load_holdout_manifest(path: Path) -> HoldoutManifest:
    return HoldoutManifest.model_validate_json(path.read_text(encoding="utf-8"))


def development_frames(
    frames: Sequence[UniverseFrame],
    manifest: HoldoutManifest,
) -> tuple[UniverseFrame, ...]:
    _verify_frames(frames, manifest)
    development = tuple(frames[: manifest.development_frames])
    if _frame_hash(development) != manifest.development_hash:
        raise ValueError("development dataset hash mismatch")
    return development


def open_holdout_once(
    frames: Sequence[UniverseFrame],
    manifest: HoldoutManifest,
    audit_path: Path,
    *,
    authorization: str,
) -> tuple[UniverseFrame, ...]:
    if authorization != HOLDOUT_AUTHORIZATION:
        raise ValueError("exact one-time holdout authorization is required")
    if audit_path.exists():
        raise ValueError("terminal holdout has already been opened")
    _verify_frames(frames, manifest)
    holdout = tuple(frames[manifest.development_frames :])
    if _frame_hash(holdout) != manifest.holdout_hash:
        raise ValueError("terminal holdout dataset hash mismatch")
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    audit_path.write_text(
        json.dumps(
            {
                "dataset_hash": manifest.dataset_hash,
                "holdout_hash": manifest.holdout_hash,
                "opened_at": datetime.now(UTC).isoformat(),
                "authorization": authorization,
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return holdout


def _verify_frames(
    frames: Sequence[UniverseFrame],
    manifest: HoldoutManifest,
) -> None:
    if len(frames) != manifest.total_frames:
        raise ValueError("sealed holdout frame count mismatch")
    if _frame_hash(frames) != manifest.dataset_hash:
        raise ValueError("sealed holdout dataset hash mismatch")


def _frame_hash(frames: Sequence[UniverseFrame]) -> str:
    canonical = "\n".join(bar.model_dump_json() for frame in frames for bar in frame.bars)
    return sha256(canonical.encode()).hexdigest()
