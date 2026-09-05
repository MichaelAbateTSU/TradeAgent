from __future__ import annotations

from pathlib import Path

import pytest

from tradeagent.data import synthetic_bars
from tradeagent.holdout import (
    HOLDOUT_AUTHORIZATION,
    development_frames,
    load_holdout_manifest,
    open_holdout_once,
    seal_holdout,
)
from tradeagent.universe import align_universe


def _frames(count: int = 100):
    return align_universe(
        {
            "SPY": list(synthetic_bars(symbol="SPY", count=count, seed=1)),
            "QQQ": list(synthetic_bars(symbol="QQQ", count=count, seed=2)),
        }
    ).frames


def test_holdout_seal_is_immutable_and_development_only(tmp_path: Path) -> None:
    frames = _frames()
    path = tmp_path / "holdout.json"

    manifest = seal_holdout(frames, path, holdout_fraction=0.20)
    repeated = seal_holdout(frames, path, holdout_fraction=0.30)
    development = development_frames(frames, manifest)

    assert manifest == repeated
    assert load_holdout_manifest(path) == manifest
    assert manifest.development_frames == 80
    assert manifest.holdout_frames == 20
    assert len(development) == 80


def test_holdout_opens_once_with_exact_authorization(tmp_path: Path) -> None:
    frames = _frames()
    manifest = seal_holdout(frames, tmp_path / "holdout.json")
    audit = tmp_path / "opened.json"

    with pytest.raises(ValueError, match="exact"):
        open_holdout_once(
            frames,
            manifest,
            audit,
            authorization="yes",
        )
    holdout = open_holdout_once(
        frames,
        manifest,
        audit,
        authorization=HOLDOUT_AUTHORIZATION,
    )

    assert len(holdout) == 20
    assert audit.exists()
    with pytest.raises(ValueError, match="already"):
        open_holdout_once(
            frames,
            manifest,
            audit,
            authorization=HOLDOUT_AUTHORIZATION,
        )


def test_holdout_rejects_dataset_drift_and_small_inputs(tmp_path: Path) -> None:
    frames = _frames()
    path = tmp_path / "holdout.json"
    manifest = seal_holdout(frames, path)
    changed = _frames(count=99)

    with pytest.raises(ValueError, match="does not match"):
        seal_holdout(changed, path)
    with pytest.raises(ValueError, match="count mismatch"):
        development_frames(changed, manifest)
    with pytest.raises(ValueError, match="at least 20"):
        seal_holdout(_frames(count=10), tmp_path / "small.json")
    with pytest.raises(ValueError, match="between"):
        seal_holdout(frames, tmp_path / "fraction.json", holdout_fraction=0.05)
