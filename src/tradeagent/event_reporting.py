"""Evidence labels for reporting only; these never grant order permission."""

from __future__ import annotations

from typing import Any, Literal

Purpose = Literal["research", "iex-practice"]

IEX_PRACTICE_LIMITATION = (
    "Real-time Alpaca IEX is single-venue data, not consolidated SIP/NBBO. "
    "Paper practice fills and quote paths do not validate strategy economics."
)
NO_INFERENCE_LIMITATIONS = {
    "NO_CONFIGURED_INFERENCE_PROVIDER",
    "NO_CONFIGURED_INFERENCE_PROVIDER_DETERMINISTIC_ONLY",
}


def reporting_purpose(*sources: dict[str, Any] | None) -> Purpose:
    for source in sources:
        if not source:
            continue
        settings = source.get("settings", {})
        if source.get("purpose") == "iex-practice" or (
            isinstance(settings, dict) and settings.get("purpose") == "iex-practice"
        ):
            return "iex-practice"
    return "research"


def evidence_labels(purpose: str) -> dict[str, Any]:
    practice = purpose == "iex-practice"
    return {
        "purpose": purpose,
        "qualification_eligible": not practice,
        "evidence_use": "operational_practice_only" if practice else "research",
        "performance_label": (
            "IEX paper practice only; not validated strategy economics"
            if practice
            else "experimental; edge unproven"
        ),
    }


def is_calibration(payload: dict[str, Any]) -> bool:
    return (
        payload.get("entry_kind") == "calibration"
        or payload.get("trade_classification") == "calibration"
        or payload.get("calibration") is True
    )


def reported_calibration(details: dict[str, Any]) -> dict[str, Any] | str | None:
    for key in ("calibration", "calibration_status"):
        value = details.get(key)
        if isinstance(value, dict | str):
            return value
    return None


def observation_labels(payload: dict[str, Any], purpose: str) -> dict[str, Any]:
    labels = evidence_labels(reporting_purpose({"purpose": purpose}, payload))
    if is_calibration(payload):
        labels.update(trade_classification="calibration", qualification_eligible=False)
    elif payload.get("qualification_eligible") is False:
        labels["qualification_eligible"] = False
    return labels


def reporting_limitations(details: dict[str, Any], purpose: str) -> dict[str, list[str]]:
    blockers = [str(value) for value in details.get("blockers", [])]
    source = details.get("source_capabilities", {})
    if isinstance(source, dict):
        blockers.extend(str(value) for value in source.get("last_errors", []))
    blockers.extend(str(value) for value in details.get("market_errors", []))
    capabilities = [str(value) for value in details.get("capability_limitations", [])]
    sources = [str(value) for value in details.get("source_limitations", [])]
    for value in [*blockers, *[str(value) for value in details.get("limitations", [])]]:
        if value in NO_INFERENCE_LIMITATIONS:
            capabilities.append(value)
    sources.extend(
        str(value)
        for value in details.get("limitations", [])
        if str(value) not in NO_INFERENCE_LIMITATIONS
    )
    if purpose == "iex-practice":
        sources.append(IEX_PRACTICE_LIMITATION)
    return {
        "blockers": list(
            dict.fromkeys(value for value in blockers if value not in NO_INFERENCE_LIMITATIONS)
        ),
        "capability_limitations": list(dict.fromkeys(capabilities)),
        "source_limitations": list(dict.fromkeys(sources)),
    }


def performance_labels(payload: dict[str, Any], purpose: str) -> dict[str, Any]:
    purpose = reporting_purpose({"purpose": purpose}, payload)
    practice = purpose == "iex-practice"
    return {
        **payload,
        **evidence_labels(purpose),
        "validated_strategy_economics": False,
        "economic_paper_pnl_label": (
            "Practice P&L after modeled reserves; operational estimate only"
            if practice
            else "Economic-paper P&L after modeled reserves; edge unproven"
        ),
        "qualification": "excluded_operational_practice" if practice else "unproven",
        "qualifying_closed_round_trips": (
            0
            if practice
            else payload.get("qualifying_closed_round_trips", payload.get("closed_round_trips", 0))
        ),
        **({"source_limitations": [IEX_PRACTICE_LIMITATION]} if practice else {}),
    }
