from __future__ import annotations

import math
import statistics
from decimal import Decimal
from typing import TypedDict


class PerformanceMetrics(TypedDict):
    total_return: Decimal
    annualized_return: Decimal
    annualized_volatility: Decimal
    sharpe_ratio: Decimal | None
    sortino_ratio: Decimal | None
    calmar_ratio: Decimal | None
    max_drawdown: Decimal
    turnover: Decimal


def _decimal(value: float) -> Decimal:
    if not math.isfinite(value):
        return Decimal(0)
    return Decimal(str(value))


def performance_metrics(
    equities: list[Decimal],
    traded_notional: Decimal,
    *,
    periods_per_year: int = 252,
) -> PerformanceMetrics:
    if not equities:
        raise ValueError("at least one equity observation is required")

    starting = equities[0]
    ending = equities[-1]
    returns = [
        float(equities[index] / equities[index - 1] - Decimal(1))
        for index in range(1, len(equities))
        if equities[index - 1] > 0
    ]
    total_return = ending / starting - Decimal(1)
    periods = max(1, len(returns))
    annualized_return = _decimal((float(ending / starting) ** (periods_per_year / periods)) - 1)

    volatility = 0.0
    sharpe: Decimal | None = None
    sortino: Decimal | None = None
    if len(returns) >= 2:
        standard_deviation = statistics.stdev(returns)
        volatility = standard_deviation * math.sqrt(periods_per_year)
        if standard_deviation > 0:
            sharpe = _decimal(
                statistics.mean(returns) / standard_deviation * math.sqrt(periods_per_year)
            )
        downside = math.sqrt(statistics.mean(min(value, 0.0) ** 2 for value in returns))
        if downside > 0:
            sortino = _decimal(statistics.mean(returns) / downside * math.sqrt(periods_per_year))

    peak = equities[0]
    maximum_drawdown = Decimal(0)
    for equity in equities:
        peak = max(peak, equity)
        maximum_drawdown = min(maximum_drawdown, equity / peak - Decimal(1))
    calmar = annualized_return / abs(maximum_drawdown) if maximum_drawdown < 0 else None
    average_equity = sum(equities, Decimal(0)) / Decimal(len(equities))
    turnover = traded_notional / average_equity if average_equity > 0 else Decimal(0)
    return {
        "total_return": total_return,
        "annualized_return": annualized_return,
        "annualized_volatility": _decimal(volatility),
        "sharpe_ratio": sharpe,
        "sortino_ratio": sortino,
        "calmar_ratio": calmar,
        "max_drawdown": maximum_drawdown,
        "turnover": turnover,
    }
