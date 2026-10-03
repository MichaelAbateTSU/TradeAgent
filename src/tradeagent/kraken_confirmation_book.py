"""Public Kraken depth-ten CRC reconstruction, isolated from trading market data."""

from __future__ import annotations

import zlib
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator


def decimal(value: object) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (Decimal, str, int)):
        raise ValueError("decode book numbers as Decimal, never binary floats")
    try:
        result = Decimal(value)
    except InvalidOperation as error:
        raise ValueError("invalid book decimal") from error
    if not result.is_finite():
        raise ValueError("non-finite book decimal")
    return result


def checksum(bids: Mapping[Decimal, Decimal], asks: Mapping[Decimal, Decimal]) -> int:
    chunks: list[str] = []
    for levels, reverse in ((asks, False), (bids, True)):
        for price in sorted(levels, reverse=reverse)[:10]:
            chunks.extend(
                format(value, "f").replace(".", "").lstrip("0") for value in (price, levels[price])
            )
    return zlib.crc32("".join(chunks).encode("ascii")) & 0xFFFFFFFF


class BookQuote(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)

    bid: Decimal = Field(gt=0)
    ask: Decimal = Field(gt=0)
    bid_size: Decimal = Field(gt=0)
    ask_size: Decimal = Field(gt=0)
    size_units: Literal["base_asset"] = "base_asset"
    price_units: Literal["USD_per_base_asset"] = "USD_per_base_asset"
    provider_book_update_ns: int
    bid_price_last_changed_ns: int
    ask_price_last_changed_ns: int
    bid_size_last_changed_ns: int
    ask_size_last_changed_ns: int
    checksum_expected: int
    checksum_computed: int
    checksum_valid: Literal[True] = True
    whole_top10_book_consistency_only: Literal[True] = True

    @field_validator("bid", "ask", "bid_size", "ask_size", mode="before")
    @classmethod
    def precise_scalar(cls, value: object) -> Decimal:
        return decimal(value)


class Book:
    """Typed port of the validated diagnostic helper; update order is wire order."""

    def __init__(self) -> None:
        self.bids: dict[Decimal, Decimal] = {}
        self.asks: dict[Decimal, Decimal] = {}
        self.valid = False
        self.last_provider_ns: int | None = None
        self.last_bbo: tuple[Decimal, Decimal, Decimal, Decimal] | None = None
        self.last_price_change_ns = {"bid": 0, "ask": 0}
        self.last_size_change_ns = {"bid": 0, "ask": 0}
        self.checks = 0
        self.failures = 0
        self.last_reason = "snapshot_required"

    def invalidate(self, reason: str) -> None:
        self.bids.clear()
        self.asks.clear()
        self.valid = False
        self.last_provider_ns = None
        self.last_bbo = None
        self.last_price_change_ns = {"bid": 0, "ask": 0}
        self.last_size_change_ns = {"bid": 0, "ask": 0}
        self.last_reason = reason

    def apply(
        self, row: Mapping[str, JsonValue | Decimal], message_type: str, provider_ns: int
    ) -> BookQuote | None:
        if message_type not in ("snapshot", "update"):
            raise ValueError("unsupported book message type")
        if message_type == "update" and not self.valid:
            self.last_reason = "update_before_valid_snapshot"
            return None
        bids = {} if message_type == "snapshot" else dict(self.bids)
        asks = {} if message_type == "snapshot" else dict(self.asks)
        for updates, side in ((row.get("bids", []), bids), (row.get("asks", []), asks)):
            if not isinstance(updates, list):
                raise ValueError("book levels must be an ordered array")
            for level in updates:
                if not isinstance(level, dict):
                    raise ValueError("book level must be an object")
                price, quantity = decimal(level["price"]), decimal(level["qty"])
                if price <= 0 or quantity < 0:
                    self.invalidate("invalid_book_level")
                    raise ValueError("invalid book price or quantity")
                if quantity == 0:
                    side.pop(price, None)
                else:
                    side[price] = quantity
        bids = {price: bids[price] for price in sorted(bids, reverse=True)[:10]}
        asks = {price: asks[price] for price in sorted(asks)[:10]}
        expected = row["checksum"]
        if isinstance(expected, bool) or not isinstance(expected, int):
            raise ValueError("checksum must be an unsigned integer")
        self.checks += 1
        actual = checksum(bids, asks)
        if actual != expected:
            self.failures += 1
            self.invalidate("checksum_mismatch")
            return None
        if not bids or not asks or max(bids) >= min(asks):
            self.invalidate("empty_locked_or_crossed_book")
            return None
        if self.last_provider_ns is not None and provider_ns < self.last_provider_ns:
            self.failures += 1
            self.invalidate("provider_book_timestamp_regression")
            return None
        bid, ask = max(bids), min(asks)
        bbo = (bid, bids[bid], ask, asks[ask])
        for side_name, price_index, quantity_index in (("bid", 0, 1), ("ask", 2, 3)):
            if self.last_bbo is None or bbo[price_index] != self.last_bbo[price_index]:
                self.last_price_change_ns[side_name] = provider_ns
            if self.last_bbo is None or bbo[quantity_index] != self.last_bbo[quantity_index]:
                self.last_size_change_ns[side_name] = provider_ns
        self.bids, self.asks, self.last_bbo = bids, asks, bbo
        self.last_provider_ns = provider_ns
        self.valid = True
        self.last_reason = "checksum_validated"
        return BookQuote(
            bid=bid,
            ask=ask,
            bid_size=bids[bid],
            ask_size=asks[ask],
            provider_book_update_ns=provider_ns,
            bid_price_last_changed_ns=self.last_price_change_ns["bid"],
            ask_price_last_changed_ns=self.last_price_change_ns["ask"],
            bid_size_last_changed_ns=self.last_size_change_ns["bid"],
            ask_size_last_changed_ns=self.last_size_change_ns["ask"],
            checksum_expected=expected,
            checksum_computed=actual,
        )
