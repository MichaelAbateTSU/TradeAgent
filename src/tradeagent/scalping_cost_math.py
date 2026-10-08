"""Exact fee arithmetic shared by offline research; no execution or fee defaults."""

from decimal import Decimal


def retained_base_quantity(quantity: Decimal, entry_fee_bps: Decimal) -> Decimal:
    """Entry fees withheld from base inventory, not a second cash deduction."""
    return quantity * (1 - entry_fee_bps / 10000)


def long_cash_return_bps(
    price_ratio: Decimal,
    entry_fee_bps: Decimal,
    exit_fee_bps: Decimal,
) -> Decimal:
    """Raw ask-to-bid ratio, base-inventory entry fee, then cash exit fee."""
    return (price_ratio * (1 - entry_fee_bps / 10000) * (1 - exit_fee_bps / 10000) - 1) * 10000
