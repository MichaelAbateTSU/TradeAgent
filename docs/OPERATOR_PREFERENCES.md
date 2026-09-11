# Operator preferences

## September 11, 2026: v30 autonomous paper scalping

At **02:18:28.634 America/New_York**, the owner requested v30 implementation
of the attached algorithmic-scalping research, autonomous operation while
asleep, and "no safety gaurdrails AT ALL" for paper trading. The owner also
explicitly requested that this preference be remembered.

For the new **v30 paper-experiment profile**, this means:

- Standing autonomous operation, not a one-off equipment demonstration or a
  daily approval request.
- No paper P&L-loss breaker, drawdown ceiling, exposure ceiling, daily trade
  quota, news/R1 approval requirement, fixed morning authorization window, or
  research/shadow-qualification prerequisite.
- Strategy entry selection, order sizing and signal/time-based exits remain
  algorithm parameters, not profitability guarantees.
- Use the existing Render deployment and available broker/data access.
  Prefer a working 24/7 paper path over waiting for unconfigured futures
  infrastructure.

This is **not authority for live money**, new paid services, new credentials,
additional recipients, market manipulation, or fabricated execution evidence.
The paper-endpoint/account boundary, ownership and idempotency, truthful data
capabilities, broker order constraints, reconciliation and recovery remain
implementation requirements. Paper losses do not eliminate real API,
infrastructure or operational costs.

The preference applies only to the explicitly versioned v30 profile. Preserve
older immutable cohorts, approvals, missed windows, accounting and evidence.
Any supersession of an earlier scheduled session must be explicit and audited,
not an overwrite or replay. A user-requested stop must remain possible.

## September 11, 2026: email preference

At **11:50 Eastern**, the owner requested only **one email each day at 18:00
America/New_York**, not an email for each trade or a recurring intraday
digest. Its body must contain exactly **five easy-to-read, plain-English
paragraphs** explaining the reporting day's trading and profit or loss.
Do not include raw trade details, order identifiers, JSON dumps or additional
automatic startup/incident messages. Preserve honest loss reporting and
uncertain fee attribution. See [DAILY_EMAIL.md](DAILY_EMAIL.md).
