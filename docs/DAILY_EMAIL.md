# Daily plain-English email

The owner's September 11, 2026, 11:50 Eastern instruction replaces the
earlier trade, startup and 30-minute digest emails.

## Delivery policy

- One automatic email per reporting day, scheduled for **18:00 America/New_York**.
- Exactly **five plain-English paragraphs**. No separate headings, tables,
  JSON, order identifiers or appended technical report in the email body.
- No separate trade, startup, digest or operational-incident emails.
- The existing recipient and provider remain unchanged.
- The reporting day runs from the previous 18:00 boundary to the current
  18:00 boundary, including overnight crypto activity without gaps or overlap.
- Before 18:00, the notifier cannot claim a message for delivery. A delayed
  same-day retry retains the original daily identity; an expired prior-day
  message is not delivered as a backlog.

The summary describes aggregate activity, completed buy-and-sell trades,
profit or loss, execution behavior and the latest recorded position status.
It reports losses honestly. Unmatched fees or incomplete valuations remain
estimates or unavailable values, never a fabricated final profit figure.
The complete trade and audit records remain in the dashboard/database.

## Enforcement

The delivery boundary accepts only the canonical daily notification identity
for the current reporting date. Producer messages queued concurrently after
suppression cannot bypass that claim filter. Existing non-daily pending or
failed messages are marked `suppressed` without altering their original
payloads or pretending that they were delivered. Suppression is audited.
Accepted messages are not resent or rewritten; ambiguous prior deliveries
are not reclaimed as non-daily sends.

An unattempted legacy daily payload can be replaced with the new summary
format before its first send. The prior payload is retained in an audit.
Already-attempted or accepted daily messages are not rewritten. Provider
idempotency and the existing finite retry/unknown-outcome handling remain.

Only the notifier requires deployment for this change. The running trading
worker, its financial policy, credentials, resource plan and ownership are
not changed. Older producer code may still create status records, but the
notifier suppresses them rather than emailing them.

The notifier's `notifier-daily` command is an alias for the same daily-only
sender. Older releases do not recognize this command and exit before delivery,
so it can also prevent an old image from sending queued digests during the
suspended-service rollout. There is no command-line option to restore per-trade
delivery.

## Deployment status

The notifier was intentionally suspended while this change was implemented,
to prevent another unwanted digest during the transition. Trading continued.
Final deployment, observed policy state and the actual five-paragraph preview
will be recorded here. No out-of-schedule test email is authorized.
