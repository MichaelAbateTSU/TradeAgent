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

The notifier's `notifier-daily` command selects the same daily-only sender and
waits for the existing delivery lease without dispatching or overwriting its
owner's heartbeat. Older releases do not recognize this command.

Render rejects deployment while a service is suspended. Its pending command
change alone is not sufficient protection when resuming an old image. The
transition therefore also uses a notifier-only maintenance lease, acquired
only after the suspended sender's old lease has naturally expired. The
maintenance job cannot send mail. Release follows verification of the new
live deployment and the old instance's shutdown grace period. Trading
ownership is never acquired or changed. There is no command-line option to
restore per-trade delivery.

## Deployment status

The change is deployed to the existing Render notifier:

- Reviewed code: `282f0d91079425f6f5760706a8f8810bd07f65da`.
- Service: `srv-dadnn6mq1p3s73ef7ef0`.
- Deployment: `dep-dai3ftrm8hqs738kdtmg`, observed live at 13:20 Eastern
  on September 11, 2026.
- Command: `tradeagent notifier-daily`.
- Observed sender: `srv-dadnn6mq1p3s73ef7ef0-858b98d886-zqx8z`, with a
  fresh matching delivery lease after the maintenance job finished.
- Actual settings: enabled, `America/New_York`, hour `18`, minute `0`.

The initial suspended deploy returned HTTP 400. A later deployment response
could not be decoded as JSON and required read-only reconciliation; the controller suspended the sender again
rather than assume success. Render-created resume deployments and the original
failed observations are retained in the evidence. The final transition kept
the notifier-only maintenance lease throughout activation and the old-image
shutdown grace period. The maintenance and release jobs completed without
calling an email provider or changing trading ownership.

Actual outbox comparison preserved all **27 already-accepted messages**
unchanged, including their payload hashes, provider IDs and attempts. The
**three queued digests** became `suppressed` without changing their original
payloads or adding delivery attempts. No out-of-schedule test email was sent.
The existing trading worker then generated its real 13:30 digest. A 13:31
read-only observation found that new record suppressed too, with zero attempts
and no provider acceptance ID. The total accepted-message count stayed 27.

The new-image preview contains **five paragraphs and 314 words**. It uses real
recorded activity and explicitly labels pending trading costs. This is a
before-18:00 preview, not an email delivery or a full-day profitability claim.
The scheduled September 11 daily identity is
`ba7bf7bc-a632-5ed8-a500-f8b643c8efdf`; its eventual provider acceptance must
be observed separately and must never be inferred from a preview.
A same-session read-only check is scheduled for 18:05 Eastern to inspect the
normal daily sender's result; that check is not authorized to send another
email or replay an accepted message.

The event worker remains on
`211347c82b2f4b04aa0dd22e042dfa3eae434ee1`. Recorder and dashboard deployments,
resource plans, instance counts, environment fingerprint, recipient and
provider were unchanged. PostgreSQL remains on schema `0014_scalping_runtime`.

Evidence is under `research/results/email-daily-20260911-*`, including the
original suspension, outbox before/after reads, guarded deployment, final
resource state and actual preview. The normal notifier, not a chat-side order
or email job, owns the daily send.
