-- Advisory owns this queue; existing monitor/portfolio notifications are untouched.
CREATE TABLE IF NOT EXISTS app.advisory_notification_delivery (
    event_key TEXT NOT NULL,
    channel TEXT NOT NULL CHECK (channel IN ('email', 'webhook')),
    kind TEXT NOT NULL CHECK (kind IN ('PUBLISHED', 'SUMMARY', 'OVERDUE')),
    decision_date DATE NOT NULL,
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING','SENDING','SENT','FAILED','EXPIRED')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 6),
    available_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    lease_until TIMESTAMPTZ,
    lease_token TEXT,
    sent_at TIMESTAMPTZ,
    last_error_code TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_key, channel),
    CHECK (expires_at > available_at OR status IN ('FAILED','EXPIRED')),
    CHECK ((status = 'SENDING') = (lease_until IS NOT NULL AND lease_token IS NOT NULL)),
    CHECK ((status = 'SENT') = (sent_at IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS idx_advisory_notification_ready
    ON app.advisory_notification_delivery (available_at, created_at)
    WHERE status IN ('PENDING','SENDING');
COMMENT ON TABLE app.advisory_notification_delivery IS
    'Advisory-only per-channel delivery; SENT is external acceptance, not recommendation or economic success.';
COMMENT ON COLUMN app.advisory_notification_delivery.event_key IS 'SHA256 of typed immutable event identity; unique separately per channel.';
COMMENT ON COLUMN app.advisory_notification_delivery.channel IS 'Existing configured email or webhook channel; no credentials stored.';
COMMENT ON COLUMN app.advisory_notification_delivery.kind IS 'PUBLISHED, daily SUMMARY, or unpublished OVERDUE snapshot.';
COMMENT ON COLUMN app.advisory_notification_delivery.decision_date IS 'Shanghai closed trading day that produced the notification snapshot.';
COMMENT ON COLUMN app.advisory_notification_delivery.payload IS 'advisory_notification_message_v1: subject/message from durable publication state; UNKNOWN retained; no recipient, credential or outcome inference.';
COMMENT ON COLUMN app.advisory_notification_delivery.status IS 'Delivery-only state; SENT means positive external transport acceptance.';
COMMENT ON COLUMN app.advisory_notification_delivery.attempts IS 'Actual claimed delivery attempts including crash recovery, maximum six.';
COMMENT ON COLUMN app.advisory_notification_delivery.available_at IS 'Earliest next retry timestamp; timestamptz.';
COMMENT ON COLUMN app.advisory_notification_delivery.expires_at IS 'Target trading day 09:30 Asia/Shanghai; never deliver a new attempt at or after this time.';
COMMENT ON COLUMN app.advisory_notification_delivery.lease_until IS 'Nullable 120-second sender lease deadline; only SENDING owns a lease.';
COMMENT ON COLUMN app.advisory_notification_delivery.lease_token IS 'Nullable random fencing token for the current delivery attempt; not a credential.';
COMMENT ON COLUMN app.advisory_notification_delivery.sent_at IS 'Nullable external acceptance observation timestamp; populated only for SENT.';
COMMENT ON COLUMN app.advisory_notification_delivery.last_error_code IS 'Nullable fixed failure code; never raw exception text or secrets.';
COMMENT ON COLUMN app.advisory_notification_delivery.created_at IS 'Persistent queue insertion timestamp.';
COMMENT ON COLUMN app.advisory_notification_delivery.updated_at IS 'Most recent delivery state transition timestamp.';
