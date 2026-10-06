-- Serving schema: small, fast copies of the gold tables for dashboards (database "serving" in the airflow-db container).
-- Money is NUMERIC, not DOUBLE: avoids the floating-point drift seen in the Spark revenue sums.

CREATE TABLE IF NOT EXISTS daily_funnel (
    session_date            date PRIMARY KEY,
    sessions                bigint,
    users                   bigint,
    sessions_view           bigint,
    sessions_cart           bigint,
    sessions_purchase       bigint,
    purchase_without_cart   bigint,
    buyers                  bigint,
    revenue                 numeric(18,2),
    view_to_cart_rate       double precision,
    session_conversion_rate double precision,
    refreshed_at            timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS category_daily (
    event_date   date NOT NULL,
    category_l1  text NOT NULL,
    views        bigint,
    carts        bigint,
    purchases    bigint,
    revenue      numeric(18,2),
    users        bigint,
    refreshed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (event_date, category_l1)
);

-- ---------------------------------------------------------------------------------------------------------------
-- LIVE tables, written by the streaming application (jobs/20_streaming_app.py)
-- ---------------------------------------------------------------------------------------------------------------

-- events and revenue per minute of EVENT time, counted after de-duplication
CREATE TABLE IF NOT EXISTS live_minute (
    minute_ts    timestamptz NOT NULL,
    event_type   text        NOT NULL,
    events       bigint      NOT NULL DEFAULT 0,
    revenue      numeric(18,2) NOT NULL DEFAULT 0,
    refreshed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (minute_ts, event_type)
);

-- ledger that makes the additive writes above exactly-once: a micro-batch id is applied only once, even if Spark
-- re-runs the batch after a crash
CREATE TABLE IF NOT EXISTS live_batches (
    query      text   NOT NULL,
    batch_id   bigint NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (query, batch_id)
);

-- one row: how fresh is the data? (dashboard tile + the refresh DAG reads it)
CREATE TABLE IF NOT EXISTS live_status (
    id            int PRIMARY KEY,
    max_event_ts  timestamptz,
    rows_in_batch bigint,
    updated_at    timestamptz NOT NULL DEFAULT now()
);
