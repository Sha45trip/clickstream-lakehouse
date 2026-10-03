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