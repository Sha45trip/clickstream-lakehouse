-- run:  docker compose exec spark-client spark-sql -f /opt/sql/sample_queries.sql
-- or interactively: make sql

SHOW DATABASES;
SHOW TABLES IN silver;

-- 1. Data quality: what got rejected and why
SELECT ingest_date, reject_reasons, COUNT(*) AS n
FROM silver.events_quarantine GROUP BY 1, 2 ORDER BY 1, 3 DESC;

-- 2. Daily funnel (gold)
SELECT session_date, SUM(sessions) sessions, SUM(sessions_product_view) viewed,
       SUM(sessions_add_to_cart) carted, SUM(sessions_checkout) checkout, SUM(sessions_purchase) purchased,
       ROUND(SUM(sessions_purchase) / SUM(sessions), 4) AS conv, ROUND(SUM(revenue), 2) revenue
FROM gold.daily_funnel GROUP BY 1 ORDER BY 1;

-- 3. Top products by views vs purchases-in-cart (partition-pruned on event_date)
SELECT product_id, category,
       SUM(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) AS views,
       SUM(CASE WHEN event_type = 'add_to_cart'  THEN quantity ELSE 0 END) AS units_carted
FROM silver.events
WHERE event_date BETWEEN '2026-09-01' AND '2026-09-03' AND product_id IS NOT NULL
GROUP BY 1, 2 ORDER BY views DESC LIMIT 10;

-- 4. Channel performance
SELECT COALESCE(utm_source, 'direct') AS channel, COUNT(*) sessions,
       ROUND(AVG(duration_sec), 1) avg_duration_sec,
       ROUND(100 * AVG(CASE WHEN converted THEN 1 ELSE 0 END), 2) conv_pct,
       ROUND(SUM(revenue), 2) revenue
FROM gold.sessions GROUP BY 1 ORDER BY revenue DESC;

-- 5. Session length distribution
SELECT ROUND(percentile_approx(duration_sec, 0.5), 0) p50, ROUND(percentile_approx(duration_sec, 0.9), 0) p90,
       ROUND(percentile_approx(n_events, 0.9), 0) p90_events
FROM gold.sessions;

-- 6. Storage layout check (feeds the format benchmark later)
DESCRIBE FORMATTED silver.events;
