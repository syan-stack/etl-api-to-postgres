CREATE TABLE IF NOT EXISTS fact_daily_product_price AS
SELECT
    product_id,
    title,
    category,
    snapshot_date,
    ROUND(AVG(simulated_price), 2) AS avg_price,
    ROUND(MIN(simulated_price), 2) AS min_price,
    ROUND(MAX(simulated_price), 2) AS max_price,
    COUNT(*) AS row_count
FROM raw_product_prices
GROUP BY
    product_id,
    title,
    category,
    snapshot_date;

