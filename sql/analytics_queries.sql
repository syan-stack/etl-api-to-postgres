SELECT
    category,
    snapshot_date,
    ROUND(AVG(avg_price), 2) AS category_avg_price
FROM fact_daily_product_price
GROUP BY category, snapshot_date
ORDER BY snapshot_date;


--2 Most unpredictble product (on price range)

SELECT
    title,
    ROUND(AVG(max_price - min_price), 2) AS avg_daily_price_range
FROM fact_daily_product_price
GROUP BY title
ORDER BY avg_daily_price_range DESC
LIMIT 10;


--3. higest average priced product(most expensive)

SELECT
    title,
    ROUND(AVG(avg_price), 2) as overall_avg_price
    FROM fact_daily_product_price
    GROUP BY title
    ORDER BY overall_avg_price DESC;

