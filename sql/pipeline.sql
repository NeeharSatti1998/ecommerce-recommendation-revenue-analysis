
DROP TABLE IF EXISTS clean_data;

CREATE TABLE clean_data AS
SELECT * FROM data;



-- KPI metrics

SELECT
    ROUND(SUM(Revenue), 2)                              AS total_revenue,
    COUNT(DISTINCT CustomerID)                          AS total_customers,
    COUNT(DISTINCT InvoiceNo)                           AS total_orders,
    ROUND(SUM(Revenue) / COUNT(DISTINCT InvoiceNo), 2) AS avg_order_value
FROM clean_data;



-- top 10 products by revenue

SELECT
    Description,
    ROUND(SUM(Revenue), 2) AS product_revenue
FROM clean_data
GROUP BY Description
ORDER BY product_revenue DESC
LIMIT 10;



-- top 10 countries by revenue

SELECT
    Country,
    ROUND(SUM(Revenue), 2) AS country_revenue
FROM clean_data
GROUP BY Country
ORDER BY country_revenue DESC
LIMIT 10;



-- revenue by month

SELECT
    substr(InvoiceDate_ISO, 1, 7)  AS month,
    ROUND(SUM(Revenue), 2)         AS monthly_revenue
FROM clean_data
GROUP BY month
ORDER BY month;



-- RFM raw metrics

DROP TABLE IF EXISTS rfm_raw;

CREATE TABLE rfm_raw AS
WITH customer_metrics AS (
    SELECT
        CustomerID,
        MAX(JULIANDAY(InvoiceDate_ISO)) AS last_purchase_jd,
        COUNT(DISTINCT InvoiceNo)       AS frequency,
        SUM(Revenue)                    AS monetary
    FROM clean_data
    GROUP BY CustomerID
)
SELECT
    CustomerID,
    ROUND(JULIANDAY('2011-12-10') - last_purchase_jd, 0) AS recency,
    frequency,
    ROUND(monetary, 2) AS monetary
FROM customer_metrics
ORDER BY CustomerID;

-- sanity check: should return 0
SELECT COUNT(*) AS null_recency FROM rfm_raw WHERE recency IS NULL;

-- preview
SELECT * FROM rfm_raw LIMIT 10;



-- RFM scoring using NTILE(5)
-- recency: lower is better so score is reversed
-- frequency and monetary: higher is better

DROP TABLE IF EXISTS rfm_scored;

CREATE TABLE rfm_scored AS
WITH ntile_scores AS (
    SELECT
        CustomerID,
        recency,
        frequency,
        monetary,
        6 - NTILE(5) OVER (ORDER BY recency DESC) AS r_score,
        NTILE(5) OVER (ORDER BY frequency ASC)    AS f_score,
        NTILE(5) OVER (ORDER BY monetary ASC)     AS m_score
    FROM rfm_raw
)
SELECT
    CustomerID,
    recency,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score,
    r_score + f_score + m_score AS rfm_score
FROM ntile_scores;

SELECT * FROM rfm_scored LIMIT 10;



-- RFM segmentation

DROP TABLE IF EXISTS rfm_segments;

CREATE TABLE rfm_segments AS
SELECT
    CustomerID,
    recency,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score,
    rfm_score,
    CASE
        WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
        WHEN r_score >= 3 AND f_score >= 3                  THEN 'Loyal Customers'
        WHEN r_score >= 4 AND f_score <= 2                  THEN 'Recent Customers'
        WHEN r_score >= 3 AND f_score <= 2 AND m_score >= 3 THEN 'Potential Loyalists'
        WHEN r_score <= 2 AND f_score >= 3 AND m_score >= 3 THEN 'At Risk'
        WHEN r_score = 1  AND f_score >= 4 AND m_score >= 4 THEN 'Cant Lose Them'
        WHEN r_score <= 2 AND f_score <= 2                  THEN 'Lost'
        ELSE 'Needs Attention'
    END AS segment
FROM rfm_scored;

-- segment breakdown
SELECT
    segment,
    COUNT(*)                   AS customers,
    ROUND(AVG(recency), 1)     AS avg_recency,
    ROUND(AVG(frequency), 1)   AS avg_frequency,
    ROUND(AVG(monetary), 2)    AS avg_monetary,
    ROUND(SUM(monetary), 2)    AS total_revenue
FROM rfm_segments
GROUP BY segment
ORDER BY total_revenue DESC;



-- product co-purchase pairs

DROP TABLE IF EXISTS product_pairs;

CREATE TABLE product_pairs AS
SELECT
    a.Description AS product_a,
    b.Description AS product_b,
    COUNT(DISTINCT a.InvoiceNo) AS co_purchases
FROM clean_data a
JOIN clean_data b
    ON a.InvoiceNo = b.InvoiceNo
    AND a.Description < b.Description
GROUP BY a.Description, b.Description
HAVING co_purchases >= 10
ORDER BY co_purchases DESC;

SELECT * FROM product_pairs LIMIT 10;



-- rule-based recommender

SELECT
    CASE
        WHEN product_a = 'WHITE HANGING HEART T-LIGHT HOLDER' THEN product_b
        ELSE product_a
    END AS recommended_product,
    co_purchases
FROM product_pairs
WHERE product_a = 'WHITE HANGING HEART T-LIGHT HOLDER'
   OR product_b = 'WHITE HANGING HEART T-LIGHT HOLDER'
ORDER BY co_purchases DESC
LIMIT 5;



-- A/B test revenue uplift framework

DROP TABLE IF EXISTS ab_groups;

CREATE TABLE ab_groups AS
SELECT
    CustomerID,
    monetary,
    CASE WHEN CustomerID % 2 = 0 THEN 'control' ELSE 'treatment' END AS ab_group
FROM rfm_segments;

SELECT
    ab_group,
    COUNT(*)                AS customers,
    ROUND(AVG(monetary), 2) AS avg_revenue_per_customer,
    ROUND(SUM(monetary), 2) AS total_revenue
FROM ab_groups
GROUP BY ab_group;

-- simulated 15% uplift on treatment group
SELECT 'control' AS group_label,
    ROUND(AVG(monetary), 2)         AS avg_revenue,
    ROUND(SUM(monetary), 2)         AS total_revenue,
    0.0                             AS uplift_pct
FROM ab_groups WHERE ab_group = 'control'
UNION ALL
SELECT 'treatment' AS group_label,
    ROUND(AVG(monetary) * 1.15, 2) AS avg_revenue,
    ROUND(SUM(monetary) * 1.15, 2) AS total_revenue,
    15.0                            AS uplift_pct
FROM ab_groups WHERE ab_group = 'treatment';
