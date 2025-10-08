-- ==========================================
-- Redshift Analysis Queries (Valid for Current Schema)
-- ==========================================

-- 1. Product Popularity
SELECT product_name, number_of_reviews
FROM fact_sales
ORDER BY number_of_reviews DESC
LIMIT 10;

-- 2. Best Seller Trends
SELECT dp.category, COUNT(*) AS bestseller_count
FROM dim_product dp
JOIN fact_sales fs ON dp.product_name = fs.product_name
WHERE dp.product_name ILIKE '%Best Seller%'
GROUP BY dp.category
ORDER BY bestseller_count DESC;

-- 3. Average Ratings Distribution
SELECT 
    rating_bucket,
    COUNT(*) AS product_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS pct_share
FROM fact_sales
GROUP BY rating_bucket;

-- 4. Price vs Rating Analysis
SELECT rating_bucket,
       AVG(current_discounted_price) AS avg_discounted_price,
       AVG(listed_price) AS avg_listed_price
FROM fact_sales
GROUP BY rating_bucket;

-- 5. Discount Effectiveness
SELECT discount_flag,
       AVG(current_discounted_price) AS avg_discounted_price,
       AVG(listed_price) AS avg_listed_price,
       COUNT(*) AS product_count
FROM fact_sales
GROUP BY discount_flag;

-- 6. Sponsored vs Organic Products
SELECT CASE WHEN dp.product_name ILIKE '%Sponsored%' THEN 'Sponsored' ELSE 'Organic' END AS listing_type,
       AVG(fs.rating) AS avg_rating,
       AVG(fs.number_of_reviews) AS avg_reviews
FROM fact_sales fs
JOIN dim_product dp ON fs.product_name = dp.product_name
GROUP BY listing_type;

-- 7. Pricing Insights
(
  SELECT product_name, current_discounted_price
  FROM fact_sales
  ORDER BY current_discounted_price DESC
  LIMIT 10
)
UNION ALL
(
  SELECT product_name, current_discounted_price
  FROM fact_sales
  ORDER BY current_discounted_price ASC
  LIMIT 10
);

-- 8. Duplicate Listings Detection
SELECT product_name, listed_price, COUNT(*) AS variants
FROM fact_sales
GROUP BY product_name, listed_price
HAVING COUNT(*) > 1;

-- 9. Price Change Analysis
SELECT AVG((listed_price - current_discounted_price) / listed_price * 100) AS avg_discount_pct
FROM fact_sales
WHERE listed_price > 0;

-- 10. Time-based Snapshot
SELECT crawl_year, crawl_month,
       COUNT(*) AS total_products,
       AVG(rating) AS avg_rating,
       AVG(current_discounted_price) AS avg_price
FROM fact_sales
GROUP BY crawl_year, crawl_month
ORDER BY crawl_year, crawl_month;
