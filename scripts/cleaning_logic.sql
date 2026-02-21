/* Project: Ben Jay Enterprise Data Scrubbing
   Description: Standardizing currency, date formats, and category naming 
                to analyze sales leakage in the 2025 grocery market.
*/

-- 1. Create the Staging Table (The "Dirty" Raw Layer)
CREATE TABLE raw_sales_data (
    transaction_date VARCHAR(50),
    product_name VARCHAR(100),
    category VARCHAR(50),
    location VARCHAR(100),
    unit_price_ngn VARCHAR(50), -- String because of '₦' and 'NAN'
    qty_sold INT,
    competitor_price FLOAT,
    lead_time_days INT,
    customer_feedback TEXT);

-- 2. The Cleaning Transformation
-- We use a CTE to demonstrate a clear pipeline for recruiters
WITH cleaned_data AS (
    SELECT 
        -- Standardizing the Date: Handling multiple formats and "Last Friday"
        CASE 
            WHEN transaction_date = 'Last Friday' THEN '2025-12-26'::DATE
            WHEN transaction_date ~ '^\d{4}-\d{2}-\d{2}$' THEN transaction_date::DATE
            ELSE TO_DATE(transaction_date, 'DD/MM/YYYY') -- Adjust based on most common format
        END AS sale_date,

        -- Standardizing Text: Fixing case sensitivity and whitespace
        TRIM(INITCAP(product_name)) AS product_name,
        TRIM(UPPER(category)) AS category_group,
        location,

        -- Cleaning the Price: Removing '₦', 'NGN', and text like 'Promo Price'
        CAST(
            NULLIF(
                REGEXP_REPLACE(unit_price_ngn, '[^0-9.]', '', 'g'), 
                ''
            ) AS NUMERIC
        ) AS unit_price_cleaned,

        qty_sold,
        competitor_price,
        lead_time_days,
        customer_feedback
    FROM raw_sales_data
)

-- 3. Create the Final Production Table with Business Logic
SELECT 
    *,
    -- Calculate actual revenue (Ignoring returns for gross sales analysis)
    (unit_price_cleaned * GREATEST(qty_sold, 0)) AS calculated_revenue,
    
    -- Identify the "Competitor Gap" for the GitHub Readme
    (unit_price_cleaned - competitor_price) AS price_diff_ngn,
    
    -- Flagging Logistics Failures
    CASE 
        WHEN lead_time_days > 7 THEN 'Delayed'
        ELSE 'On-Time'
    END AS delivery_status
FROM cleaned_data
WHERE unit_price_cleaned IS NOT NULL; -- Dropping "Price Hidden" or "NAN" rows
